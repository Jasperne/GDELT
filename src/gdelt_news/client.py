from __future__ import annotations

import json
import time
from datetime import timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder, format_gdelt_datetime, format_iso_datetime
from .scraper import ArticleScraper
from .tls import build_certificate_error_message, configure_ssl_context, is_certificate_verification_error

DEFAULT_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


class GDELTAPIError(RuntimeError):
    """Raised when GDELT returns an error or an unexpected response."""


def _quote_term(value: str) -> str:
    escaped = value.replace('"', "").strip()
    if not escaped:
        raise ValueError("query fragments cannot be empty")
    return f'"{escaped}"'


def _keyword_to_fragment(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("keywords cannot be empty")
    if cleaned.startswith(("theme:", "domain:", "domainis:", "sourcecountry:", "sourcelang:")):
        return cleaned
    if " " in cleaned:
        return _quote_term(cleaned)
    return cleaned


def _normalize_compact_value(value: str) -> str:
    return value.strip().lower().replace(" ", "")


def _append_grouped_fragments(target: list[str], fragments: list[str]) -> None:
    cleaned_fragments = [fragment for fragment in fragments if fragment.strip()]
    if not cleaned_fragments:
        return
    if len(cleaned_fragments) == 1:
        target.append(cleaned_fragments[0])
        return
    target.append(f"({' OR '.join(cleaned_fragments)})")


def build_query(filters: GDELTFilterSpec) -> str:
    include_fragments: list[str] = []
    scoped_fragments: list[str] = []

    include_fragments.extend(_keyword_to_fragment(keyword) for keyword in filters.keywords)
    include_fragments.extend(_quote_term(phrase) for phrase in filters.exact_phrases)
    include_fragments.extend(_quote_term(entity) for entity in filters.entities)
    include_fragments.extend(f"theme:{theme.strip()}" for theme in filters.themes if theme.strip())

    if include_fragments:
        if filters.match_mode == "all":
            scoped_fragments.extend(include_fragments)
        elif filters.match_mode == "any":
            if len(include_fragments) == 1:
                scoped_fragments.append(include_fragments[0])
            else:
                scoped_fragments.append(f"({' OR '.join(include_fragments)})")
        else:
            raise ValueError("filters.match_mode must be either 'any' or 'all'")

    source_country_fragments = [
        f"sourcecountry:{_normalize_compact_value(country)}"
        for country in filters.source_countries
        if country.strip()
    ]
    _append_grouped_fragments(scoped_fragments, source_country_fragments)

    source_language_fragments = [
        f"sourcelang:{_normalize_compact_value(language)}"
        for language in filters.source_languages
        if language.strip()
    ]
    _append_grouped_fragments(scoped_fragments, source_language_fragments)

    domain_fragments = [f"domain:{domain.strip()}" for domain in filters.domains if domain.strip()]
    _append_grouped_fragments(scoped_fragments, domain_fragments)

    exact_domain_fragments = [
        f"domainis:{domain.strip()}" for domain in filters.exact_domains if domain.strip()
    ]
    _append_grouped_fragments(scoped_fragments, exact_domain_fragments)

    if filters.tone_min is not None:
        scoped_fragments.append(f"tone>{filters.tone_min}")
    if filters.tone_max is not None:
        scoped_fragments.append(f"tone<{filters.tone_max}")
    if filters.tone_abs_min is not None:
        scoped_fragments.append(f"toneabs>{filters.tone_abs_min}")
    if filters.tone_abs_max is not None:
        scoped_fragments.append(f"toneabs<{filters.tone_abs_max}")

    scoped_fragments.extend(item.to_query_fragment() for item in filters.proximity)
    scoped_fragments.extend(
        f'repeat{count}:"{term.strip()}"'
        for term, count in filters.repeated_terms.items()
        if term.strip() and count > 0
    )

    for term in filters.exclude_terms:
        fragment = _keyword_to_fragment(term)
        scoped_fragments.append(fragment if fragment.startswith("-") else f"-{fragment}")

    scoped_fragments.extend(fragment for fragment in filters.raw_fragments if fragment.strip())

    if not scoped_fragments:
        raise ValueError("At least one filter or raw query fragment is required")

    return " ".join(scoped_fragments)


class GDELTClient:
    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 30,
        min_request_interval: float = 6.0,
        user_agent: str = "gdelt-news-module/0.1",
        ca_bundle: str | None = None,
        rate_limit_retries: int = 2,
        rate_limit_backoff_seconds: float = 15.0,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.user_agent = user_agent
        self.ssl_context, self.ca_bundle, self.ca_bundle_source = configure_ssl_context(ca_bundle)
        self.rate_limit_retries = rate_limit_retries
        self.rate_limit_backoff_seconds = rate_limit_backoff_seconds
        self._last_request_at = 0.0

    def fetch_bundle(self, request: QueryRequest) -> dict[str, Any]:
        request.validate()
        query = build_query(request.filters)

        article_result = self._fetch_articles(request, query=query)
        article_params = self.build_params(request, query=query, mode="artlist")

        timeline_payload = None
        tone_payload = None
        warnings: list[str] = list(article_result["warnings"])

        if request.include_timeline:
            try:
                timeline_payload = self._request_json(
                    self.build_params(request, query=query, mode="timelinevolraw")
                )
            except GDELTAPIError as exc:
                warnings.append(f"timelinevolraw unavailable: {exc}")

        if request.include_tone_timeline:
            try:
                tone_payload = self._request_json(
                    self.build_params(request, query=query, mode="timelinetone")
                )
            except GDELTAPIError as exc:
                warnings.append(f"timelinetone unavailable: {exc}")

        if request.scrape_articles:
            scraper = ArticleScraper(
                timeout=request.scrape_timeout,
                ca_bundle=self.ca_bundle,
            )
            scraping = scraper.enrich_articles(
                article_result["articles"],
                limit=request.scrape_limit,
            )
        else:
            scraping = {
                "requested": False,
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "skipped": 0,
                "limit": request.scrape_limit,
                "timeout": request.scrape_timeout,
                "errors": [],
            }

        return {
            "query": query,
            "request": request,
            "article_params": article_params,
            "timeline_params": self.build_params(request, query=query, mode="timelinevolraw")
            if request.include_timeline
            else None,
            "tone_params": self.build_params(request, query=query, mode="timelinetone")
            if request.include_tone_timeline
            else None,
            "article_payload": {"articles": article_result["articles"]},
            "timeline_payload": timeline_payload,
            "tone_payload": tone_payload,
            "article_fetch": article_result["diagnostics"],
            "scraping": scraping,
            "warnings": warnings,
        }

    def build_params(
        self,
        request: QueryRequest,
        query: str,
        mode: str,
        date_range: DateRange | None = None,
    ) -> dict[str, str]:
        params = {
            "query": query,
            "mode": mode,
            "format": "json",
        }

        effective_date_range = date_range or request.date_range

        if effective_date_range.timespan:
            params["timespan"] = effective_date_range.timespan
        else:
            if effective_date_range.start:
                params["STARTDATETIME"] = format_gdelt_datetime(effective_date_range.start)
            if effective_date_range.end:
                params["ENDDATETIME"] = format_gdelt_datetime(effective_date_range.end)

        if mode == "artlist":
            params["maxrecords"] = str(request.max_records)
            params["sort"] = request.sort.value

        return params

    def _fetch_articles(self, request: QueryRequest, query: str) -> dict[str, Any]:
        windows = self._build_article_windows(request)
        raw_articles: list[dict[str, Any]] = []
        batch_diagnostics: list[dict[str, Any]] = []
        warnings: list[str] = []
        split_window_count = 0
        fetch_attempt_count = 0

        for window in windows:
            window_result = self._collect_window_articles(request, query=query, window=window, split_depth=0)
            raw_articles.extend(window_result["articles"])
            batch_diagnostics.extend(window_result["windows"])
            warnings.extend(window_result["warnings"])
            split_window_count += window_result["split_window_count"]
            fetch_attempt_count += window_result["fetch_attempt_count"]

        for index, window_diagnostic in enumerate(batch_diagnostics, start=1):
            window_diagnostic["index"] = index

        if request.deduplicate:
            articles, duplicates_removed = self._deduplicate_articles(raw_articles)
        else:
            articles = list(raw_articles)
            duplicates_removed = 0

        articles = self._sort_articles(articles, request.sort)

        return {
            "articles": articles,
            "warnings": warnings,
            "diagnostics": {
                "batching_enabled": len(windows) > 1 or split_window_count > 0,
                "batch_window_hours": request.batch_window_hours,
                "batch_count": len(batch_diagnostics),
                "fetch_attempt_count": fetch_attempt_count,
                "split_window_count": split_window_count,
                "raw_article_count": len(raw_articles),
                "deduplicated_article_count": len(articles),
                "duplicates_removed": duplicates_removed,
                "truncated_window_count": sum(1 for item in batch_diagnostics if item["truncated"]),
                "windows": batch_diagnostics,
            },
        }

    def _collect_window_articles(
        self,
        request: QueryRequest,
        query: str,
        window: DateRange,
        split_depth: int,
    ) -> dict[str, Any]:
        params = self.build_params(request, query=query, mode="artlist", date_range=window)
        payload = self._request_json(params)
        articles = payload.get("articles") or []
        truncated = len(articles) >= request.max_records
        fetch_attempt_count = 1
        warnings: list[str] = []

        if truncated and self._can_split_window(window):
            left_window, right_window = self._split_window(window)
            warnings.append(
                f"article window {self._describe_window(window)} reached max_records={request.max_records}; splitting into smaller windows"
            )

            left_result = self._collect_window_articles(
                request,
                query=query,
                window=left_window,
                split_depth=split_depth + 1,
            )
            right_result = self._collect_window_articles(
                request,
                query=query,
                window=right_window,
                split_depth=split_depth + 1,
            )

            return {
                "articles": left_result["articles"] + right_result["articles"],
                "warnings": warnings + left_result["warnings"] + right_result["warnings"],
                "windows": left_result["windows"] + right_result["windows"],
                "split_window_count": 1
                + left_result["split_window_count"]
                + right_result["split_window_count"],
                "fetch_attempt_count": fetch_attempt_count
                + left_result["fetch_attempt_count"]
                + right_result["fetch_attempt_count"],
            }

        if truncated:
            warnings.append(
                f"article window {self._describe_window(window)} reached max_records={request.max_records}; results may be truncated"
            )

        return {
            "articles": articles,
            "warnings": warnings,
            "windows": [
                {
                    "index": 0,
                    "start": format_iso_datetime(window.start),
                    "end": format_iso_datetime(window.end),
                    "timespan": window.timespan,
                    "returned_articles": len(articles),
                    "truncated": truncated,
                    "split_depth": split_depth,
                    "adaptive_split": split_depth > 0,
                }
            ],
            "split_window_count": 0,
            "fetch_attempt_count": fetch_attempt_count,
        }

    def _build_article_windows(self, request: QueryRequest) -> list[DateRange]:
        date_range = request.date_range
        if (
            date_range.timespan
            or date_range.start is None
            or date_range.end is None
            or request.batch_window_hours is None
        ):
            return [date_range]

        window_size = timedelta(hours=request.batch_window_hours)
        windows: list[DateRange] = []
        cursor = date_range.start

        while cursor < date_range.end:
            window_end = min(cursor + window_size, date_range.end)
            windows.append(DateRange(start=cursor, end=window_end))
            if window_end >= date_range.end:
                break
            cursor = window_end

        return windows or [date_range]

    def _can_split_window(self, window: DateRange) -> bool:
        if window.timespan or window.start is None or window.end is None:
            return False
        return int((window.end - window.start).total_seconds()) >= 2

    def _split_window(self, window: DateRange) -> tuple[DateRange, DateRange]:
        if not self._can_split_window(window):
            raise ValueError("window cannot be split further")

        assert window.start is not None
        assert window.end is not None

        total_seconds = int((window.end - window.start).total_seconds())
        midpoint = window.start + timedelta(seconds=max(1, total_seconds // 2))
        if midpoint <= window.start or midpoint >= window.end:
            raise ValueError("calculated midpoint does not split the window")

        return (
            DateRange(start=window.start, end=midpoint),
            DateRange(start=midpoint, end=window.end),
        )

    def _describe_window(self, window: DateRange) -> str:
        if window.timespan:
            return f"timespan={window.timespan}"
        return f"{format_iso_datetime(window.start)} to {format_iso_datetime(window.end)}"

    def _deduplicate_articles(
        self, raw_articles: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], int]:
        unique_articles: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        duplicates_removed = 0

        for article in raw_articles:
            key = self._article_identity(article)
            if key in seen_keys:
                duplicates_removed += 1
                continue
            seen_keys.add(key)
            unique_articles.append(article)

        return unique_articles, duplicates_removed

    def _article_identity(self, article: dict[str, Any]) -> str:
        url = str(article.get("url") or "").strip().lower()
        if url:
            return f"url:{url}"

        title = str(article.get("title") or "").strip().lower()
        seendate = str(article.get("seendate") or "").strip()
        domain = str(article.get("domain") or "").strip().lower()
        return f"fallback:{title}|{seendate}|{domain}"

    def _sort_articles(
        self, articles: list[dict[str, Any]], sort_order: SortOrder
    ) -> list[dict[str, Any]]:
        if sort_order == SortOrder.DATE_DESC:
            return sorted(articles, key=lambda item: item.get("seendate") or "", reverse=True)
        if sort_order == SortOrder.DATE_ASC:
            return sorted(articles, key=lambda item: item.get("seendate") or "")
        return articles

    def _request_json(self, params: dict[str, str]) -> dict[str, Any]:
        request_url = f"{self.base_url}?{urlencode(params)}"
        http_request = Request(
            request_url,
            headers={
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
        )

        attempts = self.rate_limit_retries + 1
        for attempt in range(1, attempts + 1):
            self._throttle()
            try:
                with urlopen(http_request, timeout=self.timeout, context=self.ssl_context) as response:
                    payload = response.read().decode("utf-8")
            except Exception as exc:  # pragma: no cover - network failures vary by environment
                self._last_request_at = time.monotonic()
                if self._is_rate_limit_error(exc) and attempt < attempts:
                    time.sleep(self._rate_limit_delay(attempt, exc))
                    continue
                raise GDELTAPIError(self._format_request_error(exc)) from exc
            else:
                self._last_request_at = time.monotonic()

            if "Please limit requests to one every 5 seconds" in payload:
                if attempt < attempts:
                    time.sleep(self._rate_limit_delay(attempt))
                    continue
                raise GDELTAPIError(
                    self._format_request_error(RuntimeError("rate limited by GDELT response body"))
                )

            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise GDELTAPIError(f"invalid JSON response: {exc}") from exc

            if isinstance(parsed, dict) and parsed.get("error"):
                error_text = str(parsed["error"])
                if self._is_rate_limit_text(error_text) and attempt < attempts:
                    time.sleep(self._rate_limit_delay(attempt))
                    continue
                raise GDELTAPIError(error_text)
            return parsed

        raise GDELTAPIError("rate limited by GDELT after multiple retry attempts")

    def _format_request_error(self, exc: Exception) -> str:
        if is_certificate_verification_error(exc):
            return build_certificate_error_message(
                target="the GDELT API",
                ca_bundle=self.ca_bundle,
                ca_bundle_source=self.ca_bundle_source,
            )
        if self._is_rate_limit_error(exc):
            retry_window = self._rate_limit_delay(self.rate_limit_retries or 1)
            return (
                f"GDELT rate-limited the request (HTTP 429). Wait about {int(retry_window)} seconds "
                "and try again, or reduce request volume with --no-timeline, --no-tone-timeline, "
                "or a larger --batch-window-hours."
            )
        return str(exc)

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        if isinstance(exc, HTTPError) and exc.code == 429:
            return True
        return self._is_rate_limit_text(str(exc))

    def _is_rate_limit_text(self, value: str) -> bool:
        lowered = value.lower()
        return "429" in lowered or "too many requests" in lowered or "please limit requests" in lowered

    def _rate_limit_delay(self, attempt: int, exc: Exception | None = None) -> float:
        if isinstance(exc, HTTPError):
            retry_after = exc.headers.get("Retry-After")
            if retry_after:
                try:
                    return max(float(retry_after), self.min_request_interval)
                except ValueError:
                    pass
        return max(self.rate_limit_backoff_seconds * attempt, self.min_request_interval)

    def _throttle(self) -> None:
        if not self.min_request_interval:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.min_request_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)