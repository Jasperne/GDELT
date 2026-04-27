from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from .analysis import build_entity_tracking, build_local_volume_series, build_ranked_counts, rebucket_series
from .client import DEFAULT_BASE_URL
from .models import QueryRequest


def _parse_gdelt_seen_date(value: str | None) -> str | None:
    if not value:
        return None
    parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return parsed.isoformat().replace("+00:00", "Z")


def _normalize_bucket_timestamp(value: str | None) -> str | None:
    if not value:
        return None

    for pattern in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S", "%Y%m%d"):
        try:
            parsed = datetime.strptime(value, pattern).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        return parsed.isoformat().replace("+00:00", "Z")
    return value


def _slugify_terms(values: list[str]) -> list[str]:
    return [value.strip() for value in values if value and value.strip()]


def _match_terms(text: str, terms: list[str]) -> list[str]:
    lowered = text.casefold()
    return [term for term in terms if term.casefold() in lowered]


def _scraped_value(scraped: dict[str, Any], key: str) -> str | None:
    value = scraped.get(key)
    if value in (None, ""):
        return None
    return str(value).strip() or None


def normalize_article(raw_article: dict[str, Any], request: QueryRequest) -> dict[str, Any]:
    url = str(raw_article.get("url") or "").strip()
    title = str(raw_article.get("title") or "").strip()
    seen_at = _parse_gdelt_seen_date(raw_article.get("seendate"))
    scraped = raw_article.get("scraped") or {}
    scraped_text = _scraped_value(scraped, "text")
    title_match_space = " ".join(
        value
        for value in [
            title,
            raw_article.get("domain") or "",
            raw_article.get("sourcecountry") or "",
            _scraped_value(scraped, "title") or "",
            _scraped_value(scraped, "description") or "",
            scraped_text or "",
        ]
        if value
    )

    keyword_terms = _slugify_terms(request.filters.keywords + request.filters.exact_phrases)
    entity_terms = _slugify_terms(request.filters.entities)

    return {
        "article_id": hashlib.sha256(url.encode("utf-8")).hexdigest() if url else None,
        "title": title or None,
        "url": url or None,
        "mobile_url": str(raw_article.get("url_mobile") or "").strip() or None,
        "seen_at": seen_at,
        "domain": str(raw_article.get("domain") or "").strip() or None,
        "language": str(raw_article.get("language") or "").strip() or None,
        "source_country": str(raw_article.get("sourcecountry") or "").strip() or None,
        "social_image_url": str(raw_article.get("socialimage") or "").strip() or None,
        "matched_keywords": _match_terms(title_match_space, keyword_terms),
        "matched_entities": _match_terms(title_match_space, entity_terms),
        "requested_themes": list(request.filters.themes),
        "scrape_status": _scraped_value(scraped, "status") or "not_requested",
        "scrape_success": bool(scraped.get("success")) if scraped else False,
        "scrape_error": _scraped_value(scraped, "error"),
        "scraped_title": _scraped_value(scraped, "title"),
        "scraped_description": _scraped_value(scraped, "description"),
        "scraped_published_at": _scraped_value(scraped, "published_at"),
        "scraped_language": _scraped_value(scraped, "language"),
        "scraped_final_url": _scraped_value(scraped, "final_url"),
        "scraped_excerpt": _scraped_value(scraped, "excerpt"),
        "scraped_text": scraped_text,
        "scraped_text_length": int(scraped.get("text_length", 0) or 0),
        "raw": raw_article,
    }


def _extract_timeline_rows(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload or not isinstance(payload, dict):
        return []

    for value in payload.values():
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return value
    return []


def _first_numeric(row: dict[str, Any], keys: tuple[str, ...]) -> float | int | None:
    for key in keys:
        if key not in row or row[key] in (None, ""):
            continue
        value = row[key]
        if isinstance(value, (int, float)):
            return value
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric.is_integer():
            return int(numeric)
        return numeric
    return None


def normalize_timeline_rows(
    payload: dict[str, Any] | None,
    metric: str,
) -> list[dict[str, Any]]:
    rows = []
    for raw_row in _extract_timeline_rows(payload):
        bucket = raw_row.get("date") or raw_row.get("datetime") or raw_row.get("time")
        value = _first_numeric(raw_row, ("value", "count", "tone", "ratio"))
        if bucket is None or value is None:
            continue
        rows.append(
            {
                "bucket_start": _normalize_bucket_timestamp(str(bucket)),
                "value": value,
                "metric": metric,
                "norm": _first_numeric(raw_row, ("norm", "total", "allarts")),
                "label": raw_row.get("label"),
            }
        )
    return rows


def normalize_dataset(bundle: dict[str, Any]) -> dict[str, Any]:
    request: QueryRequest = bundle["request"]
    article_fetch = {
        "batching_enabled": False,
        "batch_window_hours": request.batch_window_hours,
        "batch_count": 0,
        "fetch_attempt_count": 0,
        "split_window_count": 0,
        "raw_article_count": 0,
        "deduplicated_article_count": 0,
        "duplicates_removed": 0,
        "truncated_window_count": 0,
        "windows": [],
        **(bundle.get("article_fetch") or {}),
    }
    scraping = {
        "requested": False,
        "attempted": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "limit": request.scrape_limit,
        "timeout": request.scrape_timeout,
        "errors": [],
        **(bundle.get("scraping") or {}),
    }
    articles = [
        normalize_article(raw_article, request)
        for raw_article in (bundle.get("article_payload", {}) or {}).get("articles", [])
    ]

    trend_series = normalize_timeline_rows(bundle.get("timeline_payload"), metric="article_count")
    trend_source = "gdelt-timelinevolraw"
    if not trend_series:
        trend_series = build_local_volume_series(articles, request.bucket_size)
        trend_source = "article-bucket-fallback"
    trend_series = rebucket_series(trend_series, request.bucket_size, aggregation="sum")

    sentiment_series = normalize_timeline_rows(bundle.get("tone_payload"), metric="average_tone")
    sentiment_series = rebucket_series(
        sentiment_series,
        request.bucket_size,
        aggregation="weighted_average",
    )
    sentiment_source = "gdelt-timelinetone" if sentiment_series else "not_available"

    tracked_entities = _slugify_terms(request.filters.entities)
    entity_tracking = build_entity_tracking(articles, tracked_entities, request.bucket_size)

    return {
        "$schema": "schemas/gdelt_news_dataset.schema.json",
        "schema_version": "1.0.0",
        "request": {
            "source": "GDELT DOC 2.0 API",
            "base_url": DEFAULT_BASE_URL,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "request_label": request.request_label,
            "query": bundle["query"],
            "filters": request.filters.to_dict(),
            "date_range": request.date_range.to_dict(),
            "max_records": request.max_records,
            "sort": request.sort.value,
            "bucket_size": request.bucket_size,
            "batch_window_hours": request.batch_window_hours,
            "deduplicate": request.deduplicate,
            "validate_schema": request.validate_schema,
            "scrape_articles": request.scrape_articles,
            "scrape_limit": request.scrape_limit,
            "scrape_timeout": request.scrape_timeout,
            "article_params": bundle.get("article_params"),
            "timeline_params": bundle.get("timeline_params"),
            "tone_params": bundle.get("tone_params"),
        },
        "articles": articles,
        "analytics": {
            "analysis_targets": {
                "trend_metric": "article_count",
                "sentiment_metric": "average_tone",
                "tracked_entities": tracked_entities,
                "bucket_size": request.bucket_size,
            },
            "trend_series": trend_series,
            "sentiment_series": sentiment_series,
            "entity_tracking": entity_tracking,
            "summary": {
                "article_count": len(articles),
                "raw_article_count": article_fetch.get("raw_article_count", len(articles)),
                "duplicates_removed": article_fetch.get("duplicates_removed", 0),
                "scraped_article_count": sum(1 for article in articles if article["scrape_success"]),
                "scrape_failed_count": sum(
                    1
                    for article in articles
                    if article["scrape_status"] not in {"ok", "not_requested"}
                ),
                "languages": build_ranked_counts([article["language"] for article in articles]),
                "source_countries": build_ranked_counts([article["source_country"] for article in articles]),
                "domains": build_ranked_counts([article["domain"] for article in articles]),
                "matched_entities": build_ranked_counts(
                    [entity for article in articles for entity in article["matched_entities"]]
                ),
            },
        },
        "diagnostics": {
            "trend_source": trend_source,
            "sentiment_source": sentiment_source,
            "warnings": list(bundle.get("warnings", [])),
            "article_fetch": article_fetch,
            "scraping": scraping,
        },
    }