from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.request import Request, urlopen

from .tls import build_certificate_error_message, configure_ssl_context, is_certificate_verification_error


def _compact_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


class _ArticleHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._capture_title = False
        self._capture_paragraph = False
        self._in_body = False
        self._title_parts: list[str] = []
        self._paragraph_parts: list[str] = []
        self._current_paragraph: list[str] = []
        self._body_parts: list[str] = []
        self._meta: dict[str, str] = {}
        self._canonical_url: str | None = None
        self._language: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {key.lower(): value for key, value in attrs if value is not None}
        tag = tag.lower()

        if tag == "html" and attrs_dict.get("lang"):
            self._language = attrs_dict["lang"].strip()

        if tag == "body":
            self._in_body = True

        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return

        if tag == "title":
            self._capture_title = True
        elif tag == "p":
            self._capture_paragraph = True
            self._current_paragraph = []
        elif tag == "br" and self._capture_paragraph:
            self._current_paragraph.append(" ")
        elif tag == "meta":
            meta_key = (
                attrs_dict.get("property")
                or attrs_dict.get("name")
                or attrs_dict.get("http-equiv")
            )
            content = attrs_dict.get("content")
            if meta_key and content:
                self._meta[meta_key.strip().lower()] = content.strip()
        elif tag == "link":
            rel = attrs_dict.get("rel", "").lower()
            href = attrs_dict.get("href")
            if "canonical" in rel and href:
                self._canonical_url = href.strip()

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()

        if tag in {"script", "style", "noscript"}:
            self._skip_depth = max(0, self._skip_depth - 1)
            return

        if tag == "body":
            self._in_body = False
        elif tag == "title":
            self._capture_title = False
        elif tag == "p" and self._capture_paragraph:
            paragraph = _compact_whitespace("".join(self._current_paragraph))
            if paragraph:
                self._paragraph_parts.append(paragraph)
            self._current_paragraph = []
            self._capture_paragraph = False

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return

        if self._capture_title:
            self._title_parts.append(data)

        if self._capture_paragraph:
            self._current_paragraph.append(data)

        if self._in_body:
            cleaned = _compact_whitespace(data)
            if cleaned:
                self._body_parts.append(cleaned)

    def to_dict(self) -> dict[str, Any]:
        title = _compact_whitespace("".join(self._title_parts)) or None
        text = "\n\n".join(self._paragraph_parts).strip()
        if not text:
            text = _compact_whitespace(" ".join(self._body_parts)) or None

        description = (
            self._meta.get("og:description")
            or self._meta.get("description")
            or self._meta.get("twitter:description")
        )
        published_at = (
            self._meta.get("article:published_time")
            or self._meta.get("parsely-pub-date")
            or self._meta.get("pubdate")
            or self._meta.get("date")
            or self._meta.get("og:pubdate")
        )
        meta_title = self._meta.get("og:title") or self._meta.get("twitter:title")
        if meta_title and not title:
            title = meta_title

        excerpt = None
        if text:
            excerpt = text[:280].strip()
            if len(text) > 280:
                excerpt += "..."

        return {
            "title": title,
            "description": description or None,
            "published_at": published_at or None,
            "language": self._language or None,
            "canonical_url": self._canonical_url,
            "text": text,
            "excerpt": excerpt,
            "text_length": len(text) if text else 0,
        }


class ArticleScraper:
    def __init__(
        self,
        timeout: int = 15,
        min_request_interval: float = 0.0,
        user_agent: str = "gdelt-news-module/0.1 scraper",
        ca_bundle: str | None = None,
    ) -> None:
        self.timeout = timeout
        self.min_request_interval = min_request_interval
        self.user_agent = user_agent
        self.ssl_context, self.ca_bundle, self.ca_bundle_source = configure_ssl_context(ca_bundle)
        self._last_request_at = 0.0

    def enrich_articles(
        self,
        articles: list[dict[str, Any]],
        limit: int | None = None,
    ) -> dict[str, Any]:
        attempted = 0
        succeeded = 0
        failed = 0
        skipped = 0
        errors: list[str] = []

        for index, article in enumerate(articles):
            if limit is not None and index >= limit:
                skipped += 1
                continue

            url = str(article.get("url") or "").strip()
            if not url:
                skipped += 1
                article["scraped"] = {
                    "status": "skipped",
                    "success": False,
                    "error": "missing url",
                    "fetched_at": _utc_now(),
                }
                continue

            attempted += 1
            result = self.scrape_url(url)
            article["scraped"] = result
            if result["success"]:
                succeeded += 1
            else:
                failed += 1
                if result.get("error"):
                    errors.append(f"{url}: {result['error']}")

        return {
            "requested": True,
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "limit": limit,
            "timeout": self.timeout,
            "errors": errors,
        }

    def scrape_url(self, url: str) -> dict[str, Any]:
        try:
            final_url, html, content_type = self._fetch_html(url)
            parsed = self.parse_html(html)
            return {
                "status": "ok",
                "success": True,
                "requested_url": url,
                "final_url": final_url,
                "content_type": content_type,
                "fetched_at": _utc_now(),
                **parsed,
            }
        except Exception as exc:  # pragma: no cover - network failures vary by environment
            error_message = (
                build_certificate_error_message(
                    target=url,
                    ca_bundle=self.ca_bundle,
                    ca_bundle_source=self.ca_bundle_source,
                )
                if is_certificate_verification_error(exc)
                else str(exc)
            )
            return {
                "status": "error",
                "success": False,
                "requested_url": url,
                "final_url": None,
                "content_type": None,
                "fetched_at": _utc_now(),
                "title": None,
                "description": None,
                "published_at": None,
                "language": None,
                "canonical_url": None,
                "text": None,
                "excerpt": None,
                "text_length": 0,
                "error": error_message,
            }

    def parse_html(self, html: str) -> dict[str, Any]:
        parser = _ArticleHTMLParser()
        parser.feed(html)
        parser.close()
        return parser.to_dict()

    def _fetch_html(self, url: str) -> tuple[str, str, str | None]:
        self._throttle()
        request = Request(url, headers={"User-Agent": self.user_agent, "Accept": "text/html,application/xhtml+xml"})
        with urlopen(request, timeout=self.timeout, context=self.ssl_context) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            html = response.read().decode(charset, errors="replace")
            self._last_request_at = time.monotonic()
            return response.geturl(), html, response.headers.get_content_type()

    def _throttle(self) -> None:
        if not self.min_request_interval:
            return
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.min_request_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
