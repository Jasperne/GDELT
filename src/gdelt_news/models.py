from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _clean_list(values: list[Any] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip() for value in values if str(value).strip()]


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    candidate = value.strip()
    if not candidate:
        return None

    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    parsed = datetime.fromisoformat(candidate)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_gdelt_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y%m%d%H%M%S")


def format_iso_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class SortOrder(str, Enum):
    HYBRID_REL = "HybridRel"
    DATE_DESC = "DateDesc"
    DATE_ASC = "DateAsc"
    TONE_DESC = "ToneDesc"
    TONE_ASC = "ToneAsc"


@dataclass(slots=True)
class ProximityFilter:
    distance: int
    terms: list[str]

    def to_query_fragment(self) -> str:
        cleaned_terms = _clean_list(self.terms)
        if self.distance < 1:
            raise ValueError("proximity distance must be greater than zero")
        if len(cleaned_terms) < 2:
            raise ValueError("proximity filters require at least two terms")
        return f'near{self.distance}:"{" ".join(cleaned_terms)}"'

    def to_dict(self) -> dict[str, Any]:
        return {"distance": self.distance, "terms": list(self.terms)}


@dataclass(slots=True)
class DateRange:
    start: datetime | None = None
    end: datetime | None = None
    timespan: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "DateRange":
        payload = payload or {}
        return cls(
            start=parse_datetime(payload.get("start")),
            end=parse_datetime(payload.get("end")),
            timespan=(payload.get("timespan") or None),
        )

    def validate(self) -> None:
        if self.start and self.end and self.start > self.end:
            raise ValueError("date_range.start must be earlier than date_range.end")
        if self.timespan and (self.start or self.end):
            raise ValueError("Use either timespan or explicit start/end dates, not both")

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": format_iso_datetime(self.start),
            "end": format_iso_datetime(self.end),
            "timespan": self.timespan,
        }


@dataclass(slots=True)
class GDELTFilterSpec:
    keywords: list[str] = field(default_factory=list)
    exact_phrases: list[str] = field(default_factory=list)
    themes: list[str] = field(default_factory=list)
    entities: list[str] = field(default_factory=list)
    source_countries: list[str] = field(default_factory=list)
    source_languages: list[str] = field(default_factory=list)
    domains: list[str] = field(default_factory=list)
    exact_domains: list[str] = field(default_factory=list)
    exclude_terms: list[str] = field(default_factory=list)
    tone_min: float | None = None
    tone_max: float | None = None
    tone_abs_min: float | None = None
    tone_abs_max: float | None = None
    proximity: list[ProximityFilter] = field(default_factory=list)
    repeated_terms: dict[str, int] = field(default_factory=dict)
    raw_fragments: list[str] = field(default_factory=list)
    match_mode: str = "any"

    @classmethod
    def from_dict(cls, payload: dict[str, Any] | None) -> "GDELTFilterSpec":
        payload = payload or {}
        proximity = [
            ProximityFilter(distance=int(item["distance"]), terms=list(item["terms"]))
            for item in payload.get("proximity", [])
        ]
        repeated_terms = {
            str(term).strip(): int(count)
            for term, count in (payload.get("repeated_terms") or {}).items()
            if str(term).strip()
        }
        return cls(
            keywords=_clean_list(payload.get("keywords")),
            exact_phrases=_clean_list(payload.get("exact_phrases")),
            themes=_clean_list(payload.get("themes")),
            entities=_clean_list(payload.get("entities")),
            source_countries=_clean_list(payload.get("source_countries")),
            source_languages=_clean_list(payload.get("source_languages")),
            domains=_clean_list(payload.get("domains")),
            exact_domains=_clean_list(payload.get("exact_domains")),
            exclude_terms=_clean_list(payload.get("exclude_terms")),
            tone_min=payload.get("tone_min"),
            tone_max=payload.get("tone_max"),
            tone_abs_min=payload.get("tone_abs_min"),
            tone_abs_max=payload.get("tone_abs_max"),
            proximity=proximity,
            repeated_terms=repeated_terms,
            raw_fragments=_clean_list(payload.get("raw_fragments")),
            match_mode=str(payload.get("match_mode", "any")).strip().lower() or "any",
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "keywords": list(self.keywords),
            "exact_phrases": list(self.exact_phrases),
            "themes": list(self.themes),
            "entities": list(self.entities),
            "source_countries": list(self.source_countries),
            "source_languages": list(self.source_languages),
            "domains": list(self.domains),
            "exact_domains": list(self.exact_domains),
            "exclude_terms": list(self.exclude_terms),
            "tone_min": self.tone_min,
            "tone_max": self.tone_max,
            "tone_abs_min": self.tone_abs_min,
            "tone_abs_max": self.tone_abs_max,
            "proximity": [item.to_dict() for item in self.proximity],
            "repeated_terms": dict(self.repeated_terms),
            "raw_fragments": list(self.raw_fragments),
            "match_mode": self.match_mode,
        }


@dataclass(slots=True)
class QueryRequest:
    filters: GDELTFilterSpec
    date_range: DateRange = field(default_factory=DateRange)
    max_records: int = 75
    sort: SortOrder = SortOrder.HYBRID_REL
    include_timeline: bool = True
    include_tone_timeline: bool = True
    bucket_size: str = "day"
    request_label: str | None = None
    batch_window_hours: int | None = 24
    deduplicate: bool = True
    validate_schema: bool = True
    scrape_articles: bool = False
    scrape_limit: int | None = None
    scrape_timeout: int = 15

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "QueryRequest":
        payload = payload or {}
        sort_value = payload.get("sort", SortOrder.HYBRID_REL.value)
        return cls(
            filters=GDELTFilterSpec.from_dict(payload.get("filters")),
            date_range=DateRange.from_dict(payload.get("date_range")),
            max_records=int(payload.get("max_records", 75)),
            sort=SortOrder(sort_value),
            include_timeline=bool(payload.get("include_timeline", True)),
            include_tone_timeline=bool(payload.get("include_tone_timeline", True)),
            bucket_size=str(payload.get("bucket_size", "day")).strip().lower() or "day",
            request_label=(payload.get("request_label") or None),
            batch_window_hours=(
                int(payload["batch_window_hours"])
                if payload.get("batch_window_hours") is not None
                else 24
            ),
            deduplicate=bool(payload.get("deduplicate", True)),
            validate_schema=bool(payload.get("validate_schema", True)),
            scrape_articles=bool(payload.get("scrape_articles", False)),
            scrape_limit=(
                int(payload["scrape_limit"])
                if payload.get("scrape_limit") is not None
                else None
            ),
            scrape_timeout=int(payload.get("scrape_timeout", 15)),
        )

    def validate(self) -> None:
        self.date_range.validate()
        if self.max_records < 1:
            raise ValueError("max_records must be greater than zero")
        if self.max_records > 250:
            raise ValueError("max_records must be at most 250 for GDELT ArticleList mode")
        if self.bucket_size not in {"minute", "hour", "day"}:
            raise ValueError("bucket_size must be one of: minute, hour, day")
        if self.batch_window_hours is not None and self.batch_window_hours < 1:
            raise ValueError("batch_window_hours must be at least 1 when enabled")
        if self.scrape_limit is not None and self.scrape_limit < 1:
            raise ValueError("scrape_limit must be at least 1 when enabled")
        if self.scrape_timeout < 1:
            raise ValueError("scrape_timeout must be at least 1 second")

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_label": self.request_label,
            "filters": self.filters.to_dict(),
            "date_range": self.date_range.to_dict(),
            "max_records": self.max_records,
            "sort": self.sort.value,
            "include_timeline": self.include_timeline,
            "include_tone_timeline": self.include_tone_timeline,
            "bucket_size": self.bucket_size,
            "batch_window_hours": self.batch_window_hours,
            "deduplicate": self.deduplicate,
            "validate_schema": self.validate_schema,
            "scrape_articles": self.scrape_articles,
            "scrape_limit": self.scrape_limit,
            "scrape_timeout": self.scrape_timeout,
        }
