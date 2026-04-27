"""GDELT DOC 2.0 ingestion helpers."""

from .client import GDELTAPIError, GDELTClient, build_query
from .models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder
from .normalize import normalize_dataset
from .scraper import ArticleScraper
from .validate import SchemaValidationError, validate_dataset

__all__ = [
    "DateRange",
    "GDELTAPIError",
    "GDELTClient",
    "GDELTFilterSpec",
    "QueryRequest",
    "ArticleScraper",
    "SchemaValidationError",
    "SortOrder",
    "build_query",
    "normalize_dataset",
    "validate_dataset",
]
