import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder
from gdelt_news.normalize import normalize_dataset
from gdelt_news.validate import SchemaValidationError, validate_dataset


def _build_bundle():
    request = QueryRequest(
        filters=GDELTFilterSpec(keywords=["climate change"], entities=["European Union"]),
        date_range=DateRange(
            start=datetime(2026, 4, 15, tzinfo=timezone.utc),
            end=datetime(2026, 4, 16, tzinfo=timezone.utc),
        ),
        max_records=50,
        sort=SortOrder.DATE_DESC,
        bucket_size="hour",
    )
    return {
        "query": '"climate change"',
        "request": request,
        "article_params": {"mode": "artlist"},
        "timeline_params": {"mode": "timelinevolraw"},
        "tone_params": {"mode": "timelinetone"},
        "article_payload": {
            "articles": [
                {
                    "url": "https://example.com/a",
                    "title": "European Union debates climate change package",
                    "url_mobile": "",
                    "seendate": "20260416T144500Z",
                    "socialimage": "",
                    "domain": "example.com",
                    "language": "English",
                    "sourcecountry": "Germany",
                }
            ]
        },
        "timeline_payload": None,
        "tone_payload": None,
        "article_fetch": {
            "batching_enabled": False,
            "batch_window_hours": 24,
            "batch_count": 1,
            "raw_article_count": 1,
            "deduplicated_article_count": 1,
            "duplicates_removed": 0,
            "truncated_window_count": 0,
            "windows": [],
        },
        "warnings": [],
    }


class SchemaValidationTests(unittest.TestCase):
    def test_validate_dataset_passes_for_normalized_output(self) -> None:
        dataset = normalize_dataset(_build_bundle())
        schema_path = validate_dataset(dataset)
        self.assertTrue(schema_path.name.endswith(".json"))

    def test_validate_dataset_fails_when_required_key_is_missing(self) -> None:
        dataset = normalize_dataset(_build_bundle())
        del dataset["diagnostics"]["article_fetch"]

        with self.assertRaises(SchemaValidationError):
            validate_dataset(dataset)

    def test_validate_dataset_fails_for_malformed_trend_row(self) -> None:
        dataset = normalize_dataset(_build_bundle())
        dataset["analytics"]["trend_series"] = [
            {
                "value": 1,
                "metric": "article_count",
                "norm": None,
                "label": None,
            }
        ]

        with self.assertRaises(SchemaValidationError):
            validate_dataset(dataset)


if __name__ == "__main__":
    unittest.main()
