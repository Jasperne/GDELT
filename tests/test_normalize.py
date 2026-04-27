import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder
from gdelt_news.normalize import normalize_article, normalize_dataset, normalize_timeline_rows


class NormalizeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.request = QueryRequest(
            filters=GDELTFilterSpec(
                keywords=["climate change"],
                entities=["European Union"],
            ),
            date_range=DateRange(
                start=datetime(2026, 4, 15, tzinfo=timezone.utc),
                end=datetime(2026, 4, 16, tzinfo=timezone.utc),
            ),
            max_records=50,
            sort=SortOrder.DATE_DESC,
            bucket_size="hour",
        )

    def test_normalize_article(self) -> None:
        raw_article = {
            "url": "https://example.com/news",
            "url_mobile": "",
            "title": "Climate package update",
            "seendate": "20260416T144500Z",
            "socialimage": "https://example.com/image.jpg",
            "domain": "example.com",
            "language": "English",
            "sourcecountry": "Germany",
            "scraped": {
                "status": "ok",
                "success": True,
                "title": "European Union debates climate policy",
                "description": "A long report about climate change measures.",
                "published_at": "2026-04-16T12:00:00Z",
                "language": "en",
                "final_url": "https://example.com/news",
                "excerpt": "European Union climate change report...",
                "text": "European Union leaders discussed climate change legislation in detail.",
                "text_length": 73,
            },
        }

        article = normalize_article(raw_article, self.request)

        self.assertEqual(article["seen_at"], "2026-04-16T14:45:00Z")
        self.assertEqual(article["matched_keywords"], ["climate change"])
        self.assertEqual(article["matched_entities"], ["European Union"])
        self.assertEqual(article["domain"], "example.com")
        self.assertEqual(article["scrape_status"], "ok")
        self.assertEqual(article["scraped_language"], "en")
        self.assertEqual(article["scraped_text_length"], 73)

    def test_normalize_timeline_rows(self) -> None:
        payload = {
            "timeline": [
                {"date": "20260416000000", "value": 12, "norm": 1000},
                {"date": "20260416010000", "value": 18, "norm": 1100}
            ]
        }

        series = normalize_timeline_rows(payload, metric="article_count")

        self.assertEqual(len(series), 2)
        self.assertEqual(series[0]["value"], 12)
        self.assertEqual(series[1]["norm"], 1100)

    def test_dataset_falls_back_to_local_trend_series(self) -> None:
        bundle = {
            "query": '"climate change"',
            "request": self.request,
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
                        "sourcecountry": "Germany"
                    },
                    {
                        "url": "https://example.com/b",
                        "title": "Climate change policy update",
                        "url_mobile": "",
                        "seendate": "20260416T145500Z",
                        "socialimage": "",
                        "domain": "example.com",
                        "language": "English",
                        "sourcecountry": "Germany"
                    }
                ]
            },
            "timeline_payload": None,
            "tone_payload": None,
            "article_fetch": {
                "batching_enabled": False,
                "batch_window_hours": 24,
                "batch_count": 1,
                "raw_article_count": 2,
                "deduplicated_article_count": 2,
                "duplicates_removed": 0,
                "truncated_window_count": 0,
                "windows": [],
            },
            "scraping": {
                "requested": True,
                "attempted": 2,
                "succeeded": 1,
                "failed": 1,
                "skipped": 0,
                "limit": 2,
                "timeout": 15,
                "errors": ["https://example.com/b: timeout"],
            },
            "warnings": []
        }

        dataset = normalize_dataset(bundle)

        self.assertEqual(dataset["diagnostics"]["trend_source"], "article-bucket-fallback")
        self.assertEqual(dataset["diagnostics"]["sentiment_source"], "not_available")
        self.assertEqual(dataset["analytics"]["summary"]["article_count"], 2)
        self.assertEqual(dataset["analytics"]["summary"]["raw_article_count"], 2)
        self.assertEqual(dataset["analytics"]["summary"]["scraped_article_count"], 0)
        self.assertEqual(dataset["analytics"]["analysis_targets"]["trend_metric"], "article_count")
        self.assertEqual(dataset["analytics"]["analysis_targets"]["tracked_entities"], ["European Union"])
        self.assertEqual(dataset["analytics"]["entity_tracking"][0]["entity"], "European Union")
        self.assertEqual(dataset["diagnostics"]["scraping"]["attempted"], 2)

    def test_dataset_rebuckets_gdelt_timeline_series_to_requested_bucket_size(self) -> None:
        bundle = {
            "query": '"climate change"',
            "request": self.request,
            "article_params": {"mode": "artlist"},
            "timeline_params": {"mode": "timelinevolraw"},
            "tone_params": {"mode": "timelinetone"},
            "article_payload": {"articles": []},
            "timeline_payload": {
                "timeline": [
                    {"date": "20260416001000", "value": 2, "norm": 20},
                    {"date": "20260416004000", "value": 3, "norm": 30},
                ]
            },
            "tone_payload": {
                "timeline": [
                    {"date": "20260416000500", "value": 1.0, "norm": 2},
                    {"date": "20260416005500", "value": 4.0, "norm": 1},
                ]
            },
            "article_fetch": {
                "batching_enabled": False,
                "batch_window_hours": 24,
                "batch_count": 1,
                "fetch_attempt_count": 1,
                "split_window_count": 0,
                "raw_article_count": 0,
                "deduplicated_article_count": 0,
                "duplicates_removed": 0,
                "truncated_window_count": 0,
                "windows": [],
            },
            "scraping": {
                "requested": False,
                "attempted": 0,
                "succeeded": 0,
                "failed": 0,
                "skipped": 0,
                "limit": None,
                "timeout": 15,
                "errors": [],
            },
            "warnings": [],
        }

        dataset = normalize_dataset(bundle)

        self.assertEqual(dataset["analytics"]["trend_series"], [
            {
                "bucket_start": "2026-04-16T00:00:00Z",
                "value": 5,
                "metric": "article_count",
                "norm": 50,
                "label": None,
            }
        ])
        self.assertAlmostEqual(dataset["analytics"]["sentiment_series"][0]["value"], 2.0)
        self.assertEqual(dataset["analytics"]["sentiment_series"][0]["bucket_start"], "2026-04-16T00:00:00Z")
        self.assertEqual(dataset["analytics"]["sentiment_series"][0]["norm"], 3)


if __name__ == "__main__":
    unittest.main()
