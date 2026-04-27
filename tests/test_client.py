import ssl
import sys
import unittest
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.client import GDELTAPIError, GDELTClient
from gdelt_news.models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder


class FakeHTTPResponse:
    def __init__(self, payload: str):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self.payload.encode("utf-8")


class StubClient(GDELTClient):
    def __init__(self, responses):
        super().__init__(min_request_interval=0)
        self.responses = list(responses)
        self.recorded_params = []

    def _request_json(self, params):  # type: ignore[override]
        self.recorded_params.append(params)
        return self.responses.pop(0)


class AdaptiveStubClient(GDELTClient):
    def __init__(self, responses_by_window):
        super().__init__(min_request_interval=0)
        self.responses_by_window = dict(responses_by_window)
        self.recorded_params = []

    def _request_json(self, params):  # type: ignore[override]
        self.recorded_params.append(params)
        key = (params.get("STARTDATETIME"), params.get("ENDDATETIME"))
        return self.responses_by_window[key]


class ClientBatchingTests(unittest.TestCase):
    def test_fetch_bundle_batches_and_deduplicates_articles(self) -> None:
        request = QueryRequest(
            filters=GDELTFilterSpec(keywords=["climate change"]),
            date_range=DateRange(
                start=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
                end=datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc),
            ),
            max_records=100,
            sort=SortOrder.DATE_DESC,
            include_timeline=False,
            include_tone_timeline=False,
            batch_window_hours=24,
        )

        client = StubClient(
            responses=[
                {
                    "articles": [
                        {
                            "url": "https://example.com/a",
                            "title": "Older article",
                            "seendate": "20260415T120000Z",
                            "domain": "example.com",
                        },
                        {
                            "url": "https://example.com/shared",
                            "title": "Duplicate article",
                            "seendate": "20260415T235959Z",
                            "domain": "example.com",
                        },
                    ]
                },
                {
                    "articles": [
                        {
                            "url": "https://example.com/shared",
                            "title": "Duplicate article",
                            "seendate": "20260415T235959Z",
                            "domain": "example.com",
                        },
                        {
                            "url": "https://example.com/b",
                            "title": "Newest article",
                            "seendate": "20260416T110000Z",
                            "domain": "example.com",
                        },
                    ]
                },
            ]
        )

        bundle = client.fetch_bundle(request)

        articles = bundle["article_payload"]["articles"]
        self.assertEqual(len(client.recorded_params), 2)
        self.assertEqual(len(articles), 3)
        self.assertEqual(articles[0]["url"], "https://example.com/b")
        self.assertEqual(bundle["article_fetch"]["duplicates_removed"], 1)
        self.assertEqual(bundle["article_fetch"]["batch_count"], 2)

    def test_fetch_bundle_adaptively_splits_truncated_windows(self) -> None:
        request = QueryRequest(
            filters=GDELTFilterSpec(keywords=["climate change"]),
            date_range=DateRange(
                start=datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc),
                end=datetime(2026, 4, 15, 4, 0, tzinfo=timezone.utc),
            ),
            max_records=2,
            sort=SortOrder.DATE_ASC,
            include_timeline=False,
            include_tone_timeline=False,
            batch_window_hours=4,
        )

        client = AdaptiveStubClient(
            responses_by_window={
                ("20260415000000", "20260415040000"): {
                    "articles": [
                        {
                            "url": "https://example.com/a",
                            "title": "Window article A",
                            "seendate": "20260415T010000Z",
                            "domain": "example.com",
                        },
                        {
                            "url": "https://example.com/b",
                            "title": "Window article B",
                            "seendate": "20260415T030000Z",
                            "domain": "example.com",
                        },
                    ]
                },
                ("20260415000000", "20260415020000"): {
                    "articles": [
                        {
                            "url": "https://example.com/a",
                            "title": "Window article A",
                            "seendate": "20260415T010000Z",
                            "domain": "example.com",
                        }
                    ]
                },
                ("20260415020000", "20260415040000"): {
                    "articles": [
                        {
                            "url": "https://example.com/b",
                            "title": "Window article B",
                            "seendate": "20260415T030000Z",
                            "domain": "example.com",
                        }
                    ]
                },
            }
        )

        bundle = client.fetch_bundle(request)

        self.assertEqual(len(client.recorded_params), 3)
        self.assertEqual(bundle["article_fetch"]["fetch_attempt_count"], 3)
        self.assertEqual(bundle["article_fetch"]["split_window_count"], 1)
        self.assertEqual(bundle["article_fetch"]["batch_count"], 2)
        self.assertEqual(bundle["article_fetch"]["truncated_window_count"], 0)
        self.assertEqual(bundle["article_fetch"]["raw_article_count"], 2)
        self.assertIn("splitting into smaller windows", bundle["warnings"][0])

    def test_request_json_surfaces_actionable_ssl_error(self) -> None:
        client = GDELTClient(min_request_interval=0)

        with patch(
            "gdelt_news.client.urlopen",
            side_effect=URLError(ssl.SSLCertVerificationError("certificate verify failed")),
        ):
            with self.assertRaises(GDELTAPIError) as context:
                client._request_json(
                    {
                        "query": "climate",
                        "mode": "artlist",
                        "format": "json",
                        "maxrecords": "1",
                        "sort": "DateDesc",
                    }
                )

        message = str(context.exception)
        self.assertIn("TLS certificate verification failed", message)
        self.assertIn("--ca-bundle $(python3 -m certifi)", message)

    def test_request_json_retries_after_rate_limit(self) -> None:
        client = GDELTClient(
            min_request_interval=0,
            rate_limit_retries=1,
            rate_limit_backoff_seconds=0,
        )
        headers = Message()
        headers["Retry-After"] = "0"

        with patch(
            "gdelt_news.client.urlopen",
            side_effect=[
                HTTPError(
                    url="https://api.gdeltproject.org",
                    code=429,
                    msg="Too Many Requests",
                    hdrs=headers,
                    fp=None,
                ),
                FakeHTTPResponse('{"articles": []}'),
            ],
        ) as mocked_urlopen, patch("gdelt_news.client.time.sleep") as mocked_sleep:
            payload = client._request_json(
                {
                    "query": "climate",
                    "mode": "artlist",
                    "format": "json",
                    "maxrecords": "1",
                    "sort": "DateDesc",
                }
            )

        self.assertEqual(payload, {"articles": []})
        self.assertEqual(mocked_urlopen.call_count, 2)
        mocked_sleep.assert_called_once()


if __name__ == "__main__":
    unittest.main()
