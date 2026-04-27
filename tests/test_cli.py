import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.cli import _build_interactive_request, _write_articles_csv, build_parser
from gdelt_news.models import SortOrder


class CLITests(unittest.TestCase):
    def test_parser_accepts_interactive_without_output(self) -> None:
        args = build_parser().parse_args(["--interactive"])
        self.assertTrue(args.interactive)
        self.assertIsNone(args.output)

    def test_write_articles_csv_ignores_non_tabular_fields(self) -> None:
        article = {
            "article_id": "1",
            "title": "Example title",
            "url": "https://example.com/story",
            "mobile_url": None,
            "seen_at": "2026-04-16T10:00:00Z",
            "domain": "example.com",
            "language": "English",
            "source_country": "Germany",
            "social_image_url": None,
            "matched_keywords": ["climate change"],
            "matched_entities": ["European Union"],
            "requested_themes": ["ENV_CLIMATECHANGE"],
            "scrape_status": "ok",
            "scrape_success": True,
            "scrape_error": None,
            "scraped_title": "Example title",
            "scraped_description": "Description",
            "scraped_published_at": "2026-04-16T09:30:00Z",
            "scraped_language": "en",
            "scraped_final_url": "https://example.com/final",
            "scraped_excerpt": "Excerpt",
            "scraped_text": "Full article text",
            "scraped_text_length": 17,
            "raw": {"url": "https://example.com/story"},
        }

        output_path = Path(tempfile.gettempdir()) / "gdelt_cli_articles.csv"
        _write_articles_csv(output_path, [article])

        with output_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["title"], "Example title")
        self.assertEqual(rows[0]["matched_keywords"], "climate change")
        self.assertEqual(rows[0]["matched_entities"], "European Union")
        self.assertEqual(rows[0]["requested_themes"], "ENV_CLIMATECHANGE")

    def test_build_interactive_request_collects_prompt_values(self) -> None:
        args = build_parser().parse_args(["--interactive"])
        responses = [
            "",
            "2026-04-01T00:00:00Z",
            "2026-04-16T23:59:59Z",
            "Merz, CDU",
            "",
            "",
            "Friedrich Merz",
            "germany, austria",
            "german",
            "",
            "",
            "sports",
            "",
            "any",
            "y",
            "y",
            "y",
            "100",
            "DateDesc",
            "hour",
            "merz-april",
            "72",
            "n",
            "n",
            "10",
            "15",
            "data/merz.json",
            "data/merz.csv",
        ]

        with patch("builtins.input", side_effect=responses):
            request, output_path, csv_output = _build_interactive_request(args)

        self.assertEqual(request.date_range.to_dict()["start"], "2026-04-01T00:00:00Z")
        self.assertEqual(request.date_range.to_dict()["end"], "2026-04-16T23:59:59Z")
        self.assertEqual(request.filters.keywords, ["Merz", "CDU"])
        self.assertEqual(request.filters.entities, ["Friedrich Merz"])
        self.assertEqual(request.filters.source_countries, ["germany", "austria"])
        self.assertEqual(request.filters.source_languages, ["german"])
        self.assertEqual(request.filters.exclude_terms, ["sports"])
        self.assertEqual(request.sort, SortOrder.DATE_DESC)
        self.assertFalse(request.include_timeline)
        self.assertFalse(request.include_tone_timeline)
        self.assertTrue(request.scrape_articles)
        self.assertEqual(request.scrape_limit, 10)
        self.assertEqual(request.scrape_timeout, 15)
        self.assertEqual(request.request_label, "merz-april")
        self.assertEqual(output_path, Path("data/merz.json"))
        self.assertEqual(csv_output, Path("data/merz.csv"))


if __name__ == "__main__":
    unittest.main()
