import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.scraper import ArticleScraper


SAMPLE_HTML = """
<html lang="en">
  <head>
    <title>Example Story</title>
    <meta name="description" content="Example description for the article.">
    <meta property="article:published_time" content="2026-04-16T10:30:00Z">
    <link rel="canonical" href="https://example.com/final-story">
  </head>
  <body>
    <article>
      <p>First paragraph with European Union references.</p>
      <p>Second paragraph about climate change policy.</p>
    </article>
  </body>
</html>
"""


class StubScraper(ArticleScraper):
    def __init__(self):
        super().__init__(timeout=1)

    def _fetch_html(self, url):  # type: ignore[override]
        return "https://example.com/final-story", SAMPLE_HTML, "text/html"


class ScraperTests(unittest.TestCase):
    def test_parse_html_extracts_structured_article_fields(self) -> None:
        scraper = ArticleScraper()
        parsed = scraper.parse_html(SAMPLE_HTML)

        self.assertEqual(parsed["title"], "Example Story")
        self.assertEqual(parsed["language"], "en")
        self.assertEqual(parsed["published_at"], "2026-04-16T10:30:00Z")
        self.assertIn("European Union", parsed["text"])
        self.assertEqual(parsed["canonical_url"], "https://example.com/final-story")

    def test_enrich_articles_adds_scraped_payload(self) -> None:
        scraper = StubScraper()
        articles = [{"url": "https://example.com/story"}]

        diagnostics = scraper.enrich_articles(articles, limit=1)

        self.assertEqual(diagnostics["attempted"], 1)
        self.assertEqual(diagnostics["succeeded"], 1)
        self.assertEqual(articles[0]["scraped"]["status"], "ok")
        self.assertIn("climate change", articles[0]["scraped"]["text"])


if __name__ == "__main__":
    unittest.main()
