import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.client import build_query
from gdelt_news.models import GDELTFilterSpec, ProximityFilter


class QueryBuilderTests(unittest.TestCase):
    def test_build_query_with_rich_filters(self) -> None:
        filters = GDELTFilterSpec(
            keywords=["climate change", "emissions"],
            entities=["European Union"],
            source_countries=["Germany"],
            source_languages=["English"],
            exact_domains=["un.org"],
            exclude_terms=["sports"],
            tone_max=-2,
            proximity=[ProximityFilter(distance=5, terms=["climate", "policy"])],
            repeated_terms={"emissions": 2},
            match_mode="any",
        )

        query = build_query(filters)

        self.assertIn('("climate change" OR emissions OR "European Union")', query)
        self.assertIn("sourcecountry:germany", query)
        self.assertIn("sourcelang:english", query)
        self.assertIn("domainis:un.org", query)
        self.assertIn("tone<-2", query)
        self.assertIn('near5:"climate policy"', query)
        self.assertIn('repeat2:"emissions"', query)
        self.assertIn("-sports", query)

    def test_build_query_ors_multiple_country_and_language_filters(self) -> None:
        filters = GDELTFilterSpec(
            keywords=["climate change"],
            source_countries=["Germany", "France"],
            source_languages=["English", "German"],
        )

        query = build_query(filters)

        self.assertIn("(sourcecountry:germany OR sourcecountry:france)", query)
        self.assertIn("(sourcelang:english OR sourcelang:german)", query)

    def test_requires_at_least_one_fragment(self) -> None:
        with self.assertRaises(ValueError):
            build_query(GDELTFilterSpec())


if __name__ == "__main__":
    unittest.main()
