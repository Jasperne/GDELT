from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from .client import GDELTClient
from .models import DateRange, GDELTFilterSpec, QueryRequest, SortOrder, parse_datetime
from .normalize import normalize_dataset
from .validate import validate_dataset


def _load_request_from_file(path: Path) -> QueryRequest:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return QueryRequest.from_dict(payload)


def _split_prompt_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _read_prompt(prompt: str) -> str:
    try:
        return input(prompt)
    except (EOFError, KeyboardInterrupt) as exc:
        raise SystemExit("\nInteractive setup canceled.") from exc


def _prompt_text(prompt: str, default: str | None = None) -> str | None:
    suffix = f" [{default}]" if default else ""
    value = _read_prompt(f"{prompt}{suffix}: ").strip()
    if value:
        return value
    return default or None


def _prompt_list(prompt: str, default: list[str] | None = None) -> list[str]:
    default_text = ", ".join(default or [])
    value = _prompt_text(prompt, default_text or None)
    return _split_prompt_values(value)


def _prompt_choice(prompt: str, choices: list[str], default: str) -> str:
    allowed = {choice.lower(): choice for choice in choices}
    while True:
        value = _prompt_text(f"{prompt} ({', '.join(choices)})", default)
        if value is None:
            return default
        selected = allowed.get(value.lower())
        if selected:
            return selected
        print(f"Please choose one of: {', '.join(choices)}")


def _prompt_int(prompt: str, default: int | None = None, allow_empty: bool = False) -> int | None:
    default_text = str(default) if default is not None else None
    while True:
        value = _prompt_text(prompt, default_text)
        if value is None:
            if allow_empty:
                return None
            print("Please enter a number.")
            continue
        try:
            return int(value)
        except ValueError:
            print("Please enter a whole number.")


def _prompt_bool(prompt: str, default: bool) -> bool:
    default_text = "Y/n" if default else "y/N"
    while True:
        value = _read_prompt(f"{prompt} [{default_text}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes", "j", "ja"}:
            return True
        if value in {"n", "no", "nein"}:
            return False
        print("Please answer with y or n.")


def _prompt_date_range(args: argparse.Namespace) -> DateRange:
    default_timespan = args.timespan
    default_start = args.start
    default_end = args.end

    while True:
        timespan = _prompt_text(
            "Timespan (optional, for example 24h or 7days; leave empty to use start/end)",
            default_timespan,
        )
        if timespan:
            return DateRange(start=None, end=None, timespan=timespan)

        start = _prompt_text(
            "Start datetime in ISO format, for example 2026-04-16T00:00:00Z",
            default_start,
        )
        end = _prompt_text(
            "End datetime in ISO format, for example 2026-04-16T23:59:59Z",
            default_end,
        )
        if start and end:
            try:
                return DateRange(start=parse_datetime(start), end=parse_datetime(end), timespan=None)
            except ValueError:
                print("Please enter valid ISO datetimes, for example 2026-04-16T23:59:59Z.")
                default_start = start
                default_end = end
                continue

        print("Please enter either a timespan or both start and end datetimes.")
        default_start = start
        default_end = end


def _build_interactive_request(
    args: argparse.Namespace,
) -> tuple[QueryRequest, Path, Path | None]:
    print("Interactive GDELT setup")
    print("Press Enter to keep the default value shown in brackets or to skip optional fields.\n")

    date_range = _prompt_date_range(args)

    filters = GDELTFilterSpec(
        keywords=_prompt_list("Keywords (comma-separated)", args.keyword),
        exact_phrases=_prompt_list("Exact phrases (comma-separated)", args.phrase),
        themes=_prompt_list("Themes / GDELT codes (comma-separated)", args.theme),
        entities=_prompt_list("Entities (comma-separated)", args.entity),
        source_countries=_prompt_list("Source countries (comma-separated)", args.source_country),
        source_languages=_prompt_list("Source languages (comma-separated)", args.source_language),
        domains=_prompt_list("Domains (comma-separated)", args.domain),
        exact_domains=_prompt_list("Exact domains (comma-separated)", args.exact_domain),
        exclude_terms=_prompt_list("Excluded terms (comma-separated)", args.exclude),
        raw_fragments=_prompt_list("Advanced raw query fragments (comma-separated)", args.raw_fragment),
        match_mode=_prompt_choice("Match mode", ["any", "all"], args.match_mode),
    )

    has_filter = any(
        [
            filters.keywords,
            filters.exact_phrases,
            filters.themes,
            filters.entities,
            filters.source_countries,
            filters.source_languages,
            filters.domains,
            filters.exact_domains,
            filters.exclude_terms,
            filters.raw_fragments,
        ]
    )
    if not has_filter:
        raise SystemExit("Interactive setup canceled: at least one filter is required.")

    include_timeline = not _prompt_bool("Skip timeline volume data", args.no_timeline)
    include_tone_timeline = not _prompt_bool("Skip tone timeline data", args.no_tone_timeline)
    scrape_articles = _prompt_bool("Scrape matched article pages", args.scrape_articles)

    request = QueryRequest(
        filters=filters,
        date_range=date_range,
        max_records=_prompt_int("Max records per GDELT article window", args.max_records),
        sort=SortOrder(
            _prompt_choice(
                "Sort order",
                [item.value for item in SortOrder],
                args.sort,
            )
        ),
        include_timeline=include_timeline,
        include_tone_timeline=include_tone_timeline,
        bucket_size=_prompt_choice("Bucket size", ["minute", "hour", "day"], args.bucket_size),
        request_label=_prompt_text("Run label (optional)", args.label),
        batch_window_hours=_prompt_int("Batch window size in hours", args.batch_window_hours),
        deduplicate=not _prompt_bool("Keep duplicates across windows", args.no_deduplicate),
        validate_schema=not _prompt_bool("Skip schema validation", args.skip_schema_validation),
        scrape_articles=scrape_articles,
        scrape_limit=(
            _prompt_int("Maximum number of articles to scrape (optional)", args.scrape_limit, allow_empty=True)
            if scrape_articles
            else args.scrape_limit
        ),
        scrape_timeout=(
            _prompt_int("Scrape timeout in seconds", args.scrape_timeout)
            if scrape_articles
            else args.scrape_timeout
        ),
    )
    try:
        request.validate()
    except ValueError as exc:
        raise SystemExit(f"Interactive setup canceled: {exc}") from exc

    output_default = str(args.output) if args.output else "data/gdelt_dataset.json"
    csv_default = str(args.csv_output) if args.csv_output else None
    output_path = Path(
        _prompt_text("JSON output path", output_default)
        or output_default
    )
    csv_value = _prompt_text("CSV output path (optional)", csv_default)
    csv_output = Path(csv_value) if csv_value else None

    return request, output_path, csv_output


def _build_request_from_args(args: argparse.Namespace) -> QueryRequest:
    filters = GDELTFilterSpec(
        keywords=args.keyword or [],
        exact_phrases=args.phrase or [],
        themes=args.theme or [],
        entities=args.entity or [],
        source_countries=args.source_country or [],
        source_languages=args.source_language or [],
        domains=args.domain or [],
        exact_domains=args.exact_domain or [],
        exclude_terms=args.exclude or [],
        raw_fragments=args.raw_fragment or [],
        match_mode=args.match_mode,
    )

    date_range = DateRange(
        start=parse_datetime(args.start),
        end=parse_datetime(args.end),
        timespan=args.timespan,
    )

    return QueryRequest(
        filters=filters,
        date_range=date_range,
        max_records=args.max_records,
        sort=SortOrder(args.sort),
        include_timeline=not args.no_timeline,
        include_tone_timeline=not args.no_tone_timeline,
        bucket_size=args.bucket_size,
        request_label=args.label,
        batch_window_hours=args.batch_window_hours,
        deduplicate=not args.no_deduplicate,
        validate_schema=not args.skip_schema_validation,
        scrape_articles=args.scrape_articles,
        scrape_limit=args.scrape_limit,
        scrape_timeout=args.scrape_timeout,
    )


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_articles_csv(path: Path, articles: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "article_id",
        "title",
        "url",
        "mobile_url",
        "seen_at",
        "domain",
        "language",
        "source_country",
        "social_image_url",
        "matched_keywords",
        "matched_entities",
        "requested_themes",
        "scrape_status",
        "scraped_title",
        "scraped_description",
        "scraped_published_at",
        "scraped_language",
        "scraped_text_length",
        "scraped_excerpt",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for article in articles:
            row = {field: article.get(field) for field in fieldnames}
            row["matched_keywords"] = " | ".join(article.get("matched_keywords") or [])
            row["matched_entities"] = " | ".join(article.get("matched_entities") or [])
            row["requested_themes"] = " | ".join(article.get("requested_themes") or [])
            writer.writerow(row)

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch and normalize GDELT DOC 2.0 news data.")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Ask for filters and output paths step by step in the terminal",
    )
    parser.add_argument("--query-file", type=Path, help="JSON file describing the request")
    parser.add_argument("--output", type=Path, help="Output JSON dataset path")
    parser.add_argument("--csv-output", type=Path, help="Optional flat CSV export for articles")
    parser.add_argument("--label", help="Optional label stored in the output metadata")

    parser.add_argument("--keyword", action="append", help="Keyword filter")
    parser.add_argument("--phrase", action="append", help="Exact phrase filter")
    parser.add_argument("--theme", action="append", help="GKG theme filter")
    parser.add_argument("--entity", action="append", help="Tracked entity or name")
    parser.add_argument("--source-country", action="append", help="Source country filter")
    parser.add_argument("--source-language", action="append", help="Source language filter")
    parser.add_argument("--domain", action="append", help="Domain filter")
    parser.add_argument("--exact-domain", action="append", help="Exact domain filter")
    parser.add_argument("--exclude", action="append", help="Excluded keyword or phrase")
    parser.add_argument("--raw-fragment", action="append", help="Advanced GDELT query fragment")

    parser.add_argument("--start", help="UTC start datetime in ISO 8601 format")
    parser.add_argument("--end", help="UTC end datetime in ISO 8601 format")
    parser.add_argument("--timespan", help="Relative GDELT timespan like 7days or 24h")
    parser.add_argument("--match-mode", choices=["any", "all"], default="any")
    parser.add_argument("--max-records", type=int, default=75)
    parser.add_argument("--sort", choices=[item.value for item in SortOrder], default=SortOrder.HYBRID_REL.value)
    parser.add_argument("--bucket-size", choices=["minute", "hour", "day"], default="day")
    parser.add_argument("--no-timeline", action="store_true", help="Skip timeline volume mode")
    parser.add_argument(
        "--no-tone-timeline",
        action="store_true",
        help="Skip tone timeline mode used for sentiment-ready output",
    )
    parser.add_argument("--batch-window-hours", type=int, default=24)
    parser.add_argument("--no-deduplicate", action="store_true", help="Keep duplicate articles across windows")
    parser.add_argument("--scrape-articles", action="store_true", help="Enrich matched article URLs with scraped page content")
    parser.add_argument("--scrape-limit", type=int, help="Maximum number of article URLs to scrape")
    parser.add_argument("--scrape-timeout", type=int, default=15, help="Timeout in seconds for each article scrape")
    parser.add_argument(
        "--skip-schema-validation",
        action="store_true",
        help="Skip validation against the bundled dataset schema",
    )
    parser.add_argument(
        "--ca-bundle",
        help="Path to a PEM CA bundle used for HTTPS certificate verification",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--min-request-interval", type=float, default=6.0)
    parser.add_argument(
        "--rate-limit-retries",
        type=int,
        default=2,
        help="How many times to retry after a GDELT 429 rate-limit response",
    )
    parser.add_argument(
        "--rate-limit-backoff-seconds",
        type=float,
        default=15.0,
        help="Base wait time in seconds before retrying a 429 rate-limit response",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.interactive and args.query_file:
        parser.error("--interactive cannot be combined with --query-file")

    if args.interactive:
        request, output_path, csv_output_path = _build_interactive_request(args)
    elif args.query_file:
        if not args.output:
            parser.error("--output is required when using --query-file")
        request = _load_request_from_file(args.query_file)
        if args.label and not request.request_label:
            request.request_label = args.label
        output_path = args.output
        csv_output_path = args.csv_output
    else:
        if not args.output:
            parser.error("--output is required unless you use --interactive")
        request = _build_request_from_args(args)
        output_path = args.output
        csv_output_path = args.csv_output

    client = GDELTClient(
        timeout=args.timeout,
        min_request_interval=args.min_request_interval,
        ca_bundle=args.ca_bundle,
        rate_limit_retries=args.rate_limit_retries,
        rate_limit_backoff_seconds=args.rate_limit_backoff_seconds,
    )
    bundle = client.fetch_bundle(request)

    dataset = normalize_dataset(bundle)

    if request.validate_schema and not args.skip_schema_validation:
        schema_path = validate_dataset(dataset)
        dataset.setdefault("diagnostics", {})["schema_validation"] = {
            "validated": True,
            "schema_path": str(schema_path),
        }
    else:
        dataset.setdefault("diagnostics", {})["schema_validation"] = {
            "validated": False,
            "schema_path": None,
        }

    _write_json(output_path, dataset)
    if csv_output_path:
        _write_articles_csv(csv_output_path, dataset["articles"])

    return 0
