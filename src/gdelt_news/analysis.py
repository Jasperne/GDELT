from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any


def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def bucket_datetime(value: datetime, bucket_size: str) -> datetime:
    value = value.astimezone(timezone.utc)
    if bucket_size == "minute":
        return value.replace(second=0, microsecond=0)
    if bucket_size == "hour":
        return value.replace(minute=0, second=0, microsecond=0)
    if bucket_size == "day":
        return value.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unsupported bucket_size: {bucket_size}")


def isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_local_volume_series(articles: list[dict[str, Any]], bucket_size: str) -> list[dict[str, Any]]:
    counter: Counter[datetime] = Counter()
    for article in articles:
        seen_at = article.get("seen_at")
        if not seen_at:
            continue
        bucket = bucket_datetime(parse_iso_datetime(seen_at), bucket_size)
        counter[bucket] += 1

    return [
        {
            "bucket_start": isoformat(bucket),
            "value": count,
            "metric": "article_count",
            "norm": None,
            "label": None,
        }
        for bucket, count in sorted(counter.items())
    ]


def build_entity_tracking(
    articles: list[dict[str, Any]],
    tracked_entities: list[str],
    bucket_size: str,
) -> list[dict[str, Any]]:
    if not tracked_entities:
        return []

    buckets: dict[tuple[datetime, str], int] = defaultdict(int)
    for article in articles:
        seen_at = article.get("seen_at")
        if not seen_at:
            continue
        bucket = bucket_datetime(parse_iso_datetime(seen_at), bucket_size)
        matched_entities = article.get("matched_entities") or []
        for entity in matched_entities:
            buckets[(bucket, entity)] += 1

    return [
        {
            "bucket_start": isoformat(bucket),
            "entity": entity,
            "article_count": count,
        }
        for (bucket, entity), count in sorted(buckets.items(), key=lambda item: (item[0][0], item[0][1]))
    ]


def build_ranked_counts(values: list[str]) -> list[dict[str, Any]]:
    counts = Counter(value for value in values if value)
    return [{"value": value, "count": count} for value, count in counts.most_common()]


def rebucket_series(
    series: list[dict[str, Any]],
    bucket_size: str,
    aggregation: str,
) -> list[dict[str, Any]]:
    if aggregation not in {"sum", "weighted_average"}:
        raise ValueError(f"unsupported aggregation: {aggregation}")

    grouped_rows: dict[tuple[datetime, str], list[dict[str, Any]]] = defaultdict(list)
    for row in series:
        bucket_start = row.get("bucket_start")
        metric = row.get("metric")
        value = row.get("value")
        if not bucket_start or metric is None or value is None:
            continue
        try:
            bucket = bucket_datetime(parse_iso_datetime(str(bucket_start)), bucket_size)
        except ValueError:
            continue
        grouped_rows[(bucket, str(metric))].append(row)

    rebucketed: list[dict[str, Any]] = []
    for (bucket, metric), rows in sorted(grouped_rows.items(), key=lambda item: item[0]):
        values = [float(row["value"]) for row in rows]
        norms = [_coerce_numeric(row.get("norm")) for row in rows]

        if aggregation == "sum":
            value: float | int = sum(values)
            if float(value).is_integer():
                value = int(value)
        else:
            weights = [norm if norm is not None and norm > 0 else 1.0 for norm in norms]
            weighted_total = sum(current_value * weight for current_value, weight in zip(values, weights))
            value = weighted_total / sum(weights)

        norm_total = sum(norm for norm in norms if norm is not None)
        if norm_total.is_integer():
            norm_value: float | int | None = int(norm_total)
        else:
            norm_value = norm_total
        if norm_total == 0:
            norm_value = None

        rebucketed.append(
            {
                "bucket_start": isoformat(bucket),
                "value": value,
                "metric": metric,
                "norm": norm_value,
                "label": None,
            }
        )

    return rebucketed


def _coerce_numeric(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
