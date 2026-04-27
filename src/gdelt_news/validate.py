from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "gdelt_news_dataset.schema.json"


class SchemaValidationError(ValueError):
    """Raised when a normalized dataset does not match the expected schema."""


def load_dataset_schema(schema_path: str | Path | None = None) -> tuple[dict[str, Any], Path]:
    resolved_path = Path(schema_path) if schema_path else DEFAULT_SCHEMA_PATH
    return json.loads(resolved_path.read_text(encoding="utf-8")), resolved_path


def validate_dataset(
    dataset: dict[str, Any],
    schema_path: str | Path | None = None,
) -> Path:
    schema, resolved_path = load_dataset_schema(schema_path)
    _validate_node(dataset, schema, path="$")
    return resolved_path


def _validate_node(value: Any, schema: dict[str, Any], path: str) -> None:
    expected_types = schema.get("type")
    if expected_types is not None and not _matches_allowed_types(value, expected_types):
        raise SchemaValidationError(
            f"{path}: expected {expected_types}, got {_describe_type(value)}"
        )

    if "enum" in schema and value not in schema["enum"]:
        raise SchemaValidationError(f"{path}: expected one of {schema['enum']}, got {value!r}")

    if value is None:
        return

    normalized_types = (
        set(expected_types) if isinstance(expected_types, list) else {expected_types}
    ) if expected_types is not None else set()

    if "object" in normalized_types and isinstance(value, dict):
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                raise SchemaValidationError(f"{path}: missing required key '{key}'")

        properties = schema.get("properties", {})
        additional_properties = schema.get("additionalProperties", True)
        if additional_properties is False:
            extras = sorted(set(value) - set(properties))
            if extras:
                raise SchemaValidationError(
                    f"{path}: unexpected keys {', '.join(repr(key) for key in extras)}"
                )
        for key, property_schema in properties.items():
            if key in value:
                _validate_node(value[key], property_schema, f"{path}.{key}")
        return

    if "array" in normalized_types and isinstance(value, list):
        item_schema = schema.get("items")
        if item_schema:
            for index, item in enumerate(value):
                _validate_node(item, item_schema, f"{path}[{index}]")


def _matches_allowed_types(value: Any, expected_types: str | list[str]) -> bool:
    if isinstance(expected_types, list):
        return any(_matches_type(value, type_name) for type_name in expected_types)
    return _matches_type(value, expected_types)


def _matches_type(value: Any, type_name: str) -> bool:
    if type_name == "null":
        return value is None
    if type_name == "object":
        return isinstance(value, dict)
    if type_name == "array":
        return isinstance(value, list)
    if type_name == "string":
        return isinstance(value, str)
    if type_name == "boolean":
        return isinstance(value, bool)
    if type_name == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if type_name == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    return True


def _describe_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__
