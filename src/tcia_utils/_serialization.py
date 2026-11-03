"""Dependency-free JSON exports of public result envelopes."""

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
import json
import math
from pathlib import PurePath


def _json_value(value):
    """Copy supported values, preserving JSON types and excluding internal fields."""
    if is_dataclass(value) and not isinstance(value, type):
        return {
            item.name: _json_value(getattr(value, item.name))
            for item in fields(value)
            if not item.name.startswith("_")
        }
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("JSON export requires string dictionary keys")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, PurePath):
        return str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON export requires finite numbers")
        return value
    raise TypeError(f"Unsupported JSON export value: {type(value).__name__}")


class JsonExportMixin:
    def to_dict(self) -> dict:
        """Return an independent JSON-compatible copy of the public result fields.

        No request is made and no further pages are fetched. Tuples become lists,
        paths become strings, and unsupported values raise rather than being lost.
        """
        return _json_value(self)

    def to_json(self, *, indent: int | None = None) -> str:
        """Return the full result as JSON text; use indent=2 for readable output."""
        return json.dumps(
            self.to_dict(), ensure_ascii=False, allow_nan=False, indent=indent
        )
