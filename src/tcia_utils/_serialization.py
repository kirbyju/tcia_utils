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
    """Pandas-free dictionary and JSON exports shared by public result objects."""
    def to_dict(self) -> dict:
        """Copy the public result fields into a JSON-compatible Python dictionary.

        Returns:
            A new nested dictionary retaining records and available provenance, scope, notes
            and completeness. Tuples become lists, local paths become strings, and None stays None.

        Raises:
            TypeError: A value cannot be represented or a dictionary key is not a string.
            ValueError: A number is nonfinite (NaN or infinity).

        Notes:
            Makes no requests and never collects more pages. Internal dataclass fields such
            as a page's fetch callback are omitted; metadata dictionary keys are retained.
            Changing the exported dictionary does not change the original result."""
        return _json_value(self)

    def to_json(self, *, indent: int | None = None) -> str:
        """Serialize the full public result envelope to JSON text without pandas.

        Args:
            indent: Optional JSON indentation width; None (default) uses one line. Use 2 for readable output.

        Returns:
            A Unicode JSON string with the same content as .to_dict(). Does not write a file.

        Raises:
            TypeError: A value or dictionary key is unsupported.
            ValueError: A number is nonfinite (NaN or infinity).

        Notes:
            Makes no requests and exports only the current search page. Preserve paging and
            completeness fields when saving results; use iter_pages() explicitly for more pages.

        Example:
            Path("metadata.json").write_text(result.to_json(indent=2), encoding="utf-8")"""
        return json.dumps(
            self.to_dict(), ensure_ascii=False, allow_nan=False, indent=indent
        )
