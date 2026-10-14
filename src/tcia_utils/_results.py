"""Small immutable result envelopes; records retain service field names."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Iterator, Mapping, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Provenance:
    base_url: str
    release_fingerprint: str
    release_tag: str | None
    installed_profile: str
    capabilities: Mapping[str, bool | None]
    observed_at_utc: str


@dataclass(frozen=True)
class Completeness:
    state: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {"complete", "partial", "unknown", "unavailable"}:
            raise ValueError(f"Invalid completeness state: {self.state}")


@dataclass(frozen=True)
class ServiceStatus:
    readiness: Mapping[str, Any]
    capabilities: Mapping[str, bool | None]
    release_fingerprint: str
    release_tag: str | None
    installed_profile: str


def _frame(records: Any) -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("Install tcia_utils[tables] to use to_dataframe()") from exc
    return pd.DataFrame(records)


@dataclass(frozen=True)
class Result(Generic[T]):
    data: T
    notes: tuple[str, ...]
    provenance: Provenance
    completeness: Completeness

    def to_dataframe(self, key: str | None = None) -> Any:
        """Convert an explicit record list without changing evidence fields."""
        records: Any = self.data
        if isinstance(records, Mapping):
            if key is None:
                candidates = [value for value in records.values() if isinstance(value, list)]
                if len(candidates) != 1:
                    raise ValueError("Specify the record-list key for this result")
                records = candidates[0]
            else:
                records = records[key]
        if not isinstance(records, (list, tuple)):
            raise ValueError("Result does not contain a record list")
        return _frame(records)


@dataclass(frozen=True)
class Page(Generic[T]):
    items: tuple[T, ...]
    count: int
    limit: int
    has_more: bool
    truncated: bool
    next_cursor: str | None
    query: Mapping[str, Any]
    response: Mapping[str, Any]
    provenance: Provenance
    completeness: Completeness
    _fetch_next: Callable[[], "Page[T]"] | None = field(default=None, repr=False, compare=False)

    def next_page(self) -> "Page[T] | None":
        """Fetch exactly one following page, if advertised by the service."""
        return self._fetch_next() if self._fetch_next is not None else None

    def iter_pages(self) -> Iterator["Page[T]"]:
        """Yield this page and following pages lazily."""
        current: Page[T] | None = self
        while current is not None:
            yield current
            current = current.next_page()

    def to_dataframe(self) -> Any:
        return _frame(self.items)
