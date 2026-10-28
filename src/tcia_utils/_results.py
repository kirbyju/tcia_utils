"""Small immutable result envelopes; records retain service field names."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
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


def _frame(records: Any, columns=None) -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("Install tcia_utils[tables] to use to_dataframe()") from exc
    return pd.DataFrame(records, columns=columns)


@dataclass(frozen=True)
class Result(Generic[T]):
    data: T
    notes: tuple[str, ...]
    provenance: Provenance
    completeness: Completeness

    def to_dataframe(self, key: str | None = None, *, columns=None) -> Any:
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
        return _frame(records, columns=columns)


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

    def to_dataframe(self, *, all_pages: bool = False) -> Any:
        """Convert this page, or explicitly collect all cursor pages first."""
        if not isinstance(all_pages, bool):
            raise ValueError("all_pages must be a boolean")
        rows = self.items
        if all_pages:
            rows = []
            for page in self.iter_pages():
                if (page.provenance.release_fingerprint, page.provenance.installed_profile) != (self.provenance.release_fingerprint, self.provenance.installed_profile):
                    from ._errors import ProtocolError
                    raise ProtocolError("Metadata changed during page collection")
                rows.extend(page.items)
        frame = _frame(rows)
        frame.attrs["tcia_provenance"] = asdict(self.provenance)
        frame.attrs["all_pages_collected"] = all_pages or not self.has_more
        return frame
