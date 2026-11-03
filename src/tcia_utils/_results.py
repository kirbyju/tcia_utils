"""Small immutable result envelopes; records retain service field names."""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from copy import deepcopy
from typing import Any, Callable, Generic, Iterator, Mapping, TypeVar

from ._serialization import JsonExportMixin

T = TypeVar("T")


@dataclass(frozen=True)
class Provenance(JsonExportMixin):
    """Observed service identity retained with a query result.

    Attributes:
        base_url: Service root used for this observation.
        release_fingerprint: Identity of the installed TCIA metadata generation.
        release_tag: Reported release tag, or None.
        installed_profile: Installed metadata profile, such as research_detail.
        capabilities: Advertised component capabilities, including unknown values.
        observed_at_utc: UTC observation timestamp in ISO format.

    This is request-level provenance, not per-record source evidence or proof that the
    service is running the latest published generation."""
    base_url: str
    release_fingerprint: str
    release_tag: str | None
    installed_profile: str
    capabilities: Mapping[str, bool | None]
    observed_at_utc: str


@dataclass(frozen=True)
class Completeness(JsonExportMixin):
    """Scope-aware description of a result's completeness.

    Attributes:
        state: complete, partial, unknown or unavailable.
        reason: Optional explanation of the scope or limitation.

    complete concerns the requested operation; it does not establish complete source
    coverage. partial advertises further pages; unknown applies to bounded/unpaged
    routes. Invalid state labels raise ValueError."""
    state: str
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.state not in {"complete", "partial", "unknown", "unavailable"}:
            raise ValueError(f"Invalid completeness state: {self.state}")


@dataclass(frozen=True)
class ServiceStatus(JsonExportMixin):
    """Readiness and capabilities observed by TCIAClient.status().

    Attributes:
        readiness: Service readiness response.
        capabilities: Observed optional-component flags.
        release_fingerprint: Installed metadata generation identity.
        release_tag: Reported release tag, or None.
        installed_profile: Installed metadata profile.

    Status does not independently compare the deployed service with the latest release."""
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


def format_table(data, *, columns=None, missing_label="Not reported"):
    """Make a display table with readable missing values while preserving list cells.

    Args:
        data: A pandas DataFrame or record collection accepted by pandas.DataFrame.
        columns: Optional display column names/order; absent columns are shown as missing.
        missing_label: Display text for missing cells (default "Not reported"); must be a string.

    Returns:
        A new pandas.DataFrame for presentation. The original records and exports are unchanged.

    Raises:
        ImportError: The optional tables dependency is unavailable.
        TypeError: missing_label is not a string or data cannot form a table.

    Notes:
        Requires the tables extra and does not fetch metadata. Python lists stay lists;
        missing counts are not converted to zero."""
    if not isinstance(missing_label, str):
        raise TypeError("missing_label must be a string")
    frame = _frame(data)
    if columns is not None:
        frame = frame.reindex(columns=columns)
    values = frame.convert_dtypes().astype(object).to_numpy(copy=True)
    values[frame.isna().to_numpy()] = missing_label
    import pandas as pd

    result = pd.DataFrame(values, index=frame.index, columns=frame.columns, dtype=object)
    result.attrs = deepcopy(getattr(data, "attrs", {}))
    return result


@dataclass(frozen=True)
class Result(JsonExportMixin, Generic[T]):
    """Detail response with source fields and explicit evidence scope.

    Attributes:
        data: Route-specific dictionaries/lists or a ServiceStatus object.
        notes: Retained caveats or scope explanations.
        provenance: Observed service metadata identity.
        completeness: Scope-aware completeness; bounded routes remain unknown.

    Use data directly without pandas or export the full envelope with to_dict()/
    to_json(). to_dataframe(key=...) converts a named record list with the tables extra."""
    data: T
    notes: tuple[str, ...]
    provenance: Provenance
    completeness: Completeness

    def to_dataframe(self, key: str | None = None, *, columns=None) -> Any:
        """Convert one named record list from this result to a pandas table.

        Args:
            key: Key of the list inside .data. May be omitted when .data is a list or has exactly one list-valued field.
            columns: Optional column names/order, useful when the record list is empty.

        Returns:
            A new pandas.DataFrame containing the selected records.

        Raises:
            ImportError: The optional tables dependency is unavailable.
            ValueError: The result has no unambiguous record list.
            KeyError: The requested key is absent.

        Notes:
            Requires the tables extra. Does not fetch additional records. Use .to_dict() or
            .to_json() to retain the complete result envelope, notes and provenance."""
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
class Page(JsonExportMixin, Generic[T]):
    """One query page, retaining filters, paging state and metadata identity.

    Attributes:
        items: Tuple of record dictionaries on this page.
        count: Records returned on this page, not an archive-wide total.
        limit: Requested page size.
        has_more: Whether another page is advertised.
        truncated: Whether this response is limited to a partial page sequence.
        next_cursor: Opaque cursor for the next page, or None.
        query: Query scope retained by the client.
        response: Route response, including record lists and route-specific notes.
        provenance: Observed service metadata identity.
        completeness: Completeness for this page/query scope.

    next_page()/iter_pages() explicitly fetch more pages. to_dict()/to_json() export
    only this page, without fetching or serializing the internal paging callback."""
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
        """Fetch exactly the next page when the service supplied a cursor.

        Returns:
            The next Page, or None when this is the terminal page.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            Uses the original query and page size and rejects changed snapshot identities
            or repeated/invalid cursors. Does not refetch this page."""
        return self._fetch_next() if self._fetch_next is not None else None

    def iter_pages(self) -> Iterator["Page[T]"]:
        """Iterate over this page and all following pages lazily.

        Returns:
            Iterator of Page objects, beginning with this already-fetched page.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            Each later page requires a request. Filters, page size and snapshot identity
            are preserved; failures are raised rather than silently yielding a complete cohort.
            Use each page's .to_dict()/.to_json() to keep its paging metadata."""
        current: Page[T] | None = self
        while current is not None:
            yield current
            current = current.next_page()

    def to_dataframe(self, *, all_pages: bool = False) -> Any:
        """Convert this page, or explicitly collect every page, into a pandas table.

        Args:
            all_pages: False (default) converts only this page. True fetches subsequent pages with snapshot checks.

        Returns:
            A new pandas.DataFrame with tcia_provenance and all_pages_collected in .attrs.

        Raises:
            ImportError: The optional tables dependency is unavailable.
            ValueError: all_pages is not a boolean.
            ProtocolError: Metadata identity changes during page collection.
            TCIAError: A later page cannot be fetched.

        Notes:
            Requires the tables extra. This is the table helper that can collect a query;
            .to_dict() and .to_json() always export only the current page."""
        if not isinstance(all_pages, bool):
            raise ValueError("all_pages must be a boolean")
        rows = self.items
        if all_pages:
            rows = []
            for page in self.iter_pages():
                if (page.provenance.release_fingerprint, page.provenance.installed_profile) != (
                    self.provenance.release_fingerprint,
                    self.provenance.installed_profile,
                ):
                    from ._errors import ProtocolError

                    raise ProtocolError("Metadata changed during page collection")
                rows.extend(page.items)
        frame = _frame(rows)
        frame.attrs["tcia_provenance"] = asdict(self.provenance)
        frame.attrs["all_pages_collected"] = all_pages or not self.has_more
        return frame
