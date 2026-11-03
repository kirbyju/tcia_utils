"""DataCite enrichment and verified analytical-use methods."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from ._datasets import _filters, _segment
from ._errors import InvalidRequestError, ProtocolError
from ._results import Page, Result


_CITATION_STYLES = {"apa", "vancouver", "chicago-author-date", "bibtex", "ris"}
_SORT_COLUMNS = {"doi", "citation_count", "view_count", "download_count", "publication_year"}


class CitationMethods:
    """DataCite citation enrichment and TCIA-verified analytical-use methods."""

    def search_citations(
        self, *, query: str | None = None, dois: Sequence[str] | None = None,
        short_titles: Sequence[str] | None = None, sort_by: str = "doi",
        limit: int = 25, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """Search DataCite DOI metadata and source-specific metrics.

        Args:
            query: Optional free-text filter.
            dois: Optional nonempty sequence of DOI strings.
            short_titles: Optional nonempty sequence of TCIA dataset short titles.
            sort_by: "doi" (default), "citation_count", "view_count", "download_count", or "publication_year".
            limit: Maximum records on this page, not an archive-wide total; see the bound below.
            cursor: Opaque next_cursor from the same query, or None for the first page.

        Returns:
            Page of dictionaries. Use .items for this page, .iter_pages() for further pages,
            .to_dict()/.to_json() for metadata-preserving exports, or .to_dataframe() with the tables extra.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            limit is 1–200 (default 25). DataCite download events are not TCIA image
            download counts. Citation metadata enriches a dataset; it does not establish TCIA publication."""
        if sort_by not in _SORT_COLUMNS:
            raise InvalidRequestError("sort_by must be doi, citation_count, view_count, download_count, or publication_year")
        params = _filters(query=query, dois=dois, short_titles=short_titles, sort_by=sort_by)
        return self._page(
            "GET", "/v2/datacite/dois", "dois", params=params,
            limit=limit, cursor=cursor, max_limit=200, capability="datacite_metadata",
            required_fields={"metrics_note": str},
        )

    def get_citation(self, doi: str, *, style: str = "apa") -> Result[dict[str, Any]]:
        """Retrieve a dataset DOI record and a formatted citation.

        Args:
            doi: Nonblank DOI string.
            style: "apa" (default), "vancouver", "chicago-author-date", "bibtex", or "ris".

        Returns:
            Result with .data["record"] containing metadata, citation_style and formatted_citation,
            plus provenance_note. Convert using .to_dict()/.to_json() without pandas.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            Use the Collection or Analysis Result page for TCIA publication identity and
            acknowledgement requirements; DataCite is the citation metadata source."""
        if not isinstance(style, str) or style not in _CITATION_STYLES:
            raise InvalidRequestError("style must be apa, vancouver, chicago-author-date, bibtex, or ris")
        if not isinstance(doi, str) or not doi.strip():
            raise InvalidRequestError("doi must be a nonempty string")
        result = self._result(
            "GET", f"/v2/datacite/dois/{quote(doi.strip(), safe='/')}",
            params={"citation_style": style}, capability="datacite_metadata",
            required_fields={"record": dict, "provenance_note": str},
            note_keys=("provenance_note",), bounded=False,
        )
        record = result.data["record"]
        if (not isinstance(record.get("doi"), str) or
            record.get("citation_style") != style or
            not isinstance(record.get("formatted_citation"), str) or
            not isinstance(record.get("metadata"), dict)):
            raise ProtocolError("Citation record is missing required citation fields")
        return result

    def search_publications(
        self, *, query: str | None = None, dataset_dois: Sequence[str] | None = None,
        short_titles: Sequence[str] | None = None, from_year: int | None = None,
        to_year: int | None = None, limit: int = 25, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """Find publications verified to have analyzed TCIA data.

        Args:
            query: Optional free-text filter.
            dataset_dois: Optional nonempty sequence of dataset DOIs.
            short_titles: Optional nonempty sequence of dataset short titles.
            from_year: Optional inclusive first publication year.
            to_year: Optional inclusive last publication year.
            limit: Maximum records on this page, not an archive-wide total; see the bound below.
            cursor: Opaque next_cursor from the same query, or None for the first page.

        Returns:
            Page of dictionaries. Use .items for this page, .iter_pages() for further pages,
            .to_dict()/.to_json() for metadata-preserving exports, or .to_dataframe() with the tables extra.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            limit is 1–200 (default 25). Results use TCIA-maintained verified analytical use,
            not all mentions or all papers citing the DOI. Preserve the verification note."""
        params = _filters(
            query=query, dataset_dois=dataset_dois, short_titles=short_titles,
            from_year=from_year, to_year=to_year,
        )
        return self._page(
            "GET", "/v2/publications/verified-use", "publications", params=params,
            limit=limit, cursor=cursor, max_limit=200,
            capability="verified_analytical_publications", required_fields={"verification_note": str},
        )

    def get_impact(self, short_title: str, *, publication_limit: int = 25) -> Result[dict[str, Any]]:
        """Read verified analytical use alongside DataCite event metrics.

        Args:
            short_title: Published dataset short title.
            publication_limit: Maximum verified publications included, 1–200 (default 25).

        Returns:
            Result with .data["impacts"], verified_publications, publication_limit and notes;
            completeness is unknown for the bounded publication list.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            Keep each metric attributed to its source. DataCite views/downloads are not
            measurements of TCIA file transfers, and a bounded publication list is not exhaustive."""
        if not isinstance(publication_limit, int) or isinstance(publication_limit, bool) or not 1 <= publication_limit <= 200:
            raise InvalidRequestError("publication_limit must be between 1 and 200")
        _, before = self._observe_status()
        self._require_capability(before, "datacite_metadata")
        self._require_capability(before, "verified_analytical_publications")
        result = self._result(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/impact",
            params={"publication_limit": publication_limit},
            capability="verified_analytical_publications",
            required_fields={"impacts": list, "verified_publications": list,
                             "publication_limit": int, "notes": list},
            note_keys=("notes",), bounded=True,
        )
        # Impact includes both DataCite metrics and TCIA-maintained publication use.
        if (before.release_fingerprint, before.installed_profile) != (
            result.provenance.release_fingerprint, result.provenance.installed_profile
        ):
            raise ProtocolError("Service release changed before impact request")
        if result.data["publication_limit"] != publication_limit:
            raise ProtocolError("Service changed publication_limit")
        return result
