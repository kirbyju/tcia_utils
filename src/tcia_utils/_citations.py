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
    """Mixed into TCIAClient by T09; does not establish dataset publication."""

    def search_citations(
        self, *, query: str | None = None, dois: Sequence[str] | None = None,
        short_titles: Sequence[str] | None = None, sort_by: str = "doi",
        limit: int = 25, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        if sort_by not in _SORT_COLUMNS:
            raise InvalidRequestError("sort_by must be doi, citation_count, view_count, download_count, or publication_year")
        params = _filters(query=query, dois=dois, short_titles=short_titles, sort_by=sort_by)
        return self._page(
            "GET", "/v2/datacite/dois", "dois", params=params,
            limit=limit, cursor=cursor, max_limit=200, capability="datacite_metadata",
            required_fields={"metrics_note": str},
        )

    def get_citation(self, doi: str, *, style: str = "apa") -> Result[dict[str, Any]]:
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
