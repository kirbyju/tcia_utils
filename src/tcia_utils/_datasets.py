"""Dataset discovery, access, and download methods for the v4 facade."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from ._errors import InvalidRequestError, ProtocolError
from ._results import Page, Result


def _segment(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError(f"{name} must be a nonempty string")
    return quote(value.strip(), safe="")


def _filters(**values: Any) -> dict[str, Any]:
    """Copy supplied filters without treating a string as a list of characters."""
    result: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if key in {"has_external_clinical_resource", "requires_annotations"} and not isinstance(value, bool):
            raise InvalidRequestError(f"{key} must be a boolean")
        if key.endswith("s") and key not in {"has_external_clinical_resource", "requires_annotations"}:
            if (isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or
                not value or not all(isinstance(item, str) and item.strip() for item in value)):
                raise InvalidRequestError(f"{key} must be a nonempty sequence of nonblank strings")
            value = list(value)
        result[key] = value
    return result


class DatasetMethods:
    """Dataset discovery and published access metadata exposed by TCIAClient."""

    def search_datasets(
        self, *, query: str | None = None, short_titles: Sequence[str] | None = None,
        dataset_type: str = "both", access_levels: Sequence[str] | None = None,
        modalities: Sequence[str] | None = None, data_types: Sequence[str] | None = None,
        download_types: Sequence[str] | None = None, file_types: Sequence[str] | None = None,
        cancer_types: Sequence[str] | None = None, cancer_locations: Sequence[str] | None = None,
        species: Sequence[str] | None = None, programs: Sequence[str] | None = None,
        external_resources: Sequence[str] | None = None,
        has_external_clinical_resource: bool | None = None, doi: str | None = None,
        limit: int = 25, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """Find published Collections and Analysis Results by dataset-level metadata.

        Args:
            query: Optional free-text search; this does not search participant clinical facts.
            short_titles: Nonempty sequence of exact dataset short titles, or None.
            dataset_type: "collection", "analysis_result", or "both" (default).
            access_levels: Nonempty sequence of published labels, or None to omit this filter. Mixed datasets may include several access levels.
            modalities: Nonempty sequence of published labels, or None to omit this filter.
            data_types: Nonempty sequence of published labels, or None to omit this filter.
            download_types: Nonempty sequence of download-category labels, or None.
            file_types: Nonempty sequence of published labels, or None to omit this filter.
            cancer_types: Nonempty sequence of published labels, or None to omit this filter.
            cancer_locations: Nonempty sequence of published labels, or None to omit this filter.
            species: Nonempty sequence of published labels, or None to omit this filter.
            programs: Nonempty sequence of published labels, or None to omit this filter.
            external_resources: Nonempty sequence of external-resource labels, or None.
            has_external_clinical_resource: True/False to require/exclude a linked external clinical resource; None for either.
            doi: Optional dataset DOI filter.
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
            limit is 1–200 (default 25). Labels describe datasets, not every participant.
            Keep dataset_type and short_title together when following a result.

        Example:
            page = tcia.search_datasets(modalities=["CT"], cancer_locations=["Lung"])
            print(page.to_json(indent=2))"""
        if dataset_type not in {"both", "collection", "analysis_result"}:
            raise InvalidRequestError("dataset_type must be both, collection, or analysis_result")
        body = _filters(
            query=query, short_titles=short_titles, dataset_type=dataset_type,
            access_levels=access_levels, modalities=modalities, data_types=data_types,
            download_types=download_types, file_types=file_types, cancer_types=cancer_types,
            cancer_locations=cancer_locations, species=species, programs=programs,
            external_resources=external_resources,
            has_external_clinical_resource=has_external_clinical_resource, doi=doi,
        )
        return self._page("POST", "/v2/datasets/search", "datasets",
                          json_body=body, limit=limit, cursor=cursor, max_limit=200,
                          required_fields={"note": str})

    def get_dataset(self, short_title: str) -> Result[dict[str, Any]]:
        """Read the description and current download inventory for a dataset.

        Args:
            short_title: Published dataset short title, such as "CMB-GEC".

        Returns:
            Result whose .data contains datasets, current_downloads, related_analysis_results
            and caveats, with observed provenance and bounded/unknown completeness.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            A short title may resolve to more than one dataset type. Inspect datasets before
            choosing a record. Use plan_downloads() to inspect software requirements and Python
            support; this method does not resolve files or download anything."""
        result = self._result(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}",
            required_fields={"datasets": list, "current_downloads": list,
                             "related_analysis_results": list, "caveats": list},
            note_keys=("caveats",), bounded=True,
        )
        if not result.data["datasets"] or not all(isinstance(item, dict) for item in result.data["datasets"]):
            raise ProtocolError("Dataset detail has no valid datasets")
        return result

    def get_access(self, short_title: str) -> Result[dict[str, Any]]:
        """Summarize published access terms and download groups.

        Args:
            short_title: Published dataset short title.

        Returns:
            Result with .data["dataset"], download_groups, controlled_access_policy_url and notes.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            This route returns one dataset_type; use get_dataset() to review ambiguous titles.
            Open access is subject to the license. Controlled metadata or a manifest does not
            grant authorization to retrieve files."""
        result = self._result(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/access",
            required_fields={"dataset": dict, "download_groups": dict,
                             "controlled_access_policy_url": str, "notes": list},
            note_keys=("notes",), bounded=True,
        )
        if not all(isinstance(group, str) and isinstance(rows, list) and
                   all(isinstance(row, dict) for row in rows)
                   for group, rows in result.data["download_groups"].items()):
            raise ProtocolError("Access download_groups are malformed")
        return Result(
            result.data,
            result.notes + ("Access summary describes the returned dataset_type; use get_dataset() to review all matches for a short title.",),
            result.provenance,
            result.completeness,
        )

    def list_downloads(
        self, short_title: str, *, access_levels: Sequence[str] | None = None,
        modalities: Sequence[str] | None = None, data_types: Sequence[str] | None = None,
        download_types: Sequence[str] | None = None, file_types: Sequence[str] | None = None,
        requires_annotations: bool = False, limit: int = 25, cursor: str | None = None,
    ) -> Page[dict[str, Any]]:
        """List current published download groups for one dataset.

        Args:
            short_title: Published dataset short title.
            access_levels: Nonempty sequence of published labels, or None to omit this filter.
            modalities: Nonempty sequence of published labels, or None to omit this filter.
            data_types: Nonempty sequence of published labels, or None to omit this filter.
            download_types: Nonempty sequence of download-category labels, or None.
            file_types: Nonempty sequence of published labels, or None to omit this filter.
            requires_annotations: True to require download-level annotation signals (default False).
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
            limit is 1–200 (default 25). Rows describe groups, not an enumeration of their
            image files. Use plan_downloads() for software requirements and Python route support.
            Annotation signals do not prove a segmentation-to-source-image relationship."""
        params = _filters(
            access_levels=access_levels, modalities=modalities, data_types=data_types,
            download_types=download_types, file_types=file_types,
            requires_annotations=requires_annotations,
        )
        return self._page(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/downloads",
            "downloads", params=params, limit=limit, cursor=cursor, max_limit=200,
            required_fields={"short_title": str},
        )
