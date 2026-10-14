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
    """Mixed into TCIAClient by T09; relies only on T04 helpers."""

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
                          json_body=body, limit=limit, cursor=cursor, max_limit=200)

    def get_dataset(self, short_title: str) -> Result[dict[str, Any]]:
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
        """Return the route's one named dataset; inspect get_dataset for ambiguous titles."""
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
        params = _filters(
            access_levels=access_levels, modalities=modalities, data_types=data_types,
            download_types=download_types, file_types=file_types,
            requires_annotations=requires_annotations,
        )
        return self._page(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/downloads",
            "downloads", params=params, limit=limit, cursor=cursor, max_limit=200,
        )
