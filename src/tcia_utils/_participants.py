"""Dataset-scoped participant discovery over the shared V2 service."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from urllib.parse import quote

from ._errors import InvalidRequestError, ProtocolError


_DATASET_TYPE_WIRE = {
    "both": "both",
    "collection": "collection",
    "analysis_result": "analysis result",
}


def _dataset_type(value: str, *, allow_both: bool) -> str:
    normalized = value.casefold() if isinstance(value, str) else ""
    if normalized not in _DATASET_TYPE_WIRE or (normalized == "both" and not allow_both):
        choices = "both, collection, analysis_result" if allow_both else "collection, analysis_result"
        raise InvalidRequestError(f"dataset_type must be one of: {choices}")
    return _DATASET_TYPE_WIRE[normalized]


def _segment(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError(f"{name} must be a nonempty string")
    return quote(value, safe="")


def _strings(value: Sequence[str], name: str) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise InvalidRequestError(f"{name} must be a sequence of nonempty strings")
    return list(value)


def _checked_participant_page(page):
    if not isinstance(page.response.get("identity_scope"), str) or not isinstance(
        page.response.get("public_dicom_detail_route"), str
    ):
        raise ProtocolError("Participant search lacks identity or DICOM route scope")
    for participant in page.items:
        if not all(isinstance(participant.get(key), str) and participant[key]
                   for key in ("participant_key", "dataset_type", "short_title", "display_participant_id")):
            raise ProtocolError("Participant record lacks dataset-scoped identity")
    if page.has_more:
        def next_checked():
            following = page.next_page()
            if following is None:
                raise ProtocolError("Participant search lost its next page")
            return _checked_participant_page(following)

        return replace(page, _fetch_next=next_checked)
    return page


class ParticipantsMixin:
    """Dataset-scoped participant discovery and linking coverage exposed by TCIAClient."""

    def search_participants(
        self,
        *,
        query: str | None = None,
        short_titles: Sequence[str] | None = None,
        dataset_type: str = "both",
        access_levels: Sequence[str] | None = None,
        data_categories: Sequence[str] | None = None,
        data_types: Sequence[str] | None = None,
        file_formats: Sequence[str] | None = None,
        geometry_statuses: Sequence[str] | None = None,
        modalities: Sequence[str] | None = None,
        limit: int = 25,
        cursor: str | None = None,
    ):
        """Find dataset-scoped participants by available data.

        Args:
            query: Optional participant free-text search; use clinical methods for diagnosis evidence.
            short_titles: Nonempty sequence of dataset short titles, or None.
            dataset_type: "collection", "analysis_result", or "both" (default).
            access_levels: Nonempty sequence of published labels, or None to omit this filter.
            data_categories: Nonempty sequence of published labels, or None to omit this filter.
            data_types: Nonempty sequence of published labels, or None to omit this filter.
            file_formats: Nonempty sequence of published labels, or None to omit this filter.
            geometry_statuses: Nonempty sequence of published labels, or None to omit this filter.
            modalities: Nonempty sequence of published labels, or None to omit this filter.
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
            limit is 1–500 (default 25). Carry participant_key, dataset_type and short_title
            into subsequent calls. Identical display IDs in different datasets do not establish
            the same person. A missing indexed record is not proof that source data are absent."""
        if query is not None and not isinstance(query, str):
            raise InvalidRequestError("query must be a string")
        body = {"dataset_type": _dataset_type(dataset_type, allow_both=True)}
        for name, value in (
            ("query", query), ("short_titles", short_titles),
            ("access_levels", access_levels), ("data_categories", data_categories),
            ("data_types", data_types), ("file_formats", file_formats),
            ("geometry_statuses", geometry_statuses), ("modalities", modalities),
        ):
            if value is not None:
                body[name] = value if name == "query" else _strings(value, name)
        page = self._page(
            "POST", "/v2/participants/search", "participants",
            json_body=body, limit=limit, cursor=cursor, max_limit=500,
            capability="participant_search",
        )
        return _checked_participant_page(page)

    def get_participant(self, participant_key: str):
        """Read one canonical participant and its retained source identifiers.

        Args:
            participant_key: Exact dataset-scoped key from search_participants(); not a display patient ID.

        Returns:
            Result with .data["participant"], identifiers, count and identity_scope.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            The response describes an indexed identity, not complete clinical history or all
            image files. Use list_participant_assets() or plan_participant_images() for available images."""
        result = self._result(
            "GET", f"/v2/participants/{_segment(participant_key, 'participant_key')}",
            capability="participant_search",
            required_fields={
                "participant": dict, "identifiers": list,
                "count": int, "identity_scope": str,
            },
            note_keys=("identity_scope",), bounded=False,
        )
        if result.data["count"] != 1 or not all(isinstance(item, dict) for item in result.data["identifiers"]):
            raise ProtocolError("Participant detail has invalid count or identifiers")
        participant = result.data["participant"]
        if participant.get("participant_key") != participant_key or not all(
            isinstance(participant.get(key), str) and participant[key]
            for key in ("dataset_type", "short_title", "display_participant_id")
        ):
            raise ProtocolError("Participant detail identity is inconsistent")
        if any(item.get("participant_key") != participant_key for item in result.data["identifiers"]):
            raise ProtocolError("Participant identifier belongs to a different participant")
        return result

    def get_coverage(self, short_title: str, *, dataset_type: str | None = None):
        """Inspect participant linking coverage for a dataset.

        Args:
            short_title: Published dataset short title.
            dataset_type: Optional "collection" or "analysis_result"; omit for the route default.

        Returns:
            Result with participant_count, coverage_complete, unlinked_dataset_assets,
            participant_link_issues and source summaries in .data.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            coverage_complete refers to indexed participant linking, not complete clinical
            coverage, complete image inventories, or an archive-wide count of unique people."""
        wire_type = _dataset_type(dataset_type, allow_both=False) if dataset_type is not None else None
        params = {"dataset_type": wire_type} if wire_type is not None else {}
        result = self._result(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/participant-coverage",
            params=params, capability="participant_search",
            required_fields={
                "short_title": str, "dataset_type": (str, type(None)),
                "participant_count": int,
                "unlinked_dataset_assets": list, "participant_link_issues": list,
                "sources": list, "coverage_complete": bool,
            },
            bounded=False,
        )
        data = result.data
        if data["short_title"].casefold() != short_title.casefold() or data["participant_count"] < 0:
            raise ProtocolError("Participant coverage has inconsistent dataset or count")
        if wire_type is not None and (
            not isinstance(data["dataset_type"], str)
            or data["dataset_type"].casefold() != wire_type
        ):
            raise ProtocolError("Participant coverage has inconsistent dataset type")
        for name in ("unlinked_dataset_assets", "participant_link_issues", "sources"):
            if not all(isinstance(item, dict) for item in data[name]):
                raise ProtocolError(f"Participant coverage has invalid {name}")
        return result
