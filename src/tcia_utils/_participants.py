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
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not all(
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
    """Participant methods composed into ``TCIAClient`` by the facade owner."""

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
