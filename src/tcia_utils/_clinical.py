"""Bounded clinical evidence access without client-side clinical inference."""

from __future__ import annotations

from collections.abc import Sequence
from urllib.parse import quote

from ._errors import InvalidRequestError, ProtocolError


def _clinical_path(short_title: str, suffix: str) -> str:
    if not isinstance(short_title, str) or not short_title.strip():
        raise InvalidRequestError("short_title must be a nonempty string")
    return f"/v2/clinical/{quote(short_title, safe='')}/{suffix}"


def _limit(value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 500:
        raise InvalidRequestError("limit must be between 1 and 500")


def _strings(value: Sequence[str], name: str) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise InvalidRequestError(f"{name} must be a sequence of nonempty strings")
    return list(value)


def _boolean(value: bool, name: str) -> None:
    if not isinstance(value, bool):
        raise InvalidRequestError(f"{name} must be a boolean")


def _check_bounded(data: dict, short_title: str, key: str, limit: int) -> None:
    if data["short_title"].casefold() != short_title.casefold():
        raise ProtocolError("Clinical response has inconsistent dataset")
    if data["limit"] != limit or data["count"] != len(data[key]):
        raise ProtocolError("Clinical response count or limit is inconsistent")
    if not data["dataset_types"] or not all(isinstance(item, str) and item for item in data["dataset_types"]):
        raise ProtocolError("Clinical response lacks dataset type identity")
    if not all(isinstance(item, dict) for item in data[key]):
        raise ProtocolError(f"Clinical response has invalid {key}")
    for record in data[key]:
        if not isinstance(record.get("subject_id"), str) or not record["subject_id"] or not isinstance(
            record.get("short_title"), str
        ) or record["short_title"].casefold() != short_title.casefold():
            raise ProtocolError("Clinical record has inconsistent dataset-scoped subject identity")


class ClinicalMixin:
    """Clinical methods composed into ``TCIAClient`` by the facade owner."""

    def list_clinical_subjects(
        self, short_title: str, *, subject_ids: Sequence[str] | None = None,
        include_clinical_only: bool = False, has_conflicts: bool | None = None,
        include_inferred: bool = True, limit: int = 50,
    ):
        _limit(limit)
        _boolean(include_clinical_only, "include_clinical_only")
        _boolean(include_inferred, "include_inferred")
        params = {"include_clinical_only": include_clinical_only,
                  "include_inferred": include_inferred, "limit": limit}
        if subject_ids is not None:
            params["subject_ids"] = _strings(subject_ids, "subject_ids")
        if has_conflicts is not None:
            _boolean(has_conflicts, "has_conflicts")
            params["has_conflicts"] = has_conflicts
        result = self._result(
            "GET", _clinical_path(short_title, "subjects"), params=params,
            capability="clinical_detail",
            required_fields={
                "short_title": str, "dataset_types": list, "subjects": list,
                "count": int, "limit": int, "includes_clinical_only_subjects": bool,
                "identity_scope": str,
            },
            note_keys=("identity_scope",), bounded=True,
        )
        _check_bounded(result.data, short_title, "subjects", limit)
        if result.data["includes_clinical_only_subjects"] is not include_clinical_only:
            raise ProtocolError("Clinical-only inclusion flag differs from request")
        return result

    def list_clinical_facts(
        self, short_title: str, *, subject_id: str | None = None,
        concepts: Sequence[str] | None = None,
        source_kinds: Sequence[str] | None = None,
        inferred: bool | None = None, limit: int = 100,
    ):
        _limit(limit)
        params = {"limit": limit}
        if subject_id is not None:
            if not isinstance(subject_id, str) or not subject_id.strip():
                raise InvalidRequestError("subject_id must be a nonempty string")
            params["subject_id"] = subject_id
        if concepts is not None:
            params["concepts"] = _strings(concepts, "concepts")
        if source_kinds is not None:
            params["source_kinds"] = _strings(source_kinds, "source_kinds")
        if inferred is not None:
            _boolean(inferred, "inferred")
            params["inferred"] = inferred
        result = self._result(
            "GET", _clinical_path(short_title, "facts"), params=params,
            capability="clinical_detail",
            required_fields={"short_title": str, "dataset_types": list,
                             "facts": list, "count": int, "limit": int, "note": str},
            note_keys=("note",), bounded=True,
        )
        _check_bounded(result.data, short_title, "facts", limit)
        return result

    def list_clinical_conflicts(
        self, short_title: str, *, subject_id: str | None = None,
        concepts: Sequence[str] | None = None, limit: int = 100,
    ):
        _limit(limit)
        params = {"limit": limit}
        if subject_id is not None:
            if not isinstance(subject_id, str) or not subject_id.strip():
                raise InvalidRequestError("subject_id must be a nonempty string")
            params["subject_id"] = subject_id
        if concepts is not None:
            params["concepts"] = _strings(concepts, "concepts")
        result = self._result(
            "GET", _clinical_path(short_title, "conflicts"), params=params,
            capability="clinical_detail",
            required_fields={"short_title": str, "dataset_types": list,
                             "conflicts": list, "count": int, "limit": int, "note": str},
            note_keys=("note",), bounded=True,
        )
        _check_bounded(result.data, short_title, "conflicts", limit)
        return result
