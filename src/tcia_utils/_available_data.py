"""Availability discovery; these methods fetch public metadata, never payloads."""
from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import quote

from ._errors import InvalidRequestError, ProtocolError


def _params(**values: Any) -> dict[str, Any]:
    output = {}
    for key, value in values.items():
        if value is None:
            continue
        if key in {"short_titles", "access_levels", "data_categories", "data_types",
                   "file_formats", "geometry_statuses", "data_domains", "modalities",
                   "media_kinds", "object_roles", "file_types", "route_systems"}:
            if isinstance(value, str) or not isinstance(value, Sequence) or not all(isinstance(v, str) and v for v in value):
                raise InvalidRequestError(f"{key} must be a sequence of nonempty strings")
            if not value:
                raise InvalidRequestError(f"{key} cannot be empty; omit it for no filter")
            value = list(value)
        if key in {"requires_annotations", "include_mixed", "has_drs_uri"} and not isinstance(value, bool):
            raise InvalidRequestError(f"{key} must be a boolean")
        output[key] = value
    return output


def _segment(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError("A nonempty dataset or participant identifier is required")
    return quote(value, safe="")


class AvailableDataMixin:
    def list_participant_assets(self, participant_key: str, *, access_levels=None,
                                data_categories=None, data_types=None, file_formats=None,
                                geometry_statuses=None, data_domains=None, limit=100, cursor=None):
        """Inspect linked asset summaries; public series detail remains with IDC."""
        def validate(body):
            if body.get("participant_key") != participant_key or any(
                row.get("participant_key") not in {None, participant_key} for row in body["assets"]
            ):
                raise ProtocolError("Asset response has inconsistent participant identity")

        return self._page(
            "GET", f"/v2/participants/{_segment(participant_key)}/assets", "assets",
            params=_params(access_levels=access_levels, data_categories=data_categories,
                           data_types=data_types, file_formats=file_formats,
                           geometry_statuses=geometry_statuses, data_domains=data_domains),
            limit=limit, cursor=cursor, max_limit=500, capability="participant_search",
            required_fields={"participant_key": str}, validate_response=validate,
        )

    def search_public_assets(self, *, short_titles=None, participant_id=None,
                             file_formats=None, modalities=None, geometry_statuses=None,
                             media_kinds=None, object_roles=None, requires_annotations=False,
                             limit=50, cursor=None):
        """Search public non-DICOM assets, including documented IDC-missing exceptions."""
        return self._page(
            "GET", "/v2/public-non-dicom/assets", "assets",
            params=_params(short_titles=short_titles, participant_id=participant_id,
                           file_formats=file_formats, modalities=modalities,
                           geometry_statuses=geometry_statuses, media_kinds=media_kinds,
                           object_roles=object_roles, requires_annotations=requires_annotations),
            limit=limit, cursor=cursor, max_limit=500, capability="public_non_dicom_detail",
        )

    def _available_bounded(self, path, key, *, params, limit, max_limit,
                           fields, capability=None, notes=()):
        if type(limit) is not int or not 1 <= limit <= max_limit:
            raise InvalidRequestError(f"limit must be between 1 and {max_limit}")
        result = self._result(
            "GET", path, params={**params, "limit": limit}, capability=capability,
            required_fields={key: list, "count": int, "limit": int, **fields},
            note_keys=notes, bounded=True,
        )
        body = result.data
        if body["count"] != len(body[key]) or body["count"] > limit or body["limit"] != limit:
            raise ProtocolError("Bounded response count or limit does not match its records/request")
        if not all(isinstance(record, dict) for record in body[key]):
            raise ProtocolError(f"Invalid {key} records")
        return result

    def search_controlled_datasets(self, *, modalities=None, file_types=None,
                                   requires_annotations=False, include_mixed=True, limit=25):
        """Discover publicly described controlled datasets without credentials."""
        return self._available_bounded(
            "/v2/controlled-access/datasets", "datasets",
            params=_params(modalities=modalities, file_types=file_types,
                           requires_annotations=requires_annotations, include_mixed=include_mixed),
            limit=limit, max_limit=200,
            fields={"controlled_access_policy_url": str, "note": str}, notes=("note",),
        )

    def list_controlled_files(self, short_title: str, *, route_systems=None, modalities=None,
                              file_types=None, body_part=None, participant_id=None, patient_id=None,
                              has_drs_uri=None, limit=50):
        """Inspect a bounded public inventory; returned locations do not grant access."""
        result = self._available_bounded(
            f"/v2/controlled-access/{_segment(short_title)}/files", "files",
            params=_params(route_systems=route_systems, modalities=modalities, file_types=file_types,
                           body_part=body_part, participant_id=participant_id, patient_id=patient_id,
                           has_drs_uri=has_drs_uri), limit=limit, max_limit=500,
            fields={"short_title": str, "controlled_access_policy_url": str, "warning": str},
            capability="controlled_access_detail", notes=("warning",),
        )
        if result.data["short_title"].casefold() != short_title.casefold() or any(
            not isinstance(row.get("short_title"), str) or row["short_title"].casefold() != short_title.casefold()
            for row in result.data["files"]
        ):
            raise ProtocolError("Controlled-file response has inconsistent dataset identity")
        return result

    def search_annotation_downloads(self, *, query=None, short_titles=None, modalities=None,
                                    access_levels=None, limit=25):
        """Inspect download-level annotation signals, without inferring source images."""
        return self._available_bounded(
            "/v2/dicom/annotation-downloads", "downloads",
            params=_params(query=query, short_titles=short_titles, modalities=modalities,
                           access_levels=access_levels), limit=limit, max_limit=200,
            fields={"routing_note": str, "scope": str, "public_dicom_annotation_detail": dict},
            notes=("routing_note", "scope"),
        )
