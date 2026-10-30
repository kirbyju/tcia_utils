"""Composition root and shared V2 response helpers."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

import httpx

from ._errors import InvalidRequestError, ProtocolError, ServiceUnavailableError
from ._results import Completeness, Page, Provenance, Result, ServiceStatus
from ._transport import Transport
from ._datasets import DatasetMethods
from ._citations import CitationMethods
from ._participants import ParticipantsMixin
from ._clinical import ClinicalMixin
from ._available_data import AvailableDataMixin
from ._selection import select, export_selection
from ._downloads import DownloadsMixin
from ._images import ParticipantImagesMixin
from ._research import ResearchMixin


_CAPABILITIES = (
    "participant_search", "public_non_dicom_detail", "controlled_access_detail",
    "clinical_detail", "datacite_metadata", "verified_analytical_publications",
)


def _field(body: Mapping[str, Any], name: str, kind: type | tuple[type, ...]) -> Any:
    if name not in body or not isinstance(body[name], kind) or (kind is int and isinstance(body[name], bool)):
        raise ProtocolError(f"Missing or invalid response field: {name}")
    return body[name]


def _identity_matches(first: Provenance, later: Provenance) -> None:
    if (first.release_fingerprint, first.installed_profile) != (later.release_fingerprint, later.installed_profile):
        raise ProtocolError("Service release or installed profile changed during pagination")


class TCIAClient(DatasetMethods, CitationMethods, ParticipantsMixin, ClinicalMixin, AvailableDataMixin, DownloadsMixin, ParticipantImagesMixin, ResearchMixin):
    """Reusable synchronous client for a configured TCIA V2 REST host."""

    select = staticmethod(select)
    export_selection = staticmethod(export_selection)

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float = 15.0,
        retries: int = 2,
        http_client: httpx.Client | None = None,
        pin_snapshot: bool = False,
    ) -> None:
        if not isinstance(pin_snapshot, bool):
            raise InvalidRequestError("pin_snapshot must be a boolean")
        self._pin_snapshot = pin_snapshot
        self._pinned_identity = None
        self._transport = Transport(base_url, timeout=timeout, retries=retries, client=http_client)

    def __enter__(self) -> "TCIAClient":
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._transport.close()
        # Release the lazily loaded upstream client and its in-memory indexes.
        self._imaging_client = None

    def _observe_status(self) -> tuple[ServiceStatus, Provenance]:
        ready = self._transport.request_json("GET", "/v2/ready")
        if _field(ready, "status", str) != "ready":
            raise ServiceUnavailableError("Service is not ready", code="service_not_ready")
        fingerprint = _field(ready, "release_fingerprint", str)
        profile = _field(ready, "installed_profile", str)
        if not fingerprint or not profile:
            raise ProtocolError("Readiness has no release identity")
        release_tag = ready.get("release_tag")
        if release_tag is not None and not isinstance(release_tag, str):
            raise ProtocolError("Invalid readiness release_tag")
        bundle = self._transport.request_json("GET", "/v2/bundle")
        if _field(bundle, "v2_bundle_manifest_exists", bool) is not True or _field(bundle, "v2_install_state_exists", bool) is not True:
            raise ServiceUnavailableError("V2 bundle or install receipt is unavailable", code="bundle_unavailable")
        caps = _field(bundle, "v2_capabilities", dict)
        observed_caps: dict[str, bool | None] = {}
        for name in _CAPABILITIES:
            value = caps.get(name)
            if value is not None and not isinstance(value, bool):
                raise ProtocolError(f"Invalid capability flag: {name}")
            observed_caps[name] = value
        install = _field(bundle, "v2_install", dict)
        if _field(install, "release_fingerprint", str) != fingerprint or _field(install, "installed_profile", str) != profile:
            raise ProtocolError("Readiness and bundle report different releases")
        manifest = _field(bundle, "v2_bundle", dict)
        if _field(manifest, "release_fingerprint", str) != fingerprint:
            raise ProtocolError("Readiness and bundle manifest report different releases")
        observed_at = datetime.now(timezone.utc).isoformat()
        provenance = Provenance(
            base_url=self._transport.base_url,
            release_fingerprint=fingerprint,
            release_tag=release_tag,
            installed_profile=profile,
            capabilities=observed_caps,
            observed_at_utc=observed_at,
        )
        if self._pin_snapshot:
            identity = (fingerprint, profile)
            if self._pinned_identity is not None and self._pinned_identity != identity:
                raise ProtocolError("Metadata changed during this client session; create a new client and restart the query")
            self._pinned_identity = identity
        return ServiceStatus(ready, observed_caps, fingerprint, release_tag, profile), provenance

    def status(self) -> Result[ServiceStatus]:
        status, provenance = self._observe_status()
        return Result(status, (), provenance, Completeness("complete"))

    @staticmethod
    def _require_capability(provenance: Provenance, capability: str | None) -> None:
        if capability is not None and provenance.capabilities.get(capability) is not True:
            raise ServiceUnavailableError(
                f"Service capability {capability} is unavailable",
                code="capability_unavailable", retryable=False,
            )

    def _result(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        capability: str | None = None,
        required_fields: Mapping[str, type | tuple[type, ...]],
        note_keys: tuple[str, ...] = (),
        bounded: bool = True,
    ) -> Result[dict[str, Any]]:
        _, provenance = self._observe_status()
        self._require_capability(provenance, capability)
        body = self._transport.request_json(method, path, params=params, json_body=json_body)
        for name, kind in required_fields.items():
            _field(body, name, kind)
        if "limit" in body:
            count = _field(body, "count", int)
            response_limit = _field(body, "limit", int)
            if count < 0 or response_limit < 1 or count > response_limit:
                raise ProtocolError("Bounded result has invalid count or limit")
            record_lists = [body[name] for name, kind in required_fields.items() if kind is list]
            if len(record_lists) == 1 and count != len(record_lists[0]):
                raise ProtocolError("Bounded result count does not match records")
        elif "count" in body and _field(body, "count", int) < 0:
            raise ProtocolError("Detail result has an invalid count")
        notes: list[str] = []
        for name in note_keys:
            value = body.get(name)
            if isinstance(value, str):
                notes.append(value)
            elif isinstance(value, list) and all(isinstance(item, str) for item in value):
                notes.extend(value)
        completeness = Completeness("unknown", "Route has no exhaustive cursor") if bounded else Completeness("complete")
        return Result(body, tuple(notes), provenance, completeness)

    def _page(
        self,
        method: str,
        path: str,
        item_key: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        limit: int,
        cursor: str | None = None,
        max_limit: int,
        capability: str | None = None,
        required_fields: Mapping[str, type | tuple[type, ...]] | None = None,
        validate_response: Callable[[dict[str, Any]], None] | None = None,
    ) -> Page[dict[str, Any]]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= max_limit:
            raise InvalidRequestError(f"limit must be between 1 and {max_limit}")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise InvalidRequestError("cursor must be a nonempty string")
        if (params is None) == (json_body is None):
            raise InvalidRequestError("Supply exactly one of params or json_body")
        filters = deepcopy(dict(params if params is not None else json_body))
        filters.pop("cursor", None)
        filters["limit"] = limit
        seen = {cursor} if cursor is not None else set()
        _, original = self._observe_status()
        self._require_capability(original, capability)

        def fetch(request_cursor: str | None, *, subsequent: bool) -> Page[dict[str, Any]]:
            if subsequent:
                _, before = self._observe_status()
                _identity_matches(original, before)
                self._require_capability(before, capability)
            values = deepcopy(filters)
            if request_cursor is not None:
                values["cursor"] = request_cursor
            body = self._transport.request_json(
                method, path,
                params=values if params is not None else None,
                json_body=values if json_body is not None else None,
            )
            items = _field(body, item_key, list)
            if not all(isinstance(item, dict) for item in items):
                raise ProtocolError(f"Invalid {item_key} records")
            for name, kind in (required_fields or {}).items():
                _field(body, name, kind)
            if validate_response is not None:
                validate_response(body)
            count = _field(body, "count", int)
            response_limit = _field(body, "limit", int)
            has_more = _field(body, "has_more", bool)
            truncated = _field(body, "truncated", bool)
            if "next_cursor" not in body:
                raise ProtocolError("Missing response field: next_cursor")
            next_cursor = body["next_cursor"]
            if count != len(items) or count < 0 or count > limit or response_limit != limit:
                raise ProtocolError("Page count or limit is inconsistent with the request")
            if truncated != has_more:
                raise ProtocolError("Page truncated flag conflicts with has_more")
            if has_more:
                if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
                    raise ProtocolError("Page has an invalid or repeated cursor")
            elif next_cursor is not None:
                raise ProtocolError("Terminal page unexpectedly has a cursor")
            if subsequent:
                _, after = self._observe_status()
                _identity_matches(original, after)
            completeness = Completeness("partial", "More pages available") if has_more else Completeness("complete")

            def next_fetch() -> Page[dict[str, Any]]:
                assert isinstance(next_cursor, str)
                if next_cursor in seen:
                    raise ProtocolError("Cursor has already been used in this iteration")
                seen.add(next_cursor)
                return fetch(next_cursor, subsequent=True)

            return Page(tuple(items), count, response_limit, has_more, truncated, next_cursor,
                        deepcopy(filters), body, original, completeness, next_fetch if has_more else None)

        return fetch(cursor, subsequent=False)
