"""Offline, route-specific selection exports for TCIA Utils v4.

This module has no REST, IDC, credential, or transfer dependency. The public
TCIAClient facade is wired by T09.
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass, is_dataclass
from io import StringIO
import ipaddress
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit


_PRIVATE_KEYS = {
    "apikey", "authorization", "awsaccesskeyid", "clientsecret", "credential",
    "password", "privatekey", "secret", "secretkey", "sessiontoken", "sig", "signature", "token",
    "accesskey",
    "xamzcredential", "xamzsecuritytoken", "xamzsignature", "xgoogcredential",
    "xgoogsignature", "xgoogtoken",
}
_ROUTE_COLUMNS = {"SeriesInstanceUID", "imageUrl", "drs_uri"}
_OPEN_ACCESS = {"open", "public"}
_CONTROLLED_SYSTEMS = {"ctdc", "general_commons"}


def _clean_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _private_host(value: str) -> bool:
    parsed = urlsplit(value)
    host = (parsed.hostname or "").casefold()
    if not host:
        return False
    if host == "localhost" or "." not in host or host.endswith((".local", ".internal", ".lan", ".corp")):
        return True
    try:
        return not ipaddress.ip_address(host).is_global
    except ValueError:
        return False


def _reject_secrets(value: Any, path: str = "input", *, allow_private_url: bool = False) -> None:
    """Reject unsafe input before it can reach either a manifest or companion."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} contains a non-string key")
            if _clean_key(key) in _PRIVATE_KEYS:
                raise ValueError(f"{path} contains credential-bearing field {key!r}")
            _reject_secrets(item, f"{path}.{key}", allow_private_url=allow_private_url)
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_secrets(item, f"{path}[{index}]", allow_private_url=allow_private_url)
    elif isinstance(value, str):
        if value.startswith(("/Users/", "/private/", "/home/", "~/", "file://")) or re.match(r"^[A-Za-z]:\\", value):
            raise ValueError(f"{path} contains a private filesystem path")
        if re.search(r"\bBearer\s+\S+|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----", value, re.I):
            raise ValueError(f"{path} contains credential material")
        if "://" in value:
            parsed = urlsplit(value)
            if parsed.username or parsed.password:
                raise ValueError(f"{path} contains URL user information")
            if _private_host(value) and not allow_private_url:
                raise ValueError(f"{path} contains a private service hostname")
            if parsed.query:
                raise ValueError(f"{path} contains a signed or credential-bearing URL query")


def _json_copy(value: Any, label: str, *, allow_private_url: bool = False) -> Any:
    try:
        copied = json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must contain JSON values") from exc
    _reject_secrets(copied, label, allow_private_url=allow_private_url)
    return copied


@dataclass(frozen=True)
class Selection:
    records: tuple[dict[str, Any], ...]
    release_identity: dict[str, Any]
    query_scope: dict[str, Any]
    coverage: dict[str, Any] | None
    include_source_images: bool = False


@dataclass(frozen=True)
class ExportReport:
    destination: Path
    files: tuple[Path, ...]
    exported: int
    deduplicated: int
    excluded: int
    unresolved: int


def select(
    records: Iterable[Mapping[str, Any]],
    *,
    release_identity: Any,
    query_scope: Mapping[str, Any],
    coverage: Mapping[str, Any] | None = None,
    include_source_images: bool = False,
) -> Selection:
    """Capture an explicit set of metadata rows for offline export.

    No identifier or route is inferred from an opaque asset ID or source URL.
    """
    if is_dataclass(release_identity) and not isinstance(release_identity, type):
        release_identity = asdict(release_identity)
    if not isinstance(release_identity, Mapping) or not release_identity.get("release_fingerprint"):
        raise ValueError("release_identity must include an observed release_fingerprint")
    if not isinstance(query_scope, Mapping):
        raise ValueError("query_scope must be a mapping")
    captured: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"record {index} must be a mapping")
        row = _json_copy(dict(record), f"records[{index}]")
        if not row.get("series_instance_uid") and row.get("SeriesInstanceUID"):
            row["series_instance_uid"] = row["SeriesInstanceUID"]
        if row.get("SeriesInstanceUID") and row.get("identifier_source") in {"idc-index", "reviewed"}:
            if not row.get("route_kind"):
                row["route_kind"] = "SeriesInstanceUID"
        if not row.get("record_grain"):
            default_grain = row.get("asset_granularity") or (
                "file" if row.get("drs_uri") or row.get("imageUrl") else
                "package" if row.get("package_url") and row.get("package_path") else
                "series" if row.get("series_instance_uid") and row.get("identifier_source") in {"idc-index", "reviewed"} else None
            )
            if default_grain:
                row["record_grain"] = default_grain
                row["record_grain_source"] = "source_asset_or_route_default"
        captured.append(row)
    if not isinstance(include_source_images, bool):
        raise ValueError("include_source_images must be a bool")
    return Selection(
        records=tuple(captured),
        release_identity=_json_copy(dict(release_identity), "release_identity", allow_private_url=True),
        query_scope=_json_copy(dict(query_scope), "query_scope"),
        coverage=_json_copy(dict(coverage), "coverage") if coverage is not None else None,
        include_source_images=include_source_images,
    )


def _text(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    return value if isinstance(value, str) else ""


def _classify(record: Mapping[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Return (manifest filename, column, exact value), or a reason."""
    dataset_type = _text(record, "dataset_type")
    short_title = _text(record, "short_title")
    if not dataset_type or not short_title:
        return None, None, "missing_dataset_identity"

    access = _text(record, "access_level").casefold()
    if access not in _OPEN_ACCESS | {"controlled"}:
        return None, None, "ambiguous_access"
    route_kind = _text(record, "route_kind")
    route_system = _text(record, "route_system").casefold()
    if access == "controlled" and (_text(record, "imageUrl") or _text(record, "package_url")):
        return None, None, "conflicting_access_routes"
    if access in _OPEN_ACCESS and _text(record, "drs_uri"):
        return None, None, "conflicting_access_routes"
    if access in _OPEN_ACCESS and _text(record, "imageUrl") and _text(record, "series_instance_uid"):
        return None, None, "multiple_public_routes"
    if access in _OPEN_ACCESS and _text(record, "package_url") and (
        _text(record, "imageUrl") or _text(record, "series_instance_uid")
    ):
        return None, None, "multiple_public_routes"
    if route_system == "aspera" and route_kind not in {"", "aspera"}:
        return None, None, "ambiguous_route"

    if route_kind == "aspera" or (route_system == "aspera" and record.get("package_url")):
        if access not in _OPEN_ACCESS:
            return None, None, "aspera_requires_open_access"
        package_url, package_path = _text(record, "package_url"), _text(record, "package_path")
        if not package_url or not package_path:
            return None, None, "missing_aspera_package_or_path"
        if any(ord(char) < 32 for char in package_url + package_path) or package_path.startswith("-"):
            return None, None, "invalid_aspera_package_or_path"
        parsed = urlsplit(package_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
            return None, None, "invalid_aspera_package_url"
        return "aspera-handoff.json", "aspera", json.dumps(
            {"package_url": package_url, "package_path": package_path}, sort_keys=True
        )

    if access == "controlled":
        if route_system not in _CONTROLLED_SYSTEMS:
            return None, None, "unsupported_controlled_route"
        drs_uri = _text(record, "drs_uri")
        if not drs_uri or urlsplit(drs_uri).scheme != "drs" or not urlsplit(drs_uri).netloc or any(ord(char) < 32 for char in drs_uri):
            return None, None, "missing_or_invalid_drs_uri"
        if route_kind and route_kind != "drs_uri":
            return None, None, "ambiguous_route"
        return f"controlled-{route_system}-drs-uri.csv", "drs_uri", drs_uri

    if route_kind == "SeriesInstanceUID":
        uid = _text(record, "series_instance_uid")
        if not uid or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", uid):
            return None, None, "missing_or_invalid_series_uid"
        if _text(record, "identifier_source") not in {"idc-index", "reviewed"}:
            return None, None, "unreviewed_series_identity"
        if _text(record, "record_grain") != "series":
            return None, None, "series_grain_required"
        return "public-series.csv", "SeriesInstanceUID", uid

    if route_kind == "imageUrl":
        image_url = _text(record, "imageUrl")
        if any(ord(char) < 32 for char in image_url):
            return None, None, "missing_or_invalid_image_url"
        parsed = urlsplit(image_url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
            return None, None, "missing_or_invalid_image_url"
        return "public-image-url.csv", "imageUrl", image_url

    return None, None, "no_supported_exact_route"


def _csv_bytes(header: str, values: list[str]) -> bytes:
    output = StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([header])
    for value in values:
        writer.writerow([value])
    return output.getvalue().encode("utf-8")


def export_selection(
    selection: Selection,
    destination: str | os.PathLike[str],
    *,
    overwrite: bool = False,
) -> ExportReport:
    """Write safe route manifests and an accounting companion to a directory."""
    if not isinstance(selection, Selection):
        raise TypeError("selection must be a Selection")
    target = Path(destination)
    if target == Path("") or target.resolve() == Path(target.anchor):
        raise ValueError("destination must be a specific directory")
    _reject_secrets(selection.release_identity, "release_identity", allow_private_url=True)
    _reject_secrets(selection.query_scope, "query_scope")
    _reject_secrets(selection.coverage, "coverage")

    groups: dict[tuple[str, str], list[str]] = {}
    aspera: list[dict[str, str]] = []
    accounted: list[dict[str, Any]] = []
    datasets: dict[tuple[str, str], dict[str, Any]] = {}
    seen: set[tuple[str, str, str]] = set()
    known_bytes = 0
    unknown_sizes = 0
    for index, record in enumerate(selection.records):
        _reject_secrets(record, f"records[{index}]")
        identity = (_text(record, "dataset_type"), _text(record, "short_title"))
        if all(identity):
            context = datasets.setdefault(identity, {
                "dataset_type": identity[0], "short_title": identity[1],
                "dois": [], "license_labels": [], "access_levels": [],
            })
            for field, value in (("dois", _text(record, "doi")),
                                 ("license_labels", _text(record, "license_label")),
                                 ("access_levels", _text(record, "access_level"))):
                if value and value not in context[field]:
                    context[field].append(value)
        if _text(record, "object_role") == "source_image" and not selection.include_source_images:
            accounted.append({"input_index": index, "disposition": "excluded",
                              "reason": "source_images_not_selected", "record": record})
            continue
        filename, column, value = _classify(record)
        if filename is None:
            accounted.append({"input_index": index, "disposition": "unresolved",
                              "reason": value, "record": record})
            continue
        assert column is not None and value is not None
        key = (filename, column, value)
        if key in seen:
            accounted.append({"input_index": index, "disposition": "deduplicated",
                              "reason": "same_manifest_identifier", "record": record})
            continue
        seen.add(key)
        if column == "aspera":
            aspera.append({**json.loads(value), "dataset_type": identity[0], "short_title": identity[1]})
        else:
            assert column in _ROUTE_COLUMNS
            groups.setdefault((filename, column), []).append(value)
        size = next((record.get(field) for field in ("file_size_bytes", "known_size_bytes", "size_bytes")
                     if isinstance(record.get(field), int) and not isinstance(record.get(field), bool)
                     and record.get(field) >= 0), None)
        if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
            known_bytes += size
        else:
            unknown_sizes += 1
        accounted.append({"input_index": index, "disposition": "exported",
                          "file": filename, "record": record})

    counts = {name: sum(item["disposition"] == name for item in accounted)
              for name in ("exported", "deduplicated", "excluded", "unresolved")}
    assert sum(counts.values()) == len(selection.records)
    public_release = {key: selection.release_identity.get(key) for key in (
        "release_fingerprint", "release_tag", "installed_profile", "observed_at_utc", "capabilities"
    ) if key in selection.release_identity}
    base_url = selection.release_identity.get("base_url")
    if isinstance(base_url, str) and urlsplit(base_url).scheme == "https" and not _private_host(base_url):
        public_release["base_url"] = base_url
    completeness = selection.query_scope.get("completeness", "unknown")
    if completeness not in {"complete", "partial", "unknown", "unavailable"}:
        raise ValueError("query_scope.completeness must be complete, partial, unknown, or unavailable")
    basis = selection.query_scope.get("completeness_basis")
    if completeness == "complete" and basis not in {"terminal_cursor_guarded", "explicit_source_complete"}:
        raise ValueError("complete selections require an explicit completeness_basis")
    companion = {
        "schema_version": 1,
        "release_identity": public_release,
        "query_scope": selection.query_scope,
        "coverage": selection.coverage,
        "completeness": completeness,
        "completeness_source": "caller_supplied" if "completeness" in selection.query_scope else "default_unknown",
        "completeness_basis": basis,
        "include_source_images": selection.include_source_images,
        "dataset_context": list(datasets.values()),
        "counts": {"input": len(selection.records), **counts, "known_size_bytes": known_bytes,
                   "exported_unknown_size": unknown_sizes},
        "accounting": accounted,
        "notes": [
            "Captured service identity is observed provenance, not a per-row server stamp.",
            "Series identifiers supplied by callers require a separate IDC coverage check.",
            "Controlled manifests are metadata handoffs; authorization is separate.",
        ],
    }
    _reject_secrets(companion, "companion")
    payloads = {(name): _csv_bytes(column, values) for (name, column), values in groups.items()}
    if aspera:
        payloads["aspera-handoff.json"] = (json.dumps({"format": "ascli_faspex5_package_paths_v1",
            "items": aspera}, indent=2, sort_keys=True) + "\n").encode("utf-8")
    payloads["selection.json"] = (json.dumps(companion, indent=2, sort_keys=True) + "\n").encode("utf-8")

    if target.exists():
        if not target.is_dir():
            raise ValueError("destination exists and is not a directory")
        existing = {path.name for path in target.iterdir()}
        if existing and not overwrite:
            raise FileExistsError(f"destination is not empty: {target}")
        if existing - payloads.keys():
            raise FileExistsError("destination contains files outside this export; choose a new directory")
    else:
        target.mkdir(parents=True)
    written: list[Path] = []
    for name, content in sorted(payloads.items()):
        with tempfile.NamedTemporaryFile(dir=target, prefix=".tcia-v4-", delete=False) as temporary:
            temporary.write(content)
            temp_path = Path(temporary.name)
        final = target / name
        temp_path.replace(final)
        written.append(final)
    return ExportReport(target, tuple(written), **counts)
