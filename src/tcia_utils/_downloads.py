"""Small download adapters over supported clients and official manifest routes.

No transfer or optional dependency is initialized at import. Credentials are
confined to the recognized controlled-data service; signed URLs stay in memory.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urljoin, quote
import csv
import hashlib
import importlib.util
import io
import ipaddress
import json
import os
import re
import tempfile
import warnings

import httpx
from ._errors import (
    TCIAError,
    InvalidRequestError,
    ProtocolError,
    NotFoundError,
    ServiceUnavailableError,
)
from ._results import _frame
from ._research import records
from ._datasets import _segment, _filters

_OPEN = {"open", "public", "open_noncommercial"}
_DRS_HOSTS = {"nci-crdc.datacommons.io"}
_MANIFEST_HOSTS = {
    "www.cancerimagingarchive.net",
    "cancerimagingarchive.net",
    "wiki.cancerimagingarchive.net",
}


class DownloadError(TCIAError):
    """A transfer did not complete; the report retains all selected entries."""

    def __init__(self, message, report=None):
        super().__init__(message, code="download_failed")
        self.report = report


@dataclass(frozen=True)
class DownloadPlan:
    rows: tuple[dict, ...]
    provenance: object
    scope: dict = field(default_factory=dict)
    software: dict = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def to_dataframe(self):
        rows = [
            {
                **row,
                "download_requirements": self.software.get(row.get("software_requirement"), {}).get(
                    "label", "Download requirements need review"
                ),
            }
            for row in self.rows
        ]
        if rows:
            return _frame(rows)
        frame = _frame(
            [],
            columns=[
                "short_title",
                "dataset_type",
                "download_id",
                "download_title",
                "download_category",
                "download_requirements",
                "access_level",
                "python_route",
                "python_supported",
                "python_support_message",
            ],
        )
        frame["python_supported"] = frame["python_supported"].astype(bool)
        return frame


@dataclass(frozen=True)
class ManifestReport:
    destination: Path
    records: tuple[dict, ...]

    @property
    def files(self):
        return tuple(self.destination / row["manifest"] for row in self.records)

    def to_dataframe(self):
        return _frame(self.records)


@dataclass(frozen=True)
class DownloadReport:
    entries: tuple[dict, ...]
    scope: dict = field(default_factory=dict)

    @property
    def complete(self):
        """All selected entries completed; a sample never establishes full-dataset completion."""
        return bool(self.entries) and all(
            r["status"] in {"downloaded", "verified_existing", "engine_completed"}
            for r in self.entries
        )

    def to_dataframe(self):
        return _frame(self.entries)


def _notify(progress, message):
    if not isinstance(progress, bool):
        raise InvalidRequestError("progress must be a boolean")
    if progress:
        print(message, flush=True)


def _validate_max_items(max_items):
    if max_items is not None and (type(max_items) is not int or max_items < 1):
        raise InvalidRequestError("max_items must be a positive integer or None")


def _sample_selection(rows, max_items, scope, manifest_inputs):
    """Build a bounded selection using metadata only, before any payload or credentials."""
    routes = [_route(row) for row in rows]
    blocked = [
        route not in {"public_dicom", "public_manifest", "controlled_files", "public_file"}
        for route in routes
    ]
    if any(blocked):
        report = DownloadReport(
            tuple(
                {
                    "short_title": row.get("short_title"),
                    "download_id": row.get("download_id"),
                    "route": route,
                    "status": "unsupported_sample" if problem else "not_attempted",
                    "is_sample": True,
                    "message": (
                        "This group does not provide a supported file-level sample route."
                        if problem
                        else "No sample transfer started."
                    ),
                    "next_step": "Select a manifest download group or pass indexed file records to download_files(); external applications are outside this Python downloader.",
                }
                for row, route, problem in zip(rows, routes, blocked)
            ),
            {
                **scope,
                "is_sample": True,
                "max_items": max_items,
            },
        )
        raise DownloadError(
            "Cannot sample this selection. Choose a supported manifest or file-level selection; no payload transfer started.",
            report,
        )
    selected, inputs, groups = [], {}, []
    remaining = max_items
    for index, (row, route) in enumerate(zip(rows, routes)):
        row = dict(row)
        source_hash = subset_hash = None
        if route == "public_file" or row.get("drs_uri"):
            items, column, content = [row], None, None
            unit = "file"
        else:
            if manifest_inputs and index in manifest_inputs:
                content, suffix = manifest_inputs[index]
            else:
                parsed = _url(row.get("download_url"))
                suffix = Path(parsed.path).suffix.lower()
                if parsed.hostname not in _MANIFEST_HOSTS or suffix not in {
                    ".tcia",
                    ".csv",
                    ".tsv",
                }:
                    raise DownloadError("Manifest must come from an official TCIA download link")
                content = _get_bytes(row["download_url"])
            column, items = _manifest(content, suffix)
            if row.get("access_level") == "controlled" and column != "drs_uri":
                raise DownloadError("Controlled manifest has no DRS route column")
            if column != "SeriesInstanceUID":
                _manifest_records(row, column, items)
            unit = "DICOM series" if column == "SeriesInstanceUID" else "file"
            source_hash = hashlib.sha256(content).hexdigest()
        count = min(remaining, len(items))
        if count:
            if column:
                output = io.StringIO(newline="")
                writer = csv.DictWriter(output, fieldnames=list(items[0]))
                writer.writeheader()
                writer.writerows(items[:count])
                subset = output.getvalue().encode("utf-8")
                inputs[len(selected)] = (subset, ".csv")
                subset_hash = hashlib.sha256(subset).hexdigest()
                row["manifest_item_count"] = count
            row.update(
                sample_selected_items=count,
                sample_available_items=len(items),
                sample_unit=unit,
                sample_manifest_sha256=subset_hash,
            )
            selected.append(row)
            remaining -= count
        groups.append(
            {
                "short_title": row.get("short_title"),
                "download_id": row.get("download_id"),
                "unit": unit,
                "available_items": len(items),
                "selected_items": count,
                "omitted_items": len(items) - count,
                "source_manifest_sha256": source_hash,
                "sample_manifest_sha256": subset_hash,
            }
        )
    sample_scope = {
        **scope,
        "is_sample": True,
        "max_items": max_items,
        "sample_selected_items": max_items - remaining,
        "sample_available_items": sum(g["available_items"] for g in groups),
        "sample_omitted_items": sum(g["omitted_items"] for g in groups),
        "sample_groups": groups,
        "sample_policy": "first items in input order, across all selected groups",
        "sample_note": "Whole files or DICOM series; item count does not bound bytes. A successful sample does not validate the remaining data or authorization.",
    }
    return selected, inputs, sample_scope


def _url(value):
    try:
        p = urlsplit(value)
        port = p.port
    except (TypeError, ValueError):
        raise DownloadError("Invalid download URL") from None
    host = (p.hostname or "").lower()
    if (
        p.scheme != "https"
        or not host
        or p.username
        or p.password
        or p.fragment
        or port not in {None, 443}
    ):
        raise DownloadError("An HTTPS URL without embedded user credentials is required")
    if host == "localhost" or "." not in host or host.endswith((".local", ".internal", ".lan")):
        raise DownloadError("Private download hosts are not supported")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if not address.is_global:
            raise DownloadError("Private download addresses are not supported")
    return p


def _safe_name(value):
    if (
        not isinstance(value, str)
        or not value
        or value in {".", ".."}
        or value.startswith("-")
        or any(ord(c) < 32 for c in value)
    ):
        raise DownloadError("Invalid download filename")
    if "/" in value or "\\" in value or Path(value).name != value:
        raise DownloadError("Download filenames must not contain directories")
    return value


def _http():
    return httpx.Client(timeout=120, follow_redirects=False)


def _get_bytes(url, *, max_bytes=10_000_000):
    try:
        return _manifest_bytes(url, max_bytes=max_bytes)
    except httpx.HTTPError:
        raise DownloadError("Manifest request failed") from None


def _manifest_bytes(url, *, max_bytes):
    # Small manifest requests have no credentials and a strict size cap.
    with _http() as client:
        for _ in range(6):
            _url(url)
            with client.stream("GET", url) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    url = urljoin(url, response.headers.get("location", ""))
                    continue
                if response.status_code != 200:
                    raise DownloadError(f"Manifest request failed (HTTP {response.status_code})")
                data = bytearray()
                for chunk in response.iter_bytes():
                    data.extend(chunk)
                    if len(data) > max_bytes:
                        raise DownloadError("Manifest exceeded the metadata size limit")
                return bytes(data)
    raise DownloadError("Too many manifest redirects")


def _manifest(content, suffix):
    try:
        text = content.decode("utf-8-sig")
        if suffix == ".tcia":
            marker = "ListOfSeriesToDownload="
            if marker not in text:
                raise ValueError()
            uids = [s.strip() for s in text.split(marker, 1)[1].splitlines() if s.strip()]
            if not uids or any(
                not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", uid) or len(uid) > 64 for uid in uids
            ):
                raise ValueError()
            return "SeriesInstanceUID", [{"SeriesInstanceUID": uid} for uid in dict.fromkeys(uids)]
        reader = csv.DictReader(io.StringIO(text), delimiter="\t" if suffix == ".tsv" else ",")
        rows = list(reader)
        columns = set(reader.fieldnames or []) & {
            "SeriesInstanceUID",
            "imageUrl",
            "drs_uri",
        }
        if len(columns) != 1 or not rows or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError()
        column = next(iter(columns))
        if any(None in row or not row.get(column) for row in rows):
            raise ValueError()
        for row in rows:
            row[column] = row[column].strip()
        if column == "SeriesInstanceUID" and any(
            not re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", r[column]) or len(r[column]) > 64 for r in rows
        ):
            raise ValueError()
        if column == "drs_uri" and any(not r[column].startswith("drs://") for r in rows):
            raise ValueError()
        return column, rows
    except (UnicodeError, ValueError, csv.Error):
        raise DownloadError("Response is not a supported nonempty manifest") from None


def _route(row):
    access = row.get("access_level")
    if access not in _OPEN | {"controlled"} or (
        row.get("controlled_access") is True and access in _OPEN
    ):
        return "unsupported"
    if row.get("drs_uri"):
        return "controlled_files" if access == "controlled" else "unsupported"
    if row.get("requirement_status") in {"conflict", "unknown"}:
        return "unsupported"
    method = row.get("download_method")
    if method == "dicom_manifest":
        return "public_dicom" if access in _OPEN else "unsupported"
    if method == "tabular_manifest":
        return "controlled_files" if access == "controlled" else "public_manifest"
    if method == "aspera_package" and access in _OPEN:
        p = _url(row.get("download_url"))
        return (
            "public_package" if p.hostname == "faspex.cancerimagingarchive.net" else "unsupported"
        )
    if method == "direct_file" and access in _OPEN:
        _url(row.get("imageUrl") or row.get("download_url"))
        return "public_file"
    return "unsupported"


def _credential(path):
    if not path:
        raise DownloadError("Controlled downloads require your own approved JSON key-file path")
    try:
        p = Path(path)
        data = json.loads(p.read_text())
        key = data.get("api_key") if isinstance(data, dict) else None
        if not isinstance(key, str) or not key.strip():
            raise ValueError()
        if os.name == "posix" and p.stat().st_mode & 0o077:
            warnings.warn(
                "Credential file is accessible to other users; consider chmod 600",
                stacklevel=3,
            )
        return key.strip()
    except (OSError, ValueError):
        raise DownloadError(
            "Could not read a JSON credential file with a nonempty api_key"
        ) from None


def _drs(uri):
    try:
        p = urlsplit(uri)
        port = p.port
    except (TypeError, ValueError):
        raise DownloadError("Invalid controlled-file identifier") from None
    if (
        p.scheme != "drs"
        or p.hostname not in _DRS_HOSTS
        or port
        or p.username
        or p.password
        or p.query
        or p.fragment
    ):
        raise DownloadError("Unrecognized controlled-download host; no credential will be sent")
    identifier = p.path.lstrip("/")
    if (
        not identifier
        or not re.fullmatch(r"[A-Za-z0-9._/-]+", identifier)
        or any(part in {".", ".."} for part in identifier.split("/"))
    ):
        raise DownloadError("Invalid controlled-file identifier")
    return p.hostname, identifier


def _authorized_url(uri, key, tokens):
    host, identifier = _drs(uri)
    with _http() as client:
        for attempt in range(2):
            if host not in tokens:
                response = client.post(
                    f"https://{host}/user/credentials/api/access_token",
                    json={"api_key": key},
                )
                if response.status_code != 200:
                    raise DownloadError(
                        f"Controlled authentication failed (HTTP {response.status_code})"
                    )
                try:
                    token = response.json()["access_token"]
                    if not isinstance(token, str) or not token:
                        raise ValueError()
                except (KeyError, ValueError, TypeError):
                    raise DownloadError("Controlled service returned no access token") from None
                tokens[host] = token
            response = client.get(
                f"https://{host}/user/data/download/{quote(identifier,safe='')}",
                headers={"Authorization": f"Bearer {tokens[host]}"},
            )
            if response.status_code == 401 and attempt == 0:
                tokens.pop(host, None)
                continue
            if response.status_code != 200:
                raise DownloadError(
                    f"Controlled download authorization failed (HTTP {response.status_code})"
                )
            try:
                location = response.json()["url"]
                _url(location)
            except (ValueError, KeyError, TypeError):
                raise DownloadError(
                    "Controlled service returned no valid HTTPS file location"
                ) from None
            return location
    raise DownloadError("Controlled download authorization failed")


def _size(row):
    value = next(
        (
            row.get(k)
            for k in ("file_size_bytes", "size_bytes", "CTDC Size", "File Size")
            if row.get(k) not in {None, ""}
        ),
        None,
    )
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _stream_file(url, path, *, size=None, checksum=None, algorithm=None, overwrite=False):
    _url(url)
    algorithm = algorithm.lower() if isinstance(algorithm, str) else None
    if checksum and algorithm not in {"md5", "sha256"}:
        raise DownloadError("Unsupported checksum algorithm")
    if path.is_symlink():
        raise DownloadError("Refusing to replace a symlink")
    if path.exists() and not overwrite:
        if checksum:
            h = hashlib.new(algorithm)
            with path.open("rb") as existing:
                for chunk in iter(lambda: existing.read(1024 * 1024), b""):
                    h.update(chunk)
            if h.hexdigest().lower() == checksum.lower() and (
                size is None or path.stat().st_size == size
            ):
                return "verified_existing"
        elif size is not None and path.stat().st_size == size:
            return "verified_existing"
        raise DownloadError("Existing file cannot be verified; use overwrite=True to replace it")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with _http() as client:
            for _ in range(6):
                _url(url)
                with client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
                    if response.status_code in {301, 302, 303, 307, 308}:
                        url = urljoin(url, response.headers.get("location", ""))
                        continue
                    if response.status_code != 200:
                        raise DownloadError(f"File transfer failed (HTTP {response.status_code})")
                    length = response.headers.get("content-length")
                    expected = (
                        int(length)
                        if length
                        and length.isdigit()
                        and response.headers.get("content-encoding", "identity") == "identity"
                        else None
                    )
                    h = hashlib.new(algorithm or "sha256")
                    total = 0
                    with tempfile.NamedTemporaryFile(
                        dir=path.parent,
                        prefix="." + path.name + ".",
                        suffix=".part",
                        delete=False,
                    ) as out:
                        temporary = Path(out.name)
                        for chunk in response.iter_bytes():
                            out.write(chunk)
                            h.update(chunk)
                            total += len(chunk)
                    if (size is not None and total != size) or (
                        expected is not None and total != expected
                    ):
                        raise DownloadError(
                            "Downloaded byte count does not match the expected size"
                        )
                    if checksum and h.hexdigest().lower() != checksum.lower():
                        raise DownloadError("Downloaded checksum does not match")
                    temporary.replace(path)
                    temporary = None
                    return "downloaded"
        raise DownloadError("Too many file redirects")
    except httpx.HTTPError:
        raise DownloadError("File transfer failed; partial file was not promoted") from None
    finally:
        # A failed transfer never overwrites a previously complete file.
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _prerequisite_guidance(status):
    guidance = {
        "requires_external_tool": (
            "This download group requires separate transfer software and is outside the Python download scope.",
            "Use the official collection download link or its separate software notebook. Installing another application does not enable this route in tcia_utils.",
            "https://github.com/kirbyju/TCIA_Notebooks/blob/main/TCIA_Aspera_CLI_Downloads.ipynb",
        ),
        "requires_python_download_extra": (
            "The public DICOM Python dependency is missing.",
            "Install tcia_utils[downloads] in the environment running the notebook.",
            "https://github.com/kirbyju/tcia_utils/blob/main/docs/v4/user-guide.md#download-from-python",
        ),
        "requires_credential": (
            "This selected download requires an approved controlled-data key file.",
            "Supply credential_file= with the path to your approved JSON key. Keep the key contents out of the notebook.",
            "https://www.cancerimagingarchive.net/nih-controlled-data-access-policy/",
        ),
        "unsupported": (
            "This record has no supported, unambiguous Python download route.",
            "Follow the official collection instructions; no replacement URL will be invented.",
            "",
        ),
    }
    message, next_step, documentation_url = guidance.get(status, ("", "", ""))
    return {"message": message, "next_step": next_step, "documentation_url": documentation_url}


def _manifest_records(row, column, items):
    if column == "drs_uri" and row.get("access_level") != "controlled":
        raise DownloadError("DRS manifest entries require a controlled-access route")
    child = []
    names = set()
    for item in items:
        record = {
            **item,
            "short_title": row["short_title"],
            "download_id": row["download_id"],
            "access_level": row["access_level"],
            "download_method": "direct_file",
        }
        for key in ("file_name", "CTDC File Name", "File Name", "filename", "name"):
            if item.get(key):
                record["file_name"] = _safe_name(item[key])
                break
        if column == "drs_uri":
            _drs(item[column])
            if not record.get("file_name"):
                record["file_name"] = _safe_name(item[column].rsplit("/", 1)[-1])
        elif column == "imageUrl":
            _url(item[column])
            if not record.get("file_name"):
                record["file_name"] = _safe_name(Path(urlsplit(item[column]).path).name)
        if record.get("file_name") in names:
            raise DownloadError(
                "Manifest repeats a destination filename; no payload transfer started"
            )
        names.add(record.get("file_name"))
        for key in ("md5", "MD5", "md5sum", "MD5sum"):
            if item.get(key):
                record.update(checksum=item[key], checksum_algorithm="md5")
                break
        child.append(record)
    return child


class DownloadsMixin:
    def _get_imaging_client(self, progress):
        client = getattr(self, "_imaging_client", None)
        if client is None:
            _notify(
                progress,
                "Preparing the public DICOM index (first use in this client may take a moment)...",
            )
            try:
                from idc_index import IDCClient
            except ImportError:
                raise DownloadError(
                    "Install tcia_utils[downloads] for public DICOM downloads"
                ) from None
            client = IDCClient()
            self._imaging_client = client
        else:
            _notify(progress, "Reusing the prepared public DICOM index...")
        return client

    def plan_downloads(
        self, short_title, *, download_ids=None, access_levels=None, dataset_type=None
    ):
        """Get the shared REST/MCP plan; software interpretation lives on the service."""
        if dataset_type not in {None, "collection", "analysis_result"}:
            raise InvalidRequestError("dataset_type must be collection or analysis_result")
        params = _filters(download_ids=download_ids, access_levels=access_levels)
        if dataset_type is not None:
            params["dataset_type"] = dataset_type
        try:
            result = self._result(
                "GET",
                f"/v2/datasets/{_segment(short_title, 'short_title')}/download-plan",
                params=params or None,
                required_fields={
                    "plan_version": int,
                    "short_title": str,
                    "dataset_type": str,
                    "downloads": list,
                    "software": dict,
                    "scope": dict,
                    "notes": list,
                },
                note_keys=("notes",),
                bounded=False,
            )
        except NotFoundError as error:
            if error.detail.startswith("No visible TCIA dataset"):
                raise
            raise ServiceUnavailableError(
                "The REST service does not provide this download plan or dataset. Update the service to the shared download-plan contract and verify the dataset short title.",
                code="download_plan_unavailable",
            ) from error
        data = result.data
        if data["plan_version"] != 1:
            raise ProtocolError("Unsupported download-plan version")
        if data["short_title"].casefold() != short_title.strip().casefold():
            raise ProtocolError("Download plan identifies a different dataset")
        if (
            dataset_type is not None
            and data["dataset_type"].casefold().replace(" ", "_") != dataset_type
        ):
            raise ProtocolError("Download plan identifies a different dataset type")
        rows = []
        for item in data["downloads"]:
            if (
                not isinstance(item, dict)
                or item.get("software_requirement") not in data["software"]
            ):
                raise ProtocolError("Download plan has an undefined software requirement")
            if item.get("requirement_status") not in {
                "declared",
                "link_fallback",
                "unknown",
                "conflict",
            } or item.get("download_method") not in {
                "dicom_manifest",
                "tabular_manifest",
                "aspera_package",
                "direct_file",
                "unsupported",
            }:
                raise ProtocolError("Download plan has an invalid routing contract")
            if (
                item.get("short_title") != data["short_title"]
                or item.get("dataset_type") != data["dataset_type"]
                or not isinstance(item.get("download_id"), str)
                or not item["download_id"]
            ):
                raise ProtocolError("Download plan has inconsistent dataset identity")
            if download_ids is not None and item["download_id"] not in download_ids:
                raise ProtocolError("Download plan includes an unrequested download ID")
            row = dict(item)
            kind = _route(row)
            row.update(
                download_category="; ".join(row.get("download_types") or []),
                python_route=kind,
                python_supported=kind
                in {"public_dicom", "public_manifest", "controlled_files", "public_file"},
                python_support_message=(
                    "Supported through Python and pip dependencies"
                    if kind
                    in {"public_dicom", "public_manifest", "controlled_files", "public_file"}
                    else "Use the official download link; this group requires a separate application or has no supported Python route"
                ),
            )
            rows.append(row)
        if data["scope"].get("selected_groups") != len(rows):
            raise ProtocolError("Download-plan count does not match the selected records")
        inventory_count = data["scope"].get("inventory_groups")
        if (
            not isinstance(inventory_count, int)
            or isinstance(inventory_count, bool)
            or inventory_count < len(rows)
        ):
            raise ProtocolError("Download-plan inventory count is invalid")
        for definition in data["software"].values():
            if not isinstance(definition, dict) or any(
                not isinstance(definition.get(k), str)
                for k in ("label", "help_url", "instructions")
            ):
                raise ProtocolError("Download plan has invalid software help")
        scope = {
            **data["scope"],
            "short_title": data["short_title"],
            "dataset_type": data["dataset_type"],
            "metadata_snapshot": result.provenance.release_fingerprint,
            "plan_version": 1,
        }
        return DownloadPlan(tuple(rows), result.provenance, scope, data["software"], result.notes)

    def save_manifests(self, downloads, directory):
        """Save unchanged official TCIA manifests with hashes; never download payloads."""
        rows = list(downloads.rows) if isinstance(downloads, DownloadPlan) else records(downloads)
        destination = Path(directory)
        destination.mkdir(parents=True, exist_ok=True)
        saved = []
        for row in rows:
            route = _route(row)
            if route not in {
                "public_dicom",
                "public_manifest",
                "controlled_files",
            } or row.get("drs_uri"):
                raise DownloadError(
                    "Select official manifest download groups, not payload files or packages"
                )
            p = _url(row["download_url"])
            suffix = Path(p.path).suffix.lower()
            if p.hostname not in _MANIFEST_HOSTS or suffix not in {
                ".tcia",
                ".csv",
                ".tsv",
            }:
                raise DownloadError("Manifest must come from an official TCIA download link")
            content = _get_bytes(row["download_url"])
            column, items = _manifest(content, suffix)
            expected = "drs_uri" if row.get("access_level") == "controlled" else None
            if expected and column != expected:
                raise DownloadError("Controlled manifest has no DRS route column")
            name = _safe_name(f"{row['short_title']}_{row['download_id']}{suffix}")
            target = destination / name
            if target.is_symlink():
                raise DownloadError("Refusing to replace a symlink")
            with tempfile.NamedTemporaryFile(dir=destination, delete=False) as temp:
                temp.write(content)
                temp_path = Path(temp.name)
            temp_path.replace(target)
            saved.append(
                {
                    "download_id": row["download_id"],
                    "manifest": name,
                    "url": row["download_url"],
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "route_column": column,
                    "item_count": len(items),
                }
            )
        return ManifestReport(destination, tuple(saved))

    def download_manifest(
        self,
        manifest,
        destination,
        *,
        credential_file=None,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
        access_level=None,
    ):
        """Download a local/HTTPS .tcia, CSV or TSV manifest without manual parsing.

        max_items selects at most that many whole series/files in input order.
        It limits item count, not bytes; the report marks the selection as a sample.
        A dry run reads this small manifest and checks local prerequisites only.
        UID manifests use the public imaging index; missing series fail before
        transfer. DRS manifests require approved credentials. No dataset/participant
        identity is guessed from filenames or opaque identifiers.
        """
        _validate_max_items(max_items)
        _notify(progress, "Preparing download selection...")
        if not isinstance(manifest, (str, os.PathLike)) or not str(manifest):
            raise InvalidRequestError("manifest must be a local path or HTTPS URL")
        if not isinstance(dry_run, bool) or not isinstance(overwrite, bool):
            raise InvalidRequestError("dry_run and overwrite must be booleans")
        source = str(manifest)
        try:
            parsed = urlsplit(source)
        except ValueError:
            raise DownloadError("Invalid manifest URL") from None
        if parsed.scheme == "https":
            _url(source)
            suffix = Path(parsed.path).suffix.lower()
            if suffix not in {".tcia", ".csv", ".tsv"}:
                raise DownloadError("Supported manifest formats are .tcia, .csv and .tsv")
            source_label = f"https://{parsed.netloc}{parsed.path}"
            content = _get_bytes(source)
        elif parsed.scheme and len(parsed.scheme) > 1:
            raise DownloadError("Manifest must be a local path or an HTTPS URL")
        else:
            path = Path(manifest).expanduser()
            suffix, source_label = path.suffix.lower(), path.name
            if suffix not in {".tcia", ".csv", ".tsv"}:
                raise DownloadError("Supported manifest formats are .tcia, .csv and .tsv")
            try:
                if path.stat().st_size > 10_000_000:
                    raise DownloadError("Manifest exceeded the metadata size limit")
                with path.open("rb") as source_file:
                    content = source_file.read(10_000_001)
            except OSError:
                raise DownloadError("Could not read the local manifest") from None
        if suffix not in {".tcia", ".csv", ".tsv"}:
            raise DownloadError("Supported manifest formats are .tcia, .csv and .tsv")
        if len(content) > 10_000_000:
            raise DownloadError("Manifest exceeded the metadata size limit")
        column, items = _manifest(content, suffix)
        controlled = column == "drs_uri"
        access = access_level or ("controlled" if controlled else "open")
        if controlled and access != "controlled":
            raise DownloadError("DRS manifests require controlled access and approved credentials")
        if not controlled and access not in _OPEN:
            raise DownloadError(
                "Controlled downloads require a DRS manifest; public UID/URL manifests cannot establish an authorized controlled route"
            )
        digest = hashlib.sha256(content).hexdigest()
        row = {
            "short_title": "manifest",
            "download_id": digest[:16],
            "access_level": access,
            "controlled_access": controlled,
            "download_url": source if parsed.scheme == "https" else None,
            "download_method": (
                "dicom_manifest" if column == "SeriesInstanceUID" else "tabular_manifest"
            ),
            "requirement_status": "declared",
            "manifest_item_count": len(items),
        }
        if column != "SeriesInstanceUID":
            _manifest_records(row, column, items)  # Validate every route/name before any transfer.
        return self._download(
            [row],
            destination,
            scope={
                "manifest_source": source_label,
                "manifest_sha256": digest,
                "manifest_route": column,
                "manifest_items": len(items),
            },
            credential_file=credential_file,
            dry_run=dry_run,
            progress=progress,
            max_items=max_items,
            overwrite=overwrite,
            manifest_inputs={0: (content, suffix)},
        )

    def download_dataset(
        self,
        short_title,
        destination,
        *,
        download_ids=None,
        access_levels=None,
        credential_file=None,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
        dataset_type=None,
    ):
        """Download selected groups. Preflight all groups before transferring any.

        max_items selects a sample across the chosen groups, in inventory order.
        Original packages without file-level routes cannot be sampled.
        All groups are in scope unless filtered explicitly. Missing authorization
        or engine support stops the entire selection rather than silently dropping
        data. dry_run only queries metadata and checks local prerequisites.
        """
        _validate_max_items(max_items)
        _notify(progress, "Preparing download selection...")
        plan = self.plan_downloads(
            short_title,
            download_ids=download_ids,
            access_levels=access_levels,
            dataset_type=dataset_type,
        )
        return self._download(
            list(plan.rows),
            destination,
            scope=plan.scope,
            credential_file=credential_file,
            dry_run=dry_run,
            progress=progress,
            max_items=max_items,
            overwrite=overwrite,
        )

    def download_files(
        self,
        files,
        destination,
        *,
        credential_file=None,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
    ):
        """Download explicit files; max_items samples the first N records, without adding related images."""
        _validate_max_items(max_items)
        _notify(progress, "Preparing download selection...")
        rows = records(files)
        for row in rows:
            if row.get("asset_granularity", "file") != "file":
                raise InvalidRequestError("download_files requires file-grain records")
            if not row.get("drs_uri") and row.get("download_method") in {
                "aspera_package",
                "dicom_manifest",
                "tabular_manifest",
            }:
                raise InvalidRequestError(
                    "Use download_dataset or download_manifest for a package/manifest"
                )
            if not row.get("drs_uri") and "download_method" not in row:
                if row.get("asset_granularity", "file") != "file":
                    raise InvalidRequestError("download_files requires file-grain records")
                p = _url(row.get("imageUrl") or row.get("download_url"))
                if (
                    p.hostname == "faspex.cancerimagingarchive.net"
                    or Path(p.path).suffix.lower() == ".tcia"
                ):
                    raise InvalidRequestError(
                        "Use download_dataset or download_manifest for a package/manifest"
                    )
                row["download_method"] = "direct_file"
        return self._download(
            rows,
            destination,
            scope={"explicit_file_records": len(rows)},
            credential_file=credential_file,
            dry_run=dry_run,
            progress=progress,
            max_items=max_items,
            overwrite=overwrite,
        )

    def _download(
        self,
        rows,
        destination,
        *,
        scope,
        credential_file,
        dry_run,
        progress,
        overwrite,
        manifest_inputs=None,
        max_items=None,
    ):
        if not isinstance(dry_run, bool) or not isinstance(overwrite, bool):
            raise InvalidRequestError("dry_run and overwrite must be booleans")
        scope = {**scope, "dry_run": dry_run}
        if not rows:
            raise InvalidRequestError("No download records were selected")
        _validate_max_items(max_items)
        if max_items is not None:
            try:
                rows, manifest_inputs, scope = _sample_selection(
                    rows, max_items, scope, manifest_inputs
                )
            except DownloadError as error:
                if dry_run and error.report is not None:
                    return error.report
                raise
        entries = []
        key = None
        for row in rows:
            route = _route(row)
            problem = None
            if row.get("access_level") == "controlled":
                if row.get("drs_uri"):
                    _drs(row["drs_uri"])
                if not credential_file:
                    problem = "requires_credential"
            if route == "unsupported":
                problem = "unsupported"
            if route == "public_dicom" and importlib.util.find_spec("idc_index") is None:
                problem = "requires_python_download_extra"
            if route == "public_package":
                problem = "requires_external_tool"
            entries.append(
                {
                    "short_title": row.get("short_title"),
                    "download_id": row.get("download_id"),
                    "file_name": row.get("file_name"),
                    "manifest_item_count": row.get("manifest_item_count"),
                    "download_category": "; ".join(row.get("download_types") or []),
                    "route": route,
                    **(
                        {
                            "is_sample": True,
                            "sample_selected_items": row["sample_selected_items"],
                            "sample_available_items": row["sample_available_items"],
                            "sample_unit": row["sample_unit"],
                            "sample_manifest_sha256": row["sample_manifest_sha256"],
                        }
                        if scope.get("is_sample")
                        else {}
                    ),
                    "status": problem or "planned",
                    **_prerequisite_guidance(problem),
                }
            )
        report = DownloadReport(tuple(entries), scope)
        if dry_run:
            return report
        if any(e["status"] != "planned" for e in entries):
            missing = [entry for entry in entries if entry["status"] != "planned"]
            reasons = " ".join(dict.fromkeys(entry["message"] for entry in missing))
            actions = " ".join(dict.fromkeys(entry["next_step"] for entry in missing))
            raise DownloadError(f"Download cannot start. {reasons} {actions}", report)
        if any(r.get("access_level") == "controlled" for r in rows):
            key = _credential(credential_file)
        target = Path(destination)
        tokens = {}
        prepared = {}
        imaging_clients = {}
        sync_required = {}
        # Expand and validate every manifest before sending credentials or payloads.
        # This also discovers dependencies hidden by a generic CSV route.
        try:
            for i, row in enumerate(rows):
                _safe_name(str(row.get("short_title") or "selection"))
                _safe_name(str(row.get("download_id") or "files"))
                route = entries[i]["route"]
                if route not in {
                    "public_dicom",
                    "public_manifest",
                    "controlled_files",
                } or row.get("drs_uri"):
                    if row.get("file_name"):
                        _safe_name(row["file_name"])
                    continue
                folder = (
                    target
                    / _safe_name(str(row.get("short_title") or "selection"))
                    / _safe_name(str(row.get("download_id") or "files"))
                )
                sync_required[i] = folder.is_dir() and any(
                    child.name != "manifests" for child in folder.iterdir()
                )
                if manifest_inputs and i in manifest_inputs:
                    content, suffix = manifest_inputs[i]
                    column, items = _manifest(content, suffix)
                    manifest_folder = folder / "manifests"
                    manifest_folder.mkdir(parents=True, exist_ok=True)
                    name = "selection" + suffix
                    path = manifest_folder / name
                    if path.is_symlink():
                        raise DownloadError("Refusing to replace a symlink")
                    with tempfile.NamedTemporaryFile(dir=manifest_folder, delete=False) as temp:
                        temp.write(content)
                        temporary = Path(temp.name)
                    temporary.replace(path)
                    manifest = ManifestReport(manifest_folder, ({"manifest": name},))
                else:
                    manifest = self.save_manifests([row], folder / "manifests")
                    column, items = _manifest(
                        manifest.files[0].read_bytes(), manifest.files[0].suffix
                    )
                if column != "SeriesInstanceUID":
                    _manifest_records(row, column, items)
                if column == "SeriesInstanceUID":
                    client = self._get_imaging_client(progress)
                    uids = [r[column] for r in items]
                    missing = set(uids) - set(client.index["SeriesInstanceUID"])
                    if missing:
                        entries[i]["missing_series_uids"] = sorted(missing)
                        raise DownloadError(
                            f"{len(missing)} requested public series are absent from the current imaging index; no payload transfer started"
                        )
                    if callable(getattr(client, "get_idc_version", None)):
                        entries[i]["imaging_index_version"] = client.get_idc_version()
                    imaging_clients[i] = client
                if scope.get("is_sample"):
                    entries[i]["sample_manifest"] = str(manifest.files[0])
                prepared[i] = (manifest, column, items)
        except Exception as error:
            for entry in entries:
                entry["status"] = "not_attempted"
            if entries:
                entries[i]["status"] = "failed_preflight"
            raise DownloadError(
                (
                    str(error)
                    if isinstance(error, DownloadError)
                    else "Manifest preflight failed; no payload transfer started"
                ),
                DownloadReport(tuple(entries), scope),
            ) from None
        for i, row in enumerate(rows):
            try:
                route = entries[i]["route"]
                folder = (
                    target
                    / _safe_name(str(row.get("short_title") or "selection"))
                    / _safe_name(str(row.get("download_id") or "files"))
                )
                entries[i]["destination"] = str(folder)
                if route == "public_file":
                    _notify(progress, f"Downloading file {i + 1} of {len(rows)}...")
                    url = row.get("imageUrl") or row["download_url"]
                    name = _safe_name(row.get("file_name") or Path(urlsplit(url).path).name)
                    entries[i]["status"] = _stream_file(
                        url,
                        folder / name,
                        size=_size(row),
                        checksum=row.get("checksum"),
                        algorithm=row.get("checksum_algorithm"),
                        overwrite=overwrite,
                    )
                elif row.get("drs_uri"):
                    _notify(progress, f"Authorizing and downloading file {i + 1} of {len(rows)}...")
                    location = _authorized_url(row["drs_uri"], key, tokens)
                    name = _safe_name(row.get("file_name") or row["drs_uri"].rsplit("/", 1)[-1])
                    entries[i]["status"] = _stream_file(
                        location,
                        folder / name,
                        size=_size(row),
                        checksum=row.get("checksum"),
                        algorithm=row.get("checksum_algorithm"),
                        overwrite=overwrite,
                    )
                else:
                    manifest, column, items = prepared[i]
                    if column == "SeriesInstanceUID":
                        client = imaging_clients[i]
                        uids = [r[column] for r in items]
                        _notify(
                            progress,
                            "Resolving selected public DICOM files and starting transfer...",
                        )
                        client.download_dicom_series(
                            seriesInstanceUID=uids,
                            downloadDir=str(folder),
                            quiet=True,
                            show_progress_bar=progress,
                            use_s5cmd_sync=sync_required[i],
                        )
                        entries[i]["status"] = "engine_completed"
                    else:
                        child = _manifest_records(row, column, items)
                        nested = self._download(
                            child,
                            target,
                            scope={"manifest_items": len(child)},
                            credential_file=credential_file,
                            dry_run=False,
                            progress=progress,
                            overwrite=overwrite,
                        )
                        if not nested.complete:
                            raise DownloadError("A manifest item did not complete")
                        entries[i]["status"] = "downloaded"
                entries[i]["verification"] = (
                    "upstream engine completion; no independent byte-level verification"
                    if entries[i]["status"] == "engine_completed"
                    else "HTTP byte count; metadata size/checksum when supplied"
                )
            except Exception as error:
                entries[i]["status"] = "failed"
                entries[i]["error"] = (
                    str(error)
                    if isinstance(error, DownloadError)
                    else "Transfer failed; backend details were suppressed to protect credentials"
                )
                for following in entries[i + 1 :]:
                    following["status"] = "not_attempted"
                raise DownloadError(
                    "Download did not complete; inspect error.report for accounting",
                    DownloadReport(tuple(entries), scope),
                ) from None
        return DownloadReport(tuple(entries), scope)
