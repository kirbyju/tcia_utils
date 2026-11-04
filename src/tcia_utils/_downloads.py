"""Small download adapters over supported clients and official manifest routes.

No transfer or optional dependency is initialized at import. Actual transfers,
retries, verification and authentication belong to the packaged TCIA Data Retriever. This module only prepares and accounts for selections.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urljoin
import csv
import hashlib
import io
import ipaddress
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
from ._serialization import JsonExportMixin

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
    """A download could not start or complete. Inspect .report when present for
    selection outcomes; the exception inherits TCIAError problem fields."""

    def __init__(self, message, report=None):
        """Create a transfer error with optional selection outcomes.

        Args:
            message: Human-readable failure summary.
            report: Optional DownloadReport containing readiness or transfer outcomes.

        Returns:
            None. Initializes an error with code="download_failed" and an optional .report."""
        super().__init__(message, code="download_failed")
        self.report = report


@dataclass(frozen=True)
class DownloadPlan(JsonExportMixin):
    """Inspectable dataset download groups and their published requirements.

    Attributes:
        rows: Selected download-group dictionaries with Python-route support labels.
        provenance: Observed TCIA metadata identity.
        scope: Dataset identity, selected/inventory group counts and filters.
        software: Shared catalog of software labels, help links and instructions.
        notes: Retained caveats from the shared planning service.

    This is metadata, not an authenticated preflight or promise that every group is
    Python-supported. to_dataframe() resolves readable requirement labels; to_dict()/
    to_json() retain rows and the catalog without pandas."""
    rows: tuple[dict, ...]
    provenance: object
    scope: dict = field(default_factory=dict)
    software: dict = field(default_factory=dict)
    notes: tuple[str, ...] = ()

    def to_dataframe(self):
        """Display download groups with readable published software requirements.

        Returns:
            A pandas.DataFrame with one row per group and a download_requirements label resolved
            from this plan's software catalog.

        Raises:
            ImportError: The optional tables dependency is unavailable.

        Notes:
            Requires the tables extra. python_supported describes route support, not access
            approval or successful prerequisite checks. No files are transferred."""
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
class ManifestReport(JsonExportMixin):
    """Accounting for small official manifests saved locally.

    Attributes:
        destination: Local directory where manifests were saved.
        records: Manifest names, source URLs, hashes, route columns and item counts.

    The files property forms local manifest paths. Saving a manifest does not download
    its listed images or authorize controlled access."""
    destination: Path
    records: tuple[dict, ...]

    @property
    def files(self):
        """Get local paths of the manifests named in this report.

        Returns:
            Tuple of pathlib.Path objects formed from destination and each record's manifest name.

        Notes:
            This property does not check file existence or download their contents."""
        return tuple(self.destination / row["manifest"] for row in self.records)

    def to_dataframe(self):
        """Display the saved manifest inventory.

        Returns:
            A pandas.DataFrame of records, including manifest names, source URLs, hashes and item counts.

        Raises:
            ImportError: The optional tables dependency is unavailable.

        Notes:
            Requires the tables extra; does not reread files or download images."""
        return _frame(self.records)


@dataclass(frozen=True)
class DownloadReport(JsonExportMixin):
    """Transfer or dry-run outcomes for an explicitly scoped selection.

    Attributes:
        entries: Per-entry status, route, sample counts and available guidance.
        scope: Selection/sample/dry-run flags and transfer-engine context.

    complete is False for empty or unexecuted/dry-run selections. A successful sample
    covers only that sample. to_dict()/to_json() include entries, scope and complete;
    to_dataframe() displays entries only."""
    entries: tuple[dict, ...]
    scope: dict = field(default_factory=dict)

    @property
    def complete(self):
        """Report whether every entry in this selected transfer completed.

        Returns:
            True for a nonempty report containing only downloaded, verified_existing or
            engine_completed entries; False for empty reports, dry runs and other outcomes.

        Notes:
            Completion covers this report's scope. A successful max_items sample does not
            establish that the whole manifest or dataset has been downloaded."""
        return bool(self.entries) and all(
            r["status"] in {"downloaded", "verified_existing", "engine_completed"}
            for r in self.entries
        )

    def to_dict(self):
        """Export outcomes, sample/dry-run scope and selection completion without pandas.

        Returns:
            An independent JSON-compatible dictionary with entries, scope and complete.

        Raises:
            TypeError: A nested value or dictionary key is unsupported.
            ValueError: A nested number is nonfinite.

        Notes:
            complete describes only this selection; inspect scope["is_sample"] when present.
            No transfer, authentication or verification is performed by exporting a report."""
        return {**super().to_dict(), "complete": self.complete}

    def to_dataframe(self):
        """Display the per-entry outcomes of this selected transfer.

        Returns:
            A pandas.DataFrame containing entries; use .scope and .complete for selection accounting.

        Raises:
            ImportError: The optional tables dependency is unavailable.

        Notes:
            Requires the tables extra. Use .to_dict()/.to_json() to preserve both outcomes
            and scope in one export. This conversion does not verify files on disk."""
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


def _transfer_size(row, column):
    if "download_size_bytes" in row:
        value = row["download_size_bytes"]
        if (value is None or isinstance(value, str) and not value.strip()
                or isinstance(value, float) and value != value):
            return None
        try:
            size = int(value)
            if isinstance(value, bool) or not 0 <= size <= 2**63 - 1:
                raise ValueError()
            if isinstance(value, str):
                if not re.fullmatch(r"[0-9]+", value.strip()):
                    raise ValueError()
            elif size != value:
                raise ValueError()
        except (TypeError, ValueError, OverflowError):
            raise DownloadError("download_size_bytes must be an exact nonnegative integer byte count") from None
        return size
    if column == "drs_uri" and (
        "source_metadata_url" in row or "source_manifest_url" in row
    ):
        # Public controlled-inventory sizes can describe uncompressed series or
        # rounded archive estimates. Neither is an exact payload assertion.
        return None
    return _size(row)


def _prerequisite_guidance(status):
    guidance = {
        "requires_external_tool": (
            "This download group requires separate transfer software and is outside the Python download scope.",
            "Use the official collection download link or its separate software notebook. Installing another application does not enable this route in tcia_utils.",
            "https://github.com/kirbyju/TCIA_Notebooks/blob/main/TCIA_Aspera_CLI_Downloads.ipynb",
        ),
        "requires_python_download_extra": (
            "The packaged Python transfer dependency is missing or its imported wrapper is outdated.",
            "Install tcia_utils[downloads] in this environment, then restart the Colab/Jupyter runtime if it already imported the older transfer package.",
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


def _tdr():
    try:
        import tcia_data_retriever
    except ImportError:
        raise DownloadError("Install tcia_utils[downloads] for Python downloads") from None
    from inspect import signature
    if "progress" not in signature(tcia_data_retriever.download).parameters:
        raise DownloadError(
            "Install tcia_utils[downloads] with TDR 0.1.0rc3 and restart the Python/Colab runtime"
        )
    return tcia_data_retriever


def _validate_policy(accept_data_policy, dry_run):
    if type(dry_run) is not bool:
        raise InvalidRequestError("dry_run must be a boolean")
    if type(accept_data_policy) is not bool:
        raise InvalidRequestError("accept_data_policy must be a boolean")
    if not dry_run and not accept_data_policy:
        raise InvalidRequestError(
            "Review https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/ "
            "and pass accept_data_policy=True to download"
        )


def _transfer_csv(items, column):
    output = io.StringIO(newline="")
    fields = (
        [column]
        if column == "SeriesInstanceUID"
        else [
            column,
            "file_name",
            "collection",
            "participant_id",
            "study_id",
            "file_size_bytes",
            "md5",
            "sha256",
        ]
    )
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in items:
        record = {column: item[column]}
        if column != "SeriesInstanceUID":
            algorithm = str(item.get("checksum_algorithm") or "").lower()
            payload_size = _transfer_size(item, column)
            record.update(
                file_name=item.get("file_name") or "",
                collection=item.get("short_title") or "selection",
                participant_id=item.get("participant_id") or item.get("subject_id") or "",
                study_id=item.get("study_instance_uid") or "",
                file_size_bytes=payload_size if payload_size is not None else "",
                md5=item.get("md5") or (item.get("checksum") if algorithm == "md5" else "") or "",
                sha256=item.get("sha256")
                or (item.get("checksum") if algorithm == "sha256" else "")
                or "",
            )
        writer.writerow(record)
    return output.getvalue().encode()


def _engine_summary(report, expected=None):
    fields = ("total", "downloaded", "synced", "skipped", "failed")
    if not isinstance(report, dict) or report.get("schema_version") != 1:
        raise DownloadError("Transfer engine returned an unsupported completion report")
    if any(type(report.get(k)) is not int or report[k] < 0 for k in fields):
        raise DownloadError("Transfer engine returned invalid completion counts")
    safe = {
        key: report[key]
        for key in (
            "schema_version",
            "status",
            "version",
            "git_commit",
            *fields,
            "elapsed_seconds",
            "error_code",
        )
        if key in report
    }
    if report.get("status") != "success" or report["failed"] or not report["total"]:
        raise DownloadError("Transfer engine did not complete the requested selection")
    if sum(report[k] for k in ("downloaded", "synced", "skipped")) != report["total"]:
        raise DownloadError("Transfer engine completion counts do not cover its selection")
    if expected is not None and report["total"] != expected:
        raise DownloadError("Transfer engine did not account for every selected series/file")
    return safe


class DownloadsMixin:

    """Dataset planning, manifest convenience and explicit transfers exposed by TCIAClient."""
    def plan_downloads(
        self, short_title, *, download_ids=None, access_levels=None, dataset_type=None
    ):
        """Review download groups, access terms and supported Python routes without transferring files.

        Args:
            short_title: Published dataset short title.
            download_ids: Optional nonempty sequence of exact download IDs; omit for all current groups.
            access_levels: Optional nonempty sequence of access labels to select groups.
            dataset_type: Optional "collection" or "analysis_result" to disambiguate the title.

        Returns:
            DownloadPlan with rows, software-help catalog, scope, notes and provenance.
            .to_dataframe() adds readable download_requirements; .to_dict()/.to_json() retain
            raw rows plus the shared software catalog without requiring pandas.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            The shared service interprets published software requirements; the SDK labels
            its supported Python routes. python_supported describes a route, not installed
            prerequisites or your access approval. This call installs nothing, launches no
            transfer engine, and does not authenticate controlled files. download_dataset()
            calls it internally; an explicit inspection step is optional. download_manifest()
            can use an existing manifest without this method.

        Example:
            plan = tcia.plan_downloads("CMB-GEC")
            print(plan.to_json(indent=2))"""
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
                download_category=list(row.get("download_types") or []),
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
        """Save unchanged official TCIA manifest files, not their image payloads.

        Args:
            downloads: DownloadPlan, list/tuple of download-group dictionaries, or DataFrame. Select manifest groups, not packages or individual files.
            directory: Destination directory (string or pathlib.Path); created if needed.

        Returns:
            ManifestReport with destination and records containing local manifest names,
            source URLs, SHA-256 hashes, recognized route columns and item counts.

        Raises:
            DownloadError: A row is not an official supported manifest or its contents are invalid.
            InvalidRequestError: Input is not a supported collection of records.
            OSError: The destination cannot be written.
            TCIAError: A network request fails.

        Notes:
            Fetches the small manifests and validates their format. Existing regular files
            with the generated names are replaced; symlinks are rejected. A controlled manifest
            can be saved without credentials and does not grant access to its listed files."""
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
        accept_data_policy=False,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
        access_level=None,
    ):
        """Download a local or HTTPS manifest without manually extracting its identifiers.

        Args:
            manifest: Local path or HTTPS URL for .tcia, CSV or TSV with exactly one SeriesInstanceUID, imageUrl or drs_uri route column.
            destination: Local destination root (string or pathlib.Path), passed directly to TDR without SDK dataset/manifest subfolders.
            credential_file: Path to your approved JSON API key for controlled files, or None for open data; pass a path, not key contents.
            accept_data_policy: Set True after accepting the TCIA data usage policy. Required for actual transfers; default False.
            dry_run: True to inspect metadata and prerequisites without a payload transfer or authentication; default False.
            progress: Show SDK preparation messages (default True). TDR streams periodic progress lines into Python/notebook output.
            max_items: Positive limit on whole series/files, not bytes, or None for the full selection. Samples keep their scope in the report.
            overwrite: Request replacement behavior; default False.
            access_level: Optional "open"/"public"/"open_noncommercial" or "controlled" label; otherwise inferred from the manifest route.

        Returns:
            DownloadReport with per-entry outcomes and selection scope. .complete describes
            only the selected sample/full selection; inspect entries and scope before claiming
            a dataset is complete. DownloadError.report retains outcomes when a transfer fails.

        Raises:
            InvalidRequestError: Selection or options are invalid.
            DownloadError: A route, prerequisite, authorization or transfer fails; inspect .report when present.
            TCIAError: A metadata request fails.
            OSError: A local input or destination cannot be read or written.

        Notes:
            Actual transfers use the downloads extra (pip-installed tcia-data-retriever).
            TDR owns authorization, retry/checksum behavior, file layout and route-specific extraction;
            supplied CLI/Python keys are used only for this invocation, without a saved copy.
            TDR automatically checks/reuses existing files. Keep the same
            destination when expanding a sample. Files accepted by existing-file checks
            count as Skipped; Synced denotes public DICOM sync jobs. For controlled inventory
            records, only download_size_bytes is an exact payload-size assertion; reported
            file_size_bytes may describe uncompressed series data. Rerun the same call with
            the same destination to retry or expand a sample; partial-file byte-range
            resume is not promised.
            A dry run still reads/fetches the small manifest. max_items selects first items in
            manifest order. Standalone direct/controlled file manifests use the generic
            collection label "manifest"; TDR preserves supplied filenames and keeps these
            ZIP files as archives. Use download_dataset() when you want known dataset context.
            This method does not require a preceding plan_downloads() call.

        Example:
            report = tcia.download_manifest("cohort.tcia", "sample", max_items=1,
                                            accept_data_policy=True)"""
        _validate_max_items(max_items)
        _validate_policy(accept_data_policy, dry_run)
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
            accept_data_policy=accept_data_policy,
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
        accept_data_policy=False,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
        dataset_type=None,
    ):
        """Download selected dataset groups after checking every selected route.

        Args:
            short_title: Published dataset short title.
            destination: Local destination root (string or pathlib.Path), passed directly to TDR without SDK dataset/manifest subfolders.
            credential_file: Path to your approved JSON API key for controlled files, or None for open data; pass a path, not key contents.
            accept_data_policy: Set True after accepting the TCIA data usage policy. Required for actual transfers; default False.
            dry_run: True to inspect metadata and prerequisites without a payload transfer or authentication; default False.
            progress: Show SDK preparation messages (default True). TDR streams periodic progress lines into Python/notebook output.
            max_items: Positive limit on whole series/files, not bytes, or None for the full selection. Samples keep their scope in the report.
            overwrite: Request replacement behavior; default False.
            download_ids: Optional nonempty sequence of exact download IDs; None includes all current groups.
            access_levels: Optional nonempty sequence of access labels to select groups.
            dataset_type: Optional "collection" or "analysis_result" to disambiguate the title.

        Returns:
            DownloadReport with per-entry outcomes and selection scope. .complete describes
            only the selected sample/full selection; inspect entries and scope before claiming
            a dataset is complete. DownloadError.report retains outcomes when a transfer fails.

        Raises:
            InvalidRequestError: Selection or options are invalid.
            DownloadError: A route, prerequisite, authorization or transfer fails; inspect .report when present.
            TCIAError: A metadata request fails.
            OSError: A local input or destination cannot be read or written.

        Notes:
            Actual transfers use the downloads extra (pip-installed tcia-data-retriever).
            TDR owns authorization, retry/checksum behavior, file layout and route-specific extraction;
            supplied CLI/Python keys are used only for this invocation, without a saved copy.
            TDR automatically checks/reuses existing files. Keep the same
            destination when expanding a sample. Files accepted by existing-file checks
            count as Skipped; Synced denotes public DICOM sync jobs. For controlled inventory
            records, only download_size_bytes is an exact payload-size assertion; reported
            file_size_bytes may describe uncompressed series data. Rerun the same call with
            the same destination to retry or expand a sample; partial-file byte-range
            resume is not promised.
            Calls plan_downloads() internally; you need not call it first. Unsupported groups
            or missing prerequisites stop the selection rather than silently omitting data.
            max_items samples across groups in inventory order. Packages without file-level
            routes cannot be sampled. A dry run checks readiness but does not establish access."""
        _validate_max_items(max_items)
        _validate_policy(accept_data_policy, dry_run)
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
            accept_data_policy=accept_data_policy,
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
        accept_data_policy=False,
        dry_run=False,
        progress=True,
        max_items=None,
        overwrite=False,
    ):
        """Download explicit file records without automatically adding related images.

        Args:
            files: List/tuple of file-grain dictionaries or DataFrame with an explicit public imageUrl/download_url or approved controlled drs_uri.
            destination: Local destination root (string or pathlib.Path), passed directly to TDR without SDK dataset/manifest subfolders.
            credential_file: Path to your approved JSON API key for controlled files, or None for open data; pass a path, not key contents.
            accept_data_policy: Set True after accepting the TCIA data usage policy. Required for actual transfers; default False.
            dry_run: True to inspect metadata and prerequisites without a payload transfer or authentication; default False.
            progress: Show SDK preparation messages (default True). TDR streams periodic progress lines into Python/notebook output.
            max_items: Positive limit on whole series/files, not bytes, or None for the full selection. Samples keep their scope in the report.
            overwrite: Request replacement behavior; default False.

        Returns:
            DownloadReport with per-entry outcomes and selection scope. .complete describes
            only the selected sample/full selection; inspect entries and scope before claiming
            a dataset is complete. DownloadError.report retains outcomes when a transfer fails.

        Raises:
            InvalidRequestError: Selection or options are invalid.
            DownloadError: A route, prerequisite, authorization or transfer fails; inspect .report when present.
            TCIAError: A metadata request fails.
            OSError: A local input or destination cannot be read or written.

        Notes:
            Actual transfers use the downloads extra (pip-installed tcia-data-retriever).
            TDR owns authorization, retry/checksum behavior, file layout and route-specific extraction;
            supplied CLI/Python keys are used only for this invocation, without a saved copy.
            TDR automatically checks/reuses existing files. Keep the same
            destination when expanding a sample. Files accepted by existing-file checks
            count as Skipped; Synced denotes public DICOM sync jobs. For controlled inventory
            records, only download_size_bytes is an exact payload-size assertion; reported
            file_size_bytes may describe uncompressed series data. Rerun the same call with
            the same destination to retry or expand a sample; partial-file byte-range
            resume is not promised.
            max_items selects the first N records. Package or manifest rows must use
            download_dataset() or download_manifest(); source_url alone is not a file route."""
        _validate_max_items(max_items)
        _validate_policy(accept_data_policy, dry_run)
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
            accept_data_policy=accept_data_policy,
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
        accept_data_policy=False,
    ):
        _validate_policy(accept_data_policy, dry_run)
        if type(dry_run) is not bool or type(overwrite) is not bool:
            raise InvalidRequestError("dry_run and overwrite must be booleans")
        if type(progress) is not bool:
            raise InvalidRequestError("progress must be a boolean")
        scope = {
            **scope,
            "dry_run": dry_run,
            "transfer_engine": "tcia-data-retriever",
            "data_policy_accepted": accept_data_policy,
        }
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
        try:
            engine = _tdr()
            engine_missing = False
        except DownloadError:
            engine, engine_missing = None, True
        for row in rows:
            kind, problem = _route(row), None
            if kind in {"unsupported", "public_package"}:
                problem = (
                    "requires_external_tool"
                    if row.get("download_method") == "aspera_package"
                    else "unsupported"
                )
            elif engine_missing:
                problem = "requires_python_download_extra"
            elif row.get("access_level") == "controlled" and not credential_file:
                problem = "requires_credential"
            entries.append(
                {
                    "short_title": row.get("short_title"),
                    "download_id": row.get("download_id"),
                    "file_name": row.get("file_name"),
                    "manifest_item_count": row.get("manifest_item_count"),
                    "download_category": list(row.get("download_types") or []),
                    "route": kind,
                    "status": problem or "planned",
                    **_prerequisite_guidance(problem),
                    **(
                        {
                            key: row[key]
                            for key in (
                                "sample_selected_items",
                                "sample_available_items",
                                "sample_unit",
                                "sample_manifest_sha256",
                            )
                            if key in row
                        }
                        if scope.get("is_sample")
                        else {}
                    ),
                }
            )
        report = DownloadReport(tuple(entries), scope)
        if dry_run:
            return report
        blocked = [e for e in entries if e["status"] != "planned"]
        if blocked:
            reason = " ".join(dict.fromkeys(e["message"] + " " + e["next_step"] for e in blocked))
            raise DownloadError("Download cannot start. " + reason, report)
        key_path = None
        if any(row.get("access_level") == "controlled" for row in rows):
            try:
                key_path = Path(credential_file).expanduser().resolve(strict=True)
                if not key_path.is_file():
                    raise ValueError()
            except (OSError, TypeError, ValueError):
                raise DownloadError(
                    "Supply a readable controlled-access credential file", report
                ) from None
        target = Path(destination)
        jobs, file_batches = [], {}
        file_targets = set()

        def check_file_targets(items):
            # All jobs share the caller's root. Keep collision checks across groups,
            # including separate public and controlled batches, before any transfer.
            for item in items:
                identity = (
                    item.get("short_title") or "selection",
                    item.get("participant_id") or item.get("subject_id") or "",
                    item.get("study_instance_uid") or "",
                    item["file_name"],
                )
                if identity in file_targets:
                    raise DownloadError(
                        "Selection repeats a filename in the same participant/study scope"
                    )
                file_targets.add(identity)

        # Read and validate every route before launching the transfer engine.
        try:
            for i, row in enumerate(rows):
                title = _safe_name(str(row.get("short_title") or "selection"))
                if row.get("drs_uri") or entries[i]["route"] == "public_file":
                    column = "drs_uri" if row.get("drs_uri") else "imageUrl"
                    item = dict(row)
                    item[column] = (
                        row[column]
                        if column == "drs_uri"
                        else row.get("imageUrl") or row["download_url"]
                    )
                    checked = _manifest_records(row, column, [item])[0]
                    check_file_targets([checked])
                    group = (title, row["access_level"], column)
                    file_batches.setdefault(group, []).append((i, checked))
                    continue
                if manifest_inputs and i in manifest_inputs:
                    content, suffix = manifest_inputs[i]
                else:
                    url = row.get("download_url") or ""
                    parsed = _url(url)
                    suffix = Path(parsed.path).suffix.lower()
                    if parsed.hostname not in _MANIFEST_HOSTS or suffix not in {
                        ".tcia",
                        ".csv",
                        ".tsv",
                    }:
                        raise DownloadError(
                            "Manifest must come from an official TCIA download link"
                        )
                    content = _get_bytes(url)
                column, items = _manifest(content, suffix)
                if row.get("access_level") == "controlled" and column != "drs_uri":
                    raise DownloadError("Controlled downloads require a DRS manifest")
                checked = (
                    items
                    if column == "SeriesInstanceUID"
                    else _manifest_records(row, column, items)
                )
                if column != "SeriesInstanceUID":
                    check_file_targets(checked)
                jobs.append(
                    (
                        [i],
                        target,
                        _transfer_csv(checked, column),
                        len(items),
                        row.get("access_level") == "controlled",
                    )
                )
            for (_, access, column), batch in file_batches.items():
                # TDR's file route derives a job key from the source basename.
                # Separate colliding keys so similarly named participant files
                # cannot overwrite each other in the engine's job bookkeeping.
                batches = []
                for index, item in batch:
                    backend_key = Path(urlsplit(item[column]).path).stem
                    for keys, members in batches:
                        if backend_key not in keys:
                            keys.add(backend_key)
                            members.append((index, item))
                            break
                    else:
                        batches.append(({backend_key}, [(index, item)]))
                for _, members in batches:
                    selected, records_ = zip(*members)
                    jobs.append(
                        (
                            list(selected),
                            target,
                            _transfer_csv(records_, column),
                            len(records_),
                            access == "controlled",
                        )
                    )
        except Exception as error:
            for entry in entries:
                entry["status"] = "not_attempted"
            entries[i]["status"] = "failed_preflight"
            raise DownloadError(
                (
                    str(error)
                    if isinstance(error, DownloadError)
                    else "Manifest preflight failed; no transfer started"
                ),
                DownloadReport(tuple(entries), scope),
            ) from None
        if not progress:
            warnings.warn(
                "progress=False silences SDK messages; TDR transfer progress remains enabled.",
                UserWarning,
                stacklevel=2,
            )
        for indexes, folder, content, expected, controlled in jobs:
            try:
                metadata = folder / "manifests"
                metadata.mkdir(parents=True, exist_ok=True)
                manifest = metadata / (
                    "transfer-" + hashlib.sha256(content).hexdigest()[:16] + ".csv"
                )
                if manifest.is_symlink():
                    raise DownloadError("Refusing to replace a symlink")
                with tempfile.NamedTemporaryFile(dir=metadata, delete=False) as temp:
                    temp.write(content)
                    temporary = Path(temp.name)
                temporary.replace(manifest)
                for i in indexes:
                    entries[i]["destination"] = str(folder)
                    if scope.get("is_sample"):
                        entries[i]["sample_manifest"] = str(manifest)
                _notify(progress, "Starting the selected image/file transfer...")
                raw = engine.download(
                    manifest,
                    folder,
                    accept_data_policy=True,
                    credential_file=key_path if controlled else None,
                    progress="plain",
                )
                summary = _engine_summary(raw, expected)
                for i in indexes:
                    entries[i].update(
                        status="engine_completed",
                        verification="transfer_engine_report",
                        engine_summary=summary,
                    )
            except Exception as error:
                safe = None
                raw = getattr(error, "report", None)
                if isinstance(raw, dict):
                    safe = {
                        key: raw[key]
                        for key in (
                            "schema_version",
                            "status",
                            "error_code",
                            "total",
                            "downloaded",
                            "synced",
                            "skipped",
                            "failed",
                        )
                        if key in raw
                    }
                for i in indexes:
                    entries[i]["status"] = "failed"
                    if safe is not None:
                        entries[i]["engine_summary"] = safe
                for entry in entries:
                    if entry["status"] == "planned":
                        entry["status"] = "not_attempted"
                raise DownloadError(
                    "Transfer did not complete; inspect error.report for selection accounting",
                    DownloadReport(tuple(entries), scope),
                ) from None
        return DownloadReport(tuple(entries), scope)
