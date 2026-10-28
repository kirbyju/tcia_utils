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
import shutil
import subprocess
import tempfile
import warnings

import httpx
from ._errors import TCIAError, InvalidRequestError
from ._results import _frame
from ._research import records

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

    def to_dataframe(self):
        return _frame(self.rows)


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
        return bool(self.entries) and all(
            r["status"] in {"downloaded", "verified_existing", "engine_completed"}
            for r in self.entries
        )

    def to_dataframe(self):
        return _frame(self.entries)


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
        if len(columns) != 1 or not rows:
            raise ValueError()
        column = next(iter(columns))
        if any(not row.get(column) for row in rows):
            raise ValueError()
        if column == "drs_uri" and any(not r[column].startswith("drs://") for r in rows):
            raise ValueError()
        return column, rows
    except (UnicodeError, ValueError, csv.Error):
        raise DownloadError("Response is not a supported nonempty manifest") from None


def _route(row):
    access = row.get("access_level")
    if access not in _OPEN | {"controlled"}:
        return "unsupported"
    if row.get("controlled_access") is True and access in _OPEN:
        return "unsupported"
    if row.get("drs_uri"):
        return "controlled_files" if access == "controlled" else "unsupported"
    url = row.get("download_url") or row.get("imageUrl")
    if not url:
        return "unsupported"
    p = _url(url)
    suffix = Path(p.path).suffix.lower()
    if access == "controlled":
        return (
            "controlled_files"
            if suffix in {".csv", ".tsv"} and p.hostname in _MANIFEST_HOSTS
            else "unsupported"
        )
    if p.hostname == "faspex.cancerimagingarchive.net":
        return "public_package"
    if suffix == ".tcia" or (
        suffix in {".csv", ".tsv"} and row.get("requirements_label") == "TCIA Data Retriever"
    ):
        return "public_dicom" if suffix == ".tcia" else "public_manifest"
    return "public_file"


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


def _engine(command):
    try:
        result = subprocess.run(
            command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except OSError:
        raise DownloadError("Download engine could not be started") from None
    if result.returncode:
        # Never echo external stdout/stderr: legacy engines may log credentials.
        raise DownloadError(
            f"Download engine reported failure (exit {result.returncode}); no completion is claimed"
        )


class DownloadsMixin:
    def plan_downloads(
        self, short_title, *, download_ids=None, access_levels=None, dataset_type=None
    ):
        """Collect download groups with categories, access, routes, and requirements.

        This queries metadata only. Filters define the entire requested scope;
        nothing is silently omitted because a transfer prerequisite is missing.
        """
        page = self.list_downloads(short_title, limit=100)
        rows = [dict(row) for part in page.iter_pages() for row in part.items]
        types = {r.get("dataset_type") for r in rows}
        if dataset_type is None and len(types) > 1:
            raise InvalidRequestError(
                "This short title identifies multiple dataset types; specify dataset_type"
            )
        if dataset_type is not None:
            if dataset_type not in {"collection", "analysis_result"}:
                raise InvalidRequestError("dataset_type must be collection or analysis_result")
            label = dataset_type.replace("_", " ")
            rows = [r for r in rows if str(r.get("dataset_type", "")).casefold() == label]
        wanted = set(download_ids) if download_ids is not None else None
        if wanted is not None and (isinstance(download_ids, str) or not wanted):
            raise InvalidRequestError("download_ids must be a nonempty sequence")
        if wanted is not None and wanted - {r["download_id"] for r in rows}:
            raise InvalidRequestError(
                "A requested download ID is not in the current dataset inventory"
            )
        if access_levels is not None and (isinstance(access_levels, str) or not access_levels):
            raise InvalidRequestError("access_levels must be a nonempty sequence")
        selected = []
        for row in rows:
            if wanted is not None and row["download_id"] not in wanted:
                continue
            if access_levels is not None and not (
                row.get("access_level") in access_levels
                or ("open" in access_levels and row.get("access_level") in _OPEN)
            ):
                continue
            kind = _route(row)
            row.update(
                download_category="; ".join(row.get("download_types") or []),
                python_route=kind,
                data_retriever_supported=kind
                in {"public_dicom", "public_manifest", "controlled_files"},
            )
            selected.append(row)
        return DownloadPlan(
            tuple(selected),
            page.provenance,
            {
                "short_title": short_title,
                "download_ids": (list(download_ids) if download_ids is not None else None),
                "access_levels": (list(access_levels) if access_levels is not None else None),
                "dataset_type": dataset_type,
                "selected_groups": len(selected),
                "inventory_groups": len(rows),
                "metadata_snapshot": page.provenance.release_fingerprint,
            },
        )

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

    @staticmethod
    def data_retriever_command(
        manifest, destination, *, executable="TCIA_Data_Retriever", credential_file=None
    ):
        command = [
            str(executable),
            "--cli",
            "--input",
            str(manifest),
            "--output",
            str(destination),
            "--skip-existing",
        ]
        if credential_file:
            command += ["--auth", str(credential_file)]
        return command

    def download_dataset(
        self,
        short_title,
        destination,
        *,
        download_ids=None,
        access_levels=None,
        credential_file=None,
        backend="python",
        dry_run=False,
        overwrite=False,
        dataset_type=None,
        retriever="TCIA_Data_Retriever",
        package_client="ascli",
    ):
        """Download selected groups. Preflight all groups before transferring any.

        All groups are in scope unless filtered explicitly. Missing authorization
        or engine support stops the entire selection rather than silently dropping
        data. dry_run only queries metadata and checks local prerequisites.
        """
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
            backend=backend,
            dry_run=dry_run,
            overwrite=overwrite,
            retriever=retriever,
            package_client=package_client,
        )

    def download_files(
        self,
        files,
        destination,
        *,
        credential_file=None,
        backend="python",
        dry_run=False,
        overwrite=False,
        retriever="TCIA_Data_Retriever",
    ):
        """Download an explicit file-grain selection, without adding related images."""
        rows = records(files)
        return self._download(
            rows,
            destination,
            scope={"explicit_file_records": len(rows)},
            credential_file=credential_file,
            backend=backend,
            dry_run=dry_run,
            overwrite=overwrite,
            retriever=retriever,
            package_client="ascli",
        )

    def _download(
        self,
        rows,
        destination,
        *,
        scope,
        credential_file,
        backend,
        dry_run,
        overwrite,
        retriever,
        package_client,
    ):
        if not isinstance(dry_run, bool) or not isinstance(overwrite, bool):
            raise InvalidRequestError("dry_run and overwrite must be booleans")
        scope = {**scope, "dry_run": dry_run}
        if backend not in {"python", "data_retriever"}:
            raise InvalidRequestError("backend must be python or data_retriever")
        if not rows:
            raise InvalidRequestError("No download records were selected")
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
            if backend == "data_retriever" and route not in {
                "public_dicom",
                "public_manifest",
                "controlled_files",
            }:
                problem = "unsupported_by_data_retriever"
            if backend == "data_retriever" and not shutil.which(str(retriever)):
                problem = "requires_data_retriever"
            if (
                backend == "python"
                and route == "public_dicom"
                and importlib.util.find_spec("idc_index") is None
            ):
                problem = "requires_python_download_extra"
            if (
                backend == "python"
                and route == "public_package"
                and not shutil.which(str(package_client))
            ):
                problem = "requires_package_transfer_tools"
            entries.append(
                {
                    "short_title": row.get("short_title"),
                    "download_id": row.get("download_id"),
                    "file_name": row.get("file_name"),
                    "download_category": "; ".join(row.get("download_types") or []),
                    "route": route,
                    "status": problem or "planned",
                }
            )
        report = DownloadReport(tuple(entries), scope)
        if dry_run:
            return report
        if any(e["status"] != "planned" for e in entries):
            raise DownloadError(
                "Selection is not ready. Inspect error.report.to_dataframe(); install the downloads extra, configure package tools, or supply approved credentials as indicated.",
                report,
            )
        if any(r.get("access_level") == "controlled" for r in rows):
            key = _credential(credential_file)
        target = Path(destination)
        tokens = {}
        prepared = {}
        imaging_clients = {}
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
                manifest = self.save_manifests([row], folder / "manifests")
                column, items = _manifest(manifest.files[0].read_bytes(), manifest.files[0].suffix)
                for item in items:
                    for name_key in (
                        "file_name",
                        "CTDC File Name",
                        "File Name",
                        "filename",
                        "name",
                    ):
                        if item.get(name_key):
                            _safe_name(item[name_key])
                            break
                    if column == "drs_uri":
                        _drs(item[column])
                    elif column == "imageUrl":
                        _url(item[column])
                if backend == "python" and column == "SeriesInstanceUID":
                    try:
                        from idc_index import IDCClient
                    except ImportError:
                        raise DownloadError(
                            "Install tcia_utils[downloads] for public DICOM downloads"
                        ) from None
                    client = IDCClient()
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
                if backend == "data_retriever":
                    if row.get("drs_uri"):
                        folder.mkdir(parents=True, exist_ok=True)
                        manifest = folder / "selection.csv"
                        with manifest.open("w", newline="") as f:
                            w = csv.writer(f)
                            w.writerow(["drs_uri"])
                            w.writerow([row["drs_uri"]])
                    else:
                        manifest = prepared[i][0].files[0]
                    _engine(
                        self.data_retriever_command(
                            manifest,
                            folder,
                            executable=retriever,
                            credential_file=(
                                credential_file if row.get("access_level") == "controlled" else None
                            ),
                        )
                    )
                    entries[i]["status"] = "engine_completed"
                elif route == "public_package":
                    folder.mkdir(parents=True, exist_ok=True)
                    _engine(
                        [
                            str(package_client),
                            "--format=json",
                            f"--to-folder={folder}",
                            "faspex5",
                            "packages",
                            "receive",
                            f"--url={row['download_url']}",
                        ]
                    )
                    entries[i]["status"] = "engine_completed"
                elif route == "public_file":
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
                        client.download_dicom_series(
                            seriesInstanceUID=uids,
                            downloadDir=str(folder),
                            quiet=True,
                            use_s5cmd_sync=True,
                        )
                        entries[i]["status"] = "engine_completed"
                    else:
                        child = []
                        for item in items:
                            record = {
                                **item,
                                "short_title": row["short_title"],
                                "download_id": row["download_id"],
                                "access_level": row["access_level"],
                            }
                            if column == "imageUrl":
                                record["file_name"] = Path(urlsplit(item[column]).path).name
                            for checksum_key in ("md5", "MD5", "md5sum", "MD5sum"):
                                if item.get(checksum_key):
                                    record.update(
                                        checksum=item[checksum_key],
                                        checksum_algorithm="md5",
                                    )
                                    break
                            for name_key in (
                                "file_name",
                                "CTDC File Name",
                                "File Name",
                                "filename",
                                "name",
                            ):
                                if item.get(name_key):
                                    record["file_name"] = item[name_key]
                                    break
                            child.append(record)
                        nested = self._download(
                            child,
                            target,
                            scope={"manifest_items": len(child)},
                            credential_file=credential_file,
                            backend="python",
                            dry_run=False,
                            overwrite=overwrite,
                            retriever=retriever,
                            package_client=package_client,
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
