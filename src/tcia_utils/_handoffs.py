"""Metadata-only handoffs to separately installed transfer applications."""

import csv
from dataclasses import dataclass
import io
import json
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urlsplit

from ._downloads import _validate_max_items
from ._errors import InvalidRequestError, ProtocolError
from ._images import _same_snapshot
from ._research import records
from ._serialization import JsonExportMixin


_OPEN = {"open", "public", "open_noncommercial"}


def _public_package_url(value):
    if not isinstance(value, str) or any(ord(c) < 32 for c in value):
        return False
    try:
        url = urlsplit(value)
        return (url.scheme == "https" and url.hostname == "faspex.cancerimagingarchive.net"
                and not url.username and not url.password and url.port in {None, 443}
                and not url.fragment and url.path.rstrip("/") in {
                    "/aspera/faspex", "/aspera/faspex/public/package"}
                and set(parse_qs(url.query)) == {"context"}
                and len(parse_qs(url.query)["context"]) == 1)
    except ValueError:
        return False


def _download_ids(value):
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            parsed = value
        value = parsed if isinstance(parsed, list) else [value]
    if not isinstance(value, list) or not all(isinstance(v, (str, int)) and not isinstance(v, bool) for v in value):
        raise ProtocolError("Public file inventory has invalid download IDs")
    return {str(v) for v in value}


@dataclass(frozen=True)
class AsperaManifestReport(JsonExportMixin):
    """Saved public package/file routes, cohort coverage and observed metadata identity.

    rows contains the exact exported files. coverage retains every requested
    dataset-scoped participant, including zero matches. max_items bounds files,
    not bytes, and does not establish image/mask correspondence or source completeness.
    No application is installed or invoked and no payload is downloaded.
    """

    path: str
    rows: tuple[dict, ...]
    coverage: tuple[dict, ...]
    scope: dict
    provenance: object


class HandoffsMixin:
    def export_aspera_manifest(self, participants, destination, *, modality=None,
                               file_formats=None, max_items=None, overwrite=False):
        """Write an exact public Aspera file list for an explicit participant cohort.

        Args:
            participants: List/tuple of search_participants() records, or a DataFrame.
                Preserve participant_key, dataset_type, short_title and display_participant_id.
            destination: CSV path. Its two columns are packageUrl and packagePath,
                matching the TCIA Participant Explorer's Aspera file-list convention.
            modality: Optional modality such as "MR". None includes all indexed modalities.
            file_formats: Optional nonempty list of formats such as ["NIFTI"].
            max_items: Optional positive limit on exported files across the whole cohort.
                This bounds file count, not bytes. Omit it to export all matching files.
            overwrite: Allow replacement of an existing manifest (default False).

        Returns:
            AsperaManifestReport with exported rows, every participant's coverage,
            sample scope and observed TCIA provenance. No pandas is required.

        Raises:
            InvalidRequestError: Cohort/options/path are invalid, or an indexed file
                cannot be bound unambiguously to a current open Aspera download.
            ProtocolError: Metadata changes or an asset has inconsistent identity.
            FileExistsError: destination exists and overwrite is False.
            TCIAError: Metadata is unavailable or a request fails.

        Notes:
            Metadata queries and a local CSV write only. Install and invoke ascli
            separately, for example with TCIA_Aspera_CLI_Downloads.ipynb. This is
            not a Data Retriever manifest. Only exact file-grain package paths are
            exported; folders, archive members and guessed paths are not expanded.
            Current download links are matched by published download ID, never
            reconstructed from participant identifiers or stale artifact URLs.
        """
        _validate_max_items(max_items)
        if type(overwrite) is not bool:
            raise InvalidRequestError("overwrite must be a boolean")
        if modality is not None and (not isinstance(modality, str) or not modality.strip()):
            raise InvalidRequestError("modality must be a nonempty string")
        if file_formats is not None and (not isinstance(file_formats, (list, tuple))
                or not file_formats or any(not isinstance(v, str) or not v for v in file_formats)):
            raise InvalidRequestError("file_formats must be a nonempty sequence of strings")
        people = records(participants)
        identities = {}
        for person in people:
            fields = ("participant_key", "dataset_type", "short_title", "display_participant_id")
            if any(not isinstance(person.get(k), str) or not person[k].strip() for k in fields):
                raise InvalidRequestError("Use complete dataset-scoped participant query records")
            key = person["participant_key"]
            identity = {k: person[k] for k in fields}
            if key in identities and identities[key] != identity:
                raise InvalidRequestError("A participant key has conflicting dataset-scoped identities")
            identities[key] = identity
        if not identities:
            raise InvalidRequestError("Select at least one participant")
        path = Path(destination)
        if path.is_symlink() or (path.exists() and not overwrite):
            raise FileExistsError(path)
        _, provenance = self._observe_status()
        packages, matching, coverage = {}, {}, []
        for key, person in sorted(identities.items()):
            title, kind = person["short_title"], person["dataset_type"]
            detail = self.get_participant(key)
            _same_snapshot(detail, provenance)
            if any(detail.data["participant"].get(k) != v for k, v in person.items()):
                raise ProtocolError("The selected participant key does not match its dataset-scoped identity")
            if (title, kind) not in packages:
                plan = self.plan_downloads(title, dataset_type=kind.casefold().replace(" ", "_"))
                _same_snapshot(plan, provenance)
                packages[title, kind] = {row["download_id"]: row for row in plan.rows}
            assets = self.search_public_assets(
                short_titles=[title], participant_id=person["display_participant_id"],
                modalities=[modality] if modality else None, file_formats=file_formats, limit=500)
            for page in assets.iter_pages():
                _same_snapshot(page, provenance)
                for asset in page.items:
                    if (asset.get("short_title") != title or asset.get("dataset_type") != kind
                            or asset.get("participant_key") not in {None, key}
                            or asset.get("subject_id") != person["display_participant_id"]
                            or asset.get("subject_id_namespace") != "tcia_dataset:" + title):
                        raise ProtocolError("Public inventory changed a dataset-scoped participant identity")
                    # File inventories may omit access. The current exact download
                    # record below must establish an open package; explicit file
                    # restrictions must never be overridden by the package label.
                    if asset.get("access_level") not in _OPEN | {None, "unknown", "unresolved"}:
                        raise InvalidRequestError("Controlled access cannot enter a public Aspera manifest")
                    if asset.get("asset_granularity") != "file":
                        raise InvalidRequestError("An exact file-grain inventory is required; package/folder selection is not supported")
                    candidates = [r for i, r in packages[title, kind].items()
                                  if i in _download_ids(asset.get("download_id"))]
                    urls = {r.get("download_url") for r in candidates
                            if r.get("download_method") == "aspera_package"
                            and r.get("access_level") in _OPEN
                            and _public_package_url(r.get("download_url"))}
                    route = asset.get("package_path")
                    if (len(candidates) != 1 or len(urls) != 1 or not isinstance(route, str) or not route
                            or any(ord(c) < 32 for c in route) or "\\" in route
                            or ".." in PurePosixPath(route).parts or route.startswith(("-", "@"))
                            or route.endswith("/")):
                        raise InvalidRequestError(f"No unambiguous current public Aspera file route for {title}/{person['display_participant_id']}")
                    url = next(iter(urls))
                    record = {**person, "asset_id": asset.get("asset_id"),
                              "access_level": candidates[0]["access_level"],
                              "license_label": candidates[0].get("license_label"),
                              "file_name": asset.get("file_name"), "size_bytes": asset.get("size_bytes"),
                              "packageUrl": url, "packagePath": route}
                    token = (url, route)
                    if token in matching and matching[token]["participant_key"] != key:
                        raise ProtocolError("The same package file is linked to conflicting participants")
                    matching[token] = record
            coverage.append(dict(person))
        all_rows = sorted(matching.values(), key=lambda r: (r["short_title"], r["display_participant_id"], r["packagePath"]))
        rows = all_rows if max_items is None else all_rows[:max_items]
        for item in coverage:
            item["matching_files"] = sum(r["participant_key"] == item["participant_key"] for r in all_rows)
            item["exported_files"] = sum(r["participant_key"] == item["participant_key"] for r in rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Validate the entire selection before writing the application handoff.
        with io.StringIO(newline="") as buffer:
            writer = csv.DictWriter(buffer, fieldnames=["packageUrl", "packagePath"], extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            content = buffer.getvalue()
        with path.open("w" if overwrite else "x", encoding="utf-8", newline="") as output:
            output.write(content)
        return AsperaManifestReport(str(path), tuple(rows), tuple(coverage), {
            "requested_participants": len(identities), "matching_files": len(all_rows),
            "exported_files": len(rows), "max_items": max_items,
            "is_sample": len(rows) < len(all_rows), "modality": modality,
            "file_formats": list(file_formats) if file_formats else None,
            "metadata_snapshot": provenance.release_fingerprint,
            "scope_note": "Exact indexed public package files; not proof of complete dataset coverage."}, provenance)
