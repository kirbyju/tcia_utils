"""Dataset-scoped image selections from an existing participant cohort."""

from dataclasses import dataclass
from pathlib import Path
import csv
import hashlib
import io
import math

from ._downloads import DownloadError, _get_bytes, _manifest, _notify, _url, _validate_max_items
from ._errors import InvalidRequestError, ProtocolError
from ._research import records
from ._results import _frame


@dataclass(frozen=True)
class ParticipantImagePlan:
    rows: tuple[dict, ...]
    coverage: tuple[dict, ...]
    unresolved: tuple[dict, ...]
    scope: dict
    provenance: object

    def to_dataframe(self):
        return _frame(
            self.rows,
            columns=(
                None
                if self.rows
                else [
                    "short_title",
                    "participant_id",
                    "modality",
                    "file_format",
                    "file_name",
                    "series_instance_uid",
                    "instance_count",
                    "size_bytes",
                    "python_supported",
                    "download_representation",
                ]
            ),
        )

    def coverage_to_dataframe(self):
        return _frame(
            self.coverage,
            columns=(
                None
                if self.coverage
                else ["short_title", "participant_id", "matching_items", "downloadable_items"]
            ),
        )


def _same_snapshot(result, provenance):
    if (result.provenance.release_fingerprint, result.provenance.installed_profile) != (
        provenance.release_fingerprint,
        provenance.installed_profile,
    ):
        raise ProtocolError("Metadata changed during image planning; restart the cohort query")


def _count(value):
    try:
        return int(value) if value is not None and math.isfinite(float(value)) else None
    except (TypeError, ValueError):
        return None


class ParticipantImagesMixin:
    def plan_participant_images(self, participants, *, modality, file_format, progress=True):
        """Find public images for exact dataset-scoped participants, without payload transfers.

        file_format is DICOM or non-DICOM. DICOM selections are anchored to an
        official TCIA manifest and exact IDC PatientID. Non-DICOM selections use
        explicit public file routes, never arbitrary source URLs. Missing routes
        remain unresolved and block execution. Coverage includes zero-match subjects.
        """
        if not isinstance(modality, str) or not modality.strip():
            raise InvalidRequestError("modality must be a nonempty string")
        if not isinstance(file_format, str) or file_format.casefold() not in {"dicom", "non-dicom"}:
            raise InvalidRequestError("file_format must be DICOM or non-DICOM")
        requested = records(participants)
        identities = {}
        for row in requested:
            kind = str(row.get("dataset_type") or "").casefold().replace("_", " ")
            title, person, key = (
                row.get("short_title"),
                row.get("display_participant_id"),
                row.get("participant_key"),
            )
            if kind not in {"collection", "analysis result"} or not all(
                isinstance(v, str) and v.strip() for v in (title, person, key)
            ):
                raise InvalidRequestError(
                    "Use participant query records with dataset_type, short_title, display_participant_id and participant_key"
                )
            identity = (kind, title, person)
            if identity in identities and identities[identity]["participant_key"] != key:
                raise InvalidRequestError(
                    "A dataset-scoped participant has conflicting participant keys"
                )
            identities[identity] = row
        _notify(progress, "Planning images for the selected participants...")
        _, provenance = self._observe_status()
        rows, unresolved, notes = [], [], []
        manifest_sources = []
        groups = {}
        for (kind, title, person), row in identities.items():
            groups.setdefault((kind, title), {})[person] = row
        index_version = None
        for (kind, title), people in groups.items():
            if file_format.casefold() == "dicom":
                plan = self.plan_downloads(
                    title, dataset_type=kind.replace(" ", "_"), access_levels=["open"]
                )
                _same_snapshot(plan, provenance)
                manifest_uids = {}
                for group in plan.rows:
                    if group.get("download_method") not in {"dicom_manifest", "tabular_manifest"}:
                        continue
                    url = _url(group.get("download_url"))
                    if url.hostname not in {
                        "www.cancerimagingarchive.net",
                        "cancerimagingarchive.net",
                        "wiki.cancerimagingarchive.net",
                    }:
                        raise DownloadError(
                            "DICOM manifest must use an official TCIA download link"
                        )
                    suffix = Path(url.path).suffix.lower()
                    if suffix not in {".tcia", ".csv", ".tsv"}:
                        raise DownloadError("Unsupported DICOM manifest format")
                    content = _get_bytes(group["download_url"])
                    column, items = _manifest(content, suffix)
                    manifest_sources.append(
                        {
                            "short_title": title,
                            "download_id": group["download_id"],
                            "source_url": group["download_url"],
                            "sha256": hashlib.sha256(content).hexdigest(),
                        }
                    )
                    if column == "SeriesInstanceUID":
                        for item in items:
                            manifest_uids.setdefault(item[column], group)
                if not manifest_uids:
                    notes.append(f"{title}: no supported public DICOM manifest found")
                    continue
                client = self._get_imaging_client(progress)
                index_version = client.get_idc_version()
                missing = sorted(set(manifest_uids) - set(client.index["SeriesInstanceUID"]))
                if missing:
                    unresolved.append(
                        {
                            "short_title": title,
                            "reason": "published_series_absent_from_public_index",
                            "series_instance_uids": missing,
                        }
                    )
                selected = client.index.loc[
                    client.index["SeriesInstanceUID"].isin(manifest_uids)
                    & client.index["PatientID"].isin(people)
                    & client.index["Modality"].str.casefold().eq(modality.casefold())
                ]
                for item in selected.to_dict("records"):
                    person, uid = item["PatientID"], item["SeriesInstanceUID"]
                    group = manifest_uids[uid]
                    size_mb = item.get("series_size_MB")
                    rows.append(
                        {
                            "dataset_type": group["dataset_type"],
                            "short_title": title,
                            "participant_id": person,
                            "participant_key": people[person]["participant_key"],
                            "study_instance_uid": item.get("StudyInstanceUID"),
                            "series_instance_uid": uid,
                            "modality": item["Modality"],
                            "file_format": "DICOM",
                            "asset_granularity": "series",
                            "instance_count": _count(item.get("instanceCount")),
                            "size_bytes": (
                                _count(float(size_mb) * 1_000_000) if size_mb is not None else None
                            ),
                            "access_level": group["access_level"],
                            "identifier_source": "idc-index",
                            "download_representation": "public_dicom_series",
                            "python_supported": True,
                            "source_download_id": group["download_id"],
                        }
                    )
            else:
                page = self.search_public_assets(
                    short_titles=[title], modalities=[modality], limit=500
                )
                for current in page.iter_pages():
                    _same_snapshot(current, provenance)
                    for asset in current.items:
                        if (
                            str(asset.get("dataset_type") or "").casefold().replace("_", " ")
                            != kind
                            or asset.get("short_title") != title
                            or str(asset.get("modality") or "").casefold() != modality.casefold()
                            or str(asset.get("file_format") or "").casefold() == "dicom"
                        ):
                            continue
                        person = asset.get("subject_id")
                        if person not in people:
                            continue
                        if asset.get("subject_id_namespace") != f"tcia_dataset:{title}":
                            unresolved.append(
                                {
                                    "short_title": title,
                                    "participant_id": person,
                                    "asset_id": asset.get("asset_id"),
                                    "reason": "unverified_participant_namespace",
                                }
                            )
                            continue
                        route = (
                            asset.get("asset_granularity") == "file"
                            and bool(asset.get("imageUrl"))
                            and asset.get("access_level")
                            in {"open", "public", "open_noncommercial"}
                        )
                        record = {
                            **asset,
                            "participant_id": person,
                            "participant_key": people[person]["participant_key"],
                            "python_supported": bool(route),
                            "file_name": asset.get("download_file_name") or asset.get("file_name"),
                            "download_id": asset.get("asset_id"),
                            "download_method": "direct_file",
                        }
                        rows.append(record)
                        if not route:
                            unresolved.append(
                                {
                                    "short_title": title,
                                    "participant_id": person,
                                    "asset_id": asset.get("asset_id"),
                                    "reason": "no_explicit_public_file_download_route; update the API or use official collection instructions",
                                }
                            )
        unique = {}
        for row in rows:
            key = (
                row["dataset_type"],
                row["short_title"],
                row["participant_id"],
                row.get("series_instance_uid") or row.get("asset_id"),
            )
            unique.setdefault(key, row)
        rows = sorted(
            unique.values(),
            key=lambda r: (
                r["short_title"],
                r["participant_id"],
                str(r.get("series_instance_uid") or r.get("asset_id")),
            ),
        )
        coverage = []
        for (kind, title, person), participant in identities.items():
            matched = [
                r
                for r in rows
                if str(r["dataset_type"]).casefold().replace("_", " ") == kind
                and r["short_title"] == title
                and r["participant_id"] == person
            ]
            coverage.append(
                {
                    "dataset_type": participant["dataset_type"],
                    "short_title": title,
                    "participant_id": person,
                    "participant_key": participant["participant_key"],
                    "matching_items": len(matched),
                    "downloadable_items": sum(bool(r["python_supported"]) for r in matched),
                }
            )
        return ParticipantImagePlan(
            tuple(rows),
            tuple(coverage),
            tuple(unresolved),
            {
                "modality": modality,
                "file_format": file_format,
                "requested_participants": len(identities),
                "selected_items": len(rows),
                "metadata_snapshot": provenance.release_fingerprint,
                "imaging_index_version": index_version,
                "manifest_sources": manifest_sources,
                "notes": notes,
                "scope_note": "All indexed matching public images for exact dataset-scoped participants; zero-match subjects and unresolved routes remain explicit. Controlled files are not included.",
            },
            provenance,
        )

    def download_images(
        self, plan, destination, *, max_items=None, dry_run=False, overwrite=False, progress=True
    ):
        """Execute a participant image plan through the existing Python download adapters."""
        if not isinstance(plan, ParticipantImagePlan):
            raise InvalidRequestError(
                "Supply a ParticipantImagePlan from plan_participant_images()"
            )
        _validate_max_items(max_items)
        _notify(progress, "Preparing the selected participant images...")
        if plan.unresolved:
            raise DownloadError(
                "Image selection has unresolved routes or public-index coverage; inspect plan.unresolved before downloading"
            )
        if not plan.rows:
            raise InvalidRequestError(
                "No matching public images were found for these participants and filters"
            )
        rows, inputs, dicom = [], {}, {}
        for record in plan.rows:
            if record.get("asset_granularity") == "series" and record.get("file_format") == "DICOM":
                key = (record["dataset_type"], record["short_title"], record["access_level"])
                dicom.setdefault(key, []).append(record["series_instance_uid"])
            else:
                rows.append(dict(record))
        for (kind, title, access), uids in dicom.items():
            stream = io.StringIO(newline="")
            writer = csv.writer(stream)
            writer.writerow(["SeriesInstanceUID"])
            writer.writerows([[uid] for uid in uids])
            content = stream.getvalue().encode()
            inputs[len(rows)] = (content, ".csv")
            rows.append(
                {
                    "dataset_type": kind,
                    "short_title": title,
                    "access_level": access,
                    "download_id": "cohort-" + hashlib.sha256(content).hexdigest()[:12],
                    "download_method": "dicom_manifest",
                    "requirement_status": "declared",
                    "manifest_item_count": len(uids),
                }
            )
        return self._download(
            rows,
            destination,
            scope={
                **plan.scope,
                "participant_image_selection": True,
                "coverage": list(plan.coverage),
            },
            credential_file=None,
            dry_run=dry_run,
            progress=progress,
            overwrite=overwrite,
            manifest_inputs=inputs,
            max_items=max_items,
        )
