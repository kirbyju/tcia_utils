"""Dataset-scoped image selections from an existing participant cohort."""

from dataclasses import dataclass

from ._downloads import (
    DownloadError,
    _notify,
    _validate_max_items,
    _validate_resume,
    _validate_policy,
)
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


class ParticipantImagesMixin:
    def plan_participant_images(self, participants, *, modality, file_format, progress=True):
        """Find public images for exact dataset-scoped participants, without payload transfers.

        This helper selects non-DICOM files using TCIA's public asset metadata.
        Public DICOM series queries belong to IDC's separate tools. Selections use
        explicit public file routes, never arbitrary source URLs. Missing routes
        remain unresolved and block execution. Coverage includes zero-match subjects.
        """
        if not isinstance(modality, str) or not modality.strip():
            raise InvalidRequestError("modality must be a nonempty string")
        if isinstance(file_format, str) and file_format.casefold() == "dicom":
            raise InvalidRequestError(
                "Public DICOM series queries are provided by IDC; use idc-index directly. "
                "Download selected series with download_manifest()."
            )
        if not isinstance(file_format, str) or file_format.casefold() != "non-dicom":
            raise InvalidRequestError("file_format must be non-DICOM for TCIA file planning")
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
        groups = {}
        for (kind, title, person), row in identities.items():
            groups.setdefault((kind, title), {})[person] = row
        for (kind, title), people in groups.items():
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
                "notes": notes,
                "scope_note": "Matching TCIA public non-DICOM file metadata for exact dataset-scoped participants; zero-match subjects and unresolved routes remain explicit. Controlled files are not included.",
            },
            provenance,
        )

    def download_images(
        self,
        plan,
        destination,
        *,
        max_items=None,
        dry_run=False,
        overwrite=False,
        progress=True,
        resume=False,
        accept_data_policy=False,
    ):
        """Download a participant image plan through TDR; resume=True requests skip-existing checks."""
        _validate_resume(resume)
        _validate_policy(accept_data_policy, dry_run)
        if not isinstance(plan, ParticipantImagePlan):
            raise InvalidRequestError(
                "Supply a ParticipantImagePlan from plan_participant_images()"
            )
        _validate_max_items(max_items)
        _notify(progress, "Preparing the selected participant images...")
        if plan.unresolved:
            raise DownloadError(
                "Image selection has unresolved file routes; inspect plan.unresolved before downloading"
            )
        if not plan.rows:
            raise InvalidRequestError(
                "No matching public images were found for these participants and filters"
            )
        return self._download(
            [dict(row) for row in plan.rows],
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
            resume=resume,
            accept_data_policy=accept_data_policy,
            max_items=max_items,
        )
