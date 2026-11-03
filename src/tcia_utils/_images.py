"""Dataset-scoped image selections from an existing participant cohort."""

from dataclasses import dataclass, field
import csv
import hashlib
import io

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
    providers: dict = field(default_factory=dict)

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
                else [
                    "short_title",
                    "participant_id",
                    "matching_items",
                    "downloadable_items",
                ]
            ),
        )


def _same_snapshot(result, provenance):
    if (result.provenance.release_fingerprint, result.provenance.installed_profile) != (
        provenance.release_fingerprint,
        provenance.installed_profile,
    ):
        raise ProtocolError(
            "Metadata changed during image planning; restart the cohort query"
        )


class ParticipantImagesMixin:
    def plan_participant_images(
        self, participants, *, modality, file_format, progress=True
    ):
        """Get one shared public-image plan; provider queries run on the service."""
        if not isinstance(modality, str) or not modality.strip():
            raise InvalidRequestError("modality must be a nonempty string")
        if not isinstance(file_format, str) or file_format.casefold() not in {
            "dicom",
            "non-dicom",
        }:
            raise InvalidRequestError("file_format must be DICOM or non-DICOM")
        file_format = "DICOM" if file_format.casefold() == "dicom" else "non-DICOM"
        requested = records(participants)
        identities = {}
        for row in requested:
            key = row.get("participant_key")
            if not all(
                isinstance(row.get(k), str) and row[k].strip()
                for k in (
                    "participant_key",
                    "dataset_type",
                    "short_title",
                    "display_participant_id",
                )
            ):
                raise InvalidRequestError(
                    "Use dataset-scoped participant query records with participant_key"
                )
            if key in identities and any(
                identities[key][k] != row[k]
                for k in ("dataset_type", "short_title", "display_participant_id")
            ):
                raise InvalidRequestError(
                    "A participant key has conflicting dataset-scoped identities"
                )
            identities[key] = row
        _notify(progress, "Planning images for the selected participants...")
        _, provenance = self._observe_status()
        keys = sorted(identities)
        rows, coverage, unresolved, providers = [], [], [], {}
        for start in range(0, len(keys), 100):
            batch = keys[start : start + 100]
            result = self._result(
                "POST",
                "/v2/participants/download-plan",
                json_body={
                    "participant_keys": batch,
                    "modality": modality,
                    "file_format": file_format,
                },
                capability="participant_search",
                required_fields={
                    "plan_version": int,
                    "rows": list,
                    "coverage": list,
                    "unresolved": list,
                    "scope": dict,
                    "providers": dict,
                    "complete": bool,
                },
            )
            _same_snapshot(result, provenance)
            data = result.data
            if (
                data["plan_version"] != 1
                or data["scope"].get("requested_participants") != len(batch)
                or data["scope"].get("selected_items") != len(data["rows"])
                or data["scope"].get("file_format") != file_format
                or str(data["scope"].get("modality", "")).casefold()
                != modality.casefold()
                or data["scope"].get("metadata_snapshot")
                not in {None, provenance.release_fingerprint}
            ):
                raise ProtocolError(
                    "Shared image plan has inconsistent scope or metadata snapshot"
                )
            covered = set()
            for item in data["coverage"]:
                key = item.get("participant_key")
                if key not in batch or key in covered:
                    raise ProtocolError(
                        "Shared image plan has invalid participant coverage"
                    )
                expected = identities[key]
                if (
                    str(item.get("dataset_type", "")).casefold().replace("_", " ")
                    != expected["dataset_type"].casefold().replace("_", " ")
                    or item.get("short_title") != expected["short_title"]
                    or item.get("participant_id") != expected["display_participant_id"]
                ):
                    raise ProtocolError(
                        "Shared image plan changed a dataset-scoped participant identity"
                    )
                covered.add(key)
            if covered != set(batch):
                raise ProtocolError("Shared image plan omitted participant coverage")
            for item in data["rows"]:
                key = item.get("participant_key")
                if key not in batch:
                    raise ProtocolError(
                        "Shared image plan includes an unrequested participant"
                    )
                expected = identities[key]
                if (
                    item.get("short_title") != expected["short_title"]
                    or item.get("participant_id") != expected["display_participant_id"]
                    or str(item.get("dataset_type", "")).casefold().replace("_", " ")
                    != expected["dataset_type"].casefold().replace("_", " ")
                    or str(item.get("modality", "")).casefold() != modality.casefold()
                    or item.get("access_level")
                    not in {"open", "public", "open_noncommercial"}
                    or (
                        file_format == "DICOM"
                        and (
                            item.get("file_format") != "DICOM"
                            or not item.get("series_instance_uid")
                        )
                    )
                    or (
                        file_format == "non-DICOM"
                        and item.get("file_format") == "DICOM"
                    )
                ):
                    raise ProtocolError(
                        "Shared image plan includes out-of-scope images"
                    )
            if any(r.get("participant_key") not in batch for r in data["unresolved"]):
                raise ProtocolError(
                    "Shared image plan includes unrequested unresolved identities"
                )
            if data["complete"] != (not data["unresolved"]):
                raise ProtocolError(
                    "Shared image plan has inconsistent completion accounting"
                )
            for item in data["coverage"]:
                matches = [
                    r
                    for r in data["rows"]
                    if r["participant_key"] == item["participant_key"]
                ]
                if (
                    any(type(r.get("python_supported")) is not bool for r in matches)
                    or item.get("matching_items") != len(matches)
                    or item.get("downloadable_items")
                    != sum(r["python_supported"] for r in matches)
                ):
                    raise ProtocolError(
                        "Shared image plan has inconsistent coverage counts"
                    )
            if file_format == "DICOM" and data["rows"]:
                provider = data["providers"].get("IDC")
                if not isinstance(provider, dict) or any(
                    not isinstance(provider.get(k), str) or not provider[k]
                    for k in ("idc_version", "idc_index_data_version", "api_version")
                ):
                    raise ProtocolError(
                        "Shared DICOM plan lacks its provider data version"
                    )
            for provider, version in data["providers"].items():
                if provider in providers and providers[provider] != version:
                    raise ProtocolError(
                        "Provider metadata changed between image-plan batches; restart the query"
                    )
                providers[provider] = version
            rows.extend(data["rows"])
            coverage.extend(data["coverage"])
            unresolved.extend(data["unresolved"])
        return ParticipantImagePlan(
            tuple(rows),
            tuple(coverage),
            tuple(unresolved),
            {
                "modality": modality,
                "file_format": file_format,
                "requested_participants": len(keys),
                "selected_items": len(rows),
                "metadata_snapshot": provenance.release_fingerprint,
                "plan_version": 1,
                "provider_versions": providers,
                "scope_note": "Shared dataset-scoped public image plan; controlled images are excluded.",
            },
            provenance,
            providers,
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
        rows, inputs, groups = [], {}, {}
        for item in plan.rows:
            if item.get("file_format") == "DICOM":
                groups.setdefault(
                    (item["dataset_type"], item["short_title"], item["access_level"]),
                    [],
                ).append(item["series_instance_uid"])
            else:
                rows.append(dict(item))
        for (kind, title, access), uids in groups.items():
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
            resume=resume,
            accept_data_policy=accept_data_policy,
            max_items=max_items,
            manifest_inputs=inputs,
        )
