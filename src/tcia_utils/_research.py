"""Reusable cohort preparation, retaining exact dataset-scoped identity."""

from collections.abc import Mapping
from ._errors import InvalidRequestError, ProtocolError
from ._serialization import JsonExportMixin
from ._results import Result, Completeness


def records(value):
    if hasattr(value, "to_dict") and not isinstance(value, (Mapping, JsonExportMixin)):
        value = value.to_dict("records")
    if not isinstance(value, (list, tuple)) or any(not isinstance(r, Mapping) for r in value):
        raise InvalidRequestError("Supply a list of records or a DataFrame")
    return [dict(r) for r in value]


class ResearchMixin:
    """Checked dataset and clinical enrichment of explicit record selections."""
    def describe_datasets(self, datasets):
        """Enrich explicit dataset records with their full published metadata.

        Args:
            datasets: List/tuple of dictionaries or DataFrame containing short_title and dataset_type.

        Returns:
            Result with .data["datasets"], notes and one consistent snapshot. data_categories
            retains the published download categories; no pandas is needed for record input/output.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            Search already returns categories. Use this helper when you need fuller detail.
            Ambiguous dataset identity or snapshot drift raises instead of guessing. External
            resource labels are not reclassified as TCIA download categories."""
        rows = records(datasets)
        _, provenance = self._observe_status()
        cache, described, notes = {}, [], []
        for row in rows:
            title, kind = row.get("short_title"), row.get("dataset_type")
            if not isinstance(title, str) or not title.strip() or not isinstance(kind, str):
                raise InvalidRequestError("Each dataset needs short_title and dataset_type")
            identity = kind.casefold().replace("_", " ")
            if identity not in {"collection", "analysis result"}:
                raise InvalidRequestError("Unrecognized dataset_type")
            if title not in cache:
                result = self.get_dataset(title)
                if (result.provenance.release_fingerprint, result.provenance.installed_profile) != (
                    provenance.release_fingerprint,
                    provenance.installed_profile,
                ):
                    raise ProtocolError(
                        "Metadata changed during dataset enrichment; restart the query"
                    )
                cache[title] = result.data["datasets"]
                notes.extend(f"{title}: {note}" for note in result.notes)
            matches = [
                item
                for item in cache[title]
                if item.get("short_title") == title
                and str(item.get("dataset_type", "")).casefold().replace("_", " ") == identity
            ]
            if len(matches) != 1:
                raise ProtocolError("Dataset detail does not resolve the selected dataset identity")
            detail = matches[0]
            categories = detail.get("download_types")
            if not isinstance(categories, list) or any(not isinstance(v, str) for v in categories):
                raise ProtocolError("Dataset detail is missing its published download categories")
            described.append({**row, **detail, "data_categories": list(categories)})
        return Result(
            {"datasets": described},
            tuple(dict.fromkeys(notes)),
            provenance,
            Completeness("complete", "All explicitly selected dataset records enriched"),
        )

    def clinical_for_participants(self, participants, *, include_inferred=True):
        """Enrich an explicit participant cohort with clinical evidence in checked batches.

        Args:
            participants: List/tuple of participant dictionaries or DataFrame with short_title and display_participant_id; Collections only.
            include_inferred: Include supported inferred values (default True); False requests recorded values only.

        Returns:
            Result with subjects, values, unmatched, matched_count and requested_count in .data.
            values contains flattened concepts with their source evidence and inference flags.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            IDs are matched exactly within each Collection. No ID conversion or cross-dataset
            join is guessed. Capped or inconsistent batches fail rather than silently truncating.
            Complete here means the explicit cohort was processed, not that clinical coverage is complete."""
        rows = records(participants)
        _, provenance = self._observe_status()
        groups = {}
        for row in rows:
            if row.get("dataset_type", "Collection").casefold() != "collection":
                raise InvalidRequestError(
                    "Clinical enrichment currently requires Collection participants"
                )
            title, identifier = row.get("short_title"), row.get("display_participant_id")
            if (
                not isinstance(title, str)
                or not title
                or not isinstance(identifier, str)
                or not identifier
            ):
                raise InvalidRequestError(
                    "Each participant needs short_title and display_participant_id"
                )
            groups.setdefault(title, set()).add(identifier)
        subjects, seen = [], set()
        for title, identifiers in groups.items():
            ordered = sorted(identifiers)
            for start in range(0, len(ordered), 100):
                requested = ordered[start : start + 100]
                result = self.list_clinical_subjects(
                    title,
                    subject_ids=requested,
                    include_inferred=include_inferred,
                    limit=500,
                )
                if result.provenance.release_fingerprint != provenance.release_fingerprint:
                    raise ProtocolError(
                        "Metadata changed during clinical enrichment; restart the query"
                    )
                if result.data["count"] == result.data["limit"]:
                    raise ProtocolError(
                        "Clinical response reached its cap; completeness is unknown"
                    )
                for subject in result.data["subjects"]:
                    key = (subject["short_title"], subject["subject_id"])
                    if key in seen or key[0] != title or key[1] not in requested:
                        raise ProtocolError(
                            "Clinical response has ambiguous or unrequested identity"
                        )
                    seen.add(key)
                    subjects.append(subject)
        unmatched = [
            {"short_title": title, "participant_id": identifier}
            for title, identifiers in groups.items()
            for identifier in sorted(identifiers)
            if (title, identifier) not in seen
        ]
        values = []
        for subject in subjects:
            for concept, value in (subject.get("resolved_values") or {}).items():
                source = (subject.get("resolved_sources") or {}).get(concept, {})
                values.append(
                    {
                        "short_title": subject["short_title"],
                        "participant_id": subject["subject_id"],
                        "concept": concept,
                        "value": value,
                        "is_inferred": source.get("is_inferred"),
                        "evidence_scope": source.get("evidence_scope"),
                        "source_evidence": source,
                    }
                )
        return Result(
            {
                "subjects": subjects,
                "values": values,
                "unmatched": unmatched,
                "requested_count": sum(map(len, groups.values())),
                "matched_count": len(subjects),
            },
            (
                "Exact dataset-scoped identifiers only; clinical coverage is not source completeness.",
            ),
            provenance,
            Completeness(
                "complete",
                "All requested identifier batches collected; unmatched IDs retained",
            ),
        )
