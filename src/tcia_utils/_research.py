"""Reusable cohort preparation, retaining exact dataset-scoped identity."""

from collections.abc import Mapping
from ._errors import InvalidRequestError, ProtocolError
from ._results import Result, Completeness


def records(value):
    if hasattr(value, "to_dict") and not isinstance(value, Mapping):
        value = value.to_dict("records")
    if not isinstance(value, (list, tuple)) or any(not isinstance(r, Mapping) for r in value):
        raise InvalidRequestError("Supply a list of records or a DataFrame")
    return [dict(r) for r in value]


class ResearchMixin:
    def describe_datasets(self, datasets):
        """Enrich explicit dataset records with details and published data categories.

        Search DTOs omit download categories. Resolve those through get_dataset(),
        retaining dataset type and rejecting a changed snapshot or ambiguous identity.
        No external-resource label is reclassified as a downloadable data category.
        """
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
        """Get clinical rows for exact Collection-scoped participant identifiers.

        Unmatched identifiers are reported, never guessed or joined across datasets.
        Batching cannot silently turn a capped service response into a full result.
        """
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
