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
