"""Dataset-scoped participant discovery over the shared V2 service."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from urllib.parse import quote

from ._errors import InvalidRequestError, ProtocolError


_DATASET_TYPE_WIRE = {
    "both": "both",
    "collection": "collection",
    "analysis_result": "analysis result",
}


def _dataset_type(value: str, *, allow_both: bool) -> str:
    normalized = value.casefold() if isinstance(value, str) else ""
    if normalized not in _DATASET_TYPE_WIRE or (normalized == "both" and not allow_both):
        choices = "both, collection, analysis_result" if allow_both else "collection, analysis_result"
        raise InvalidRequestError(f"dataset_type must be one of: {choices}")
    return _DATASET_TYPE_WIRE[normalized]


def _segment(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InvalidRequestError(f"{name} must be a nonempty string")
    return quote(value, safe="")


def _strings(value: Sequence[str], name: str) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence) or not value or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise InvalidRequestError(f"{name} must be a sequence of nonempty strings")
    return list(value)


def _checked_clinical_match(participant, expected):
    match = participant.get("clinical_match")
    if participant["dataset_type"].casefold() != "collection" or not isinstance(match, dict):
        raise ProtocolError("Clinical participant search lacks Collection-scoped match evidence")
    if match.get("subject_id") != participant["display_participant_id"]:
        raise ProtocolError("Clinical match belongs to a different participant")
    if type(match.get("has_patient_level_clinical_evidence")) is not bool:
        raise ProtocolError("Clinical match lacks patient-level evidence status")
    if not expected["include_inferred"] and not match["has_patient_level_clinical_evidence"]:
        raise ProtocolError("Clinical match lacks required patient-level evidence")
    for parameter, field, scope in (("diagnosis", "primary_diagnosis", "diagnosis"),
                                     ("primary_site", "primary_site", "site")):
        value = match.get(field)
        flag = match.get(field + "_is_inferred")
        evidence = match.get(scope + "_evidence_scope")
        if (value is not None and not isinstance(value, str)
                or flag is not None and type(flag) is not bool
                or evidence is not None and not isinstance(evidence, str)):
            raise ProtocolError("Clinical match has invalid values or evidence flags")
        if expected[parameter] is not None:
            if not isinstance(value, str) or expected[parameter].casefold() not in value.casefold():
                raise ProtocolError("Clinical match does not satisfy the requested filters")
            if not expected["include_inferred"] and (
                flag is not False or evidence not in {"patient", "patient_inherited"}
            ):
                raise ProtocolError("Clinical match does not satisfy recorded-only criteria")


def _checked_participant_page(page, clinical_query=None):
    if not isinstance(page.response.get("identity_scope"), str) or not isinstance(
        page.response.get("public_dicom_detail_route"), str
    ):
        raise ProtocolError("Participant search lacks identity or DICOM route scope")
    if clinical_query is not None:
        expected = {**clinical_query, "match_mode": "case_insensitive_substring",
                    "dataset_scope": "Collection",
                    "metadata_snapshot": page.provenance.release_fingerprint}
        if page.response.get("clinical_query") != expected:
            raise ProtocolError("Participant search did not confirm the requested clinical filters")
    for participant in page.items:
        if not all(isinstance(participant.get(key), str) and participant[key]
                   for key in ("participant_key", "dataset_type", "short_title", "display_participant_id")):
            raise ProtocolError("Participant record lacks dataset-scoped identity")
        if clinical_query is not None:
            _checked_clinical_match(participant, clinical_query)
    if page.has_more:
        def next_checked():
            following = page.next_page()
            if following is None:
                raise ProtocolError("Participant search lost its next page")
            return _checked_participant_page(following, clinical_query)

        return replace(page, _fetch_next=next_checked)
    return page


class ParticipantsMixin:
    """Dataset-scoped participant discovery and linking coverage exposed by TCIAClient."""

    def search_participants(
        self,
        *,
        query: str | None = None,
        diagnosis: str | None = None,
        primary_site: str | None = None,
        include_inferred: bool = True,
        short_titles: Sequence[str] | None = None,
        dataset_type: str = "both",
        access_levels: Sequence[str] | None = None,
        data_categories: Sequence[str] | None = None,
        data_types: Sequence[str] | None = None,
        file_formats: Sequence[str] | None = None,
        geometry_statuses: Sequence[str] | None = None,
        modalities: Sequence[str] | None = None,
        limit: int = 25,
        cursor: str | None = None,
    ):
        """Find participants matching available data AND optional clinical criteria.

        Args:
            query: Optional participant identifier/dataset free-text search.
            diagnosis: Literal, case-insensitive substring of resolved primary diagnosis, or None.
            primary_site: Literal, case-insensitive substring of primary disease site, or None.
                This is disease location, not necessarily the anatomy covered by an image.
            include_inferred: Allow dataset-derived clinical labels (default True). False requires
                patient-level evidence and recorded values for each selected clinical criterion.
                Unknown inference status is excluded for selected criteria.
            short_titles: Nonempty sequence of dataset short titles, or None.
            dataset_type: "collection", "analysis_result", or "both" (default).
            access_levels: Nonempty sequence of published labels, or None to omit this filter.
            data_categories: Nonempty sequence of published labels, or None to omit this filter.
            data_types: Nonempty sequence of published labels, or None to omit this filter.
            file_formats: Nonempty sequence of published labels, or None to omit this filter.
            geometry_statuses: Nonempty sequence of published labels, or None to omit this filter.
            modalities: Nonempty sequence of published labels, or None to omit this filter.
            limit: Maximum records on this page, not an archive-wide total; see the bound below.
            cursor: Opaque next_cursor from the same query, or None for the first page.

        Returns:
            Page of dictionaries. Use .items for this page, .iter_pages() for further pages,
            .to_dict()/.to_json() for metadata-preserving exports, or .to_dataframe() with the tables extra.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            limit is 1–500 (default 25). Carry participant_key, dataset_type and short_title
            into subsequent calls. Identical display IDs in different datasets do not establish
            the same person. Clinical filters apply before pagination, support Collections only,
            and use exact dataset-scoped subject IDs. With dataset_type="both", clinical searches
            return Collections. All supplied criteria are combined with AND; text matching does
            not expand medical synonyms. Rows retain clinical_match evidence; the page retains
            clinical_query. Use clinical_for_participants() for all available clinical concepts.
            Missing clinical components raise an error. A missing indexed record is not proof
            that source data are absent. Text criteria must be nonblank and at most 200 characters."""
        if query is not None and not isinstance(query, str):
            raise InvalidRequestError("query must be a string")
        body = {"dataset_type": _dataset_type(dataset_type, allow_both=True)}
        if type(include_inferred) is not bool:
            raise InvalidRequestError("include_inferred must be a boolean")
        criteria = {}
        for name, value in (("diagnosis", diagnosis), ("primary_site", primary_site)):
            if value is not None:
                if not isinstance(value, str) or not value.strip() or len(value.strip()) > 200:
                    raise InvalidRequestError(f"{name} must be a nonblank string of at most 200 characters")
                value = value.strip()
            criteria[name] = value
        clinical_query = None
        if any(value is not None for value in criteria.values()) or not include_inferred:
            if body["dataset_type"] == "analysis result":
                raise InvalidRequestError("Clinical participant filters currently support Collections only")
            clinical_query = {**criteria, "include_inferred": include_inferred}
            body.update(clinical_query)
        for name, value in (
            ("query", query), ("short_titles", short_titles),
            ("access_levels", access_levels), ("data_categories", data_categories),
            ("data_types", data_types), ("file_formats", file_formats),
            ("geometry_statuses", geometry_statuses), ("modalities", modalities),
        ):
            if value is not None:
                body[name] = value if name == "query" else _strings(value, name)
        page = self._page(
            "POST", "/v2/participants/search", "participants",
            json_body=body, limit=limit, cursor=cursor, max_limit=500,
            capability="participant_search",
        )
        return _checked_participant_page(page, clinical_query)

    def get_participant(self, participant_key: str):
        """Read one canonical participant and its retained source identifiers.

        Args:
            participant_key: Exact dataset-scoped key from search_participants(); not a display patient ID.

        Returns:
            Result with .data["participant"], identifiers, count and identity_scope.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            The response describes an indexed identity, not complete clinical history or all
            image files. Use list_participant_assets() or plan_participant_images() for available images."""
        result = self._result(
            "GET", f"/v2/participants/{_segment(participant_key, 'participant_key')}",
            capability="participant_search",
            required_fields={
                "participant": dict, "identifiers": list,
                "count": int, "identity_scope": str,
            },
            note_keys=("identity_scope",), bounded=False,
        )
        if result.data["count"] != 1 or not all(isinstance(item, dict) for item in result.data["identifiers"]):
            raise ProtocolError("Participant detail has invalid count or identifiers")
        participant = result.data["participant"]
        if participant.get("participant_key") != participant_key or not all(
            isinstance(participant.get(key), str) and participant[key]
            for key in ("dataset_type", "short_title", "display_participant_id")
        ):
            raise ProtocolError("Participant detail identity is inconsistent")
        if any(item.get("participant_key") != participant_key for item in result.data["identifiers"]):
            raise ProtocolError("Participant identifier belongs to a different participant")
        return result

    def get_coverage(self, short_title: str, *, dataset_type: str | None = None):
        """Inspect participant linking coverage for a dataset.

        Args:
            short_title: Published dataset short title.
            dataset_type: Optional "collection" or "analysis_result"; omit for the route default.

        Returns:
            Result with participant_count, coverage_complete, unlinked_dataset_assets,
            participant_link_issues and source summaries in .data.

        Raises:
            InvalidRequestError: A filter, identifier, or limit is invalid.
            ProtocolError: Returned metadata or snapshot identity is inconsistent.
            TCIAError: The service, a required component, or an HTTP request is unavailable.

        Notes:
            coverage_complete refers to indexed participant linking, not complete clinical
            coverage, complete image inventories, or an archive-wide count of unique people."""
        wire_type = _dataset_type(dataset_type, allow_both=False) if dataset_type is not None else None
        params = {"dataset_type": wire_type} if wire_type is not None else {}
        result = self._result(
            "GET", f"/v2/datasets/{_segment(short_title, 'short_title')}/participant-coverage",
            params=params, capability="participant_search",
            required_fields={
                "short_title": str, "dataset_type": (str, type(None)),
                "participant_count": int,
                "unlinked_dataset_assets": list, "participant_link_issues": list,
                "sources": list, "coverage_complete": bool,
            },
            bounded=False,
        )
        data = result.data
        if data["short_title"].casefold() != short_title.casefold() or data["participant_count"] < 0:
            raise ProtocolError("Participant coverage has inconsistent dataset or count")
        if wire_type is not None and (
            not isinstance(data["dataset_type"], str)
            or data["dataset_type"].casefold() != wire_type
        ):
            raise ProtocolError("Participant coverage has inconsistent dataset type")
        for name in ("unlinked_dataset_assets", "participant_link_issues", "sources"):
            if not all(isinstance(item, dict) for item in data[name]):
                raise ProtocolError(f"Participant coverage has invalid {name}")
        return result
