"""Clinical evidence scope, inference, and bounded result checks."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from tcia_utils import InvalidRequestError, ProtocolError, ServiceUnavailableError, TCIAClient


ClinicalClient = TCIAClient


FIXTURES = Path(__file__).parent / "fixtures" / "v4"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def client(handler):
    return ClinicalClient(base_url="https://example.test", retries=0,
                          http_client=httpx.Client(transport=httpx.MockTransport(handler)))


def identity(enabled=True):
    ready = {"status": "ready", "release_fingerprint": "release-a",
             "release_tag": "v2-test", "installed_profile": "research_detail"}
    bundle = {"v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
              "v2_capabilities": {"clinical_detail": enabled},
              "v2_install": {"release_fingerprint": "release-a", "installed_profile": "research_detail"},
              "v2_bundle": {"release_fingerprint": "release-a"}}
    return ready, bundle


def test_inferred_diagnosis_remains_dataset_scoped_and_conflicts_survive():
    ready, bundle = identity()
    subjects = fixture("inferred_clinical_subjects.json")
    facts = fixture("inferred_clinical_facts.json")
    subjects["subjects"].append({
        "subject_key": "EXAMPLE:0010", "short_title": "EXAMPLE", "subject_id": "0010",
        "has_imaging": True, "primary_diagnosis": "Reviewed inherited finding",
        "primary_diagnosis_is_inferred": False,
        "resolved_values": {"primary_diagnosis": "Reviewed inherited finding"},
        "resolved_sources": {"primary_diagnosis": {"source_kind": "patient_inherited", "priority": 200}},
        "conflicts": {},
    })
    subjects["count"] = 3
    facts["facts"].append({
        "short_title": "EXAMPLE", "subject_id": "0010", "concept": "primary_diagnosis",
        "value_text": "Reviewed inherited finding", "source_kind": "patient_inherited",
        "source_priority": 200, "evidence_scope": "patient_inherited",
        "is_inferred": False, "provenance": {"review_status": "resolved"},
    })
    facts["count"] = 3
    conflicts = {"short_title": "EXAMPLE", "dataset_types": ["collection"],
                 "conflicts": [{"short_title": "EXAMPLE", "subject_id": "0008",
                                "concept": "stage", "values_seen": ["I", "II"],
                                "source_kinds": ["tcia_official_clinical", "idc_clinical"]}],
                 "count": 1, "limit": 100, "note": "Conflicting source values are retained."}

    def handler(request):
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        data = subjects if request.url.path.endswith("/subjects") else facts if request.url.path.endswith("/facts") else conflicts
        return httpx.Response(200, json=data)

    with client(handler) as tc:
        people = tc.list_clinical_subjects("EXAMPLE")
        assert people.completeness.state == "unknown"
        inferred = people.data["subjects"][0]
        assert inferred["primary_diagnosis_is_inferred"] is True
        assert inferred["resolved_sources"]["primary_diagnosis"]["source_kind"] == "dataset"
        assert people.data["subjects"][1]["conflicts"]["stage"] == ["I", "II"]
        assert people.data["subjects"][2]["resolved_sources"]["primary_diagnosis"]["source_kind"] == "patient_inherited"
        values = tc.list_clinical_facts("EXAMPLE")
        assert values.completeness.state == "unknown"
        assert values.data["facts"][0]["evidence_scope"] == "dataset"
        assert values.data["facts"][0]["is_inferred"] is True
        assert values.data["facts"][1]["evidence_scope"] == "patient"
        assert values.data["facts"][1]["is_inferred"] is False
        assert values.data["facts"][0]["provenance"] == {"fixture": "synthetic"}
        assert values.data["facts"][2]["evidence_scope"] == "patient_inherited"
        assert values.data["facts"][2]["provenance"]["review_status"] == "resolved"
        issues = tc.list_clinical_conflicts("EXAMPLE")
        assert issues.data["conflicts"][0]["values_seen"] == ["I", "II"]
        assert issues.completeness.state == "unknown"


def test_clinical_only_inclusion_is_explicit_and_filters_are_forwarded():
    ready, bundle = identity()
    response = fixture("inferred_clinical_subjects.json")
    response["subjects"] = [{"subject_key": "EXAMPLE:0009", "short_title": "EXAMPLE",
                             "subject_id": "0009", "has_imaging": False,
                             "primary_diagnosis": None, "primary_diagnosis_is_inferred": None,
                             "resolved_values": {}, "resolved_sources": {}, "conflicts": {}}]
    response["count"] = 1
    response["limit"] = 3
    response["includes_clinical_only_subjects"] = True
    queries = []

    def handler(request):
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        queries.append(request.url.params)
        return httpx.Response(200, json=response)

    with client(handler) as tc:
        result = tc.list_clinical_subjects("EXAMPLE", subject_ids=["0009"],
                                           include_clinical_only=True, has_conflicts=False,
                                           include_inferred=False, limit=3)
        assert result.data["subjects"][0]["has_imaging"] is False
        assert result.data["subjects"][0]["primary_diagnosis"] is None
    assert queries[0].get_list("subject_ids") == ["0009"]
    assert queries[0]["include_clinical_only"] == "true"
    assert queries[0]["has_conflicts"] == "false"
    assert queries[0]["include_inferred"] == "false"


def test_empty_evidence_is_not_unavailable():
    ready, bundle = identity()
    empty = {"short_title": "EXAMPLE", "dataset_types": ["collection"],
             "facts": [], "count": 0, "limit": 100,
             "note": "Lower-priority values remain available."}
    data_calls = 0

    def handler(request):
        nonlocal data_calls
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        data_calls += 1
        return httpx.Response(200, json=empty)

    with client(handler) as tc:
        result = tc.list_clinical_facts("EXAMPLE")
        assert result.data["facts"] == [] and result.completeness.state == "unknown"
    bundle["v2_capabilities"]["clinical_detail"] = False
    with client(handler) as tc:
        with pytest.raises(ServiceUnavailableError):
            tc.list_clinical_facts("EXAMPLE")
    assert data_calls == 1


@pytest.mark.parametrize("change", [
    {"count": 3}, {"limit": 101}, {"facts": "bad"}, {"dataset_types": []},
    {"short_title": "OTHER"}, {"note": None},
    {"facts": [{"short_title": "OTHER", "subject_id": "0007"},
               {"short_title": "EXAMPLE", "subject_id": "0008"}]},
])
def test_clinical_bounded_envelope_rejects_inconsistent_results(change):
    ready, bundle = identity()
    response = {**fixture("inferred_clinical_facts.json"), **change}

    def handler(request):
        body = ready if request.url.path == "/v2/ready" else bundle if request.url.path == "/v2/bundle" else response
        return httpx.Response(200, json=body)

    with client(handler) as tc:
        with pytest.raises(ProtocolError):
            tc.list_clinical_facts("EXAMPLE")


def test_clinical_limit_and_dataset_validation_before_io():
    with client(lambda _request: pytest.fail("Invalid input must not issue HTTP requests")) as tc:
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_subjects("EXAMPLE", limit=501)
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_facts("EXAMPLE", limit=0)
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_conflicts("", limit=100)
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_subjects("EXAMPLE", subject_ids="0009")
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_facts("EXAMPLE", concepts=["stage", None])
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_facts("EXAMPLE", source_kinds="dataset")
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_conflicts("EXAMPLE", concepts="stage")
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_subjects("EXAMPLE", include_clinical_only="yes")
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_subjects("EXAMPLE", include_inferred=1)
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_subjects("EXAMPLE", has_conflicts="false")
        with pytest.raises(InvalidRequestError):
            tc.list_clinical_facts("EXAMPLE", inferred=0)
