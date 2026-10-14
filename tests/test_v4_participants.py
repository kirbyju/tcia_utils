"""Participant identity and coverage behavior over the shared REST helpers."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from tcia_utils import InvalidRequestError, ProtocolError, ServiceUnavailableError, TCIAClient
from tcia_utils._participants import ParticipantsMixin


class ParticipantClient(TCIAClient, ParticipantsMixin):
    pass


FIXTURES = Path(__file__).parent / "fixtures" / "v4"


def identity(enabled: bool = True):
    ready = {"status": "ready", "release_fingerprint": "release-a",
             "release_tag": "v2-test", "installed_profile": "research_detail"}
    bundle = {"v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
              "v2_capabilities": {"participant_search": enabled},
              "v2_install": {"release_fingerprint": "release-a", "installed_profile": "research_detail"},
              "v2_bundle": {"release_fingerprint": "release-a"}}
    return ready, bundle


def client(handler):
    return ParticipantClient(base_url="https://example.test", retries=0,
                             http_client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_same_display_id_stays_dataset_scoped_across_pages():
    ready, bundle = identity()
    first = json.loads((FIXTURES / "partial_participants.json").read_text())
    second = {**first,
              "participants": [{**first["participants"][0],
                                "participant_key": "analysis:OTHER:0007",
                                "dataset_type": "Analysis Result", "short_title": "OTHER",
                                "cross_dataset_identity_status": "not_asserted"}],
              "has_more": False, "truncated": False, "next_cursor": None}
    searches = []

    def handler(request):
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        searches.append(json.loads(request.content))
        return httpx.Response(200, json=second if searches[-1].get("cursor") else first)

    with client(handler) as tc:
        page = tc.search_participants(short_titles=["EXAMPLE", "OTHER"], limit=1)
        assert page.completeness.state == "partial"
        rows = [row for part in page.iter_pages() for row in part.items]
        assert len(rows) == 2
        assert {tuple(row[key] for key in ("dataset_type", "short_title", "participant_key"))
                for row in rows} == {
                    ("collection", "EXAMPLE", "collection:EXAMPLE:0007"),
                    ("Analysis Result", "OTHER", "analysis:OTHER:0007"),
                }
        assert rows[0]["display_participant_id"] == rows[1]["display_participant_id"]
        assert rows[1]["cross_dataset_identity_status"] == "not_asserted"
    assert searches == [
        {"dataset_type": "both", "short_titles": ["EXAMPLE", "OTHER"], "limit": 1},
        {"dataset_type": "both", "short_titles": ["EXAMPLE", "OTHER"], "limit": 1,
         "cursor": "opaque-fixture-cursor"},
    ]


def test_participant_detail_and_coverage_preserve_identity_and_link_issues():
    ready, bundle = identity()
    detail = {
        "participant": {"participant_key": "collection:EXAMPLE:0007", "dataset_type": "Collection",
                        "short_title": "EXAMPLE", "display_participant_id": "0007",
                        "cross_dataset_identity_status": "not_asserted"},
        "identifiers": [{"participant_key": "collection:EXAMPLE:0007",
                         "identifier_namespace": "tcia_subject_id", "raw_identifier": "0007"}],
        "count": 1, "identity_scope": "Dataset-scoped; no cross-dataset person identity is asserted.",
    }
    coverage = {"short_title": "EXAMPLE", "dataset_type": "Collection", "participant_count": 1,
                "unlinked_dataset_assets": [{"dataset_asset_id": "unlinked-1", "asset_count": 2}],
                "participant_link_issues": [{"issue_code": "missing_crosswalk", "status": "review_required"}],
                "sources": [{"source_name": "participant_inventory", "present": 1}],
                "coverage_complete": False}
    paths = []

    def handler(request):
        paths.append(str(request.url))
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        if request.url.path.endswith("participant-coverage"):
            return httpx.Response(200, json=coverage)
        return httpx.Response(200, json=detail)

    with client(handler) as tc:
        result = tc.get_participant("collection:EXAMPLE:0007")
        assert result.data["participant"]["cross_dataset_identity_status"] == "not_asserted"
        assert result.data["identifiers"][0]["raw_identifier"] == "0007"
        assert result.completeness.state == "complete"
        summary = tc.get_coverage("EXAMPLE", dataset_type="collection")
        assert summary.data["coverage_complete"] is False
        assert summary.data["participant_link_issues"][0]["status"] == "review_required"
        assert summary.data["unlinked_dataset_assets"][0]["asset_count"] == 2
    assert "https://example.test/v2/participants/collection%3AEXAMPLE%3A0007" in paths


def test_analysis_result_filter_uses_service_spelling_and_coverage_normalizes_case():
    ready, bundle = identity()
    page_body = {"participants": [], "count": 0, "limit": 25,
                 "has_more": False, "truncated": False, "next_cursor": None,
                 "identity_scope": "Dataset-scoped", "public_dicom_detail_route": "IDC/idc-index"}
    coverage = {"short_title": "OTHER", "dataset_type": "Analysis Result",
                "participant_count": 0, "unlinked_dataset_assets": [],
                "participant_link_issues": [], "sources": [], "coverage_complete": True}
    wire_values = []

    def handler(request):
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        if request.url.path.endswith("/search"):
            wire_values.append(json.loads(request.content)["dataset_type"])
            return httpx.Response(200, json=page_body)
        wire_values.append(request.url.params["dataset_type"])
        return httpx.Response(200, json=coverage)

    with client(handler) as tc:
        assert tc.search_participants(dataset_type="analysis_result").items == ()
        assert tc.get_coverage("OTHER", dataset_type="analysis_result").data["dataset_type"] == "Analysis Result"
    assert wire_values == ["analysis result", "analysis result"]


def test_empty_search_and_absent_component_are_distinct():
    ready, bundle = identity()
    empty = {"participants": [], "count": 0, "limit": 25, "has_more": False,
             "truncated": False, "next_cursor": None,
             "identity_scope": "Dataset-scoped", "public_dicom_detail_route": "IDC/idc-index"}
    calls = []

    def handler(request):
        calls.append(request.url.path)
        body = ready if request.url.path == "/v2/ready" else bundle if request.url.path == "/v2/bundle" else empty
        return httpx.Response(200, json=body)

    with client(handler) as tc:
        page = tc.search_participants()
        assert page.items == () and page.completeness.state == "complete"
    bundle["v2_capabilities"]["participant_search"] = False
    with client(handler) as tc:
        with pytest.raises(ServiceUnavailableError):
            tc.search_participants()
    assert calls.count("/v2/participants/search") == 1


@pytest.mark.parametrize("payload", [
    {"participants": [{"participant_key": "x"}]},
    {"identity_scope": None},
    {"public_dicom_detail_route": None},
])
def test_participant_search_rejects_missing_identity_contract(payload):
    ready, bundle = identity()
    base = json.loads((FIXTURES / "partial_participants.json").read_text())
    base.update(payload)

    def handler(request):
        data = ready if request.url.path == "/v2/ready" else bundle if request.url.path == "/v2/bundle" else base
        return httpx.Response(200, json=data)

    with client(handler) as tc:
        with pytest.raises(ProtocolError):
            tc.search_participants(limit=1)


def test_following_participant_page_rechecks_identity_contract():
    ready, bundle = identity()
    first = json.loads((FIXTURES / "partial_participants.json").read_text())
    second = {**first, "participants": [{"participant_key": "x"}],
              "has_more": False, "truncated": False, "next_cursor": None}

    def handler(request):
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        return httpx.Response(200, json=second if json.loads(request.content).get("cursor") else first)

    with client(handler) as tc:
        page = tc.search_participants(limit=1)
        with pytest.raises(ProtocolError, match="dataset-scoped identity"):
            page.next_page()


def test_participant_local_input_validation():
    with client(lambda _request: pytest.fail("Invalid input must not issue HTTP requests")) as tc:
        with pytest.raises(InvalidRequestError):
            tc.get_participant("")
        with pytest.raises(InvalidRequestError):
            tc.get_coverage("")
        with pytest.raises(InvalidRequestError):
            tc.search_participants(limit=501)
        with pytest.raises(InvalidRequestError):
            tc.search_participants(short_titles="EXAMPLE")
        with pytest.raises(InvalidRequestError):
            tc.search_participants(modalities=["MR", None])
        with pytest.raises(InvalidRequestError):
            tc.get_coverage("EXAMPLE", dataset_type=3)
        with pytest.raises(InvalidRequestError):
            tc.search_participants(dataset_type="analysys_result")
        with pytest.raises(InvalidRequestError):
            tc.get_coverage("EXAMPLE", dataset_type="both")
