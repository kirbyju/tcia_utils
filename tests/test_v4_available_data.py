"""Availability behavior through the shared HTTP/result layer, no live data."""
import json
from pathlib import Path

import httpx
import pytest

from tcia_utils import TCIAClient, InvalidRequestError, ProtocolError, ServiceUnavailableError
from tcia_utils._available_data import AvailableDataMixin


class AvailabilityClient(TCIAClient, AvailableDataMixin):
    pass


def make_client(payload, *, caps=None):
    calls = []
    capabilities = {name: True for name in ["participant_search", "public_non_dicom_detail", "controlled_access_detail"]}
    capabilities.update(caps or {})
    ready = {"status": "ready", "release_fingerprint": "fixture-a", "installed_profile": "research_detail"}
    bundle = {"v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
              "v2_capabilities": capabilities,
              "v2_install": {"release_fingerprint": "fixture-a", "installed_profile": "research_detail"},
              "v2_bundle": {"release_fingerprint": "fixture-a"}}
    def handle(request):
        calls.append(request)
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json=ready)
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json=bundle)
        return httpx.Response(200, json=payload)
    return AvailabilityClient(base_url="https://example.test", retries=0,
                              http_client=httpx.Client(transport=httpx.MockTransport(handle))), calls


def test_controlled_file_metadata_preserves_identity_and_uncertainty():
    payload = json.loads((Path(__file__).parent / "fixtures/v4/bounded_controlled_files.json").read_text())
    with make_client(payload)[0] as client:
        result = client.list_controlled_files("EXAMPLE", participant_id="001", has_drs_uri=True, limit=payload["limit"])
        assert result.data == payload
        assert result.completeness.state == "unknown"
        assert result.notes == (payload["warning"],)
        assert result.data["files"][0]["drs_uri"].startswith("drs://")


def test_public_asset_route_filters_and_additive_provenance():
    payload = {"assets": [{"asset_id": "opaque", "geometry_source": "reviewed", "inventory_status": "partial"}],
               "count": 1, "limit": 2, "has_more": False, "truncated": False, "next_cursor": None,
               "public_dicom_detail_route": "Use IDC for public series."}
    client, calls = make_client(payload)
    with client:
        page = client.search_public_assets(short_titles=["A", "B"], file_formats=["NIfTI"],
                                           object_roles=["segmentation"], requires_annotations=True, limit=2)
        assert page.items[0]["geometry_source"] == "reviewed"
        assert page.response["public_dicom_detail_route"] == payload["public_dicom_detail_route"]
        assert calls[-1].url.params.get_list("short_titles") == ["A", "B"]
        assert calls[-1].url.params["requires_annotations"] == "true"
        assert calls[-1].url.path == "/v2/public-non-dicom/assets"


def test_missing_optional_detail_is_not_empty_data():
    client, calls = make_client({}, caps={"public_non_dicom_detail": False})
    with client, pytest.raises(ServiceUnavailableError):
        client.search_public_assets()
    assert all("/assets" not in str(request.url) for request in calls)


@pytest.mark.parametrize("payload", [
    {"files": [], "count": 1, "limit": 1, "short_title": "A", "warning": "", "controlled_access_policy_url": ""},
    {"files": ["not-a-record"], "count": 1, "limit": 1, "short_title": "A", "warning": "", "controlled_access_policy_url": ""},
    {"files": [], "count": 0, "limit": 1, "short_title": "A"},
])
def test_malformed_controlled_results_raise_protocol_error(payload):
    with make_client(payload)[0] as client, pytest.raises(ProtocolError):
        client.list_controlled_files("A", limit=1)


def test_invalid_local_filters_do_not_issue_requests():
    client, calls = make_client({})
    with client:
        for kwargs in [{"short_titles": "A"}, {"short_titles": []}, {"requires_annotations": "false"}]:
            with pytest.raises(InvalidRequestError):
                client.search_public_assets(**kwargs)
        with pytest.raises(InvalidRequestError):
            client.list_controlled_files("A", limit=501)
        with pytest.raises(InvalidRequestError):
            client.list_participant_assets("")
    assert calls == []


def test_annotation_signal_does_not_become_source_relationship():
    payload = {"downloads": [], "count": 0, "limit": 1, "routing_note": "Use IDC for detail",
               "scope": "download signal", "public_dicom_annotation_detail": {"route": "IDC"}}
    with make_client(payload)[0] as client:
        result = client.search_annotation_downloads(limit=1)
        assert result.completeness.state == "unknown"
        assert result.data["public_dicom_annotation_detail"] == {"route": "IDC"}
