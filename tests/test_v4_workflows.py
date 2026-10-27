"""Cross-feature workflows through the public facade and built-package surface."""
import json
from pathlib import Path

import httpx
import pytest

from tcia_utils import TCIAClient, ServiceUnavailableError, InvalidRequestError

FIXTURES = Path(__file__).parent / "fixtures" / "v4"


def fixture(name):
    return json.loads((FIXTURES / name).read_text())


def test_all_contract_methods_are_reachable_without_provider_namespaces():
    matrix = json.loads((Path(__file__).parents[1] / "docs/v4/design/endpoint-matrix.json").read_text())
    for entry in matrix["entries"]:
        assert callable(getattr(TCIAClient, entry["public_method"]))
    for name in ["dicom", "files", "downloads", "nbia", "wordpress", "ctdc"]:
        assert not hasattr(TCIAClient, name)


def test_discover_inspect_clinical_availability_and_export(tmp_path):
    routes = []
    identity = {"release_fingerprint": "synthetic-release", "installed_profile": "research_detail"}
    caps = {key: True for key in ["participant_search", "clinical_detail", "controlled_access_detail", "datacite_metadata"]}
    ready = {"status": "ready", "release_tag": "synthetic", **identity}
    bundle = {"v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
              "v2_capabilities": caps, "v2_install": identity,
              "v2_bundle": {"release_fingerprint": identity["release_fingerprint"]}}
    detail = fixture("mixed_access_dataset.json")
    discovery = {"datasets": detail["datasets"], "count": 1, "limit": 1,
                 "has_more": False, "truncated": False, "next_cursor": None, "note": "Synthetic"}
    payloads = {
        "/v2/ready": ready, "/v2/bundle": bundle,
        "/v2/datasets/search": discovery, "/v2/datasets/EXAMPLE": detail,
        "/v2/participants/search": fixture("partial_participants.json"),
        "/v2/clinical/EXAMPLE/facts": fixture("inferred_clinical_facts.json"),
        "/v2/controlled-access/EXAMPLE/files": fixture("bounded_controlled_files.json"),
        "/v2/datacite/dois/10.0000/example": {
            "record": {"doi": "10.0000/example", "citation_style": "apa",
                       "formatted_citation": "Synthetic citation for test only", "metadata": {}},
            "provenance_note": "Synthetic DataCite fixture"},
    }
    def handler(request):
        routes.append(request.url.path)
        assert request.url.host == "example.test"
        assert request.url.path in payloads, "No provider fallback or transfer is allowed"
        return httpx.Response(200, json=payloads[request.url.path])

    with TCIAClient(base_url="https://example.test", retries=0,
                    http_client=httpx.Client(transport=httpx.MockTransport(handler))) as client:
        datasets = client.search_datasets(short_titles=["EXAMPLE"], limit=1)
        assert datasets.items[0]["resolved_access_level"] == "mixed"
        inspected = client.get_dataset("EXAMPLE")
        assert len(inspected.data["current_downloads"]) == 2
        participants = client.search_participants(short_titles=["EXAMPLE"], limit=1)
        assert participants.completeness.state == "partial"
        facts_payload = payloads["/v2/clinical/EXAMPLE/facts"]
        facts = client.list_clinical_facts("EXAMPLE", limit=facts_payload["limit"])
        assert facts.completeness.state == "unknown"
        assert facts.data["facts"][0]["evidence_scope"] == "dataset"
        assert facts.data["facts"][0]["is_inferred"] is True
        file_payload = payloads["/v2/controlled-access/EXAMPLE/files"]
        files = client.list_controlled_files("EXAMPLE", limit=file_payload["limit"])
        citation = client.get_citation("10.0000/example")
        assert citation.data["record"]["formatted_citation"].startswith("Synthetic")
        before_export = len(routes)
        selection = client.select(files.data["files"], release_identity=files.provenance,
                                  query_scope={"short_title": "EXAMPLE", "record_grain": "file",
                                               "completeness": files.completeness.state})
        report = client.export_selection(selection, tmp_path / "selection")
        assert len(routes) == before_export
        assert report.exported + report.unresolved + report.excluded + report.deduplicated == len(file_payload["files"])
        companion = json.loads((tmp_path / "selection/selection.json").read_text())
        assert companion["completeness"] == "unknown"
        assert companion["release_identity"]["release_fingerprint"] == "synthetic-release"
        assert report.exported >= 1


def test_unavailable_clinical_detail_never_looks_like_empty_cohort():
    def handler(request):
        assert request.url.path == "/v2/ready"
        return httpx.Response(503, json=fixture("unavailable_problem.json"))
    with TCIAClient(base_url="https://example.test", retries=0,
                    http_client=httpx.Client(transport=httpx.MockTransport(handler))) as client:
        with pytest.raises(ServiceUnavailableError):
            client.list_clinical_facts("EXAMPLE")


def test_empty_filters_never_expand_the_request():
    with TCIAClient(base_url="https://example.test", http_client=httpx.Client(
        transport=httpx.MockTransport(lambda r: pytest.fail("No request expected")))) as client:
        for call in [lambda: client.search_participants(short_titles=[]),
                     lambda: client.list_clinical_facts("EXAMPLE", concepts=[])]:
            with pytest.raises(InvalidRequestError):
                call()
