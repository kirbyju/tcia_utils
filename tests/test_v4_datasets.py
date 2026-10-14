"""Dataset methods use only the V2 service and preserve access evidence."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from tcia_utils import InvalidRequestError, NotFoundError, ServiceUnavailableError, TCIAClient


DatasetClient = TCIAClient


def make_client(responses, calls):
    def handler(request):
        calls.append(request)
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json={"status": "ready", "release_fingerprint": "r1",
                                             "installed_profile": "research_detail"})
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json={
                "v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
                "v2_capabilities": {},
                "v2_install": {"release_fingerprint": "r1", "installed_profile": "research_detail"},
                "v2_bundle": {"release_fingerprint": "r1"},
            })
        value = responses[request.url.path]
        return value if isinstance(value, httpx.Response) else httpx.Response(200, json=value)

    return DatasetClient(base_url="https://example.test", retries=0,
                         http_client=httpx.Client(transport=httpx.MockTransport(handler)))


def fixture(name):
    return json.loads((Path(__file__).parent / "fixtures" / "v4" / name).read_text())


def test_search_keeps_dataset_type_and_original_filters():
    calls = []
    rows = [
        {"short_title": "SHARED", "dataset_type": "collection", "doi": "10.1/collection"},
        {"short_title": "SHARED", "dataset_type": "analysis_result", "doi": "10.1/analysis"},
    ]
    response = {"datasets": rows, "count": 2, "limit": 25, "has_more": False,
                "truncated": False, "next_cursor": None, "note": "Visible TCIA records only"}
    with make_client({"/v2/datasets/search": response}, calls) as tc:
        page = tc.search_datasets(query="shared", short_titles=["SHARED"], modalities=["CT"])
    assert {(row["dataset_type"], row["doi"]) for row in page.items} == {
        ("collection", "10.1/collection"), ("analysis_result", "10.1/analysis")}
    assert page.response["note"] == "Visible TCIA records only"
    search = next(call for call in calls if call.url.path == "/v2/datasets/search")
    assert json.loads(search.content) == {"query": "shared", "short_titles": ["SHARED"],
                                          "modalities": ["CT"], "dataset_type": "both", "limit": 25}
    assert all(call.url.path.startswith("/v2/") for call in calls)


def test_detail_preserves_mixed_downloads_related_analyses_and_caveats():
    calls = []
    detail = fixture("mixed_access_dataset.json")
    detail["related_analysis_results"].append({"short_title": "ANALYSIS", "dataset_type": "analysis_result"})
    detail["datasets"].append({"short_title": "EXAMPLE", "dataset_type": "analysis_result"})
    with make_client({"/v2/datasets/EXAMPLE": detail}, calls) as tc:
        result = tc.get_dataset("EXAMPLE")
    assert len(result.data["datasets"]) == 2
    assert {row["access_level"] for row in result.data["current_downloads"]} == {"open", "controlled"}
    assert result.data["related_analysis_results"][0]["dataset_type"] == "analysis_result"
    assert result.notes == tuple(detail["caveats"])
    assert result.completeness.state == "unknown"  # related results have a fixed bound


def test_access_retains_download_groups_and_policy():
    calls = []
    dataset = fixture("mixed_access_dataset.json")["datasets"][0]
    downloads = fixture("mixed_access_dataset.json")["current_downloads"]
    response = {"dataset": dataset, "download_groups": {"open": [downloads[0]],
                 "controlled": [downloads[1]]}, "controlled_access_policy_url": "https://policy.example/",
                "notes": ["Use each download's access level."]}
    with make_client({"/v2/datasets/EXAMPLE/access": response}, calls) as tc:
        result = tc.get_access("EXAMPLE")
    assert result.data["dataset"]["resolved_access_level"] == "mixed"
    assert set(result.data["download_groups"]) == {"open", "controlled"}
    assert result.data["download_groups"]["controlled"][0]["route_manifest_kind"] == "drs_uri"
    assert result.completeness.state == "unknown"


def test_download_filters_use_supported_route_and_reject_oversize():
    calls = []
    response = {"short_title": "EXAMPLE", "downloads": [], "count": 0, "limit": 2,
                "has_more": False, "truncated": False, "next_cursor": None}
    with make_client({"/v2/datasets/EXAMPLE/downloads": response}, calls) as tc:
        page = tc.list_downloads("EXAMPLE", access_levels=["controlled"], limit=2)
        assert page.items == ()
        with pytest.raises(InvalidRequestError):
            tc.list_downloads("EXAMPLE", limit=201)
    route_calls = [call for call in calls if call.url.path.endswith("/downloads")]
    assert len(route_calls) == 1
    assert route_calls[0].url.params.get_list("access_levels") == ["controlled"]


def test_download_paging_keeps_route_filters():
    calls = []
    seen = 0

    def handler(request):
        nonlocal seen
        calls.append(request)
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json={"status": "ready", "release_fingerprint": "r1",
                                             "installed_profile": "research_detail"})
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json={"v2_bundle_manifest_exists": True,
                                              "v2_install_state_exists": True,
                                              "v2_capabilities": {},
                                              "v2_install": {"release_fingerprint": "r1", "installed_profile": "research_detail"},
                                              "v2_bundle": {"release_fingerprint": "r1"}})
        seen += 1
        return httpx.Response(200, json={"short_title": "EXAMPLE", "downloads": [], "count": 0,
                                         "limit": 2, "has_more": seen == 1,
                                         "truncated": seen == 1,
                                         "next_cursor": "next" if seen == 1 else None})

    tc = DatasetClient(base_url="https://example.test", retries=0,
                       http_client=httpx.Client(transport=httpx.MockTransport(handler)))
    with tc:
        pages = list(tc.list_downloads("EXAMPLE", access_levels=["controlled"], limit=2).iter_pages())
    assert len(pages) == 2
    downloads = [call for call in calls if call.url.path.endswith("/downloads")]
    assert downloads[0].url.params.get_list("access_levels") == ["controlled"]
    assert downloads[1].url.params.get_list("access_levels") == ["controlled"]
    assert downloads[1].url.params["cursor"] == "next"


def test_missing_and_unavailable_are_not_empty():
    for status, error in ((404, NotFoundError), (503, ServiceUnavailableError)):
        calls = []
        response = httpx.Response(status, json={"code": "problem", "detail": "missing",
                                                "status": status, "retryable": status == 503})
        with make_client({"/v2/datasets/MISSING": response}, calls) as tc:
            with pytest.raises(error):
                tc.get_dataset("MISSING")


@pytest.mark.parametrize("operation", [
    lambda tc: tc.search_datasets(short_titles=[]),
    lambda tc: tc.search_datasets(short_titles=[" "]),
    lambda tc: tc.search_datasets(dataset_type="unknown"),
    lambda tc: tc.search_datasets(has_external_clinical_resource="false"),
    lambda tc: tc.list_downloads("EXAMPLE", access_levels=[]),
    lambda tc: tc.list_downloads("EXAMPLE", requires_annotations=1),
])
def test_invalid_filters_do_not_silently_widen_query(operation):
    calls = []
    with make_client({}, calls) as tc:
        with pytest.raises(InvalidRequestError):
            operation(tc)
    assert calls == []
