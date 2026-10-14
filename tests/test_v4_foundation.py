"""Offline contract checks for the v4 client foundation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from tcia_utils import (
    InvalidRequestError, NotFoundError, ProtocolError, RetiredRouteError,
    ServiceUnavailableError, TCIAClient, TransportError,
)


FIXTURES = Path(__file__).parent / "fixtures" / "v4"


def fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def identity(fingerprint: str = "release-a", *, capability: bool = True):
    ready = {"status": "ready", "release_fingerprint": fingerprint,
             "release_tag": "v2-test", "installed_profile": "research_detail"}
    bundle = {"v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
              "v2_capabilities": {"participant_search": capability},
              "v2_install": {"release_fingerprint": fingerprint,
                             "installed_profile": "research_detail"},
              "v2_bundle": {"release_fingerprint": fingerprint}}
    return ready, bundle


def client(handler, *, retries=0):
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return TCIAClient(base_url="https://example.test", retries=retries, http_client=http)


def test_import_and_construction_are_inert():
    seen = []
    with client(lambda request: seen.append(request) or httpx.Response(500)) as tc:
        assert seen == []
        assert tc._transport.base_url == "https://example.test"
    check = subprocess.run(
        [sys.executable, "-c", "import sys, tcia_utils; assert not any(x in sys.modules for x in ('tcia_utils.nbia', 'tcia_utils.idc', 'tcia_utils.wordpress', 'pandas')); assert tcia_utils.TCIAClient.__name__ == 'TCIAClient'"],
        capture_output=True, text=True, check=False, env=os.environ.copy(),
    )
    assert check.returncode == 0, check.stderr


def test_status_merges_observed_identity_and_capability():
    ready, bundle = identity()
    paths = []

    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json=ready if request.url.path.endswith("ready") else bundle)

    with client(handler) as tc:
        result = tc.status()
        assert result.data.release_fingerprint == "release-a"
        assert result.provenance.capabilities["participant_search"] is True
        assert result.provenance.base_url == "https://example.test"
        assert result.completeness.state == "complete"
    assert paths == ["/v2/ready", "/v2/bundle"]


def test_status_rejects_missing_receipt_and_mismatched_release():
    ready, bundle = identity()
    bundle["v2_install_state_exists"] = False
    with client(lambda r: httpx.Response(200, json=ready if r.url.path.endswith("ready") else bundle)) as tc:
        with pytest.raises(ServiceUnavailableError):
            tc.status()
    bundle["v2_install_state_exists"] = True
    bundle["v2_install"]["release_fingerprint"] = "other"
    with client(lambda r: httpx.Response(200, json=ready if r.url.path.endswith("ready") else bundle)) as tc:
        with pytest.raises(ProtocolError):
            tc.status()


def test_page_is_lazy_preserves_filters_and_caveats():
    ready, bundle = identity()
    calls = []
    first = fixture("partial_participants.json")
    second = {**first, "participants": [], "count": 0, "has_more": False,
              "truncated": False, "next_cursor": None}

    def handler(request):
        calls.append(request)
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        if request.url.path.endswith("bundle"):
            return httpx.Response(200, json=bundle)
        body = json.loads(request.content)
        return httpx.Response(200, json=second if body.get("cursor") else first)

    with client(handler) as tc:
        page = tc._page("POST", "/v2/participants/search", "participants",
                        json_body={"query": "lung", "short_titles": ["EXAMPLE"]},
                        limit=1, max_limit=500, capability="participant_search")
        assert page.count == 1 and page.completeness.state == "partial"
        assert page.response["identity_scope"].startswith("Participant identity")
        assert sum(r.url.path.endswith("/search") for r in calls) == 1
        pages = list(page.iter_pages())
        assert len(pages) == 2 and pages[-1].completeness.state == "complete"
        searches = [json.loads(r.content) for r in calls if r.url.path.endswith("/search")]
        assert searches == [
            {"query": "lung", "short_titles": ["EXAMPLE"], "limit": 1},
            {"query": "lung", "short_titles": ["EXAMPLE"], "limit": 1,
             "cursor": "opaque-fixture-cursor"},
        ]
        with pytest.raises(ProtocolError, match="already been used"):
            page.next_page()


def test_page_rejects_release_change_before_following_page():
    first_ready, bundle = identity()
    changed_ready, _ = identity("release-b")
    ready_count = 0
    searches = 0

    def handler(request):
        nonlocal ready_count, searches
        if request.url.path.endswith("ready"):
            ready_count += 1
            return httpx.Response(200, json=first_ready if ready_count == 1 else changed_ready)
        if request.url.path.endswith("bundle"):
            return httpx.Response(200, json=bundle)
        searches += 1
        return httpx.Response(200, json=fixture("partial_participants.json"))

    with client(handler) as tc:
        page = tc._page("POST", "/v2/participants/search", "participants",
                        json_body={}, limit=1, max_limit=500)
        with pytest.raises(ProtocolError):
            page.next_page()
    assert searches == 1


def test_invalidated_cursor_is_reported_without_restart():
    ready, bundle = identity()
    searches = []

    def handler(request):
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        if request.url.path.endswith("bundle"):
            return httpx.Response(200, json=bundle)
        searches.append(json.loads(request.content))
        if len(searches) == 1:
            return httpx.Response(200, json=fixture("partial_participants.json"))
        return httpx.Response(422, json={"code": "invalid_cursor", "detail": "Cursor generation changed",
                                         "status": 422, "retryable": False})

    with client(handler, retries=2) as tc:
        page = tc._page("POST", "/v2/participants/search", "participants",
                        json_body={"query": "lung"}, limit=1, max_limit=500)
        with pytest.raises(InvalidRequestError) as caught:
            page.next_page()
        assert caught.value.code == "invalid_cursor"
    assert len(searches) == 2
    assert searches[1]["cursor"] == "opaque-fixture-cursor"


@pytest.mark.parametrize("change", [
    {"count": 2}, {"limit": 2}, {"has_more": True, "next_cursor": None},
    {"truncated": False}, {"next_cursor": "same"}, {"participants": "bad"},
])
def test_invalid_page_contract(change):
    ready, bundle = identity()
    payload = {**fixture("partial_participants.json"), **change}

    def handler(request):
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        return httpx.Response(200, json=bundle if request.url.path.endswith("bundle") else payload)

    with client(handler) as tc:
        with pytest.raises(ProtocolError):
            tc._page("POST", "/v2/participants/search", "participants",
                     json_body={}, limit=1, cursor="same", max_limit=500)


def test_invalid_limits_and_absent_capability_stop_before_data_request():
    ready, bundle = identity(capability=False)
    calls = []

    def handler(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=ready if request.url.path.endswith("ready") else bundle)

    with client(handler) as tc:
        with pytest.raises(InvalidRequestError):
            tc._page("POST", "/v2/participants/search", "participants",
                     json_body={}, limit=501, max_limit=500)
        assert calls == []
        with pytest.raises(ServiceUnavailableError):
            tc._page("POST", "/v2/participants/search", "participants",
                     json_body={}, limit=1, max_limit=500, capability="participant_search")
        assert calls == ["/v2/ready", "/v2/bundle"]


def test_bounded_result_is_unknown_and_rejects_bad_envelopes():
    ready, bundle = identity()
    payload = fixture("bounded_controlled_files.json")

    def handler(request):
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        return httpx.Response(200, json=bundle if request.url.path.endswith("bundle") else payload)

    with client(handler) as tc:
        result = tc._result("GET", "/v2/controlled-access/EXAMPLE/files",
                            required_fields={"files": list, "count": int, "limit": int})
        assert result.completeness.state == "unknown"
        assert len(result.data["files"]) == 2
        payload.pop("files")
        with pytest.raises(ProtocolError):
            tc._result("GET", "/v2/controlled-access/EXAMPLE/files",
                       required_fields={"files": list, "count": int, "limit": int})


def test_detail_count_does_not_imply_a_limit():
    ready, bundle = identity()
    payload = {"participant": {"participant_key": "collection:EXAMPLE:0007"},
               "identifiers": [], "count": 1, "identity_scope": "dataset"}

    def handler(request):
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        return httpx.Response(200, json=bundle if request.url.path.endswith("bundle") else payload)

    with client(handler) as tc:
        result = tc._result("GET", "/v2/participants/collection%3AEXAMPLE%3A0007",
                            required_fields={"participant": dict, "identifiers": list,
                                             "count": int, "identity_scope": str}, bounded=False)
        assert result.data["count"] == 1


def test_empty_success_is_distinct_from_unavailable():
    ready, bundle = identity()

    def handler(request):
        if request.url.path.endswith("ready"):
            return httpx.Response(200, json=ready)
        if request.url.path.endswith("bundle"):
            return httpx.Response(200, json=bundle)
        return httpx.Response(200, json=fixture("empty_datasets.json"))

    with client(handler) as tc:
        page = tc._page("POST", "/v2/datasets/search", "datasets",
                        json_body={}, limit=25, max_limit=200)
        assert page.count == 0 and page.items == () and page.completeness.state == "complete"


@pytest.mark.parametrize("status,error", [
    (404, NotFoundError), (410, RetiredRouteError), (422, InvalidRequestError),
    (503, ServiceUnavailableError),
])
def test_problem_errors_preserve_server_fields(status, error):
    problem = {"code": "fixture_code", "detail": "fixture detail", "status": status,
               "retryable": status == 503}
    with client(lambda r: httpx.Response(status, json=problem)) as tc:
        with pytest.raises(error) as caught:
            tc._transport.request_json("GET", "/v2/ready")
        assert (caught.value.code, caught.value.status, caught.value.retryable) == (
            "fixture_code", status, status == 503)


def test_transport_retries_only_read_failures_and_closes():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("offline")
        return httpx.Response(200, json={"ok": True})

    tc = client(handler, retries=1)
    assert tc._transport.request_json("GET", "/v2/ready") == {"ok": True}
    assert calls == 2
    tc.close()
    with pytest.raises(TransportError):
        tc._transport.request_json("GET", "/v2/ready")


def test_transport_timeout_retryable_status_and_malformed_json():
    def timeout(_):
        raise httpx.ReadTimeout("timed out")

    with client(timeout, retries=1) as tc:
        with pytest.raises(TransportError) as caught:
            tc._transport.request_json("GET", "/v2/ready")
        assert caught.value.retryable is True
    calls = 0

    def responses(_):
        nonlocal calls
        calls += 1
        return httpx.Response(503, json=fixture("unavailable_problem.json")) if calls == 1 else httpx.Response(200, json={"ok": True})

    with client(responses, retries=1) as tc:
        assert tc._transport.request_json("GET", "/v2/ready") == {"ok": True}
    assert calls == 2
    with client(lambda r: httpx.Response(200, text="not-json")) as tc:
        with pytest.raises(ProtocolError):
            tc._transport.request_json("GET", "/v2/ready")


def test_nonretryable_4xx_and_5xx_stop_after_one_attempt():
    for status in (422, 500):
        calls = []

        def handler(request):
            calls.append(request)
            return httpx.Response(status, json={"code": "failure", "detail": "stop",
                                                "status": status, "retryable": False})

        with client(handler, retries=2) as tc:
            with pytest.raises(Exception):
                tc._transport.request_json("GET", "/v2/ready")
        assert len(calls) == 1
