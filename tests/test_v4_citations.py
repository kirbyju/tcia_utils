"""DataCite and verified analytical-use remain distinct evidence streams."""

from __future__ import annotations

import httpx
import pytest

from tcia_utils import InvalidRequestError, ProtocolError, ServiceUnavailableError, TCIAClient


CitationClient = TCIAClient


def make_client(payload, calls, *, datacite=True, publications=True):
    def handler(request):
        calls.append(request)
        if request.url.path == "/v2/ready":
            return httpx.Response(200, json={"status": "ready", "release_fingerprint": "r1",
                                             "installed_profile": "research_detail"})
        if request.url.path == "/v2/bundle":
            return httpx.Response(200, json={
                "v2_bundle_manifest_exists": True, "v2_install_state_exists": True,
                "v2_capabilities": {"datacite_metadata": datacite,
                                    "verified_analytical_publications": publications},
                "v2_install": {"release_fingerprint": "r1", "installed_profile": "research_detail"},
                "v2_bundle": {"release_fingerprint": "r1"},
            })
        return httpx.Response(200, json=payload)

    return CitationClient(base_url="https://example.test", retries=0,
                          http_client=httpx.Client(transport=httpx.MockTransport(handler)))


@pytest.mark.parametrize("style", ["apa", "vancouver", "chicago-author-date", "bibtex", "ris"])
def test_citation_style_and_doi_path_encoding(style):
    calls = []
    record = {"doi": "10.1234/a b?c", "citation_style": style,
              "formatted_citation": "Synthetic citation", "metadata": {}}
    with make_client({"record": record, "provenance_note": "DataCite enrichment"}, calls) as tc:
        result = tc.get_citation("10.1234/a b?c", style=style)
    route = next(call for call in calls if call.url.path.startswith("/v2/datacite/dois/"))
    assert route.url.raw_path.startswith(b"/v2/datacite/dois/10.1234/a%20b%3Fc")
    assert route.url.params["citation_style"] == style
    assert result.data["record"]["formatted_citation"] == "Synthetic citation"
    assert result.notes == ("DataCite enrichment",)


def test_citation_search_keeps_event_metrics_separate():
    calls = []
    payload = {"dois": [{"doi": "10.1/example", "citation_count": 3,
                        "view_count": 7, "download_count": 11}],
               "count": 1, "limit": 25, "has_more": False, "truncated": False,
               "next_cursor": None, "metrics_note": "DataCite event metrics, not TCIA totals"}
    with make_client(payload, calls) as tc:
        page = tc.search_citations(short_titles=["EXAMPLE"], sort_by="download_count")
    assert page.items[0]["download_count"] == 11
    assert "not TCIA totals" in page.response["metrics_note"]
    route = next(call for call in calls if call.url.path == "/v2/datacite/dois")
    assert route.url.params["sort_by"] == "download_count"
    assert route.url.params.get_list("short_titles") == ["EXAMPLE"]


def test_verified_use_search_retains_source_note_and_cursor():
    calls = []
    payload = {"publications": [{"rec_number": "123", "title": "An analysis",
                                 "linked_tcia_dataset_dois": ["10.1/example"]}],
               "count": 1, "limit": 1, "has_more": True, "truncated": True,
               "next_cursor": "opaque", "verification_note": "TCIA-maintained EndNote library"}
    with make_client(payload, calls) as tc:
        page = tc.search_publications(dataset_dois=["10.1/example"], limit=1)
    assert page.completeness.state == "partial"
    assert page.items[0]["linked_tcia_dataset_dois"] == ["10.1/example"]
    assert page.response["verification_note"] == "TCIA-maintained EndNote library"


def test_impact_keeps_datacite_and_verified_use_counts_distinct():
    calls = []
    payload = {
        "impacts": [{"short_title": "EXAMPLE", "dataset_type": "collection",
                     "datacite_citation_count": 3,
                     "verified_analytical_publication_count": 8}],
        "verified_publications": [{"rec_number": "123"}], "publication_limit": 25,
        "notes": ["DataCite events and TCIA verified analytical use are different."],
    }
    with make_client(payload, calls) as tc:
        result = tc.get_impact("EXAMPLE")
    assert result.data["impacts"][0]["datacite_citation_count"] == 3
    assert result.data["impacts"][0]["verified_analytical_publication_count"] == 8
    assert result.completeness.state == "unknown"  # bounded publication sublist
    route = next(call for call in calls if call.url.path.endswith("/impact"))
    assert route.url.params["publication_limit"] == "25"


def test_invalid_style_sort_or_limit_fails_locally():
    calls = []
    with make_client({}, calls) as tc:
        with pytest.raises(InvalidRequestError):
            tc.get_citation("10.1/a", style="invented")
        with pytest.raises(InvalidRequestError):
            tc.search_citations(sort_by="invented")
        with pytest.raises(InvalidRequestError):
            tc.get_impact("EXAMPLE", publication_limit=201)
    assert calls == []


def test_malformed_citation_record_is_protocol_error():
    calls = []
    with make_client({"record": {"doi": "10.1/a"}, "provenance_note": "DataCite"}, calls) as tc:
        with pytest.raises(ProtocolError):
            tc.get_citation("10.1/a")


def test_unavailable_optional_capability_stops_before_data_route():
    calls = []
    with make_client({}, calls, datacite=False) as tc:
        with pytest.raises(ServiceUnavailableError):
            tc.search_citations()
    assert all(call.url.path in {"/v2/ready", "/v2/bundle"} for call in calls)


def test_empty_citation_filters_do_not_widen_search():
    calls = []
    with make_client({}, calls) as tc:
        with pytest.raises(InvalidRequestError):
            tc.search_citations(dois=[])
        with pytest.raises(InvalidRequestError):
            tc.search_publications(dataset_dois=[" "])
    assert calls == []
