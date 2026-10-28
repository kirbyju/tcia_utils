from dataclasses import replace
from types import SimpleNamespace
import pytest
import httpx
from tcia_utils import TCIAClient, ProtocolError, InvalidRequestError
from tcia_utils._research import ResearchMixin
from tcia_utils._results import Provenance, Page, Completeness

P = Provenance("https://example.org", "release-a", "latest", "research_detail", {}, "2026-10-28")


class Client(ResearchMixin):
    def __init__(self, fn):
        self.fn = fn

    def _observe_status(self):
        return None, P

    def list_clinical_subjects(self, title, **kwargs):
        return self.fn(title, **kwargs)


def participants():
    return [
        {
            "dataset_type": "Collection",
            "short_title": title,
            "display_participant_id": str(i),
        }
        for title in ["A", "B"]
        for i in range(101)
    ]


def test_clinical_enrichment_batches_without_cross_dataset_merges():
    calls = []

    def subjects(title, **kwargs):
        calls.append((title, kwargs))
        rows = [
            {
                "short_title": title,
                "subject_id": i,
                "resolved_values": {"diagnosis": title},
            }
            for i in kwargs["subject_ids"]
            if i != "100"
        ]
        return SimpleNamespace(
            data={"count": len(rows), "limit": 500, "subjects": rows}, provenance=P
        )

    result = Client(subjects).clinical_for_participants(participants())
    assert (
        len(calls) == 4
        and result.data["matched_count"] == 200
        and len(result.data["unmatched"]) == 2
    )
    assert len(result.data["values"]) == 200
    assert result.data["values"][0]["concept"] == "diagnosis"
    assert all(
        r["resolved_values"]["diagnosis"] == r["short_title"] for r in result.data["subjects"]
    )


@pytest.mark.parametrize("fault", ["cap", "duplicate", "unrequested", "changed_release"])
def test_clinical_enrichment_stops_at_unverifiable_results(fault):
    row = {"short_title": "A", "subject_id": "0"}

    def subjects(title, **kwargs):
        rows = (
            [row, row]
            if fault == "duplicate"
            else [{**row, "subject_id": "unknown"}] if fault == "unrequested" else [row]
        )
        return SimpleNamespace(
            data={
                "count": 500 if fault == "cap" else len(rows),
                "limit": 500,
                "subjects": rows,
            },
            provenance=(
                replace(P, release_fingerprint="other") if fault == "changed_release" else P
            ),
        )

    with pytest.raises(ProtocolError):
        Client(subjects).clinical_for_participants(participants()[:1])


def test_empty_clinical_selection_retains_explicit_coverage():
    result = Client(
        lambda *a, **k: pytest.fail("No subject query needed")
    ).clinical_for_participants([])
    assert result.data == {
        "subjects": [],
        "values": [],
        "unmatched": [],
        "requested_count": 0,
        "matched_count": 0,
    }


def test_analysis_result_identity_not_assumed_to_be_collection():
    with pytest.raises(InvalidRequestError):
        Client(None).clinical_for_participants(
            [
                {
                    "dataset_type": "Analysis Result",
                    "short_title": "A",
                    "display_participant_id": "0",
                }
            ]
        )


def test_page_dataframe_collects_pages_and_rejects_release_changes():
    pytest.importorskip("pandas")
    second = Page(({"id": 2},), 1, 1, False, False, None, {}, {}, P, Completeness("complete"))
    first = Page(
        ({"id": 1},),
        1,
        1,
        True,
        True,
        "next",
        {},
        {},
        P,
        Completeness("partial"),
        lambda: second,
    )
    assert first.to_dataframe(all_pages=True)["id"].tolist() == [1, 2]
    bad = replace(
        first,
        _fetch_next=lambda: replace(second, provenance=replace(P, release_fingerprint="other")),
    )
    with pytest.raises(ProtocolError):
        bad.to_dataframe(all_pages=True)


def test_snapshot_pin_applies_between_independent_operations():
    fingerprint = ["a"]

    def handler(request):
        fp = fingerprint[0]
        if request.url.path.endswith("ready"):
            body = {
                "status": "ready",
                "release_fingerprint": fp,
                "installed_profile": "research_detail",
            }
        else:
            body = {
                "v2_bundle_manifest_exists": True,
                "v2_install_state_exists": True,
                "v2_capabilities": {},
                "v2_install": {
                    "release_fingerprint": fp,
                    "installed_profile": "research_detail",
                },
                "v2_bundle": {"release_fingerprint": fp},
            }
        return httpx.Response(200, json=body)

    with TCIAClient(
        base_url="https://example.org",
        pin_snapshot=True,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    ) as client:
        client.status()
        fingerprint[0] = "b"
        with pytest.raises(ProtocolError):
            client.status()
