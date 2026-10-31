import copy
import httpx
import pytest
from tcia_utils import TCIAClient, ProtocolError, ServiceUnavailableError, NotFoundError


def plan_body():
    return {
        "plan_version": 1,
        "short_title": "EXAMPLE",
        "dataset_type": "Collection",
        "downloads": [
            {
                "short_title": "EXAMPLE",
                "dataset_type": "Collection",
                "download_id": "1",
                "download_types": ["Pathology Images"],
                "access_level": "open",
                "download_url": "https://faspex.cancerimagingarchive.net/?context=EXAMPLE",
                "software_requirement": "aspera",
                "requirement_status": "declared",
                "download_method": "aspera_package",
            }
        ],
        "software": {
            "aspera": {
                "label": "Requires Aspera",
                "help_url": "https://example.org/help",
                "instructions": "Install transfer software in this runtime.",
            }
        },
        "scope": {"selected_groups": 1, "inventory_groups": 1},
        "notes": ["Metadata only."],
    }


def client_for(body, calls, status=200):
    def handler(request):
        calls.append(request)
        if request.url.path.endswith("/ready"):
            return httpx.Response(
                200,
                json={
                    "status": "ready",
                    "release_fingerprint": "fp",
                    "installed_profile": "research_detail",
                },
            )
        if request.url.path.endswith("/bundle"):
            return httpx.Response(
                200,
                json={
                    "v2_bundle_manifest_exists": True,
                    "v2_install_state_exists": True,
                    "v2_capabilities": {},
                    "v2_install": {
                        "release_fingerprint": "fp",
                        "installed_profile": "research_detail",
                    },
                    "v2_bundle": {"release_fingerprint": "fp"},
                },
            )
        assert request.url.path.endswith("/download-plan")
        return httpx.Response(status, json=body)

    return TCIAClient(
        base_url="https://example.org",
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_plan_uses_shared_endpoint_and_forwards_exact_selectors():
    calls = []
    with client_for(plan_body(), calls) as client:
        plan = client.plan_downloads(
            "EXAMPLE", dataset_type="collection", download_ids=["1"], access_levels=["open"]
        )
    assert plan.rows[0]["python_route"] == "public_package"
    assert plan.rows[0]["python_supported"] is False
    assert "official download link" in plan.rows[0]["python_support_message"]
    assert plan.rows[0]["download_category"] == ["Pathology Images"]
    assert plan.software["aspera"]["label"] == "Requires Aspera"
    assert plan.scope["metadata_snapshot"] == "fp"
    request = calls[-1]
    assert request.url.params.get_list("download_ids") == ["1"]
    assert request.url.params["dataset_type"] == "collection"
    pytest.importorskip("pandas")
    assert plan.to_dataframe().iloc[0].download_requirements == "Requires Aspera"


@pytest.mark.parametrize(
    "fault", ["version", "undefined_software", "count", "invalid_method", "invalid_help"]
)
def test_bad_shared_plans_are_rejected(fault):
    body = plan_body()
    if fault == "version":
        body["plan_version"] = 2
    if fault == "undefined_software":
        body["software"] = {}
    if fault == "count":
        body["scope"]["selected_groups"] = 2
    if fault == "invalid_method":
        body["downloads"][0]["download_method"] = "guessed"
    if fault == "invalid_help":
        body["software"]["aspera"].pop("help_url")
    with client_for(body, []) as client, pytest.raises(ProtocolError):
        client.plan_downloads("EXAMPLE")


def test_missing_shared_endpoint_has_actionable_upgrade_message():
    with (
        client_for({"detail": "Not Found"}, [], 404) as client,
        pytest.raises(ServiceUnavailableError, match="Update the service"),
    ):
        client.plan_downloads("EXAMPLE")


def test_unknown_dataset_retains_not_found_semantics():
    body = {"detail": "No visible TCIA dataset found for short_title=missing", "code": "not_found"}
    with client_for(body, [], 404) as client, pytest.raises(NotFoundError):
        client.plan_downloads("missing")


def test_conflict_is_not_overridden_by_an_aspera_url():
    body = plan_body()
    body["downloads"][0]["requirement_status"] = "conflict"
    body["downloads"][0]["download_method"] = "unsupported"
    with client_for(body, []) as client:
        plan = client.plan_downloads("EXAMPLE")
    assert plan.rows[0]["python_route"] == "unsupported"


def test_empty_plan_retains_usable_table_schema():
    pytest.importorskip("pandas")
    body = plan_body()
    body["downloads"] = []
    body["software"] = {}
    body["scope"]["selected_groups"] = 0
    with client_for(body, []) as client:
        plan = client.plan_downloads("EXAMPLE")
    frame = plan.to_dataframe()
    assert frame.empty and "download_requirements" in frame.columns
    assert frame[frame["python_supported"]].empty
    assert plan.notes == ("Metadata only.",)


def test_wrong_plan_identity_is_rejected():
    body = plan_body()
    body["downloads"][0]["short_title"] = "OTHER"
    with client_for(body, []) as client, pytest.raises(ProtocolError, match="identity"):
        client.plan_downloads("EXAMPLE")
