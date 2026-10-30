"""Validate an installed review wheel against the actual shared REST adapter.

Requires the query repository's hash-locked server dependencies and local fixture
source. All requests use ASGI fixtures; no payload or upstream HTTP is requested.
"""

import argparse
from importlib.metadata import distribution, version
import json
from pathlib import Path
import sys
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--service-source", type=Path, required=True)
parser.add_argument("--report", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.service_source.resolve()))
import httpx
from fastapi.testclient import TestClient
from mcp_server.tests.test_service import TciaQueryServiceTests, connect as fixture_connect
from mcp_server.tcia_query_mcp.rest import create_app
import tcia_utils
from tcia_utils import TCIAClient

assert "site-packages" in Path(tcia_utils.__file__).parts, "Install the review wheel first"
assert not (
    json.loads(distribution("tcia_utils").read_text("direct_url.json") or "{}")
    .get("dir_info", {})
    .get("editable")
)
fixture = TciaQueryServiceTests()
fixture.setUp()
with fixture.service._connect_public_non_dicom() as conn:
    public_db = conn.execute("PRAGMA database_list").fetchone()[2]
with fixture_connect(public_db) as conn:
    conn.execute(
        "UPDATE agent_public_non_dicom_assets SET source_system='tcia_pathdb',media_kind='whole_slide_image',modality='Whole Slide Image',file_format='SVS',subject_id_namespace='tcia_dataset:TCGA-BRCA',source_url='http://pathdb.cancerimagingarchive.net/system/files/wsi/example/BRCA-1.svs' WHERE asset_id='pnd-1'"
    )
requests = []
try:
    with TestClient(create_app(fixture.service)) as rest:

        def send(request):
            assert "authorization" not in request.headers
            requests.append((request.method, request.url.path))
            response = rest.request(
                request.method,
                request.url.path + ("?" + request.url.query.decode() if request.url.query else ""),
                content=request.content,
                headers={"content-type": "application/json"},
            )
            return httpx.Response(
                response.status_code,
                content=response.content,
                headers={"content-type": response.headers["content-type"]},
            )

        with TCIAClient(
            base_url="https://fixture.tcia.example",
            pin_snapshot=True,
            http_client=httpx.Client(transport=httpx.MockTransport(send)),
        ) as client:
            search = client.search_datasets(short_titles=["TCGA-BRCA"]).to_dataframe(all_pages=True)
            assert search.iloc[0].data_categories
            plan = client.plan_downloads("TCGA-BRCA", dataset_type="collection")
            assert plan.rows and plan.scope["selected_groups"] == len(plan.rows)
            assert plan.to_dataframe().iloc[0].download_requirements
            assert all(row["python_supported"] for row in plan.rows)
            check = client.download_dataset(
                "TCGA-BRCA", Path(tempfile.gettempdir()) / "not-created-review-output", dry_run=True
            )
            assert not check.complete and all(e["status"] == "planned" for e in check.entries)
            with tempfile.TemporaryDirectory() as tmp:
                manifest = Path(tmp) / "cohort.tcia"
                manifest.write_text("ListOfSeriesToDownload=\n1.2.3\n1.2.4\n")
                before = len(requests)
                local = client.download_manifest(
                    manifest, Path(tmp) / "out", dry_run=True, max_items=1
                )
                assert (
                    len(requests) == before
                ), "Local manifests must not be uploaded to the service"
                assert not local.complete and not (Path(tmp) / "out").exists()
                assert local.scope["is_sample"] and local.scope["sample_selected_items"] == 1
                assert local.scope["sample_omitted_items"] == 1
            slides = client.plan_participant_images(
                [
                    {
                        "dataset_type": "Collection",
                        "short_title": "TCGA-BRCA",
                        "display_participant_id": "BRCA-1",
                        "participant_key": "participant-1",
                    }
                ],
                modality="Whole Slide Image",
                file_format="non-DICOM",
                progress=False,
            )
            assert len(slides.rows) == 1 and not slides.unresolved
            assert slides.rows[0]["file_name"] == "BRCA-1.svs"
            with tempfile.TemporaryDirectory() as tmp:
                sample = client.download_images(
                    slides, Path(tmp) / "out", max_items=1, dry_run=True, progress=False
                )
                assert sample.scope["is_sample"] and not (Path(tmp) / "out").exists()
            assert ("GET", "/v2/datasets/TCGA-BRCA/download-plan") in requests
            assert ("GET", "/v2/datasets/TCGA-BRCA/downloads") not in requests
            report = {
                "ok": True,
                "sdk_version": version("tcia_utils"),
                "metadata_scope": "Local SQLite/ASGI fixture bundle; not a deployed service",
                "fingerprint": plan.provenance.release_fingerprint,
                "shared_plan_used": True,
                "search_categories_present": True,
                "software_help_present": True,
                "local_manifest_uploaded": False,
                "bounded_local_sample_dry_run": True,
                "public_slide_descriptor_consumed": True,
                "participant_image_plan_used": True,
                "payload_transfers": 0,
                "request_count": len(requests),
            }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
finally:
    fixture.tmp.cleanup()
