"""Offline sample transfers verify limits at the adapter boundary, using synthetic data only."""

import csv
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from tcia_utils import DownloadError, InvalidRequestError
from tcia_utils._downloads import DownloadsMixin
import tcia_utils._downloads as downloads
from test_v4_downloads import Client, row, fake_http
from test_v4_manifest_downloads import fake_imaging


@pytest.mark.parametrize("entrypoint", ["download_dataset", "download_manifest", "download_files"])
@pytest.mark.parametrize("limit", [0, -1, True, 1.5, "2"])
def test_invalid_limit_precedes_metadata_or_transfer(tmp_path, monkeypatch, entrypoint, limit):
    fake_http(monkeypatch, lambda _: pytest.fail("No metadata or payload request"))
    c = Client([])
    monkeypatch.setattr(c, "plan_downloads", lambda *a, **k: pytest.fail("No API request"))
    with pytest.raises(InvalidRequestError, match="max_items"):
        getattr(c, entrypoint)("not-read", tmp_path, max_items=limit)


def test_manifest_sample_sends_only_selected_whole_series_to_imaging_client(tmp_path, monkeypatch):
    path = tmp_path / "all.tcia"
    original = b"ListOfSeriesToDownload=\n1.2.3\n1.2.4\n1.2.5\n"
    path.write_bytes(original)
    calls = fake_imaging(monkeypatch, ["1.2.3"])  # Unselected series need not be in this index.
    report = DownloadsMixin().download_manifest(path, tmp_path / "out", max_items=1)
    assert calls[0]["seriesInstanceUID"] == ["1.2.3"]
    assert report.complete and report.scope["is_sample"]
    assert report.scope["sample_selected_items"] == 1 and report.scope["sample_omitted_items"] == 2
    assert report.entries[0]["sample_unit"] == "DICOM series"
    subset = next((tmp_path / "out").rglob("selection.csv"))
    assert list(csv.DictReader(subset.open())) == [{"SeriesInstanceUID": "1.2.3"}]
    assert path.read_bytes() == original


def test_sample_dry_run_reads_manifest_but_not_payload_index_or_auth(tmp_path, monkeypatch):
    requests = []

    def handler(request):
        requests.append(str(request.url))
        assert request.url.path == "/cohort.csv"
        return httpx.Response(
            200,
            content=b"imageUrl,file_name\nhttps://files.example.org/a.bin,a.bin\nhttps://files.example.org/b.bin,b.bin\n",
        )

    fake_http(monkeypatch, handler)
    report = DownloadsMixin().download_manifest(
        "https://example.org/cohort.csv", tmp_path / "out", max_items=1, dry_run=True
    )
    assert len(requests) == 1 and not (tmp_path / "out").exists()
    assert report.entries[0]["sample_selected_items"] == 1
    assert report.entries[0]["sample_available_items"] == 2 and not report.complete


def test_dataset_limit_is_global_and_omitted_groups_are_accounted_for(tmp_path, monkeypatch):
    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path.endswith(".csv"):
            prefix = Path(request.url.path).stem
            return httpx.Response(
                200,
                content=f"imageUrl,file_name\nhttps://files.example.org/{prefix}1.bin,{prefix}1.bin\nhttps://files.example.org/{prefix}2.bin,{prefix}2.bin\n".encode(),
            )
        return httpx.Response(200, content=b"DATA")

    fake_http(monkeypatch, handler)
    groups = [
        row(download_id=str(i), download_url=f"https://www.cancerimagingarchive.net/{x}.csv")
        for i, x in enumerate(["a", "b", "c"])
    ]
    report = Client(groups).download_dataset("EXAMPLE", tmp_path, max_items=3)
    assert sorted(p.name for p in tmp_path.rglob("*.bin")) == ["a1.bin", "a2.bin", "b1.bin"]
    assert [g["selected_items"] for g in report.scope["sample_groups"]] == [2, 1, 0]
    assert report.scope["sample_available_items"] == 6 and report.scope["sample_omitted_items"] == 3
    assert "/b2.bin" not in requests and "/c1.bin" not in requests


def test_explicit_file_sample_does_not_mutate_input(tmp_path, monkeypatch):
    requests = []
    fake_http(
        monkeypatch, lambda r: requests.append(r.url.path) or httpx.Response(200, content=b"DATA")
    )
    files = [
        row(download_id=str(i), download_url=f"https://files.example.org/{i}.bin") for i in range(3)
    ]
    report = DownloadsMixin().download_files(files, tmp_path, max_items=2)
    assert len(requests) == 2 and report.complete
    assert report.scope["explicit_file_records"] == 3 and report.scope["sample_omitted_items"] == 1
    assert all("sample_selected_items" not in r for r in files)


def test_controlled_sample_authorizes_only_the_selected_file(tmp_path, monkeypatch):
    manifest = tmp_path / "controlled.csv"
    manifest.write_text(
        "drs_uri,file_name\ndrs://nci-crdc.datacommons.io/dg.4DFC/one,one.zip\ndrs://nci-crdc.datacommons.io/dg.4DFC/two,two.zip\n"
    )
    key = tmp_path / "key.json"
    key.write_text(json.dumps({"api_key": "FAKE_KEY"}))
    key.chmod(0o600)
    requests = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path.endswith("/access_token"):
            return httpx.Response(200, json={"access_token": "FAKE_TOKEN"})
        if "/user/data/download/" in request.url.path:
            assert request.url.path.endswith("/one")
            return httpx.Response(
                200, json={"url": "https://files.example.org/one.zip?signature=SECRET"}
            )
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"DATA")

    fake_http(monkeypatch, handler)
    report = DownloadsMixin().download_manifest(
        manifest, tmp_path / "out", max_items=1, credential_file=key
    )
    assert report.complete and len(list((tmp_path / "out").rglob("*.zip"))) == 1
    assert all("two" not in p for p in requests)
    assert "FAKE_KEY" not in str(report) and "SECRET" not in str(report)


def test_sample_does_not_relax_controlled_credentials(tmp_path, monkeypatch):
    path = tmp_path / "controlled.csv"
    path.write_text("drs_uri,file_name\ndrs://nci-crdc.datacommons.io/dg.4DFC/one,one.zip\n")
    fake_http(monkeypatch, lambda _: pytest.fail("No authorization/payload request"))
    report = DownloadsMixin().download_manifest(path, tmp_path / "out", max_items=1, dry_run=True)
    assert report.entries[0]["status"] == "requires_credential"
    with pytest.raises(DownloadError, match="approved"):
        DownloadsMixin().download_manifest(path, tmp_path / "out", max_items=1)


@pytest.mark.parametrize("dry_run", [False, True])
def test_package_sample_never_invokes_full_package_transfer(tmp_path, monkeypatch, dry_run):
    fake_http(monkeypatch, lambda _: pytest.fail("No HTTP"))
    c = Client(
        [
            row(),
            row(
                download_id="package",
                download_url="https://faspex.cancerimagingarchive.net/package/id",
            ),
        ]
    )
    if dry_run:
        report = c.download_dataset("EXAMPLE", tmp_path, max_items=1, dry_run=True)
    else:
        with pytest.raises(DownloadError, match="Cannot sample") as caught:
            c.download_dataset("EXAMPLE", tmp_path, max_items=1)
        report = caught.value.report
    assert [r["status"] for r in report.entries] == ["not_attempted", "unsupported_sample"]
    assert not list(tmp_path.iterdir())




def test_failed_sample_retains_sample_scope_and_counts(tmp_path, monkeypatch):
    fake_http(monkeypatch, lambda _: httpx.Response(500))
    with pytest.raises(DownloadError) as caught:
        DownloadsMixin().download_files([row()], tmp_path, max_items=2)
    report = caught.value.report
    assert not report.complete and report.scope["is_sample"]
    assert report.scope["sample_selected_items"] == 1 and report.entries[0]["status"] == "failed"
