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


def test_manifest_sample_sends_only_selected_whole_series_to_tdr(tmp_path, monkeypatch):
    path = tmp_path / "all.tcia"
    original = b"ListOfSeriesToDownload=\n1.2.3\n1.2.4\n1.2.5\n"
    path.write_bytes(original)
    calls = fake_imaging(monkeypatch, ["1.2.3"])
    report = DownloadsMixin().download_manifest(
        path, tmp_path / "out", max_items=1, accept_data_policy=True
    )
    assert calls[0]["seriesInstanceUID"] == ["1.2.3"]
    assert report.complete and report.scope["is_sample"]
    assert report.scope["sample_selected_items"] == 1 and report.scope["sample_omitted_items"] == 2
    assert report.entries[0]["sample_unit"] == "DICOM series"
    assert list(csv.DictReader(calls[0]["manifest"].open())) == [{"SeriesInstanceUID": "1.2.3"}]
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
        "https://example.org/cohort.csv",
        tmp_path / "out",
        max_items=1,
        dry_run=True,
        accept_data_policy=True,
    )
    assert len(requests) == 1 and not (tmp_path / "out").exists()
    assert report.entries[0]["sample_selected_items"] == 1
    assert report.entries[0]["sample_available_items"] == 2 and not report.complete


def test_dataset_sample_limit_is_global_across_manifest_groups(tmp_path, monkeypatch, tdr_calls):
    requested = []

    def handler(request):
        requested.append(request.url.path)
        prefix = Path(request.url.path).stem
        return httpx.Response(
            200,
            content=f"imageUrl,file_name\nhttps://files.example.org/{prefix}1.bin,{prefix}1.bin\nhttps://files.example.org/{prefix}2.bin,{prefix}2.bin\n".encode(),
        )

    fake_http(monkeypatch, handler)
    groups = [
        row(download_id=str(i), download_url=f"https://www.cancerimagingarchive.net/{x}.csv")
        for i, x in enumerate(["a", "b", "c"])
    ]
    report = Client(groups).download_dataset(
        "EXAMPLE", tmp_path, max_items=3, accept_data_policy=True
    )
    assert [r["file_name"] for call in tdr_calls for r in call["records"]] == [
        "a1.bin",
        "a2.bin",
        "b1.bin",
    ]
    assert [g["selected_items"] for g in report.scope["sample_groups"]] == [2, 1, 0]
    assert report.scope["sample_available_items"] == 6 and report.scope["sample_omitted_items"] == 3
    assert all(path.endswith(".csv") for path in requested)


def test_explicit_file_sample_does_not_mutate_input(tmp_path, monkeypatch, tdr_calls):
    files = [
        row(download_id=str(i), download_url=f"https://files.example.org/{i}.bin") for i in range(3)
    ]
    report = DownloadsMixin().download_files(files, tmp_path, max_items=2, accept_data_policy=True)
    assert report.complete and len(tdr_calls) == 1 and len(tdr_calls[0]["records"]) == 2
    assert report.scope["explicit_file_records"] == 3 and report.scope["sample_omitted_items"] == 1
    assert all("sample_selected_items" not in r for r in files)


def test_controlled_sample_passes_only_the_selected_file_to_tdr(tmp_path, monkeypatch, tdr_calls):
    p = tmp_path / "controlled.csv"
    p.write_text(
        "drs_uri,file_name\ndrs://nci-crdc.datacommons.io/dg.4DFC/one,one.zip\ndrs://nci-crdc.datacommons.io/dg.4DFC/two,two.zip\n"
    )
    key = tmp_path / "key.json"
    key.write_text('{"api_key":"FAKE-KEY"}')
    report = DownloadsMixin().download_manifest(
        p, tmp_path / "out", max_items=1, credential_file=key, accept_data_policy=True
    )
    assert report.complete and len(tdr_calls) == 1
    assert [r["drs_uri"] for r in tdr_calls[0]["records"]] == [
        "drs://nci-crdc.datacommons.io/dg.4DFC/one"
    ]
    assert tdr_calls[0]["options"]["credential_file"] == key.resolve()
    assert "FAKE-KEY" not in str(report) and "two.zip" not in str(tdr_calls)


def test_sample_does_not_relax_controlled_credentials(tmp_path, monkeypatch):
    path = tmp_path / "controlled.csv"
    path.write_text("drs_uri,file_name\ndrs://nci-crdc.datacommons.io/dg.4DFC/one,one.zip\n")
    fake_http(monkeypatch, lambda _: pytest.fail("No authorization/payload request"))
    report = DownloadsMixin().download_manifest(
        path, tmp_path / "out", max_items=1, dry_run=True, accept_data_policy=True
    )
    assert report.entries[0]["status"] == "requires_credential"
    with pytest.raises(DownloadError, match="approved"):
        DownloadsMixin().download_manifest(
            path, tmp_path / "out", max_items=1, accept_data_policy=True
        )


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
        report = c.download_dataset(
            "EXAMPLE", tmp_path, max_items=1, dry_run=True, accept_data_policy=True
        )
    else:
        with pytest.raises(DownloadError, match="Cannot sample") as caught:
            c.download_dataset("EXAMPLE", tmp_path, max_items=1, accept_data_policy=True)
        report = caught.value.report
    assert [r["status"] for r in report.entries] == ["not_attempted", "unsupported_sample"]
    assert not list(tmp_path.iterdir())


def test_failed_sample_retains_sample_scope_and_counts(tmp_path, monkeypatch):
    from tdr_fakes import install_tdr, EngineError

    def fail(call):
        raise EngineError()

    install_tdr(monkeypatch, handler=fail)
    with pytest.raises(DownloadError) as caught:
        DownloadsMixin().download_files([row()], tmp_path, max_items=2, accept_data_policy=True)
    report = caught.value.report
    assert not report.complete and report.scope["is_sample"]
    assert report.scope["sample_selected_items"] == 1 and report.entries[0]["status"] == "failed"
