from pathlib import Path
from types import SimpleNamespace
import csv
import hashlib
import json
import sys
import httpx
import pytest
from tcia_utils import DownloadError
from tcia_utils._downloads import DownloadsMixin
import tcia_utils._downloads as downloads


def client():
    return DownloadsMixin()


def fake_imaging(monkeypatch, uids):
    calls = []
    imaging = SimpleNamespace(
        index={"SeriesInstanceUID": uids},
        get_idc_version=lambda: "test",
        download_dicom_series=lambda **kwargs: calls.append(kwargs),
    )
    monkeypatch.setitem(sys.modules, "idc_index", SimpleNamespace(IDCClient=lambda: imaging))
    monkeypatch.setattr(downloads.importlib.util, "find_spec", lambda _: object())
    return calls


def fake_http(monkeypatch, handler):
    monkeypatch.setattr(
        downloads,
        "_http",
        lambda: httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False),
    )


@pytest.mark.parametrize(
    "suffix,content",
    [
        (
            ".tcia",
            b"downloadServerUrl=https://example.org\nListOfSeriesToDownload=\n1.2.3\n1.2.4\n",
        ),
        (".csv", b"SeriesInstanceUID\n1.2.3\n1.2.4\n"),
        (".tsv", b"SeriesInstanceUID\n1.2.3\n1.2.4\n"),
    ],
)
def test_local_series_manifest_delegates_exact_selection(tmp_path, monkeypatch, suffix, content):
    path = tmp_path / ("cohort" + suffix)
    path.write_bytes(content)
    calls = fake_imaging(monkeypatch, ["1.2.3", "1.2.4"])
    fake_http(monkeypatch, lambda r: pytest.fail("Local manifest needs no HTTP metadata request"))
    report = client().download_manifest(path, tmp_path / "out")
    assert report.complete and calls[0]["seriesInstanceUID"] == ["1.2.3", "1.2.4"]
    assert report.scope["manifest_sha256"] == hashlib.sha256(content).hexdigest()
    assert report.scope["manifest_items"] == 2


def test_url_manifest_is_read_without_credentials_and_source_query_is_not_reported(
    tmp_path, monkeypatch
):
    calls = fake_imaging(monkeypatch, ["1.2.3"])

    def handler(request):
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"SeriesInstanceUID\n1.2.3\n")

    fake_http(monkeypatch, handler)
    report = client().download_manifest(
        "https://example.org/cohort.csv?signature=PRIVATE_QUERY", tmp_path
    )
    assert report.complete and len(calls) == 1
    assert "PRIVATE_QUERY" not in str(report)


def test_missing_series_stops_the_whole_manifest(tmp_path, monkeypatch):
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n1.2.4\n")
    calls = fake_imaging(monkeypatch, ["1.2.3"])
    with pytest.raises(DownloadError) as error:
        client().download_manifest(path, tmp_path / "out")
    assert not calls and not error.value.report.complete
    assert error.value.report.entries[0]["missing_series_uids"] == ["1.2.4"]


def test_public_file_manifest_downloads_all_synthetic_files(tmp_path, monkeypatch):
    path = tmp_path / "cohort.csv"
    path.write_text(
        "imageUrl,file_name\nhttps://files.example.org/a.bin,first.bin\nhttps://files.example.org/b.bin,second.bin\n"
    )
    fake_http(monkeypatch, lambda request: httpx.Response(200, content=b"DATA"))
    report = client().download_manifest(path, tmp_path / "out")
    assert report.complete
    assert sorted(p.name for p in (tmp_path / "out").rglob("*.bin")) == ["first.bin", "second.bin"]
    assert all(p.read_bytes() == b"DATA" for p in (tmp_path / "out").rglob("*.bin"))


def test_drs_manifest_requires_key_before_authentication_or_payload(tmp_path, monkeypatch):
    path = tmp_path / "controlled.csv"
    path.write_text("drs_uri,file_name\ndrs://nci-crdc.datacommons.io/dg.4DFC/test,data.zip\n")
    fake_http(monkeypatch, lambda _: pytest.fail("No authentication without approved key"))
    report = client().download_manifest(path, tmp_path / "out", dry_run=True)
    assert report.entries[0]["status"] == "requires_credential" and not (tmp_path / "out").exists()
    with pytest.raises(DownloadError, match="approved"):
        client().download_manifest(path, tmp_path / "out")
    with pytest.raises(DownloadError, match="DRS"):
        client().download_manifest(path, tmp_path / "out", access_level="open")


def test_drs_manifest_resolves_files_with_fake_credentials(tmp_path, monkeypatch):
    path = tmp_path / "controlled.csv"
    path.write_text(
        "drs_uri,CTDC File Name,CTDC Size\ndrs://nci-crdc.datacommons.io/dg.4DFC/test,original.zip,4\n"
    )
    key = tmp_path / "key.json"
    key.write_text(json.dumps({"api_key": "FAKE_KEY"}))
    key.chmod(0o600)

    def handler(request):
        if request.url.path.endswith("/access_token"):
            return httpx.Response(200, json={"access_token": "FAKE_TOKEN"})
        if "/user/data/download/" in request.url.path:
            return httpx.Response(
                200, json={"url": "https://files.example.org/blob?signature=PRIVATE_QUERY"}
            )
        assert "authorization" not in request.headers
        return httpx.Response(200, content=b"DATA")

    fake_http(monkeypatch, handler)
    report = client().download_manifest(path, tmp_path / "out", credential_file=key)
    assert (
        report.complete and next((tmp_path / "out").rglob("original.zip")).read_bytes() == b"DATA"
    )
    assert all(value not in str(report) for value in ["FAKE_KEY", "FAKE_TOKEN", "PRIVATE_QUERY"])


@pytest.mark.parametrize(
    "content",
    [
        b"SeriesInstanceUID,imageUrl\n1.2.3,https://example.org/data.bin\n",
        b"imageUrl\nhttps://files.example.org/first.bin\nhttp://127.0.0.1/second.bin\n",
        b"imageUrl,file_name\nhttps://files.example.org/a.bin,same.bin\nhttps://files.example.org/b.bin,same.bin\n",
        b"drs_uri,file_name\ndrs://evil.example.org/id,data.bin\n",
        b"SeriesInstanceUID\n\n",
    ],
)
def test_invalid_or_mixed_manifest_never_starts_payload(tmp_path, monkeypatch, content):
    path = tmp_path / "bad.csv"
    path.write_bytes(content)
    fake_http(monkeypatch, lambda _: pytest.fail("No transfer for invalid manifest"))
    with pytest.raises(DownloadError):
        client().download_manifest(path, tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_non_manifest_payload_url_is_not_fetched(tmp_path, monkeypatch):
    fake_http(monkeypatch, lambda _: pytest.fail("Do not fetch a payload as a manifest"))
    with pytest.raises(DownloadError, match="formats"):
        client().download_manifest("https://example.org/images.zip", tmp_path)




def test_uid_manifest_dry_run_does_not_initialize_imaging_client(tmp_path, monkeypatch):
    path = tmp_path / "cohort.csv"
    path.write_text("SeriesInstanceUID\n1.2.3\n")
    monkeypatch.setitem(
        sys.modules,
        "idc_index",
        SimpleNamespace(
            IDCClient=lambda: pytest.fail("No imaging-index initialization during dry run")
        ),
    )
    monkeypatch.setattr(downloads.importlib.util, "find_spec", lambda _: object())
    report = client().download_manifest(path, tmp_path / "out", dry_run=True)
    assert report.entries[0]["status"] == "planned" and not report.complete
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    "content",
    [b"SeriesInstanceUID\nnot-a-uid\n", b"SeriesInstanceUID,SeriesInstanceUID\n1.2.3,1.2.4\n"],
)
def test_invalid_uid_csv_is_rejected_before_any_payload(tmp_path, monkeypatch, content):
    path = tmp_path / "bad.csv"
    path.write_bytes(content)
    fake_http(monkeypatch, lambda _: pytest.fail("No HTTP for malformed UID CSV"))
    with pytest.raises(DownloadError):
        client().download_manifest(path, tmp_path / "out")


def test_uid_csv_extra_metadata_does_not_control_destination_names(tmp_path, monkeypatch):
    path = tmp_path / "cohort.csv"
    path.write_text("SeriesInstanceUID,file_name\n1.2.3,original/folder/image.dcm\n")
    calls = fake_imaging(monkeypatch, ["1.2.3"])
    report = client().download_manifest(path, tmp_path / "out")
    assert report.complete and calls[0]["seriesInstanceUID"] == ["1.2.3"]
    assert "original/folder/image.dcm" not in str(calls)
