"""Python-only execution and early download progress, with no real transfers."""

import inspect
from pathlib import Path
from types import SimpleNamespace
import sys

import httpx
import pytest
from tcia_utils import DownloadError, InvalidRequestError, TCIAClient
from tcia_utils._downloads import DownloadsMixin
import tcia_utils._downloads as downloads
from test_v4_downloads import Client, row, fake_http


def install_fake_index(monkeypatch, factory):
    monkeypatch.setitem(sys.modules, "idc_index", SimpleNamespace(IDCClient=factory))
    monkeypatch.setattr(downloads.importlib.util, "find_spec", lambda _: object())


def test_early_progress_precedes_manifest_fetch_and_index_loading(tmp_path, monkeypatch, capsys):
    events = []

    def handler(request):
        assert "Preparing download selection" in capsys.readouterr().out
        events.append("manifest")
        return httpx.Response(200, content=b"ListOfSeriesToDownload=\n1.2.3\n")

    fake_http(monkeypatch, handler)

    def factory():
        assert "first use" in capsys.readouterr().out
        events.append("index")
        return SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]},
            download_dicom_series=lambda **kw: events.append("transfer"),
        )

    install_fake_index(monkeypatch, factory)
    path = tmp_path / "out"
    DownloadsMixin().download_manifest("https://example.org/cohort.tcia", path)
    assert events == ["manifest", "index", "transfer"]


def test_index_is_reused_and_sync_enabled_only_for_existing_data(tmp_path, monkeypatch, capsys):
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    constructions = []
    calls = []

    def transfer(**kwargs):
        calls.append(kwargs)
        folder = Path(kwargs["downloadDir"]) / "EXAMPLE"
        folder.mkdir(exist_ok=True)
        (folder / "synthetic.dcm").write_bytes(b"FAKE")

    def factory():
        constructions.append(True)
        return SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]}, download_dicom_series=transfer
        )

    install_fake_index(monkeypatch, factory)
    c = DownloadsMixin()
    c.download_manifest(path, tmp_path / "out")
    c.download_manifest(path, tmp_path / "out")
    assert len(constructions) == 1
    assert [v["use_s5cmd_sync"] for v in calls] == [False, True]
    assert "Reusing the prepared public DICOM index" in capsys.readouterr().out


def test_saved_manifests_alone_do_not_trigger_sync(tmp_path, monkeypatch):
    path = tmp_path / "cohort.csv"
    path.write_text("SeriesInstanceUID\n1.2.3\n")
    calls = []
    install_fake_index(
        monkeypatch,
        lambda: SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]},
            download_dicom_series=lambda **kw: calls.append(kw),
        ),
    )
    c = DownloadsMixin()
    for _ in range(2):
        c.download_manifest(path, tmp_path / "out", progress=False)
    assert [v["use_s5cmd_sync"] for v in calls] == [False, False]


def test_quiet_mode_silences_status_and_upstream_bar(tmp_path, monkeypatch, capsys):
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    calls = []
    install_fake_index(
        monkeypatch,
        lambda: SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]},
            download_dicom_series=lambda **kw: calls.append(kw),
        ),
    )
    DownloadsMixin().download_manifest(path, tmp_path / "out", progress=False)
    assert capsys.readouterr().out == "" and calls[0]["show_progress_bar"] is False


def test_distinct_clients_do_not_share_index_or_credentials(tmp_path, monkeypatch):
    created = []
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n")

    def factory():
        obj = SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]}, download_dicom_series=lambda **kw: None
        )
        created.append(obj)
        return obj

    install_fake_index(monkeypatch, factory)
    for i in range(2):
        DownloadsMixin().download_manifest(path, tmp_path / str(i), progress=False)
    assert len(created) == 2


def test_close_releases_lazy_index_reference(monkeypatch):
    obj = SimpleNamespace(index={"SeriesInstanceUID": ["1.2.3"]})
    install_fake_index(monkeypatch, lambda: obj)
    c = TCIAClient(base_url="https://metadata.example.org")
    assert c._get_imaging_client(False) is obj
    c.close()
    assert c._imaging_client is None


def test_initialization_failure_is_not_cached(tmp_path, monkeypatch):
    calls = []
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n")

    def factory():
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("FAKE_PRIVATE_DETAIL")
        return SimpleNamespace(
            index={"SeriesInstanceUID": ["1.2.3"]}, download_dicom_series=lambda **kw: None
        )

    install_fake_index(monkeypatch, factory)
    c = DownloadsMixin()
    with pytest.raises(DownloadError) as caught:
        c.download_manifest(path, tmp_path / "out", progress=False)
    assert "FAKE_PRIVATE_DETAIL" not in str(caught.value)
    assert c.download_manifest(path, tmp_path / "out", progress=False).complete
    assert len(calls) == 2


def test_external_package_blocks_entire_selection_without_install_or_payload(tmp_path, monkeypatch):
    fake_http(monkeypatch, lambda _: pytest.fail("No manifest or payload transfer"))
    c = Client(
        [
            row(),
            row(
                download_id="package",
                download_url="https://faspex.cancerimagingarchive.net/package/example",
            ),
        ]
    )
    check = c.download_dataset("EXAMPLE", tmp_path / "out", dry_run=True, progress=False)
    assert [e["status"] for e in check.entries] == ["planned", "requires_external_tool"]
    assert "outside the Python" in check.entries[1]["message"]
    with pytest.raises(DownloadError) as caught:
        c.download_dataset("EXAMPLE", tmp_path / "out", progress=False)
    assert caught.value.report.entries == check.entries and not (tmp_path / "out").exists()


def test_external_launcher_api_is_removed():
    assert not hasattr(DownloadsMixin, "data_retriever_command")
    assert not hasattr(downloads, "_engine")
    for name in ["download_manifest", "download_dataset", "download_files"]:
        params = inspect.signature(getattr(DownloadsMixin, name)).parameters
        assert not {"backend", "retriever", "package_client"} & params.keys()


def test_invalid_progress_fails_before_any_metadata(tmp_path, monkeypatch):
    fake_http(monkeypatch, lambda _: pytest.fail("No HTTP"))
    with pytest.raises(InvalidRequestError, match="progress"):
        DownloadsMixin().download_manifest(
            "https://example.org/cohort.tcia", tmp_path, progress="yes"
        )
