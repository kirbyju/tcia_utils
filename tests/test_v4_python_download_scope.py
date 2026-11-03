"""Python-only execution and early download progress, with no real transfers."""

import inspect
from pathlib import Path
import sys

import httpx
import pytest
from tcia_utils import DownloadError, InvalidRequestError, TCIAClient
from tcia_utils._downloads import DownloadsMixin
import tcia_utils._downloads as downloads
from tcia_utils._downloads import _tdr as REAL_BACKEND
from test_v4_downloads import Client, row, fake_http


def test_early_progress_precedes_manifest_fetch_and_transfer(
    tmp_path, monkeypatch, capsys, tdr_calls
):
    def handler(request):
        assert "Preparing download selection" in capsys.readouterr().out
        return httpx.Response(200, content=b"ListOfSeriesToDownload=\n1.2.3\n")

    fake_http(monkeypatch, handler)
    DownloadsMixin().download_manifest(
        "https://example.org/cohort.tcia", tmp_path, accept_data_policy=True
    )
    assert tdr_calls[0]["seriesInstanceUID"] == ["1.2.3"]


@pytest.mark.parametrize("api", ["manifest", "dataset"])
def test_transfer_resume_is_explicit_without_a_local_idc_index(
    tmp_path, monkeypatch, capsys, api, tdr_calls
):
    path = tmp_path / "cohort.tcia"
    path.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    fake_http(monkeypatch, lambda _: httpx.Response(200, content=path.read_bytes()))
    c = (
        Client(
            [
                row(
                    download_url="https://www.cancerimagingarchive.net/wp-content/uploads/cohort.tcia"
                )
            ]
        )
        if api == "dataset"
        else DownloadsMixin()
    )
    monkeypatch.setitem(sys.modules, "idc_index", None)
    monkeypatch.setitem(sys.modules, "pandas", None)
    download = c.download_dataset if api == "dataset" else c.download_manifest
    source = "EXAMPLE" if api == "dataset" else path
    for resume in [False, False, True]:
        report = download(source, tmp_path / "out", resume=resume, accept_data_policy=True)
        assert report.complete and report.scope["resume_requested"] is resume
    assert [c["options"]["skip_existing"] for c in tdr_calls] == [False, False, True]


def test_dry_run_never_launches_tdr_for_resume(tmp_path, monkeypatch, tdr_calls):
    p = tmp_path / "cohort.csv"
    p.write_text("SeriesInstanceUID\n1.2.3\n")
    report = DownloadsMixin().download_manifest(p, tmp_path / "out", resume=True, dry_run=True)
    assert not tdr_calls and not report.complete and not (tmp_path / "out").exists()


def test_progress_false_explains_the_current_engine_console_limit(tmp_path, monkeypatch, capsys):
    p = tmp_path / "cohort.tcia"
    p.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    with pytest.warns(UserWarning, match="TDR transfer progress remains enabled"):
        DownloadsMixin().download_manifest(
            p, tmp_path / "out", progress=False, accept_data_policy=True
        )
    assert capsys.readouterr().out == ""


def test_public_transfers_never_receive_a_supplied_controlled_key(tmp_path, monkeypatch, tdr_calls):
    p = tmp_path / "cohort.tcia"
    p.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    key = tmp_path / "key.json"
    key.write_text('{"api_key":"FAKE"}')
    DownloadsMixin().download_manifest(
        p, tmp_path / "out", credential_file=key, accept_data_policy=True
    )
    assert tdr_calls[0]["options"]["credential_file"] is None


def test_failed_transfer_does_not_poison_the_next_attempt(tmp_path, monkeypatch):
    from tdr_fakes import install_tdr, EngineError, success

    seen = []

    def transfer(call):
        seen.append(call)
        if len(seen) == 1:
            raise EngineError()
        return success(len(call["records"]))

    install_tdr(monkeypatch, handler=transfer)
    p = tmp_path / "cohort.tcia"
    p.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    c = DownloadsMixin()
    with pytest.raises(DownloadError):
        c.download_manifest(p, tmp_path / "out", accept_data_policy=True)
    assert c.download_manifest(p, tmp_path / "out", accept_data_policy=True).complete


def test_external_package_blocks_the_whole_selection_before_tdr(tmp_path, monkeypatch, tdr_calls):
    c = Client(
        [
            row(),
            row(
                download_id="external",
                download_url="https://faspex.cancerimagingarchive.net/package/a",
            ),
        ]
    )
    with pytest.raises(DownloadError) as caught:
        c.download_dataset("EXAMPLE", tmp_path, accept_data_policy=True)
    assert not tdr_calls and not caught.value.report.complete
    assert caught.value.report.entries[1]["status"] == "requires_external_tool"


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
            "https://example.org/cohort.tcia", tmp_path, progress="yes", accept_data_policy=True
        )


@pytest.mark.parametrize("resume", [None, 0, 1, "yes", []])
def test_invalid_resume_fails_before_metadata_or_payload(monkeypatch, tmp_path, resume):
    fake_http(
        monkeypatch, lambda _: pytest.fail("Invalid options must not fetch metadata or files")
    )
    with TCIAClient(base_url="https://metadata.example.org") as c:
        monkeypatch.setattr(c, "_result", lambda *a, **kw: pytest.fail("No service queries"))
        for download, source in [
            (c.download_manifest, "https://example.org/cohort.tcia"),
            (c.download_dataset, "EXAMPLE"),
            (c.download_images, None),
        ]:
            with pytest.raises(InvalidRequestError, match="resume must be a boolean"):
                download(source, tmp_path, resume=resume)
    assert not list(tmp_path.iterdir())


def test_resume_for_files_uses_the_engine_skip_existing_option(tmp_path, monkeypatch, tdr_calls):
    p = tmp_path / "files.csv"
    p.write_text("imageUrl,file_name\nhttps://example.org/image.nii.gz,image.nii.gz\n")
    report = DownloadsMixin().download_manifest(
        p, tmp_path / "out", resume=True, accept_data_policy=True
    )
    assert report.complete and tdr_calls[0]["options"]["skip_existing"] is True


@pytest.mark.parametrize('entrypoint', ['manifest', 'dataset', 'files'])
def test_sdk_requests_plain_progress_from_the_shared_engine(tmp_path, tdr_calls, entrypoint):
    manifest = tmp_path / 'one.csv'
    manifest.write_text('SeriesInstanceUID\n1.2.3\n')
    if entrypoint == 'manifest':
        DownloadsMixin().download_manifest(manifest, tmp_path / 'out', accept_data_policy=True)
    elif entrypoint == 'dataset':
        Client([row()]).download_dataset('EXAMPLE', tmp_path / 'out', accept_data_policy=True)
    else:
        DownloadsMixin().download_files([row()], tmp_path / 'out', accept_data_policy=True)
    assert all(call['options']['progress'] == 'plain' for call in tdr_calls)


def test_old_imported_wrapper_requests_a_runtime_restart(monkeypatch):
    from types import SimpleNamespace
    # Use the real loader despite the autouse fake engine.
    monkeypatch.setitem(sys.modules, 'tcia_data_retriever',
                        SimpleNamespace(download=lambda manifest, destination: None))
    with pytest.raises(DownloadError, match='restart'):
        REAL_BACKEND()
