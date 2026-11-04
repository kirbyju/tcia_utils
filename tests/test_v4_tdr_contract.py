"""TCIA selection safety and delegation; payload internals belong to TDR."""

import json
from pathlib import Path
import pytest
from tcia_utils import TCIAClient, DownloadError, InvalidRequestError
from tcia_utils._downloads import DownloadsMixin
import tcia_utils._downloads as downloads
from tdr_fakes import install_tdr, success
from test_v4_downloads import row


@pytest.mark.parametrize(
    "entrypoint", ["download_manifest", "download_dataset", "download_files", "download_images"]
)
def test_policy_is_explicit_before_metadata_or_transfer(
    entrypoint, tmp_path, monkeypatch, tdr_calls
):
    with TCIAClient(base_url="https://example.org") as client:
        monkeypatch.setattr(client, "_result", lambda *a, **k: pytest.fail("No metadata query"))
        monkeypatch.setattr(downloads, "_http", lambda: pytest.fail("No HTTP request"))
        with pytest.raises(InvalidRequestError, match="accept_data_policy=True"):
            getattr(client, entrypoint)("not-read", tmp_path)
    assert not tdr_calls and not list(tmp_path.iterdir())


@pytest.mark.parametrize("value", [None, 1, "yes", []])
def test_policy_boolean_is_not_coerced(value, tmp_path, tdr_calls):
    with pytest.raises(InvalidRequestError, match="accept_data_policy must be a boolean"):
        DownloadsMixin().download_manifest("not-read", tmp_path, accept_data_policy=value)
    assert not tdr_calls


def test_core_only_install_has_actionable_download_prerequisite(tmp_path, monkeypatch):
    def missing():
        raise DownloadError("Install tcia_utils[downloads]")

    monkeypatch.setattr(downloads, "_tdr", missing)
    p = tmp_path / "list.tcia"
    p.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    report = DownloadsMixin().download_manifest(p, tmp_path / "out", dry_run=True)
    assert report.entries[0]["status"] == "requires_python_download_extra"
    assert "tcia_utils[downloads]" in report.entries[0]["next_step"]
    assert not (tmp_path / "out").exists()


def test_metadata_dry_run_requires_neither_policy_acceptance_nor_launch(tmp_path, tdr_calls):
    p = tmp_path / "list.tcia"
    p.write_text("ListOfSeriesToDownload=\n1.2.3\n")
    report = DownloadsMixin().download_manifest(p, tmp_path / "out", dry_run=True)
    assert report.scope["data_policy_accepted"] is False
    assert not report.complete and not tdr_calls and not (tmp_path / "out").exists()


def test_file_batch_preserves_identity_and_checksum_columns(tmp_path, tdr_calls):
    files = [
        row(
            download_id=str(i),
            file_name="mask.nii.gz",
            participant_id=str(i),
            checksum="a" * 64,
            checksum_algorithm="sha256",
        )
        for i in range(2)
    ]
    report = DownloadsMixin().download_files(files, tmp_path, accept_data_policy=True)
    assert report.complete and len(tdr_calls) == 2
    records = [record for call in tdr_calls for record in call["records"]]
    assert [r["participant_id"] for r in records] == ["0", "1"]
    assert all(r["file_name"] == "mask.nii.gz" and r["sha256"] == "a" * 64 for r in records)


def test_colliding_files_in_same_identity_scope_fail_before_launch(tmp_path, tdr_calls):
    files = [row(download_id=str(i), file_name="mask.nii.gz", participant_id="P") for i in range(2)]
    with pytest.raises(DownloadError) as caught:
        DownloadsMixin().download_files(files, tmp_path, accept_data_policy=True)
    assert not tdr_calls and not caught.value.report.complete


@pytest.mark.parametrize(
    "change",
    [
        {"downloaded": 0},
        {"total": 0, "downloaded": 0},
        {"downloaded": True},
        {"skipped": -1},
        {"failed": 1},
        {"status": "failed"},
        {"schema_version": 99},
    ],
)
def test_incomplete_or_malformed_engine_success_never_completes_sdk_selection(
    change, tmp_path, monkeypatch
):
    install_tdr(monkeypatch, handler=lambda call: {**success(len(call["records"])), **change})
    with pytest.raises(DownloadError) as caught:
        DownloadsMixin().download_files([row()], tmp_path, accept_data_policy=True)
    assert not caught.value.report.complete
    assert caught.value.report.entries[0]["status"] == "failed"


def test_engine_skip_existing_counts_are_retained_without_secret_extras(tmp_path, monkeypatch):
    def skipped(call):
        return {
            **success(len(call["records"])),
            "downloaded": 0,
            "skipped": len(call["records"]),
            "credential": "FAKE-KEY",
            "storage_url": "https://example.org/?token=SECRET",
        }

    install_tdr(monkeypatch, handler=skipped)
    report = DownloadsMixin().download_files(
        [row()], tmp_path, accept_data_policy=True
    )
    assert report.complete and report.entries[0]["engine_summary"]["skipped"] == 1
    text = json.dumps(report.entries) + json.dumps(report.scope)
    assert "FAKE-KEY" not in text and "SECRET" not in text




def test_sdk_no_longer_implements_payload_auth_or_streaming():
    assert not hasattr(downloads, "_authorized_url")
    assert not hasattr(downloads, "_credential")
    assert not hasattr(downloads, "_stream_file")
