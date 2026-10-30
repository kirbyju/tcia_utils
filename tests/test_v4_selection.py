"""Offline selection/export behavior and Python download manifest compatibility."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path

import pytest


from tcia_utils._selection import export_selection, select


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "v4"
IDENTITY = {"release_fingerprint": "f" * 64, "release_tag": "synthetic-test",
            "installed_profile": "research_detail", "base_url": "https://example.org"}


def _input():
    return json.loads((FIXTURES / "selection_input.json").read_text())


def test_route_specific_exports_and_accounting(tmp_path):
    rows = _input()["records"]
    chosen = select(rows, release_identity=IDENTITY,
                    query_scope={"short_titles": ["EXAMPLE"], "completeness": "unknown"},
                    coverage={"coverage_complete": False})
    report = export_selection(chosen, tmp_path / "export")
    assert (report.exported, report.deduplicated, report.excluded, report.unresolved) == (3, 1, 1, 3)
    names = {path.name for path in report.files}
    assert names == {"public-series.csv", "public-image-url.csv",
                     "controlled-ctdc-drs-uri.csv", "selection.json"}
    for name, header in (("public-series.csv", "SeriesInstanceUID"),
                         ("public-image-url.csv", "imageUrl"),
                         ("controlled-ctdc-drs-uri.csv", "drs_uri")):
        with (report.destination / name).open(newline="") as stream:
            reader = csv.DictReader(stream)
            assert reader.fieldnames == [header]
            assert len(list(reader)) == 1
            assert len(set(reader.fieldnames) & {"SeriesInstanceUID", "imageUrl", "drs_uri"}) == 1
            assert not set(reader.fieldnames) & {"file_id", "access", "File ID"}
    with (report.destination / "public-series.csv").open(newline="") as stream:
        assert list(csv.DictReader(stream))[0]["SeriesInstanceUID"] == "1.02.003"
    assert not (report.destination / "aspera-handoff.json").exists()
    companion = json.loads((report.destination / "selection.json").read_text())
    assert companion["counts"]["input"] == len(rows) == 8
    assert sum(companion["counts"][name] for name in ("exported", "deduplicated", "excluded", "unresolved")) == 8
    assert companion["coverage"] == {"coverage_complete": False}
    assert companion["completeness"] == "unknown"
    assert companion["release_identity"]["base_url"] == "https://example.org"
    assert companion["accounting"][5]["record"]["file_id"] == "synthetic-0008"
    expected = FIXTURES / "selection_expected"
    assert {path.name: path.read_bytes() for path in report.files} == {
        path.name: path.read_bytes() for path in expected.iterdir() if path.is_file()
    }


def test_same_captured_selection_produces_same_bytes(tmp_path):
    chosen = select(_input()["records"], release_identity=IDENTITY, query_scope={})
    first = export_selection(chosen, tmp_path / "first")
    second = export_selection(chosen, tmp_path / "second")
    assert {path.name: path.read_bytes() for path in first.files} == {
        path.name: path.read_bytes() for path in second.files}


def test_write_requires_empty_or_explicit_overwrite(tmp_path):
    chosen = select(_input()["records"], release_identity=IDENTITY, query_scope={})
    destination = tmp_path / "export"
    first = export_selection(chosen, destination)
    with pytest.raises(FileExistsError):
        export_selection(chosen, destination)
    second = export_selection(chosen, destination, overwrite=True)
    assert {path.name: path.read_bytes() for path in first.files} == {
        path.name: path.read_bytes() for path in second.files}
    (destination / "unrelated.txt").write_text("keep")
    with pytest.raises(FileExistsError):
        export_selection(chosen, destination, overwrite=True)
    assert (destination / "unrelated.txt").read_text() == "keep"


def test_missing_route_and_ambiguous_access_stay_unresolved(tmp_path):
    rows = [
        {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "mixed",
         "route_kind": "SeriesInstanceUID", "series_instance_uid": "1.2.3"},
        {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
         "source_url": "https://example.org/asset", "route_system": "pathdb"},
        {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
         "route_kind": "SeriesInstanceUID", "series_instance_uid": "1.2.3",
         "record_grain": "series"},
        {"short_title": "EXAMPLE", "access_level": "open", "route_kind": "imageUrl",
         "imageUrl": "https://example.org/image"},
    ]
    result = export_selection(select(rows, release_identity=IDENTITY, query_scope={}), tmp_path / "none")
    assert result.unresolved == 4 and result.exported == 0
    assert {path.name for path in result.files} == {"selection.json"}
    reasons = [item["reason"] for item in json.loads(result.files[0].read_text())["accounting"]]
    assert reasons == ["ambiguous_access", "no_supported_exact_route",
                       "unreviewed_series_identity", "missing_dataset_identity"]
    assert json.loads(result.files[0].read_text())["accounting"][1]["record"]["source_url"] == "https://example.org/asset"


def test_credentials_and_signed_urls_rejected_before_writing(tmp_path):
    good = _input()["records"][0]
    unsafe = dict(good, source_url="https://example.org/file?X-Amz-Signature=abc")
    with pytest.raises(ValueError, match="signed or credential"):
        select([unsafe], release_identity=IDENTITY, query_scope={})
    with pytest.raises(ValueError, match="credential-bearing"):
        select([good], release_identity=IDENTITY, query_scope={"api_key": "secret"})
    with pytest.raises(ValueError, match="private filesystem"):
        select([good], release_identity=IDENTITY, query_scope={"workspace": "/Users/example/private"})
    assert not list(tmp_path.iterdir())


def test_unverified_series_and_implicit_source_image_are_withheld(tmp_path):
    rows = [
        {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
         "record_grain": "series", "route_kind": "SeriesInstanceUID",
         "series_instance_uid": "1.2.3", "identifier_source": "reviewed",
         "object_role": "source_image"},
    ]
    skipped = export_selection(select(rows, release_identity=IDENTITY, query_scope={}), tmp_path / "skipped")
    assert skipped.excluded == 1
    included = export_selection(select(rows, release_identity=IDENTITY, query_scope={},
                                       include_source_images=True), tmp_path / "included")
    assert included.exported == 1


def test_frozen_provenance_dataclass_is_accepted(tmp_path):
    @dataclass(frozen=True)
    class Provenance:
        base_url: str
        release_fingerprint: str
        release_tag: str
        installed_profile: str
        observed_at_utc: str

    provenance = Provenance("https://example.org", "f" * 64, "synthetic-test",
                            "research_detail", "2026-10-14T00:00:00Z")
    result = export_selection(select([], release_identity=provenance, query_scope={}), tmp_path / "empty")
    companion = json.loads(result.files[0].read_text())
    assert companion["release_identity"]["release_fingerprint"] == "f" * 64


def test_external_application_records_are_not_exported_for_transfer(tmp_path):
    base = {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
            "route_kind": "aspera", "package_url": "https://example.org/package",
            "package_path": "path/in/package"}
    with pytest.raises(ValueError, match="URL query"):
        select([dict(base, package_url="https://example.org/package?view=1")],
               release_identity=IDENTITY, query_scope={})
    rows = [dict(base, package_path="--output=/tmp/file"),
            dict(base, access_level="controlled")]
    result = export_selection(select(rows, release_identity=IDENTITY, query_scope={}), tmp_path / "unsafe")
    assert result.unresolved == 2 and result.exported == 0


def test_cross_dataset_duplicate_is_one_transfer_with_both_associations(tmp_path):
    row = {"dataset_type": "collection", "short_title": "FIRST", "access_level": "open",
           "record_grain": "series", "route_kind": "SeriesInstanceUID",
           "series_instance_uid": "1.02.003", "identifier_source": "reviewed"}
    second = dict(row, short_title="SECOND", clinical_evidence={"evidence_scope": "patient"})
    result = export_selection(select([row, second], release_identity=IDENTITY, query_scope={}),
                              tmp_path / "duplicate")
    assert (result.exported, result.deduplicated) == (1, 1)
    companion = json.loads((result.destination / "selection.json").read_text())
    assert [item["short_title"] for item in companion["dataset_context"]] == ["FIRST", "SECOND"]
    assert companion["accounting"][1]["record"]["clinical_evidence"]["evidence_scope"] == "patient"


def test_conflicting_routes_and_strict_source_image_option(tmp_path):
    row = {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
           "route_kind": "imageUrl", "imageUrl": "https://example.org/image",
           "drs_uri": "drs://example.org/object"}
    result = export_selection(select([row], release_identity=IDENTITY, query_scope={}),
                              tmp_path / "conflict")
    assert result.unresolved == 1
    with pytest.raises(ValueError, match="must be a bool"):
        select([row], release_identity=IDENTITY, query_scope={}, include_source_images="false")


def test_local_service_origin_is_omitted_from_companion(tmp_path):
    identity = dict(IDENTITY, base_url="http://127.0.0.1:8766")
    result = export_selection(select([], release_identity=identity, query_scope={}), tmp_path / "local")
    companion = json.loads(result.files[0].read_text())
    assert "base_url" not in companion["release_identity"]
    with pytest.raises(ValueError, match="private service hostname"):
        select([{"source_url": "http://127.0.0.1/internal"}],
               release_identity=IDENTITY, query_scope={})


def test_raw_idc_series_column_normalizes_only_with_reviewed_source(tmp_path):
    row = {"dataset_type": "collection", "short_title": "EXAMPLE", "access_level": "open",
           "SeriesInstanceUID": "1.02.003", "identifier_source": "idc-index",
           "size_bytes": 321}
    result = export_selection(select([row], release_identity=IDENTITY, query_scope={}),
                              tmp_path / "idc")
    assert result.exported == 1
    companion = json.loads((result.destination / "selection.json").read_text())
    assert companion["counts"]["known_size_bytes"] == 321
    assert companion["accounting"][0]["record"]["record_grain"] == "series"
    assert companion["accounting"][0]["record"]["SeriesInstanceUID"] == "1.02.003"
    unreviewed = dict(row)
    del unreviewed["identifier_source"]
    result = export_selection(select([unreviewed], release_identity=IDENTITY, query_scope={}),
                              tmp_path / "unreviewed")
    assert result.unresolved == 1


def test_unknown_completeness_default_and_invalid_value(tmp_path):
    chosen = select([], release_identity=IDENTITY, query_scope={})
    result = export_selection(chosen, tmp_path / "unknown")
    companion = json.loads(result.files[0].read_text())
    assert companion["completeness"] == "unknown"
    assert companion["completeness_source"] == "default_unknown"
    with pytest.raises(ValueError, match="query_scope.completeness"):
        export_selection(select([], release_identity=IDENTITY,
                                query_scope={"completeness": "obviously complete"}),
                         tmp_path / "invalid")
    with pytest.raises(ValueError, match="completeness_basis"):
        export_selection(select([], release_identity=IDENTITY,
                                query_scope={"completeness": "complete"}),
                         tmp_path / "unsupported-complete")
    reviewed = export_selection(select([], release_identity=IDENTITY,
                                       query_scope={"completeness": "complete",
                                                    "completeness_basis": "terminal_cursor_guarded"}),
                                tmp_path / "supported-complete")
    assert json.loads(reviewed.files[0].read_text())["completeness"] == "complete"
