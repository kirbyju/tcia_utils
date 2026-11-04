"""Shared-service image plans and bounded TDR execution; no provider client imports."""

from copy import deepcopy
from types import SimpleNamespace
import sys
import pytest
from tcia_utils import DownloadError, InvalidRequestError, ProtocolError
from tcia_utils._downloads import DownloadsMixin
from tcia_utils._images import ParticipantImagesMixin
from tcia_utils._results import Provenance, Result, Completeness

FP = Provenance(
    "https://metadata.example.org", "fp", "tag", "research_detail", {}, "2026-11-02"
)


def person(title="EXAMPLE", name="P1"):
    return dict(
        dataset_type="Collection",
        short_title=title,
        display_participant_id=name,
        participant_key=title + "-" + name,
    )


def item(p, dicom=True):
    row = dict(
        dataset_type=p["dataset_type"],
        short_title=p["short_title"],
        participant_key=p["participant_key"],
        participant_id=p["display_participant_id"],
        modality="CT" if dicom else "Whole Slide Image",
        file_format="DICOM" if dicom else "SVS",
        asset_granularity="series" if dicom else "file",
        access_level="open",
        python_supported=True,
        provider="IDC" if dicom else "TCIA",
        download_method="dicom_manifest" if dicom else "direct_file",
        download_id="asset-" + p["participant_key"],
    )
    if dicom:
        row.update(
            series_instance_uid="1.2.3", study_instance_uid="1.2", instance_count=3
        )
    else:
        row.update(
            imageUrl="https://files.example.org/" + p["participant_key"] + ".svs",
            file_name=p["participant_key"] + ".svs",
            asset_id="asset-" + p["participant_key"],
        )
    return row


class Client(DownloadsMixin, ParticipantImagesMixin):
    def __init__(self, people, dicom=True, mutate=None):
        self.people = {p["participant_key"]: p for p in people}
        self.dicom = dicom
        self.mutate = mutate
        self.calls = []

    def _observe_status(self):
        return None, FP

    def _result(self, method, path, **kwargs):
        self.calls.append((method, path, kwargs))
        body = kwargs["json_body"]
        keys = body["participant_keys"]
        rows = [item(self.people[k], self.dicom) for k in keys]
        data = dict(
            plan_version=1,
            rows=rows,
            unresolved=[],
            complete=True,
            providers=(
                {
                    "IDC": {
                        "idc_version": "v25",
                        "idc_index_data_version": "25.0.5",
                        "api_version": "3",
                    }
                }
                if self.dicom
                else {}
            ),
            coverage=[
                dict(
                    participant_key=k,
                    dataset_type=self.people[k]["dataset_type"],
                    short_title=self.people[k]["short_title"],
                    participant_id=self.people[k]["display_participant_id"],
                    matching_items=1,
                    downloadable_items=1,
                    state="matched",
                )
                for k in keys
            ],
            scope=dict(
                requested_participants=len(keys),
                selected_items=len(rows),
                modality=body["modality"],
                file_format=body["file_format"],
                metadata_snapshot="fp",
            ),
        )
        if self.mutate:
            self.mutate(data, len(self.calls))
        return Result(data, (), FP, Completeness("complete"))


def test_public_dicom_uses_shared_plan_without_an_idc_or_pandas_import(monkeypatch):
    monkeypatch.setitem(sys.modules, "idc_index", None)
    monkeypatch.setitem(sys.modules, "pandas", None)
    p = person()
    c = Client([p])
    plan = c.plan_participant_images(
        [p], modality="CT", file_format="DICOM", progress=False
    )
    assert len(plan.rows) == 1 and plan.providers["IDC"]["idc_version"] == "v25"
    method, path, args = c.calls[0]
    assert (method, path) == ("POST", "/v2/participants/download-plan")
    assert args["json_body"] == dict(
        participant_keys=[p["participant_key"]], modality="CT", file_format="DICOM"
    )


@pytest.mark.parametrize("dicom", [True, False])
def test_downloads_use_one_bounded_tdr_path(tmp_path, tdr_calls, dicom):
    p = person()
    c = Client([p], dicom=dicom)
    plan = c.plan_participant_images(
        [p],
        modality="CT" if dicom else "Whole Slide Image",
        file_format="DICOM" if dicom else "non-DICOM",
    )
    report = c.download_images(
        plan, tmp_path, max_items=1, accept_data_policy=True
    )
    assert report.complete and len(tdr_calls) == 1
    assert tdr_calls[0]["downloadDir"] == str(tmp_path)
    assert report.entries[0]["destination"] == str(tmp_path)
    assert "skip_existing" not in tdr_calls[0]["options"]
    assert tdr_calls[0]["options"]["progress"] == "plain"
    if dicom:
        assert tdr_calls[0]["seriesInstanceUID"] == ["1.2.3"]
    else:
        assert tdr_calls[0]["records"][0]["imageUrl"].endswith(".svs")


def test_batching_pins_both_tcia_and_provider_metadata():
    people = [person(name="P" + str(i)) for i in range(101)]
    c = Client(people)
    plan = c.plan_participant_images(
        people, modality="CT", file_format="DICOM", progress=False
    )
    assert len(c.calls) == 2 and len(plan.coverage) == 101

    def change(data, n):
        if n == 2:
            data["providers"]["IDC"]["idc_index_data_version"] = "25.0.6"

    with pytest.raises(ProtocolError, match="Provider metadata changed"):
        Client(people, mutate=change).plan_participant_images(
            people, modality="CT", file_format="DICOM", progress=False
        )


@pytest.mark.parametrize(
    "damage",
    [
        "other_person",
        "other_dataset",
        "other_modality",
        "controlled",
        "missing_coverage",
        "unreported_issue",
        "wrong_snapshot",
    ],
)
def test_malformed_or_out_of_scope_plan_never_reaches_transfer(
    tmp_path, tdr_calls, damage
):
    def mutate(data, n):
        if damage == "other_person":
            data["rows"][0]["participant_key"] = "unknown"
        elif damage == "other_dataset":
            data["rows"][0]["short_title"] = "OTHER"
        elif damage == "other_modality":
            data["rows"][0]["modality"] = "MR"
        elif damage == "controlled":
            data["rows"][0]["access_level"] = "controlled"
        elif damage == "missing_coverage":
            data["coverage"] = []
        elif damage == "unreported_issue":
            data["unresolved"] = [{"participant_key": "EXAMPLE-P1"}]
        elif damage == "wrong_snapshot":
            data["scope"]["metadata_snapshot"] = "other"

    p = person()
    c = Client([p], mutate=mutate)
    with pytest.raises(ProtocolError):
        c.plan_participant_images(
            [p], modality="CT", file_format="DICOM", progress=False
        )
    assert not tdr_calls and not list(tmp_path.iterdir())


def test_unresolved_routes_block_payload_and_zero_matches_remain_visible(
    tmp_path, tdr_calls
):
    def unresolved(data, n):
        data["unresolved"] = [
            {"participant_key": "EXAMPLE-P1", "reason": "no_verified_identity"}
        ]
        data["complete"] = False

    p = person()
    c = Client([p], mutate=unresolved)
    plan = c.plan_participant_images(
        [p], modality="CT", file_format="DICOM", progress=False
    )
    with pytest.raises(DownloadError, match="unresolved"):
        c.download_images(plan, tmp_path, accept_data_policy=True)
    assert not tdr_calls

    def zero(data, n):
        data["rows"] = []
        data["scope"]["selected_items"] = 0
        data["coverage"][0].update(
            matching_items=0, downloadable_items=0, state="no_matches"
        )

    plan = Client([p], mutate=zero).plan_participant_images(
        [p], modality="CT", file_format="DICOM", progress=False
    )
    assert not plan.rows and plan.coverage[0]["state"] == "no_matches"


def test_empty_selection_and_conflicting_identity():
    c = Client([])
    plan = c.plan_participant_images(
        [], modality="CT", file_format="DICOM", progress=False
    )
    assert not plan.rows and not c.calls
    p = person()
    with pytest.raises(InvalidRequestError):
        Client([p]).plan_participant_images(
            [p, dict(p, short_title="OTHER")],
            modality="CT",
            file_format="DICOM",
            progress=False,
        )
