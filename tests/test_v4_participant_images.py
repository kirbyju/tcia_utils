"""Synthetic, dataset-scoped cohort-to-image selections; no imaging payloads."""

from types import SimpleNamespace
import json
import httpx
import pytest
from tcia_utils import DownloadError, InvalidRequestError, ProtocolError
from tcia_utils._downloads import DownloadPlan, DownloadsMixin
from tcia_utils._images import ParticipantImagesMixin
from tcia_utils._results import Page, Provenance, Completeness
import tcia_utils._images as images
import tcia_utils._downloads as downloads

FP = Provenance("https://metadata.example.org", "fp", "tag", "research_detail", {}, "2026-10-30")


def person(title="EXAMPLE", name="P1"):
    return {
        "dataset_type": "Collection",
        "short_title": title,
        "display_participant_id": name,
        "participant_key": title + "-" + name,
    }


def asset(name="P1", title="EXAMPLE", **changes):
    row = {
        "dataset_type": "Collection",
        "short_title": title,
        "subject_id": name,
        "subject_id_namespace": "tcia_dataset:" + title,
        "asset_id": title + "-" + name,
        "asset_granularity": "file",
        "modality": "Whole Slide Image",
        "file_format": "SVS",
        "file_name": "source-id",
        "download_file_name": name + ".svs",
        "access_level": "open",
        "imageUrl": "https://files.example.org/" + name + ".svs",
        "download_representation": "public_pathology_slide_copy",
    }
    row.update(changes)
    return row


class Client(DownloadsMixin, ParticipantImagesMixin):
    def __init__(self, assets=(), index=None):
        self.assets = assets
        self.index = index
        self.calls = []

    def _observe_status(self):
        return None, FP

    def plan_downloads(self, title, **kwargs):
        self.calls.append(("plan", title, kwargs))
        return DownloadPlan(
            (
                {
                    "short_title": title,
                    "dataset_type": "Collection",
                    "download_id": "official",
                    "access_level": "open",
                    "download_method": "dicom_manifest",
                    "download_url": "https://www.cancerimagingarchive.net/cohort.tcia",
                },
            ),
            FP,
        )

    def _get_imaging_client(self, progress):
        return SimpleNamespace(index=self.index, get_idc_version=lambda: "v-test")

    def search_public_assets(self, **kwargs):
        self.calls.append(("assets", kwargs))
        return Page(
            items=list(self.assets),
            count=len(self.assets),
            limit=500,
            has_more=False,
            truncated=False,
            query={},
            response={},
            completeness=Completeness("complete"),
            next_cursor=None,
            provenance=FP,
            _fetch_next=None,
        )


def fake_manifest(monkeypatch, uids=("1.2.3", "1.2.4")):
    monkeypatch.setattr(
        images,
        "_get_bytes",
        lambda _: ("ListOfSeriesToDownload=\n" + "\n".join(uids) + "\n").encode(),
    )


def test_dicom_selection_uses_manifest_patient_and_modality_not_collection_name_guess(monkeypatch):
    pd = pytest.importorskip("pandas")
    fake_manifest(monkeypatch)
    index = pd.DataFrame(
        [
            {
                "PatientID": "P1",
                "Modality": "CT",
                "SeriesInstanceUID": "1.2.3",
                "StudyInstanceUID": "1.1",
                "instanceCount": 42,
                "series_size_MB": 2.5,
            },
            {"PatientID": "P1", "Modality": "MR", "SeriesInstanceUID": "1.2.4"},
            {
                "PatientID": "P1",
                "Modality": "CT",
                "SeriesInstanceUID": "9.9.9",
            },  # Other dataset despite identical ID.
        ]
    )
    c = Client(index=index)
    plan = c.plan_participant_images(
        [person(), person()], modality="CT", file_format="DICOM", progress=False
    )
    assert len(plan.rows) == 1 and plan.rows[0]["series_instance_uid"] == "1.2.3"
    assert plan.rows[0]["instance_count"] == 42 and plan.rows[0]["size_bytes"] == 2_500_000
    assert plan.scope["requested_participants"] == 1
    assert c.calls[0][2] == {"dataset_type": "collection", "access_levels": ["open"]}


def test_slide_selection_does_not_cross_dataset_or_use_source_url():
    c = Client([asset(), asset(title="OTHER"), asset("P2"), asset("P1", file_format="DICOM")])
    plan = c.plan_participant_images(
        [person()], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert len(plan.rows) == 1 and plan.rows[0]["file_name"] == "P1.svs"
    assert plan.rows[0]["download_representation"] == "public_pathology_slide_copy"
    assert not plan.unresolved


def test_no_explicit_file_route_is_retained_and_blocks_download(tmp_path):
    c = Client([asset(imageUrl=None, source_url="https://files.example.org/raw.svs")])
    plan = c.plan_participant_images(
        [person()], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert len(plan.rows) == 1 and plan.unresolved and not plan.rows[0]["python_supported"]
    with pytest.raises(DownloadError, match="unresolved"):
        c.download_images(plan, tmp_path, max_items=1, progress=False)
    assert not list(tmp_path.iterdir())


def test_controlled_files_cannot_be_downloaded_via_public_cohort_route(tmp_path):
    c = Client([asset(access_level="controlled")])
    plan = c.plan_participant_images(
        [person()], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert plan.unresolved
    with pytest.raises(DownloadError):
        c.download_images(plan, tmp_path, progress=False)


def test_zero_matches_keep_participant_coverage():
    plan = Client([]).plan_participant_images(
        [person()], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert not plan.rows and plan.coverage[0]["matching_items"] == 0


def test_unknown_namespace_never_joins_by_bare_patient_id():
    plan = Client([asset(subject_id_namespace="other")]).plan_participant_images(
        [person()], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert not plan.rows and plan.unresolved[0]["reason"] == "unverified_participant_namespace"


@pytest.mark.parametrize(
    "rows",
    [
        [{"short_title": "EXAMPLE", "display_participant_id": "P1"}],
        [person(), dict(person(), participant_key="different")],
    ],
)
def test_invalid_participant_identity_is_rejected(rows):
    with pytest.raises(InvalidRequestError):
        Client().plan_participant_images(rows, modality="CT", file_format="DICOM", progress=False)


def test_missing_manifest_series_is_not_silently_ignored(monkeypatch, tmp_path):
    pd = pytest.importorskip("pandas")
    fake_manifest(monkeypatch)
    index = pd.DataFrame([{"PatientID": "P1", "Modality": "CT", "SeriesInstanceUID": "1.2.3"}])
    c = Client(index=index)
    plan = c.plan_participant_images([person()], modality="CT", file_format="DICOM", progress=False)
    assert plan.unresolved[0]["series_instance_uids"] == ["1.2.4"]
    with pytest.raises(DownloadError):
        c.download_images(plan, tmp_path, progress=False)


def test_slide_download_applies_global_sample_limit_and_keeps_original_names(monkeypatch, tmp_path):
    c = Client([asset(), asset("P2")])
    plan = c.plan_participant_images(
        [person(), person(name="P2")],
        modality="Whole Slide Image",
        file_format="non-DICOM",
        progress=False,
    )
    requested = []

    def handler(request):
        requested.append(str(request.url))
        return httpx.Response(200, content=b"FAKE")

    monkeypatch.setattr(
        downloads, "_http", lambda: httpx.Client(transport=httpx.MockTransport(handler))
    )
    report = c.download_images(plan, tmp_path, max_items=1, progress=False)
    assert report.complete and len(requested) == 1 and requested[0].endswith("/P1.svs")
    assert len(list(tmp_path.rglob("*.svs"))) == 1
    assert report.scope["sample_selected_items"] == 1 and report.scope["sample_omitted_items"] == 1


def test_dicom_download_reuses_bounded_manifest_adapter(monkeypatch, tmp_path):
    pd = pytest.importorskip("pandas")
    fake_manifest(monkeypatch)
    index = pd.DataFrame(
        [
            {"PatientID": "P1", "Modality": "CT", "SeriesInstanceUID": uid}
            for uid in ["1.2.3", "1.2.4"]
        ]
    )
    c = Client(index=index)
    plan = c.plan_participant_images([person()], modality="CT", file_format="DICOM", progress=False)
    calls = []
    c._get_imaging_client = lambda _: SimpleNamespace(
        index=index, download_dicom_series=lambda **kw: calls.append(kw)
    )
    monkeypatch.setattr(downloads.importlib.util, "find_spec", lambda _: object())
    report = c.download_images(plan, tmp_path, max_items=1, progress=False)
    assert report.complete and calls[0]["seriesInstanceUID"] == ["1.2.3"]
    assert (
        report.scope["participant_image_selection"] and report.scope["sample_available_items"] == 2
    )


@pytest.mark.parametrize("changed_snapshot", [False, True])
def test_slide_planning_collects_all_pages_and_rejects_changed_snapshot(changed_snapshot):
    c = Client()
    later = Provenance(
        FP.base_url,
        "other" if changed_snapshot else FP.release_fingerprint,
        FP.release_tag,
        FP.installed_profile,
        FP.capabilities,
        FP.observed_at_utc,
    )
    second = Page(
        (asset("P2"),), 1, 500, False, False, None, {}, {}, later, Completeness("complete")
    )
    first = Page(
        (asset(),),
        1,
        500,
        True,
        True,
        "cursor",
        {},
        {},
        FP,
        Completeness("partial"),
        lambda: second,
    )
    c.search_public_assets = lambda **kwargs: first
    if changed_snapshot:
        with pytest.raises(ProtocolError, match="Metadata changed"):
            c.plan_participant_images(
                [person(), person(name="P2")],
                modality="Whole Slide Image",
                file_format="non-DICOM",
                progress=False,
            )
    else:
        result = c.plan_participant_images(
            [person(), person(name="P2")],
            modality="Whole Slide Image",
            file_format="non-DICOM",
            progress=False,
        )
        assert [r["participant_id"] for r in result.rows] == ["P1", "P2"]


def test_same_title_and_id_in_collection_and_analysis_result_keep_separate_coverage():
    c = Client([asset()])
    ar = dict(person(), dataset_type="Analysis Result", participant_key="analysis-P1")
    plan = c.plan_participant_images(
        [person(), ar], modality="Whole Slide Image", file_format="non-DICOM", progress=False
    )
    assert [r["matching_items"] for r in plan.coverage] == [1, 0]
