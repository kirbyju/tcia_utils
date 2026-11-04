"""Metadata-only, participant-scoped handoffs; no ascli or TDR installation."""

from copy import deepcopy
import csv
import inspect
import json
from types import SimpleNamespace
import pytest

from tcia_utils import TCIAClient, InvalidRequestError, ProtocolError
from tcia_utils._downloads import DownloadPlan
from tcia_utils._handoffs import HandoffsMixin
from test_v4_participant_images import FP, person

URL = "https://faspex.cancerimagingarchive.net/aspera/faspex/public/package?context=synthetic-public-package"


def asset(p, path="folder with spaces/P1_image.nii.gz", **extra):
    return dict(dataset_type=p["dataset_type"], short_title=p["short_title"],
                participant_key=p["participant_key"], subject_id=p["display_participant_id"],
                subject_id_namespace="tcia_dataset:" + p["short_title"], download_id='["12"]',
                asset_granularity="file", access_level="open", asset_id="a", file_name="image.nii.gz",
                size_bytes=7, package_path=path, **extra)


class Client(HandoffsMixin):
    def __init__(self, files, *, packages=None, pages=None):
        self.files, self.calls = files, []
        self.packages = packages or [dict(download_id="12", download_method="aspera_package",
                                        download_url=URL, access_level="open")]
        self.pages = pages

    def _observe_status(self):
        return None, FP

    def get_participant(self, key):
        return SimpleNamespace(data={"participant": person(name=key.rsplit('-', 1)[-1])}, provenance=FP)

    def plan_downloads(self, title, **kw):
        self.calls.append(("plan", title, kw))
        return DownloadPlan(tuple(self.packages), FP, {})

    def search_public_assets(self, **kw):
        self.calls.append(("assets", kw))
        files = [r for r in self.files if r["short_title"] == kw["short_titles"][0]
                 and r["subject_id"] == kw["participant_id"]]
        pages = self.pages or [SimpleNamespace(items=files, provenance=FP)]
        return SimpleNamespace(iter_pages=lambda: iter(pages))


def test_export_uses_current_package_and_exact_paths_preserving_followups(tmp_path, monkeypatch):
    monkeypatch.setitem(__import__('sys').modules, 'pandas', None)
    monkeypatch.setitem(__import__('sys').modules, 'tcia_data_retriever', None)
    p = person()
    files = [asset(p), asset(p, "P1_FU016d/image.nii.gz")]
    # Old artifact links are deliberately ignored in favor of download IDs/current links.
    files[0]["source_url"] = "https://old.example.org/obsolete"
    client = Client(files)
    report = client.export_aspera_manifest([p], tmp_path / 'cohort.csv', modality='MR', file_formats=['NIFTI'])
    with open(report.path, newline='') as source:
        rows = list(csv.DictReader(source))
    assert list(rows[0]) == ['packageUrl', 'packagePath']
    assert {r['packagePath'] for r in rows} == {r['package_path'] for r in files}
    assert {r['packageUrl'] for r in rows} == {URL}
    assert report.scope['matching_files'] == 2 and not report.scope['is_sample']
    assert len(report.coverage) == 1
    assert json.loads(report.to_json())['provenance']['release_fingerprint'] == FP.release_fingerprint


def test_global_sample_dedup_paging_and_zero_match_coverage(tmp_path):
    p, empty = person(), person(name='P2')
    files = [asset(p), asset(p, 'followup/image.nii.gz')]
    client = Client(files + files[:1])
    report = client.export_aspera_manifest([p, empty, p], tmp_path / 'sample.csv', max_items=1)
    assert len(report.rows) == 1 and report.scope['matching_files'] == 2
    assert report.scope['is_sample'] and report.scope['requested_participants'] == 2
    assert [r['matching_files'] for r in report.coverage] == [2, 0]
    assert sum(r['exported_files'] for r in report.coverage) == 1
    assert sum(c[0] == 'plan' for c in client.calls) == 1
    pages = [SimpleNamespace(items=files[:1], provenance=FP), SimpleNamespace(items=files[1:], provenance=FP)]
    assert len(Client(files, pages=pages).export_aspera_manifest([p], tmp_path / 'paged.csv').rows) == 2


@pytest.mark.parametrize('change', [
    {'access_level': 'controlled'}, {'subject_id_namespace': 'another_dataset'},
    {'participant_key': 'forged'}, {'asset_granularity': 'package'},
    {'package_path': '../outside'}, {'package_path': '-option'}, {'package_path': 'folder/'},
    {'package_path': 'file\nname'}, {'package_path': None}, {'download_id': '["missing"]'},
])
def test_unresolved_or_unsafe_files_never_write_a_partial_manifest(tmp_path, change):
    p = person()
    files = [asset(p), {**asset(p, 'second/image.nii.gz'), **change}]
    path = tmp_path / 'not-created' / 'cohort.csv'
    with pytest.raises((InvalidRequestError, ProtocolError)):
        Client(files).export_aspera_manifest([p], path)
    assert not path.parent.exists()


@pytest.mark.parametrize('change', [
    {'access_level': 'controlled'}, {'download_method': 'direct_file'},
    {'download_url': 'https://evil.example.org/aspera/faspex?context=a'},
    {'download_url': 'http://faspex.cancerimagingarchive.net/aspera/faspex?context=a'},
    {'download_url': 'https://faspex.cancerimagingarchive.net/aspera/faspex?token=secret'},
])
def test_current_public_route_is_required(tmp_path, change):
    p = person()
    client = Client([asset(p)])
    client.packages[0].update(change)
    with pytest.raises(InvalidRequestError):
        client.export_aspera_manifest([p], tmp_path / 'cohort.csv')
    assert not (tmp_path / 'cohort.csv').exists()


def test_drift_and_overwrite_are_explicit(tmp_path):
    from dataclasses import replace
    p = person()
    pages = [SimpleNamespace(items=[asset(p)], provenance=replace(FP, release_fingerprint='new'))]
    path = tmp_path / 'cohort.csv'
    with pytest.raises(ProtocolError):
        Client([], pages=pages).export_aspera_manifest([p], path)
    client = Client([asset(p)])
    client.export_aspera_manifest([p], path)
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        client.export_aspera_manifest([p], path)
    assert path.read_bytes() == original
    client.export_aspera_manifest([p], path, overwrite=True)
    assert path.read_bytes() == original


def test_missing_file_access_is_resolved_only_by_exact_current_open_package(tmp_path):
    p = person()
    client = Client([{**asset(p), 'access_level': None, 'participant_key': None}])
    assert client.export_aspera_manifest([p], tmp_path / 'open.csv').rows[0]['access_level'] == 'open'
    client.packages[0]['access_level'] = 'controlled'
    with pytest.raises(InvalidRequestError):
        client.export_aspera_manifest([p], tmp_path / 'controlled.csv')


@pytest.mark.parametrize('method', ['download_manifest','download_dataset','download_files','download_images'])
def test_resume_is_absent_and_rejected_before_any_work(method, tmp_path):
    assert 'resume' not in inspect.signature(getattr(TCIAClient, method)).parameters
    with TCIAClient(base_url='https://no-requests.example.org') as client:
        with pytest.raises(TypeError, match='resume'):
            getattr(client, method)(None, tmp_path, resume=True)
