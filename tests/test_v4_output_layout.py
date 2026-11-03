"""The caller chooses one root; TDR owns every payload directory below it."""

from pathlib import Path
import json

import httpx
import pytest

from tcia_utils import DownloadError
from tcia_utils._downloads import DownloadsMixin
from test_v4_downloads import Client, row, fake_http


@pytest.mark.parametrize('max_items', [None, 1])
@pytest.mark.parametrize('resume', [False, True])
def test_full_sample_and_resume_use_the_identical_caller_root(tmp_path, tdr_calls, max_items, resume):
    manifest = tmp_path / 'cohort.tcia'
    manifest.write_text('ListOfSeriesToDownload=\n1.2.3\n1.2.4\n')
    root = tmp_path / 'chosen-root'
    existing = root / 'cmb_gec' / 'P1' / 'study' / 'series' / 'existing.dcm'
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b'synthetic existing file')
    report = DownloadsMixin().download_manifest(
        manifest, root, max_items=max_items, resume=resume, accept_data_policy=True,
    )
    assert report.complete
    assert tdr_calls[0]['downloadDir'] == str(root)
    assert report.entries[0]['destination'] == str(root)
    assert tdr_calls[0]['options']['skip_existing'] is resume
    assert tdr_calls[0]['seriesInstanceUID'] == (['1.2.3'] if max_items else ['1.2.3', '1.2.4'])
    assert existing.read_bytes() == b'synthetic existing file'
    assert set(p.name for p in root.iterdir()) == {'cmb_gec', 'manifests'}
    assert tdr_calls[0]['manifest'].parent == root / 'manifests'


def test_multiple_dataset_manifests_share_the_chosen_root(tmp_path, monkeypatch, tdr_calls):
    groups = [row(download_id=str(i), download_url=f'https://www.cancerimagingarchive.net/{i}.tcia') for i in [1, 2]]
    fake_http(monkeypatch, lambda r: httpx.Response(
        200, text='ListOfSeriesToDownload=\n1.2.' + Path(r.url.path).stem + '\n'))
    root = tmp_path / 'images'
    report = Client(groups).download_dataset('EXAMPLE', root, accept_data_policy=True)
    assert report.complete and len(tdr_calls) == 2
    assert {c['downloadDir'] for c in tdr_calls} == {str(root)}
    assert {e['destination'] for e in report.entries} == {str(root)}
    assert len({c['manifest'] for c in tdr_calls}) == 2
    assert all(c['manifest'].parent == root / 'manifests' for c in tdr_calls)


def test_file_batches_keep_identity_without_adding_files_or_batch_directories(tmp_path, tdr_calls):
    files = [row(download_id=str(i), file_name='mask.nii.gz', participant_id=str(i)) for i in [1, 2]]
    root = tmp_path / 'files'
    report = DownloadsMixin().download_files(files, root, accept_data_policy=True)
    assert report.complete and len(tdr_calls) == 2
    assert {c['downloadDir'] for c in tdr_calls} == {str(root)}
    assert [c['records'][0]['participant_id'] for c in tdr_calls] == ['1', '2']
    assert set(p.name for p in root.iterdir()) == {'manifests'}


def test_conflicting_file_targets_across_access_groups_fail_before_any_transfer(tmp_path, tdr_calls):
    key = tmp_path / 'key.json'
    key.write_text(json.dumps({'api_key': 'synthetic-key'}))
    files = [
        row(file_name='image.zip', participant_id='P1'),
        row(download_id='2', file_name='image.zip', participant_id='P1', access_level='controlled',
            download_url=None, drs_uri='drs://nci-crdc.datacommons.io/dg.4DFC/fake'),
    ]
    root = tmp_path / 'out'
    with pytest.raises(DownloadError, match='same participant/study scope'):
        DownloadsMixin().download_files(files, root, credential_file=key, accept_data_policy=True)
    assert not tdr_calls and not root.exists()


def test_conflicting_targets_across_manifest_groups_fail_before_any_transfer(tmp_path, monkeypatch, tdr_calls):
    groups = [row(download_id=str(i), download_url=f'https://www.cancerimagingarchive.net/{i}.csv') for i in [1, 2]]
    fake_http(monkeypatch, lambda r: httpx.Response(
        200, text=f'imageUrl,file_name\nhttps://files.example.org/{Path(r.url.path).stem}.zip,same.zip\n'))
    root = tmp_path / 'out'
    with pytest.raises(DownloadError, match='same participant/study scope'):
        Client(groups).download_dataset('EXAMPLE', root, accept_data_policy=True)
    assert not tdr_calls and not root.exists()
