"""Pandas-free exports preserve result scope and never fetch hidden pages."""

import json
from pathlib import Path
import subprocess
import sys

import pytest

from tcia_utils import (
    Completeness, DownloadPlan, DownloadReport, ExportReport, ManifestReport,
    Page, ParticipantImagePlan, Provenance, Result, Selection, ServiceStatus,
    InvalidRequestError,
)


def provenance():
    return Provenance(
        'https://metadata.example.org', 'release-a', 'latest', 'research_detail',
        {'clinical_detail': True}, '2026-11-03T12:00:00+00:00',
    )


def page(*, fetch=None, last=False):
    row = {'short_title': 'EXAMPLE', 'modalities': ['CT', 'MR'], 'size': None}
    return Page(
        items=(row,), count=1, limit=1, has_more=not last, truncated=False,
        next_cursor=None if last else 'cursor-a', query={'modalities': ['CT']},
        response={'note': 'Bounded inventory', 'datasets': [row]},
        provenance=provenance(),
        completeness=Completeness('complete' if last else 'partial'),
        _fetch_next=fetch,
    )


def examples():
    p = provenance()
    return [
        p, Completeness('unknown', 'No exhaustive cursor'),
        ServiceStatus({'status': 'ready'}, {'participant_search': True},
                      p.release_fingerprint, p.release_tag, p.installed_profile),
        Result({'datasets': [{'title': 'Caf\u00e9', 'modalities': ['CT', 'MR'],
                              'controlled': False, 'size': None}]},
               ('Inventory is bounded',), p, Completeness('unknown')),
        page(),
        DownloadPlan(({'download_id': 'one', 'subjects': 12},), p,
                     {'is_sample': False}, {'retriever': {'label': 'Data Retriever'}},
                     ('Published requirements',)),
        ParticipantImagePlan(
            ({'participant_key': 'key-a', 'series_instance_uid': '1.2.3'},),
            ({'participant_key': 'key-a', 'matching_items': 1},),
            ({'participant_key': 'key-b', 'reason': 'no_verified_identity'},),
            {'metadata_snapshot': 'release-a'}, p,
            {'IDC': {'idc_version': 'v25'}},
        ),
        ManifestReport(Path('output'), ({'manifest': 'cohort.csv'},)),
        DownloadReport(({'status': 'dry_run', 'path': Path('output/image.dcm')},),
                       {'is_sample': True, 'sample_selected_items': 1}),
        Selection(({'SeriesInstanceUID': '1.2.3'},), {'fingerprint': 'release-a'},
                  {'modality': 'CT'}, {'complete': False}),
        ExportReport(Path('output'), (Path('output/cohort.csv'),), 1, 0, 0, 1),
    ]


@pytest.mark.parametrize('result', examples(), ids=lambda r: type(r).__name__)
def test_every_public_result_exports_a_json_compatible_dictionary(result):
    exported = result.to_dict()
    assert isinstance(exported, dict)
    assert json.loads(result.to_json()) == exported
    assert json.loads(result.to_json(indent=2)) == exported
    assert json.loads(json.dumps(exported, allow_nan=False)) == exported


def test_export_retains_evidence_types_and_is_independent():
    result = examples()[3]
    exported = result.to_dict()
    assert exported == {
        'data': {'datasets': [{'title': 'Caf\u00e9', 'modalities': ['CT', 'MR'],
                               'controlled': False, 'size': None}]},
        'notes': ['Inventory is bounded'],
        'provenance': {
            'base_url': 'https://metadata.example.org', 'release_fingerprint': 'release-a',
            'release_tag': 'latest', 'installed_profile': 'research_detail',
            'capabilities': {'clinical_detail': True},
            'observed_at_utc': '2026-11-03T12:00:00+00:00',
        },
        'completeness': {'state': 'unknown', 'reason': None},
    }
    assert 'Caf\u00e9' in result.to_json(indent=2)
    exported['data']['datasets'][0]['modalities'].append('PT')
    exported['provenance']['capabilities']['clinical_detail'] = False
    assert result.data['datasets'][0]['modalities'] == ['CT', 'MR']
    assert result.provenance.capabilities['clinical_detail'] is True


def test_page_export_never_fetches_and_preserves_paging_and_query_scope():
    def forbidden_fetch():
        raise AssertionError('Export must not fetch another page')
    first = page(fetch=forbidden_fetch)
    exported = first.to_dict()
    assert '_fetch_next' not in exported
    assert exported['has_more'] and exported['next_cursor'] == 'cursor-a'
    assert exported['count'] == 1 and exported['completeness']['state'] == 'partial'
    assert exported['query'] == {'modalities': ['CT']}
    assert exported['response']['note'] == 'Bounded inventory'
    assert json.loads(first.to_json()) == exported
    exported['items'][0]['modalities'].append('PT')
    assert first.items[0]['modalities'] == ['CT', 'MR']
    assert exported['response']['datasets'][0]['modalities'] == ['CT', 'MR']


def test_explicit_page_iteration_keeps_each_pages_completeness():
    final = page(last=True)
    first = page(fetch=lambda: final)
    exported = [part.to_dict() for part in first.iter_pages()]
    assert [p['completeness']['state'] for p in exported] == ['partial', 'complete']
    assert exported[-1]['next_cursor'] is None
    assert all(p['provenance']['release_fingerprint'] == 'release-a' for p in exported)


@pytest.mark.parametrize('status,complete', [
    ('dry_run', False), ('failed', False), ('engine_completed', True),
    ('verified_existing', True),
])
def test_download_completion_and_sample_scope_are_not_lost(status, complete):
    report = DownloadReport(({'status': status},), {'is_sample': True})
    assert report.to_dict()['complete'] is complete
    assert json.loads(report.to_json())['scope']['is_sample'] is True
    assert DownloadReport(()).to_dict()['complete'] is False


@pytest.mark.parametrize('value,error', [
    (float('nan'), ValueError), (float('inf'), ValueError),
    (object(), TypeError), ({1: 'not a JSON key'}, TypeError),
])
def test_invalid_values_fail_instead_of_silently_becoming_strings(value, error):
    result = Result({'value': value}, (), provenance(), Completeness('unknown'))
    with pytest.raises(error):
        result.to_dict()
    with pytest.raises(error):
        result.to_json()


def test_json_helpers_run_with_optional_packages_blocked():
    code = '''
import importlib.abc, json, sys
class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        if fullname.split('.')[0] in {'pandas', 'idc_index', 'tcia_data_retriever'}:
            raise AssertionError('Optional package import: ' + fullname)
sys.meta_path.insert(0, BlockOptional())
from tcia_utils import *
p = Provenance('https://example.org', 'fp', None, 'research_detail', {}, 'now')
r = Result({'datasets': [{'short_title': 'EXAMPLE'}]}, (), p, Completeness('unknown'))
assert json.loads(r.to_json())['data']['datasets'][0]['short_title'] == 'EXAMPLE'
assert r.to_dict()['completeness']['state'] == 'unknown'
plan = ParticipantImagePlan((), (), (), {}, p)
assert json.loads(plan.to_json())['rows'] == []
assert DownloadReport(()).to_dict()['complete'] is False
assert ManifestReport(__import__('pathlib').Path('output'), ()).to_dict()['destination'] == 'output'
assert not {'pandas', 'idc_index', 'tcia_data_retriever'}.intersection(sys.modules)
'''
    subprocess.run([sys.executable, '-c', code], check=True, capture_output=True, text=True)


@pytest.mark.parametrize('result', [examples()[3], page(), examples()[6]])
def test_result_envelopes_are_not_mistaken_for_dataframes(result):
    from tcia_utils._research import records
    with pytest.raises(InvalidRequestError, match='list of records'):
        records(result)


def test_dataframe_style_record_conversion_remains_supported():
    from tcia_utils._research import records
    class Table:
        def to_dict(self, orient):
            assert orient == 'records'
            return [{'participant_key': 'dataset:person'}]
    assert records(Table()) == [{'participant_key': 'dataset:person'}]
