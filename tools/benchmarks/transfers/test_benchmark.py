"""No-transfer checks for benchmark scope and consent guards."""
import json
from pathlib import Path
import subprocess
import sys
import pytest
import benchmark_tcia_downloads as benchmark


def fixture(tmp_path):
    path=tmp_path/'cohort.json'
    path.write_text(json.dumps({'series':[{'series_size_MB':29}]}))
    return path


def test_budget_guard_runs_before_any_subprocess_or_output(tmp_path,monkeypatch):
    monkeypatch.setattr(benchmark.subprocess,'Popen',lambda *a,**k:pytest.fail('No launch'))
    output=tmp_path/'out'
    with pytest.raises(ValueError,match='budget'):
        benchmark.run(fixture(tmp_path),output,repeats=100,max_total_mb=1000)
    assert not output.exists()


def test_existing_destination_cannot_turn_into_a_resume_benchmark(tmp_path,monkeypatch):
    monkeypatch.setattr(benchmark.subprocess,'Popen',lambda *a,**k:pytest.fail('No launch'))
    output=tmp_path/'out';output.mkdir()
    with pytest.raises(ValueError,match='fresh'):
        benchmark.run(fixture(tmp_path),output)


def test_cli_requires_explicit_policy_acceptance_before_reading_inputs(tmp_path):
    result=subprocess.run([sys.executable,str(Path(benchmark.__file__)),
        '--cohort',str(tmp_path/'absent.json'),'--output',str(tmp_path/'out')],capture_output=True,text=True)
    assert result.returncode==2
    assert 'Explicit policy acceptance' in result.stderr
    assert not (tmp_path/'out').exists()


def test_disk_guard_runs_before_transfer_or_output(tmp_path, monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr(benchmark.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))
    monkeypatch.setattr(benchmark.subprocess, 'Popen', lambda *a, **k: pytest.fail('No launch'))
    output = tmp_path/'out'
    with pytest.raises(ValueError, match='disk space'):
        benchmark.run(fixture(tmp_path), output)
    assert not output.exists()


def test_wrong_tdr_version_fails_before_transfer(tmp_path, monkeypatch):
    import types
    path = fixture(tmp_path)
    data = json.loads(path.read_text()); data['tdr_version'] = '0.1.0rc2'
    path.write_text(json.dumps(data))
    monkeypatch.setitem(sys.modules, 'tcia_data_retriever', types.SimpleNamespace(
        build_info=lambda: {'version':'0.1.0rc1', 'git_commit':'old'}))
    monkeypatch.setattr(benchmark.subprocess, 'Popen', lambda *a, **k: pytest.fail('No launch'))
    with pytest.raises(ValueError, match='restart the runtime'):
        benchmark.run(path, tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_large_cohort_is_whole_public_series_and_about_five_gigabytes():
    root = Path(__file__).resolve().parent
    cohort = json.loads((root/'cmb-gec-5gb-cohort.json').read_text())
    assert cohort['tdr_version'] == '0.1.0rc2'
    assert 4950 <= sum(r['series_size_MB'] for r in cohort['series']) <= 5000
    assert len({r['SeriesInstanceUID'] for r in cohort['series']}) == len(cohort['series'])
    assert all(r['license_short_name'] == 'CC BY 4.0' for r in cohort['series'])
