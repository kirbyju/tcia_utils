"""Notebook capture through the released TDR wrapper, using a synthetic child CLI.

These tests launch no real downloader and transfer no imaging payloads. The
examples CI job installs the downloads extra and requires both cases to run.
"""

import contextlib
import io
import os
from pathlib import Path
import sys
import threading

import pytest
from tcia_utils import DownloadError
import tcia_utils._downloads as downloads
from tcia_utils._downloads import DownloadsMixin, _tdr as real_backend


@pytest.mark.skipif(os.name == 'nt', reason='Synthetic executable uses a POSIX shebang')
@pytest.mark.parametrize('success', [True, False])
def test_wrapper_relays_both_streams_before_completion(tmp_path, monkeypatch, success):
    tdr = pytest.importorskip('tcia_data_retriever')
    assert tdr.build_info()['version'] == '0.1.0rc3'
    finish = tmp_path / 'finish'
    child = tmp_path / 'synthetic-tdr'
    child.write_text('#!' + sys.executable + '\n' + '''
import json, sys, time
from pathlib import Path
args=sys.argv[1:]
assert args[args.index('--progress')+1] == 'plain'
print('Fixture stdout: preparing', flush=True)
print('Fixture stderr: active bytes 1/2', file=sys.stderr, flush=True)
finish=Path(FINISH)
deadline=time.monotonic()+15
while not finish.exists() and time.monotonic()<deadline:
    time.sleep(0.01)
ok=SUCCESS
report=dict(schema_version=1, status='success' if ok else 'failed',
    version='0.1.0rc3', total=1, downloaded=1 if ok else 0,
    synced=0, skipped=0, failed=0 if ok else 1, elapsed_seconds=0.1)
Path(args[args.index('--report-json')+1]).write_text(json.dumps(report))
print('Run complete' if ok else 'Run failed', file=sys.stderr, flush=True)
sys.exit(0 if ok else 1)
'''.replace('FINISH', repr(str(finish))).replace('SUCCESS', repr(success)))
    child.chmod(0o755)
    monkeypatch.setattr(tdr, 'executable', lambda: child)
    monkeypatch.setattr(downloads, '_tdr', real_backend)
    manifest = tmp_path / 'series.csv'
    manifest.write_text('SeriesInstanceUID\n1.2.3\n')
    seen_out, seen_err = threading.Event(), threading.Event()

    class Capture(io.StringIO):
        def __init__(self, marker, event):
            super().__init__()
            self.marker, self.event = marker, event
        def write(self, text):
            result = super().write(text)
            if self.marker in self.getvalue():
                self.event.set()
            return result

    out = Capture('Fixture stdout', seen_out)
    err = Capture('Fixture stderr', seen_err)
    result = {}
    def run():
        try:
            result['report'] = DownloadsMixin().download_manifest(
                manifest, tmp_path / 'out', accept_data_policy=True,
            )
        except BaseException as error:
            result['error'] = error

    worker = threading.Thread(target=run, daemon=True)
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        worker.start()
        try:
            assert seen_out.wait(5), 'Child stdout did not reach Python capture while running'
            assert seen_err.wait(5), 'Child stderr did not reach Python capture while running'
            assert worker.is_alive(), 'Progress must be visible before completion'
            assert not result
        finally:
            finish.touch()
            worker.join(timeout=10)
    assert not worker.is_alive()
    assert '\r' not in err.getvalue() and '\x1b' not in err.getvalue()
    if success:
        assert 'error' not in result
        assert result['report'].complete and 'Run complete' in err.getvalue()
    else:
        assert isinstance(result.get('error'), DownloadError)
        assert not result['error'].report.complete
        assert result['error'].report.entries[0]['engine_summary']['failed'] == 1
        assert 'Run failed' in err.getvalue() and 'Run complete' not in err.getvalue()
