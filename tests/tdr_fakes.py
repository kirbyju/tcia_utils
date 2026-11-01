"""External TDR contract double: records manifests; never performs a transfer."""

import csv
from pathlib import Path
from types import SimpleNamespace
import tcia_utils._downloads as downloads


def success(total):
    return dict(
        schema_version=1,
        status="success",
        version="0.1.0rc1",
        total=total,
        downloaded=total,
        synced=0,
        skipped=0,
        failed=0,
        elapsed_seconds=0.01,
    )


class EngineError(RuntimeError):
    def __init__(self, report=None):
        super().__init__("private engine detail must not appear in SDK reports")
        self.report = report


def install_tdr(monkeypatch, available_uids=None, handler=None):
    calls = []

    def transfer(manifest, destination, **kwargs):
        with Path(manifest).open(newline="") as source:
            records = list(csv.DictReader(source))
        call = {
            "manifest": Path(manifest),
            "downloadDir": str(destination),
            "records": records,
            "options": kwargs,
            "seriesInstanceUID": [
                r["SeriesInstanceUID"] for r in records if "SeriesInstanceUID" in r
            ],
        }
        calls.append(call)
        if available_uids is not None and set(call["seriesInstanceUID"]) - set(available_uids):
            raise EngineError(
                dict(
                    schema_version=1,
                    status="failed",
                    error_code="items_failed",
                    total=len(records),
                    downloaded=0,
                    synced=0,
                    skipped=0,
                    failed=len(records),
                )
            )
        return handler(call) if handler else success(len(records))

    monkeypatch.setattr(downloads, "_tdr", lambda: SimpleNamespace(download=transfer))
    return calls
