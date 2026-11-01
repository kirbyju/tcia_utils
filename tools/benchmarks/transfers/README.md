# Download performance evidence for SDK maintainers

These development tools inform the choice of transfer engine in `tcia_utils`.
They are separate from the end-user workshop and are not a notebook tutorial.
No DICOM payloads or credentials are stored here. CI runs only the no-transfer
checks in `test_benchmark.py`; real transfers require an explicit CLI flag.

The runner compares the released TDR and IDC Python interfaces on exactly the
same public [CMB-GEC](https://www.cancerimagingarchive.net/collection/cmb-gec/)
objects. The selected images are licensed CC BY 4.0; consult the collection page
for the dataset citation and acknowledgements.

## Recorded results

The [small rc1 pilot](results/2026-11-01-local.json) used two CT/MR series,
96 instances and 29,013,394 bytes per trial. With three trials per tool, median
fresh-process times were 2.60 s for TDR and 4.62 s for IDC; prepared-client times
were 2.60 s and 2.83 s. Keep that release and workload separate from rc2 results.

The [rc2 larger comparison](results/2026-11-01-rc2-large-local.json) used 59
whole CT/MR series, 8,460 instances and 4,996,622,482 bytes per trial. Trial order
was TDR / IDC / IDC / TDR, with fresh destinations and no resume. All four copies
matched byte for byte, totaling 19,986,489,928 bytes.

| Tool | First trial | Second trial | Median (midpoint of two) |
|---|---:|---:|---:|
| TDR 0.1.0rc2 | 66.97 s | 75.14 s | 71.05 s |
| idc-index 0.13.4 | 90.24 s | 60.28 s | 75.26 s |

Each tool was faster in one pair. Treat performance as comparable on this local
macOS workload: the 5.6% midpoint difference is smaller than the observed variation.
This supports using TDR for transfers while retaining IDC's role in cohort metadata.
It is not a general superiority claim or a Colab measurement.

The reports preserve package versions, TDR source revision, selected IDs, counts,
sizes, licenses, source prefixes, manifest hash, settings and instance fingerprints.
The rc2 report's runner/cohort hashes still match these unchanged source files.
IDC selected s5cmd v2.3.0-991c9fb, byte-identical to its pip wheel binary. IDC used
256 object workers and ten retries by default; TDR used eight series workers and
no retries. Worker granularity differs, and IDC request-level retries were not
instrumented. TDR reported 59 downloaded and zero synced series in each trial.

## Repeating the development test

Use a separate Python environment for the exact benchmark dependencies. These
pins are for reproducing the experiment, not a change to SDK runtime dependencies.

```bash
python -m venv /tmp/tcia-transfer-benchmark
/tmp/tcia-transfer-benchmark/bin/python -m pip install \
  tcia-data-retriever==0.1.0rc2 idc-index==0.13.4 idc-index-data==25.0.5 \
  s5cmd==0.3.3 pydicom==3.0.2 pytest
/tmp/tcia-transfer-benchmark/bin/python tools/benchmarks/transfers/benchmark_tcia_downloads.py \
  --cohort tools/benchmarks/transfers/cmb-gec-5gb-cohort.json \
  --output /tmp/tcia-transfer-large-results \
  --repeats 2 --max-total-mb 22000 --timeout 1800 --accept-data-policy
```

Read the [TCIA usage policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/)
before supplying `--accept-data-policy`. This command transfers approximately
20 GB and retains every trial copy; allow at least 22 GB free. Use a new output
path on each run. The small cohort file with runner defaults uses three trials
per tool; add `--warm` for its prepared-client case.

Preflight checks the metadata-based payload budget, free disk space and TDR build.
The runner gives TDR explicit S3 prefixes and verifies that IDC's selected series
resolve to those same objects. After timing, it checks DICOM identifiers, instance
counts, total bytes and per-instance SHA-256 hashes. Failures and mismatches stop
the comparison. The larger cohort uses hash-sorted whole public CT/MR series with
at least 32 instances and at most 350 MB per series, selected to total 4.95–5.00 GB.

Fresh-process timing includes imports, index/client initialization and transfer.
Prepared-client timing excludes initialization; IDC's download query is prepared
with transfer intercepted, so no warm-up images transfer. TDR's API starts its
executable per call. Installation, benchmark-only source checks and verification
are excluded from timing. First-visible-file time is a filesystem observation.

Local and remote caches are not cleared. The budget is not a packet-level cutoff.
These tests do not cover controlled access, non-DICOM, multi-GB individual files,
interruption or resume. Two large trials per tool do not establish broad performance.
