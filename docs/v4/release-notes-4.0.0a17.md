# 4.0.0a17 preview

Download methods no longer expose the redundant `resume` argument. TDR still
checks and reuses existing files automatically. To retry or expand a sample,
rerun the same call with the same destination; remove `resume` from older v4 calls.

The new metadata-only `export_aspera_manifest()` writes exact public package/file
routes for a dataset-scoped participant cohort, using current published package
links. It supports file-count samples and preserves per-participant coverage.
No Aspera application is installed or invoked by the SDK.

The main workshop now uses public NSCLC-Radiomics CT/SEG series. The brain MR
NIfTI cohort and geometry lesson moves into the separate Aspera notebook, which
can also consume saved participant cohorts or Participant Explorer CSV exports.

TDR remains pinned to 0.1.0rc3. No TDR, REST/MCP or artifact update is required.

Validation: 346 SDK tests and 34 notebook/adapter checks passed. Both lessons
executed against public metadata and then offline from captured responses with
an installed SDK wheel. The Aspera lesson exported two of 70 indexed files for
two participants. Every imaging transfer was blocked or intercepted; Ruby/Aspera
installation and real Aspera payload transfer were not exercised.
