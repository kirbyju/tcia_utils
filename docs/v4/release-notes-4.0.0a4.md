# 4.0.0a4 preview

This release connects clinical cohort discovery to bounded Python downloads and
moves shared download interpretation into the TCIA REST/MCP service.

- Download plans include published software requirements, data categories,
  patient/subject/study/series/image counts, official links, and Python support.
- max_items limits a selection to whole DICOM series or files. Reports distinguish
  sampled scope from complete datasets; omitting the limit downloads the plan.
- download_manifest accepts local/HTTPS .tcia, CSV and TSV inputs.
- plan_participant_images and download_images connect exact dataset-scoped cohorts
  to public CT series and individual public slide files. Slide copies are not
  asserted equivalent to submitted originals. Coverage and unresolved routes remain explicit.
- Public DICOM uses a reused idc-index client, early preparation status and optional
  progress=False. Manifest-only folders do not trigger sync scans.
- Execution requires only Python and pip dependencies. External Data Retriever and
  Aspera launchers/handoffs are removed; official inventory links remain available.
  Future pip-installable TDR integration is deferred until that upstream work merges.
- Controlled transfers remain the narrow httpx-based authorized DRS route, with
  keys/tokens confined to the recognized service and signed URLs kept out of reports.

The hosted API must include the new shared-plan and explicit public slide fields.
The updated service can interpret the existing installed bundle before its normal
schema-9 rebuild. Package publication does not deploy that code. Validation uses
synthetic transfers and recorded public metadata; no real controlled/imaging
payload transfer is claimed. Stable 3.x remains the default pip selection.
