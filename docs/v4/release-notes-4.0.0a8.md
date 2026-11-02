# 4.0.0a8 preview

IDC is now an independent notebook/application integration. Remove `idc-index`
from every SDK extra and remove the SDK's lazy IDC client and public DICOM
cohort-query implementation. No `cohorts` extra is provided. The notebook installs
IDC explicitly and credits its additional public DICOM metadata features.

TCIA REST discovery, clinical enrichment, non-DICOM file planning and supported
TDR-backed downloads remain available. `tables` still provides optional pandas
conversion; `downloads` still provides the packaged TCIA Data Retriever. Explicit
UID manifests and whole-dataset DICOM downloads need no Python IDC package.

`plan_participant_images()` now supports TCIA public non-DICOM files only. DICOM
series lookup calls raise a clear instruction to use IDC directly. Use a CSV of
selected Series UIDs with `download_manifest()` after an independent IDC query.
The optional notebook example restricts collection, source DOI, participant and
modality; the pathology example works without running the IDC section.

No REST/MCP server or bundle change is required. Transfer engine policy acceptance,
max_items, explicit resume, controlled-key requirements and the pinned TDR version
are unchanged. No live controlled transfer or new payload benchmark is included.
