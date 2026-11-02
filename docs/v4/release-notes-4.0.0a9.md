# 4.0.0a9 preview

Public DICOM and non-DICOM cohort selections now share the same
`plan_participant_images()` and `download_images()` interface. Planning runs on
the TCIA query service, which validates TCIA participant keys, reuses IDC REST
metadata for public DICOM series, and uses TCIA public-file metadata for non-DICOM.
The Python package has no IDC dependency. JSON records remain usable without pandas.

Plans retain coverage (including zero matches), unresolved reasons, TCIA snapshot
identity and provider versions. Batch planning checks scope and version consistency;
unresolved or invalid selections cannot start transfers. TDR performs the actual
manifest/file download with the existing policy, max_items and resume semantics.
The planner is public-only; controlled metadata/approved downloads are unchanged.

Requires query API 0.6.0+ with POST /v2/participants/download-plan. No metadata bundle
schema change is needed, but production service code must be updated before the
new notebook/package workflow can run. The workshop removes its direct IDC code
and installs only TCIA table/download extras plus its optional NIfTI dependency.
Advanced IDC exploration remains linked to IDC's own tools.
