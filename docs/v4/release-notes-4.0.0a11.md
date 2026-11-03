# 4.0.0a11 preview

Reviewed and expanded public v4 docstrings across dataset and participant search,
clinical evidence, availability, citations, selection exports, image planning,
transfers, pagination, table/JSON helpers and result objects. Help now explains
parameters, result fields, limits, errors and side effects, including the
separation between route support, installed prerequisites and access approval.

`plan_downloads()` is an optional inspection method. It fetches download-group
metadata, software requirements and Python route support without transferring
files or checking authorization. `download_dataset()` already calls it internally;
`download_manifest()` can use a supplied manifest directly. No API is removed.

The workshop explains external resources as materials outside TCIA staff curation
and operational control, uses open-access/controlled-access terminology, and
introduces the download-options review before calling `plan_downloads()`.

Source changes are documentation only; executable Python AST is unchanged. No
server update, artifact rebuild, dependency change or transfer-engine change is
required. The workshop now pins this preview.
