# TCIA Utils 4.0 user guide

## Install and connect

The 4.0.0a2 alpha requires Python 3.11 or newer. Install the exact preview in a fresh environment as shown in the [README](../../README.md). Install `tcia_utils[tables]` only if you want pandas conversion; the core client imports no pandas, notebooks, or imaging libraries. The [notebook environment](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/requirements.txt) pins the separately tested example tools.

Set an explicit `base_url` for a TCIA V2 REST host. `TCIAClient` does not select a host, authenticate, transfer files, or contact the network until a method is called. Close it with a `with` block or `close()`.

```python
from tcia_utils import TCIAClient

with TCIAClient(base_url="https://<TCIA-REST-host>", timeout=15.0) as tcia:
    status = tcia.status()
    print(status.data.installed_profile, status.data.release_fingerprint)
    print(status.data.capabilities)
```

`status()` checks readiness and the installed bundle. A green liveness endpoint alone does not show that a component is installed. Optional capabilities include participant search, public non-DICOM detail, controlled inventory, clinical detail, DataCite metadata, and verified analytical publications.

## Discover a dataset and its citation

```python
with TCIAClient(base_url="https://<TCIA-REST-host>") as tcia:
    page = tcia.search_datasets(query="lung", dataset_type="both", limit=10)
    for row in page.items:
        print(row["dataset_type"], row["short_title"], row.get("doi"))
    detail = tcia.get_dataset("4D-Lung")
    for row in detail.data["datasets"]:
        print(row["dataset_type"], row.get("resolved_access_level"))
```

Visible TCIA Collection and Analysis Result records establish TCIA publication identity. Preserve the pair `(dataset_type, short_title)`; a short title can resolve to more than one `datasets` row. `get_dataset()` also returns `current_downloads`, bounded `related_analysis_results` suggestions, and `caveats`. Those suggestions are a heuristic list, not a complete relationship graph. `get_access()` returns one named dataset with grouped download access; inspect its `dataset_type` and use `get_dataset()` to review all candidates when a short title is ambiguous. Download-level access and license labels decide routing; a dataset can have both open and controlled downloads. A noncommercial license restriction does not itself mean controlled access.

Use `search_citations()` and `get_citation(doi, style="apa")` for DataCite metadata and formatted APA, Vancouver, Chicago author-date, BibTeX, or RIS output. The DOI record enriches a TCIA dataset; it does not establish TCIA publication independently. `search_publications()` returns TCIA-maintained verified analytical use. `get_impact()` keeps verified-use counts separate from DataCite citation, view, download, and reference event metrics. DataCite download events are not TCIA image-download totals.

## Read pages and evidence scope

`Page.items` holds only the current page; `Page.count` is the number returned on that page, not a total. `Page.next_page()` fetches one next page, and `Page.iter_pages()` advances lazily. Keep the iterator tied to one query; a cursor is bound to filters and a service release. The client rejects a changed release/profile, repeated or invalid cursor, malformed page, or failed component. `Page.response` retains route-specific notes and scope fields alongside records.

`Result.data` retains the service's named fields; `Result.notes` repeats the note fields selected for convenience. Both `Page` and `Result` include `provenance` with the observed service URL, release fingerprint/tag, profile, capability flags, and observation time. This is a request-level observation, not a per-record source stamp. Keep it with any saved selection. `complete` means the requested cursor iteration reached a terminal page; `partial` means more pages are available. `unknown` marks bounded routes with no exhaustive cursor or bounded sublists. `coverage_complete` only describes participant linking, not the completeness of clinical or file detail. A successful empty page means zero matches within its requested scope; unavailable components raise an error.

`Page.to_dataframe()` converts one page; `Page.to_dataframe(all_pages=True)` collects all cursor pages and checks their release/profile before conversion. The DataFrame retains provenance in `attrs["tcia_provenance"]`. `Result.to_dataframe(key="records_field", columns=[...])` converts a named record list, with optional columns for empty-table schemas. These methods require the optional `tables` extra. For a result with multiple lists, name the desired key explicitly.

Set `TCIAClient(..., pin_snapshot=True)` to reject a changed release/profile between independent calls in one session. Restart the workflow with a new client after a metadata update.

## Participants, clinical evidence, and available data

`search_participants()` returns dataset-scoped participant identities. Keep `(dataset_type, short_title, participant_key)` together; a display patient ID does not establish a cross-dataset join. `get_participant()` and `get_coverage()` show source identifiers and linking coverage. Clinical subject IDs are scoped by `short_title`; an inventory display ID and clinical subject ID need a reviewed crosswalk before joining.

`list_clinical_subjects()`, `list_clinical_facts()`, and `list_clinical_conflicts()` retain source precedence, evidence scope, inheritance, inference, and conflicting values. A dataset diagnosis label does not prove that diagnosis for each patient. Inspect `primary_diagnosis_is_inferred` and fact `evidence_scope`/`is_inferred` before using a result as an observed participant fact. Clinical-only subjects appear only when requested with `include_clinical_only=True`. These routes are bounded; do not treat a short return as a complete cohort export.

`clinical_for_participants(participants)` accepts participant records or a DataFrame from `search_participants()`. It fetches clinical subjects in batches using exact Collection-scoped display IDs, rejects capped or inconsistent responses, and retains unmatched IDs. Its `subjects` list contains the full clinical records; `values` flattens resolved concepts into rows with `participant_id`, inference flags, evidence scope, and complete source evidence. Use `.to_dataframe("values")` for display/export. Filter this table using the selected dataset and participant IDs. A successful batch collection does not establish complete clinical coverage or validate a new identity crosswalk.

`list_participant_assets()` and `search_public_assets()` describe available public metadata, including non-DICOM packages and narrowly reviewed IDC-missing DICOM exceptions. `search_controlled_datasets()` and `list_controlled_files()` expose public inventory metadata about controlled files without granting retrieval rights. `search_annotation_downloads()` identifies DICOM annotation downloads at a bounded level; it does not prove a source-image relationship. Keep `asset_granularity`, `inventory_status`, access route, and unknown sizes visible. A missing series identifier or route URL stays unresolved; do not parse opaque asset IDs into DICOM UIDs.

## Select and hand off

`select(records, release_identity=..., query_scope=...)` is a local operation over explicit, provenance-bearing records. `export_selection(selection, destination, overwrite=False)` writes the explicit selection locally. It writes route-specific CSV manifests and a JSON accounting companion; it does not fetch a payload or use credentials. Every input record is exported, deduplicated, excluded by an explicit rule, or unresolved with a reason.

Data Retriever CSVs have exactly one recognized route column: `SeriesInstanceUID`, `imageUrl`, or `drs_uri`. Public and controlled selections, and different routes, go to separate files. A `source_url` is not automatically a deliverable `imageUrl`. Controlled `drs_uri` is public metadata; actual retrieval requires separate authorization. An Aspera package/path handoff has its own format only when exact package/path fields are present. Selection cannot invent `.tcia` files, signed URLs, missing DICOM UIDs, or source-image relationships. Include source images only after an explicit selection choice.

For public DICOM detail, use `idc-index` directly with an evidenced dataset DOI or reviewed patient identifier. Query IDC for series identifiers and viewer URLs, check IDC coverage independently, then supply verified `SeriesInstanceUID` values to a selection. TCIA Utils does not promise series UID discovery from the current REST asset DTO and does not wrap the full `idc-index` API. The download helper delegates public DICOM transfers to `idc-index`; detailed imaging queries remain in that project.

## Download from Python

```python
with TCIAClient(base_url="https://tcia.duckdns.org", pin_snapshot=True) as tcia:
    plan = tcia.plan_downloads("CMB-GEC")
    print(plan.to_dataframe()[["download_id", "download_category", "access_level"]])
    check = tcia.download_dataset("CMB-GEC", "downloads", dry_run=True)
    print(check.to_dataframe())
    # Explicit transfer, when ready:
    # report = tcia.download_dataset("CMB-GEC", "downloads", download_ids=[...])
```

`plan_downloads()` collects the current dataset download inventory. `download_category` comes from the collection's published `download_types`; data formats and access levels remain separate fields. Use `download_ids` or `access_levels` to define a subset explicitly. Without filters, **every download group** is selected. If a short title identifies both dataset types, supply `dataset_type="collection"` or `"analysis_result"`. Unknown requested IDs fail rather than producing a partial selection.

`download_dataset(..., dry_run=True)` queries metadata and checks local prerequisites. It does not fetch manifests, authenticate, validate index coverage, or transfer files, and its report never claims completion. Actual downloads preflight all selected manifests and public DICOM index coverage before starting any payload. Missing prerequisites stop the entire selection rather than silently skipping a group.

| Data route | Python behavior | Additional requirement |
| --- | --- | --- |
| Public DICOM `.tcia` / series CSV | Reuses `IDCClient.download_dicom_series()` | `pip install "tcia_utils[downloads]"`; all selected series must be present in the current IDC index |
| Public HTTPS file / image URL CSV | Streams original files atomically | No optional transfer dependency |
| Controlled DRS files / official DRS manifest | Resolves authorized file URLs, then streams files | Approved JSON `credential_file` with `api_key`, currently for `nci-crdc.datacommons.io` |
| Original TCIA Aspera package | Calls the maintained package transfer client using the exact official package URL | Separately installed/configured [Aspera CLI](https://github.com/IBM/aspera-cli) (`ascli`) and IBM transfer tools |
| Other or incomplete route | Reports unsupported; no fallback URL is invented | Use the official collection instructions |

Pip does not install Aspera software. Install/configure it following its documentation, then pass `package_client="/path/to/ascli"` if it is not on `PATH`. Original packages are kept as published; the helper does not substitute viewer files or automatically extract archives. Current indexed NIfTI paths do not establish individual download URLs; select the published package.

`download_files(records, destination, credential_file=...)` downloads an explicit file-grain selection with authoritative `access_level` and either an HTTPS file URL or supported `drs_uri`. It does not add related source images. The current public asset DTO often describes inventory rather than a directly retrievable file; unresolved records fail.

`save_manifests(plan_or_records, directory)` saves unchanged official TCIA `.tcia`/CSV/TSV manifests with SHA-256 hashes and item counts. It never fetches imaging payloads. `data_retriever_command(manifest, destination, credential_file=...)` returns an argument list for optional CLI use. `download_dataset(..., backend="data_retriever")` invokes an installed retriever for supported manifest groups; package-only links are not supported by that backend. No policy is automatically accepted.

Controlled keys remain local. Supply a file path, never a literal key in notebook source. Keys are sent only to the recognized authorization service; file requests use a separate client without the bearer token. Signed URLs and credentials are excluded from completion reports and external engine output is suppressed. Public inventory metadata does not confer permission to retrieve controlled data.

Every `DownloadReport` retains selected scope and per-group status. `complete` means all selected entries reported success; it does not mean every group in a dataset was selected. `engine_completed` records successful upstream process/client completion **without independent byte-level verification**. HTTP transfers check response length when supplied and metadata size/checksum when available; size-only checks are weaker than checksums. Failed partial HTTP files are removed without replacing an existing complete file. Existing files are skipped only when the available checksum or size matches; `overwrite=True` explicitly replaces them. HTTP transfers restart interrupted files; delegated clients retain their own resume behavior.

A failure raises `DownloadError`; inspect `error.report.to_dataframe()` when a report is present. Completed earlier groups remain recorded, failed groups are marked, and later groups are `not_attempted`. Do not infer success from the presence of a destination directory. No adapter live tests with credentials or imaging payloads are included in the notebook's metadata validation; synthetic offline tests exercise transfer and failure behavior.

The [single guided workshop](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb) uses the installed package, real public metadata by default, and opt-in imaging transfers. Its maintainer fixture mode replays recorded public metadata for regression checks; it is not a current research result.
