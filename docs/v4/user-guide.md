# TCIA Utils 4.0 user guide

## Install and connect

The 4.0.0a1 source candidate requires Python 3.11 or newer. Build the wheel and install it in a fresh environment as shown in the [README](../../README.md). Install `tcia_utils[tables]` only if you want pandas conversion; the core client imports no pandas, notebooks, or imaging libraries. The [notebook environment](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/requirements.txt) pins the separately tested example tools.

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

`Page.to_dataframe()` and `Result.to_dataframe(key="records_field")` convert records without changing evidence fields. They require the optional `tables` extra. For a result with multiple lists, name the desired key explicitly.

## Participants, clinical evidence, and available data

`search_participants()` returns dataset-scoped participant identities. Keep `(dataset_type, short_title, participant_key)` together; a display patient ID does not establish a cross-dataset join. `get_participant()` and `get_coverage()` show source identifiers and linking coverage. Clinical subject IDs are scoped by `short_title`; an inventory display ID and clinical subject ID need a reviewed crosswalk before joining.

`list_clinical_subjects()`, `list_clinical_facts()`, and `list_clinical_conflicts()` retain source precedence, evidence scope, inheritance, inference, and conflicting values. A dataset diagnosis label does not prove that diagnosis for each patient. Inspect `primary_diagnosis_is_inferred` and fact `evidence_scope`/`is_inferred` before using a result as an observed participant fact. Clinical-only subjects appear only when requested with `include_clinical_only=True`. These routes are bounded; do not treat a short return as a complete cohort export.

`list_participant_assets()` and `search_public_assets()` describe available public metadata, including non-DICOM packages and narrowly reviewed IDC-missing DICOM exceptions. `search_controlled_datasets()` and `list_controlled_files()` expose public inventory metadata about controlled files without granting retrieval rights. `search_annotation_downloads()` identifies DICOM annotation downloads at a bounded level; it does not prove a source-image relationship. Keep `asset_granularity`, `inventory_status`, access route, and unknown sizes visible. A missing series identifier or route URL stays unresolved; do not parse opaque asset IDs into DICOM UIDs.

## Select and hand off

`select(records, release_identity=..., query_scope=...)` is a local operation over explicit, provenance-bearing records. `export_selection(selection, destination, overwrite=False)` is the only disk-writing client operation. It writes route-specific CSV manifests and a JSON accounting companion; it does not fetch a payload or use credentials. Every input record is exported, deduplicated, excluded by an explicit rule, or unresolved with a reason.

Data Retriever CSVs have exactly one recognized route column: `SeriesInstanceUID`, `imageUrl`, or `drs_uri`. Public and controlled selections, and different routes, go to separate files. A `source_url` is not automatically a deliverable `imageUrl`. Controlled `drs_uri` is public metadata; actual retrieval requires separate authorization. An Aspera package/path handoff has its own format only when exact package/path fields are present. Selection cannot invent `.tcia` files, signed URLs, missing DICOM UIDs, or source-image relationships. Include source images only after an explicit selection choice.

For public DICOM detail, use `idc-index` directly with an evidenced dataset DOI or reviewed patient identifier. Query IDC for series identifiers and viewer URLs, check IDC coverage independently, then supply verified `SeriesInstanceUID` values to a selection. TCIA Utils does not promise series UID discovery from the current REST asset DTO and does not wrap the full `idc-index` API. See the [public DICOM notebook](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/02_public_dicom_idc.ipynb) for the direct handoff.

The five maintained notebooks under [TCIA_Notebooks/tcia_utils_v4](https://github.com/kirbyju/TCIA_Notebooks/tree/main/tcia_utils_v4) contain explicit synthetic fixture and bounded live metadata modes. Executable default cells do not call a downloader. Live success depends on the configured hosted service and IDC availability; a fixture run demonstrates code and data shape only.
