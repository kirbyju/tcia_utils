# TCIA Utils 4.0 user guide

## Install and connect

The 4.0.0a13 preview requires Python 3.11 or newer. Install the preview in a fresh environment as shown in the [README](../../README.md). Install `tcia_utils[tables]` only if you want pandas conversion; the core client imports no pandas, notebooks, or imaging libraries. The [notebook environment](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/requirements.txt) pins the separately tested example tools.

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

Collection search now includes `data_categories`, distinct from `download_data_types`
and `download_file_types`. No per-dataset detail calls are needed to display these
categories. `describe_datasets()` remains available when full dataset detail is
wanted for an explicit selection.

## Read pages and evidence scope

`Page.items` holds only the current page; `Page.count` is the number returned on that page, not a total. `Page.next_page()` fetches one next page, and `Page.iter_pages()` advances lazily. Keep the iterator tied to one query; a cursor is bound to filters and a service release. The client rejects a changed release/profile, repeated or invalid cursor, malformed page, or failed component. `Page.response` retains route-specific notes and scope fields alongside records.

`Result.data` retains the service's named fields; `Result.notes` repeats the note fields selected for convenience. Both `Page` and `Result` include `provenance` with the observed service URL, release fingerprint/tag, profile, capability flags, and observation time. This is a request-level observation, not a per-record source stamp. Keep it with any saved selection. `complete` means the requested cursor iteration reached a terminal page; `partial` means more pages are available. `unknown` marks bounded routes with no exhaustive cursor or bounded sublists. `coverage_complete` only describes participant linking, not the completeness of clinical or file detail. A successful empty page means zero matches within its requested scope; unavailable components raise an error.

`Page.to_dataframe()` converts one page; `Page.to_dataframe(all_pages=True)` collects all cursor pages and checks their release/profile before conversion. The DataFrame retains provenance in `attrs["tcia_provenance"]`. `Result.to_dataframe(key="records_field", columns=[...])` converts a named record list, with optional columns for empty-table schemas. These methods require the optional `tables` extra. For a result with multiple lists, name the desired key explicitly.

Set `TCIAClient(..., pin_snapshot=True)` to reject a changed release/profile between independent calls in one session. Restart the workflow with a new client after a metadata update.

## Work with dictionaries and JSON without pandas

Install the core package with `python -m pip install "tcia_utils==4.0.0a13"`.
Pandas is optional: searching, clinical enrichment, image planning, manifest
exports and downloads accept ordinary Python records. Actual transfers also need
the `downloads` extra; table conversions need the `tables` extra.

Use `.to_dict()` for a JSON-compatible Python dictionary or `.to_json(indent=2)`
for readable JSON text. Both preserve the result envelope, including available
provenance, query scope, notes and completeness. They work on search pages, detail
results (including status and clinical results), participant image plans, dataset
download plans, selections, and manifest/export/download reports.

```python
from pathlib import Path
import json
from tcia_utils import TCIAClient

with TCIAClient(base_url="https://tcia.duckdns.org", pin_snapshot=True) as tcia:
    page = tcia.search_datasets(query="lung", limit=10)
    saved_page = page.to_dict()
    print(saved_page["items"][0]["short_title"] if saved_page["items"] else "No matches")
    print(saved_page["completeness"]["state"])
    Path("lung-datasets-page.json").write_text(page.to_json(indent=2), encoding="utf-8")

    details = tcia.get_dataset("CMB-GEC")
    saved_details = details.to_dict()
    print(saved_details["data"]["datasets"])
    Path("cmb-gec.json").write_text(details.to_json(indent=2), encoding="utf-8")

    # Fetch every page deliberately, retaining each page's scope and metadata.
    pages = [part.to_dict() for part in page.iter_pages()]
    Path("lung-datasets-pages.json").write_text(
        json.dumps(pages, indent=2, ensure_ascii=False), encoding="utf-8"
    )
```

`.to_dict()` and `.to_json()` make no requests and never collect extra pages.
A search page exports `items`, paging fields, `query`, `response`, `provenance`
and `completeness`. A detail result exports `data`, `notes`, `provenance` and
`completeness`. A partial page stays partial in the export. Clinical routes with
unknown completeness stay unknown, even when no more records were returned.
`.iter_pages()` explicitly fetches additional search pages and checks their
snapshot identity; it cannot expand a bounded clinical route into exhaustive data.

If you only need records, `.items`, `.data` and image-plan `.rows` remain available.
For example, pass `list(participant_page.items)` directly to
`clinical_for_participants()` or `plan_participant_images()` without constructing
a DataFrame. Plans retain `coverage`, `unresolved`, `scope`, `provenance` and
provider versions when serialized. Download reports include `complete` and
`scope`; completing a sample does not mean the full dataset was downloaded.

The exported dictionary is independent of the original result. Nested arrays
stay arrays, booleans stay booleans, missing values become JSON `null`, and local
paths become strings. Unsupported values and nonfinite numbers raise an error
instead of silently being converted to text. `.to_json()` returns text; it does not write a file by itself.

## Participants, clinical evidence, and available data

`search_participants()` returns dataset-scoped participant identities. Keep `(dataset_type, short_title, participant_key)` together; a display patient ID does not establish a cross-dataset join. `get_participant()` and `get_coverage()` show source identifiers and linking coverage. Clinical subject IDs are scoped by `short_title`; an inventory display ID and clinical subject ID need a reviewed crosswalk before joining.

`list_clinical_subjects()`, `list_clinical_facts()`, and `list_clinical_conflicts()` retain source precedence, evidence scope, inheritance, inference, and conflicting values. A dataset diagnosis label does not prove that diagnosis for each patient. Inspect `primary_diagnosis_is_inferred` and fact `evidence_scope`/`is_inferred` before using a result as an observed participant fact. Clinical-only subjects appear only when requested with `include_clinical_only=True`. These routes are bounded; do not treat a short return as a complete cohort export.

`clinical_for_participants(participants)` accepts participant records or a DataFrame from `search_participants()`. It fetches clinical subjects in batches using exact Collection-scoped display IDs, rejects capped or inconsistent responses, and retains unmatched IDs. Its `subjects` list contains the full clinical records; `values` flattens resolved concepts into rows with `participant_id`, inference flags, evidence scope, and complete source evidence. Use `.to_dataframe("values")` for display/export. Filter this table using the selected dataset and participant IDs. A successful batch collection does not establish complete clinical coverage or validate a new identity crosswalk.

`list_participant_assets()` and `search_public_assets()` describe available public metadata, including non-DICOM packages and narrowly reviewed IDC-missing DICOM exceptions. `search_controlled_datasets()` and `list_controlled_files()` expose public inventory metadata about controlled files without granting retrieval rights. `search_annotation_downloads()` identifies DICOM annotation downloads at a bounded level; it does not prove a source-image relationship. Keep `asset_granularity`, `inventory_status`, access route, and unknown sizes visible. A missing series identifier or route URL stays unresolved; do not parse opaque asset IDs into DICOM UIDs.

## Select and hand off

`select(records, release_identity=..., query_scope=...)` is a local operation over explicit, provenance-bearing records. `export_selection(selection, destination, overwrite=False)` writes the explicit selection locally. It writes route-specific CSV manifests and a JSON accounting companion; it does not fetch a payload or use credentials. Every input record is exported, deduplicated, excluded by an explicit rule, or unresolved with a reason.

Data Retriever CSVs have exactly one recognized route column: `SeriesInstanceUID`, `imageUrl`, or `drs_uri`. Public and controlled selections, and different routes, go to separate files. A `source_url` is not automatically a deliverable `imageUrl`. Controlled `drs_uri` is public metadata; actual retrieval requires separate authorization. An Aspera package/path handoff has its own format only when exact package/path fields are present. Selection cannot invent `.tcia` files, signed URLs, missing DICOM UIDs, or source-image relationships. Include source images only after an explicit selection choice.

For basic public DICOM series selection from a TCIA cohort, use `plan_participant_images()` as described below. The shared service checks dataset-scoped identities and reuses versioned IDC metadata. Advanced DICOM relationships, acquisition queries and viewers remain available through IDC's own tools. Actual downloads delegate to the packaged TCIA Data Retriever.

## Download from Python

Install `tcia_utils[downloads]` for the packaged transfer engine. Supported public
DICOM, public files and approved controlled files delegate to
`tcia-data-retriever==0.1.0rc2`, including its pip-bundled headless executable.
No separate application installation is required. Core imports and metadata
queries do not launch it. Basic public participant image planning and manifest
downloads need no IDC Python package. Install IDC's tools independently for
advanced public DICOM query features.

Read the [TCIA data usage policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/)
and pass `accept_data_policy=True` for actual downloads. Dry runs need no policy
acceptance and never launch a transfer executable or authenticate controlled data.
All selected metadata/routes are checked before starting any transfer. Unsupported
Aspera groups stop the selection; use the official separate application workflow.

The SDK retains discovery, cohort filtering, exact selection scope, local/URL
manifest convenience, max_items sampling and reports. TDR owns payload transfer,
retries, checksums, authorization and output layout. ZIP archives can be extracted
automatically. The SDK no longer implements token exchange, DRS resolution or HTTP
file streaming. Existing files follow TDR copy/skip-existing behavior rather than
the former SDK-specific verifier. Normal copy can download existing files again.
Pass `resume=True` with the same destination to request skip-existing checks;
this is not a byte-range resume guarantee. Do not combine resume and overwrite.

Progress is on by default. The SDK asks TDR RC2 for `plain` progress: readable
newline snapshots, normally about every five seconds during active transfers,
plus phase messages and a final result. TDR forwards child-process output through
Python's stdout/stderr so Colab and Jupyter can display it while the call runs.
Active-file byte counts/percentages appear when available; a short transfer may
finish before a periodic update. `progress=False` silences SDK messages only;
TDR progress remains enabled and the SDK reports that limitation. No separate
progress renderer or subprocess launcher is maintained in this package.

After upgrading in Colab/Jupyter, restart a runtime that already imported the older
TDR wrapper, then rerun from the top. The current downloads extra requires
`tcia-data-retriever==0.1.0rc2`.

For controlled data, pass only `credential_file=...`, never key contents. TDR RC2
uses the supplied key for that CLI/Python invocation only; it does not save a copy
or fall back to a saved GUI key. The desktop GUI's credential behavior is separate.
See [TDR's progress and credential documentation](https://github.com/TCIA/data-retriever/blob/2dcad4bd6fe0bd62b42e67834d0ad42240694f42/docs/cli-progress-and-credentials.md).
The SDK passes a credential path only to controlled jobs, validates recognized DRS
routes, and excludes secrets and temporary URLs from reports. Public metadata and
manifests do not grant access approval. No live controlled transfer is claimed by
this review.

Reports retain each selected row and sampled scope, with a sanitized versioned
engine summary. Successful engine counts must cover every selected series/file;
partial or malformed success never completes the SDK selection. On a batch failure,
per-file successes cannot be inferred from the aggregate engine report: the SDK
conservatively marks the affected batch failed and later batches not attempted.
`engine_completed` records the transfer engine's completion; it is not a separate
SDK integrity audit. Metadata-only tests are not real transfer evidence.

## Download a manifest directly

```python
report = tcia.download_manifest("cohort.tcia", "images", dry_run=True)
# report = tcia.download_manifest("cohort.tcia", "images", accept_data_policy=True)
# report = tcia.download_manifest("https://example.org/cohort.csv", "images", accept_data_policy=True)
# report = tcia.download_manifest("controlled.csv", "images", credential_file="approved-key.json", accept_data_policy=True)
```

Pass a local path (including `Path`) or an HTTPS URL. Supported formats are
`.tcia`, `.csv`, and `.tsv`. A tabular manifest must have exactly one recognized
route column. The SDK validates the selection and prepares a normalized TDR
input manifest; the shared package handles each supported route. This does not
require a REST plan or upload the manifest/key to the metadata service.

A dry run reads the small manifest (10 MB cap) and checks local prerequisites;
it transfers no payload, performs no authentication, and does not initialize the
Python imaging index. Metadata syntax and routes are checked before launching
TDR. Actual availability and transfer verification belong to the engine; a failed
run may already have transferred some files. Aggregate failures never establish
complete acquisition, and the SDK does not invent per-file failure identifiers.
Controlled UID-only manifests are not sent through a public route;
use a supported approved DRS manifest. DRS manifests cannot be declared open.

Malformed/mixed route columns, unrecognized DRS hosts, non-HTTPS file routes,
unsafe names, and duplicate destinations fail before payload transfer. Original
CSV filenames, sizes, and supported checksum fields are retained. Existing files
follow the same verification/overwrite behavior as dataset downloads. Data files
use the destination you supply as the TDR output root. TDR creates its collection,
participant, study and series hierarchy beneath that root; the SDK adds no dataset,
manifest-hash, files or batch folders around it. A normalized transfer manifest is
saved under `destination/manifests/`; TDR maintains its own `metadata/` alongside
the data folders. Use
`save_manifests()` when you also need an unmodified copy of an official manifest. Reports retain a content
hash and item count, not keys, signed URLs, or a guessed dataset/participant identity.


When continuing a download made before a12, use the previous TDR output root as
`destination` with `resume=True`. For example, if the collection folder is inside
`old-root/manifest/<hash>/cmb_gec/`, pass `old-root/manifest/<hash>/` as the
destination. Existing files are not moved automatically. Sample and full downloads
now use the same root; omit `max_items` to expand the selection without adding
another SDK directory layer.

## Try a sample before a larger transfer

Use the same download functions with `max_items` to transfer only the first few
items. This is a real transfer check; `dry_run=True` remains metadata/setup only.

```python
# First whole series from a local or HTTPS manifest.
report = tcia.download_manifest("cohort.tcia", "sample", max_items=1, accept_data_policy=True)
display(report.to_dataframe())
print(report.scope)

# Or sample a particular published download group.
report = tcia.download_dataset(
    "CMB-GEC", "sample", download_ids=[imaging_download_id], max_items=1, accept_data_policy=True
)

# File records from a participant query can be sampled the same way.
report = tcia.download_files(file_records, "sample", max_items=2, accept_data_policy=True)
```

The limit applies **across the complete selection**, in input/manifest order,
not separately to every group. It counts whole DICOM series or whole files,
including whole archives where each manifest entry is an archive. It is not a
byte limit or a random/representative sample. Choose the desired download IDs
when a dataset has several groups; a small limit may select only the first group.
For a scan/segmentation example, select a verified candidate pair first and keep
both records rather than taking the first two unrelated files.

`report.scope["is_sample"]` marks the transfer. The scope includes the item limit,
selected/available/omitted counts, and every selected group's accounting (including
zero-selected groups). Entry tables show counts and units, while subset manifest
hashes identify the precise selection. For manifest downloads, the adapter saves
and uses a new `selection.csv`; it never edits the supplied original manifest.
The original manifest's hash remains in the scope for standalone manifest inputs.
The Python adapter uses that reduced manifest.

`report.complete` means the **sample selection** completed. It does not mean the
full dataset was downloaded or that all remaining files are authorized/available.
A successful transfer also does not prove image quality or image/label alignment.
Inspect the sample and report, then omit `max_items` to request the larger
selection, preferably in a separate destination. The larger transfer repeats its
normal checks. No automatic full download follows a successful sample.

External-application packages cannot be sampled by this SDK.
The call reports `unsupported_sample` and transfers no payloads, including any
other groups in that selection. It never falls back to downloading the full
package. Use a published manifest or an indexed file selection when available;
otherwise follow the official download link using a separate application outside
this SDK.
Controlled samples require the same approved credentials as any controlled
transfer. A sample dry run may fetch small manifests to calculate the selection,
but it does not authenticate, initialize the imaging index, or download payloads.
This runtime choice belongs in the Python SDK; it does not require an API/schema
change or send credentials/local files to the metadata service.


## Download images after clinical filtering

`plan_participant_images(cohort, modality="Whole Slide Image", file_format="non-DICOM")`
selects public non-DICOM files using TCIA's asset metadata for exact dataset-scoped
participants. It collects all cursor pages, checks identity namespaces, reports
coverage including zero matches, and retains unsupported routes as unresolved.
`download_images(plan, destination, max_items=1, accept_data_policy=True)` downloads
that selection through the same TDR adapter as other supported file downloads.
Unresolved routes block transfer; source links do not become invented download URLs.

Use the same `plan_participant_images(cohort, modality="CT", file_format="DICOM")`
for public DICOM series. The SDK sends TCIA participant keys to the shared service;
provider identity matching and queries run there. Both public DICOM and non-DICOM
plans use that service contract. `plan.providers` retains IDC metadata versions,
and `scope` retains the TCIA snapshot. The SDK batches at 100 participants, checks
coverage and rejects version drift or out-of-scope records before any transfer.

The service must expose `POST /v2/participants/download-plan` (query API 0.6.0+).
It returns explicit zero matches, unresolved identities/routes, and errors for
unavailable or truncated providers. `download_images()` blocks unresolved plans
and delegates the bounded selection to TDR. No images or credentials go through
the planning service. The current planner is public-only; use the existing
controlled-file inventory and approved-key download workflow for controlled data.

Advanced public DICOM relationships, acquisition exploration and viewers remain
with IDC's own tools. The SDK does not import or install IDC's Python package.

## Multi-value fields and display tables

Dataset search/detail/access and download metadata use lists of strings for
multi-value categorical fields. A singleton is a one-item list; a missing value
is an empty list. This includes cancer types/locations, categories, data/file
types, external resources, source collections, species, licenses and programs.
Commas within labels are preserved; scalar IDs, URLs, titles and prose stay strings.
The REST/MCP server is the canonical normalizer; the client adds no legacy-server
handling. The SDK's download_category alias and reports retain category lists too.

For notebook display, use `format_table(frame, columns=[...])`. It returns a
separate display table with missing cells labeled "Not reported", preserving list
cells, zero counts, booleans and provenance attributes. The raw frame and CSV/JSON
exports retain their original missing values. Formatting avoids pandas' implicit
object downcasting and is tested with pandas 2.2 and 3.1.

When upgrading from earlier transfer backends, use a fresh destination. TDR owns
its file layout and completion metadata; resume=True does not migrate an earlier
IDC or SDK HTTP output tree. Keep prior downloads separate until you have chosen
the layout for your workflow.
