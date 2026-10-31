# TCIA Utils 4.0 user guide

## Install and connect

The unpublished 4.0.0a4.dev0 review build requires Python 3.11 or newer. Install the supplied local wheel in a fresh environment as shown in the [README](../../README.md). Install `tcia_utils[tables]` only if you want pandas conversion; the core client imports no pandas, notebooks, or imaging libraries. The [notebook environment](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/requirements.txt) pins the separately tested example tools.

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

The download scope is **Python and pip-installed dependencies**. Public DICOM
reuses `idc-index`, including its pip-supplied `s5cmd` executable. Public HTTPS
files and approved controlled files use the SDK's `httpx` adapter. Data Retriever,
Aspera CLI, and applications needing a separate installation are not invoked.

```python
plan = tcia.plan_downloads("CMB-GEC")
display(plan.to_dataframe())
# Choose the exact Python-supported group(s) you want from this inventory.
check = tcia.download_dataset("CMB-GEC", "images", download_ids=imaging_ids, dry_run=True)
# report = tcia.download_dataset("CMB-GEC", "images", download_ids=imaging_ids, max_items=1)
# report = tcia.download_files(selected_files, "images", credential_file="approved-key.json")
```

`plan_downloads()` consumes the shared REST/MCP download plan. Software labels,
requirements URLs, category/count fields, and official links describe **all**
published groups. `python_supported` and `python_support_message` describe this
SDK's execution scope, independent of the published instructions. A requirement
label mentioning another application does not prevent supported manifests from
being processed directly in Python. Unknown or conflicting routes remain
unsupported. No download group is silently discarded. Use explicit `download_ids`
or `access_levels` to select a subset; ambiguous short titles need `dataset_type`.

A group requiring an external application reports `requires_external_tool`.
Actual execution stops the entire selection before any payload transfer, even if
that application happens to be installed. Follow the official link or separate
application notebook. Such groups remain in discovery metadata and selection
accounting; `export_selection()` does not generate application-specific Aspera
handoffs. Ordinary UID/URL/DRS CSV manifests remain supported Python inputs.
The SDK no longer exposes `data_retriever_command`, `backend`, `retriever`, or
`package_client` parameters. Earlier release notes describe earlier versions.

Preparation messages start before network metadata requests or index loading.
The first public DICOM transfer initializes one IDC client; later transfers through
the same TCIAClient reuse it. `close()` releases that reference. A destination
containing only saved manifests does not trigger a sync scan. If existing data
hierarchy entries are present, IDC synchronization is enabled for resumable
transfers. Pass `progress=False` to silence SDK status output and IDC's transfer
bar. No dependency installation is performed by download calls.

A dry run checks metadata/local prerequisites without authentication or payloads;
sample dry runs may read small manifests. It never initializes the IDC index.
Actual downloads validate all selected manifests/index coverage before transferring
payloads. Missing dependencies, authorization, unsupported routes, or unavailable
series stop the selection.

Controlled downloads currently use a narrow `httpx` integration with a recognized
CRDC/Gen3 host, not the `gen3` package or a general DRS client. Your JSON key-file
path stays local. The adapter exchanges its key for an access token, resolves
selected DRS identifiers to temporary HTTPS URLs, then streams through a separate
client without the authorization token. Keys, tokens and signed URLs stay out of
reports and metadata services. This does not grant or expand access approval.

`DownloadReport.complete` describes the selected scope, including a sample when
requested. `engine_completed` records IDC's completion without independent byte
verification. HTTP files check response length and supplied size/checksum metadata,
write atomically, and preserve existing complete files on failure. HTTP downloads
restart interrupted files; they do not currently resume partial files. Existing
files are skipped only when available size/checksum checks match. Failures preserve
per-group accounting, and later groups are marked not attempted. Metadata-only
notebook validation does not prove a real authorized transfer.

## Download a manifest directly

```python
report = tcia.download_manifest("cohort.tcia", "images", dry_run=True)
# report = tcia.download_manifest("cohort.tcia", "images")
# report = tcia.download_manifest("https://example.org/cohort.csv", "images")
# report = tcia.download_manifest("controlled.csv", "images", credential_file="approved-key.json")
```

Pass a local path (including `Path`) or an HTTPS URL. Supported formats are
`.tcia`, `.csv`, and `.tsv`. A tabular manifest must have exactly one recognized
route column. UID rows use the existing public-DICOM adapter, file URLs use the
HTTP adapter, and DRS rows use the approved controlled-file adapter. The method
does not require a REST plan and does not upload the manifest or key to the service.

A dry run reads the small manifest (10 MB cap) and checks local prerequisites;
it transfers no payload, performs no authentication, and does not initialize the
imaging index. Actual UID transfers check **every** selected series against the
index before transferring any. Missing UIDs stop the selection and appear in the
failure report. Controlled UID-only manifests are not sent through a public route;
use a supported approved DRS manifest. DRS manifests cannot be declared open.

Malformed/mixed route columns, unrecognized DRS hosts, non-HTTPS file routes,
unsafe names, and duplicate destinations fail before payload transfer. Original
CSV filenames, sizes, and supported checksum fields are retained. Existing files
follow the same verification/overwrite behavior as dataset downloads. Data files
are grouped under `destination/manifest/<manifest-hash-prefix>/`; the original
small manifest is saved in its `manifests/` subdirectory. Reports retain a content
hash and item count, not keys, signed URLs, or a guessed dataset/participant identity.


## Try a sample before a larger transfer

Use the same download functions with `max_items` to transfer only the first few
items. This is a real transfer check; `dry_run=True` remains metadata/setup only.

```python
# First whole series from a local or HTTPS manifest.
report = tcia.download_manifest("cohort.tcia", "sample", max_items=1)
display(report.to_dataframe())
print(report.scope)

# Or sample a particular published download group.
report = tcia.download_dataset(
    "CMB-GEC", "sample", download_ids=[imaging_download_id], max_items=1
)

# File records from a participant query can be sampled the same way.
report = tcia.download_files(file_records, "sample", max_items=2)
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

`plan_participant_images(cohort, modality="CT", file_format="DICOM")` inventories
public series for the exact participant query records supplied. It anchors them
to each selected dataset's published public manifest before matching IDC PatientID
and modality; it does not derive IDC collection names or join IDs across datasets.
The plan records manifest hashes and index version. For non-DICOM whole slides,
use modality="Whole Slide Image", file_format="non-DICOM". That selection follows
all public-asset pages and accepts explicit API file-download descriptors, not
arbitrary source URLs or archive-internal paths. It requires the matching API
update for recognized public slide routes.

Use `plan.to_dataframe()` for matching images and `plan.coverage_to_dataframe()`
for each requested participant, including zero-match subjects. `plan.unresolved`
retains missing index coverage, unverified identity namespaces, and missing or
controlled file routes. Those issues block `download_images()` rather than
silently reducing the image selection. All selection methods retain dataset-scoped
identity and distinguish public slide copies from submitted originals.

`download_images(plan, destination, max_items=1)` delegates to the same Python
manifest/file adapters used elsewhere. The limit is global and counts whole series
or slide files; one CT series includes all its DICOM instances. Omit max_items to
retrieve the complete planned public selection. This does not include controlled
images, prove image quality or completeness outside indexed coverage, or change
clinical inclusion criteria. Rebuild the plan after changing those criteria.


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
