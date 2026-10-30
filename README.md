# TCIA Utils 4.0

TCIA Utils is a Python client for discovering TCIA datasets, participants, clinical evidence, available data, citations, and verified analytical publications. It uses the shared TCIA V2 REST service. A researcher can start with a question about a dataset without choosing the underlying catalog or storage system.

Version **4.0.0a4** is an opt-in preview for testing the redesigned interface.
Ordinary pip installations continue selecting stable 3.x. Install this version
explicitly in a fresh Python 3.11+ environment:

```sh
python -m pip install "tcia_utils[tables,downloads]==4.0.0a4"
```

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb)

The [guided workshop](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb) pins this preview and queries real public metadata. Its default run keeps image transfers off; optional samples use max_items. Report feedback through [GitHub issues](https://github.com/kirbyju/tcia_utils/issues), including the failing operation, package/Python versions, and error message. Do not include credentials. Existing 3.x scripts should stay in a separate environment; see [migration guidance](MIGRATION.md).

The client requires Python 3.11 or newer and `httpx`. Notebook dependencies are in [the tutorial requirements](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/requirements.txt). The built wheel is the package used for notebook verification; an editable checkout is not required.

Provide the URL of a TCIA V2 REST host that has a ready installed bundle. The client does not choose a host or make a request during import or construction.

```python
from tcia_utils import TCIAClient

with TCIAClient(base_url="https://tcia.duckdns.org") as tcia:
    service = tcia.status()
    page = tcia.search_datasets(query="lung", limit=10)
    for dataset in page.items:
        print(dataset["dataset_type"], dataset["short_title"], dataset.get("doi"))
    print(page.completeness.state, service.provenance.release_fingerprint)
```

Searches return one `Page` at a time. Call `page.next_page()` or iterate `page.iter_pages()` when more results are needed. Detail calls return a `Result` containing the service's named fields and notes. Both expose observed release provenance and completeness. `partial` means more cursor pages are available; `unknown` means the route has no exhaustive cursor or contains a bounded sublist. An empty successful page is a real zero-match result. Failures raise typed `TCIAError` subclasses: for example, `ServiceUnavailableError` for a missing capability or HTTP 503, `HTTPResponseError` for HTTP 500, and `TransportError` for a connection failure.

Collection search now returns `data_categories` directly from the shared service, without per-dataset enrichment calls. `plan_downloads()` consumes the shared REST/MCP download plan, including WordPress-derived software requirements and one software-help catalog. Runtime prerequisite checks remain in the Python client.

To plan and download a dataset from Python:

```python
with TCIAClient(base_url="https://tcia.duckdns.org", pin_snapshot=True) as tcia:
    plan = tcia.plan_downloads("CMB-GEC")
    print(plan.to_dataframe()[["download_id", "download_category", "access_level"]])
    readiness = tcia.download_dataset("CMB-GEC", "downloads", dry_run=True)
    print(readiness.to_dataframe())  # Metadata and local prerequisite checks only.
    # Download public groups when ready (can be large):
    # report = tcia.download_dataset("CMB-GEC", "downloads", access_levels=["open"])
```

The download scope is Python and pip-installed dependencies. The `downloads` extra supplies `idc-index` and its pip-supplied transfer executable for public DICOM. Public HTTPS files and approved controlled manifests use Python HTTP streaming. The SDK does not invoke Data Retriever or Aspera CLI. Download plans keep external-only groups visible with their requirements and official links; `python_supported` identifies supported execution routes. See the [download guide](docs/v4/user-guide.md#download-from-python).

Use [the task-oriented guide](docs/v4/user-guide.md) for methods, evidence scope, selection exports, and tool handoffs. The [migration guide](MIGRATION.md) explains the 3.x API change; [troubleshooting](docs/v4/troubleshooting.md) covers setup and response errors. The canonical [guided workshop](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb) teaches dataset discovery/download planning, participant cohorts, brain MR segmentations, and controlled CT/SEG access in one conversational lesson. It uses real public metadata by default; imaging downloads are opt-in. It shows download categories and uses package helpers for page collection, clinical enrichment, manifest saving, and direct Python downloads. Original slide packages need configured transfer software; indexed NIfTI paths still require their published package rather than an invented per-file URL.

This alpha is for community testing. The [service compatibility review](docs/v4/design/service-compatibility.md) and [notebook validation](docs/v4/design/notebook-validation.md) preserve dated verification and earlier failure history. The four original hosted-route failures were repaired and subsequently passed live checks. API availability, indexed coverage, and the active metadata snapshot can change; retain response provenance with research exports. Clinical summaries are derived metadata, and some routes are bounded rather than exhaustive.

Visible TCIA Collection and Analysis Result records establish dataset publication. DataCite metadata and publication metrics enrich those records but do not establish publication by themselves. Public metadata about controlled data does not grant payload access. For public DICOM series detail and viewer URLs, use `idc-index` directly. Selection export prepares route-specific manifests without transferring payloads. Explicit `download_dataset()` and `download_files()` calls perform transfers; all notebook image-transfer flags start off.

TCIA data use remains subject to the [TCIA Data Usage Policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/).

## Manifest paths and URLs

```python
with TCIAClient(base_url="https://tcia.duckdns.org") as tcia:
    report = tcia.download_manifest("cohort.tcia", "downloads", dry_run=True)
    # When ready, omit dry_run to transfer the entire supported selection.
    # report = tcia.download_manifest("https://example.org/cohort.csv", "downloads")
```

Supported inputs are `.tcia` and CSV/TSV with exactly one route column:
`SeriesInstanceUID`, `imageUrl`, or `drs_uri`. Keys are required for controlled DRS
transfers. Public UID manifests are checked against the maintained imaging index
before any payload starts; missing series are reported rather than silently
skipped. The client handles parsing and transfer adapters internally. The REST
service does not receive local manifests or credentials.

The new shared plan requires the unpublished query-service endpoint
`/v2/datasets/{short_title}/download-plan`. An older service produces a clear
upgrade message; the client does not silently copy the server's routing logic.
Standalone `download_manifest()` does not need that endpoint. See
[the local review handoff](docs/v4/shared-download-review.md).


To test a transfer or keep a demo small, pass `max_items=1` to
`download_manifest()`, `download_dataset()`, or `download_files()`. The report
marks a sample and counts what was selected and omitted. The limit counts whole
DICOM series/files, not bytes; original packages without file-level selection
cannot be sampled. See the sample-download section of the v4 user guide.

Download preparation is visible immediately. One IDC client is reused per TCIAClient,
and metadata-only destination folders do not trigger synchronization scans.
Use `progress=False` for quiet SDK output and no IDC transfer bar.
