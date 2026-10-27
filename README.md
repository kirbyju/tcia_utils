# TCIA Utils 4.0

TCIA Utils is a Python client for discovering TCIA datasets, participants, clinical evidence, available data, citations, and verified analytical publications. It uses the shared TCIA V2 REST service. A researcher can start with a question about a dataset without choosing the underlying catalog or storage system.

Version `4.0.0a1` is the first **opt-in alpha preview** of the new API. Ordinary pip installs and upgrades continue selecting the stable 3.x release. Use a fresh Python environment to try the preview:

```sh
python -m pip install "tcia_utils[tables]==4.0.0a1"
```

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb)

The [guided workshop](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb) installs the exact preview and queries real public metadata. Report feedback through [GitHub issues](https://github.com/kirbyju/tcia_utils/issues), including the failing operation, package/Python versions, and error message. Do not include credentials. Existing 3.x scripts should stay in a separate environment; see [migration guidance](MIGRATION.md).

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

Use [the task-oriented guide](docs/v4/user-guide.md) for methods, evidence scope, selection exports, and tool handoffs. The [migration guide](MIGRATION.md) explains the 3.x API change; [troubleshooting](docs/v4/troubleshooting.md) covers setup and response errors. The canonical [guided workshop](https://github.com/kirbyju/TCIA_Notebooks/blob/main/tcia_utils_v4/TCIA_Dataset_and_Participant_Queries.ipynb) teaches dataset discovery/download planning, participant cohorts, brain MR segmentations, and controlled CT/SEG access in one conversational lesson. It uses real public metadata by default; imaging downloads are opt-in. Its current limits are explicit: the CMB-GEC tissue-slide package still requires its collection download link, and indexed NIfTI files do not yet have a per-file API download route.

This alpha is for community testing. The [service compatibility review](docs/v4/design/service-compatibility.md) and [notebook validation](docs/v4/design/notebook-validation.md) preserve dated verification and earlier failure history. The four original hosted-route failures were repaired and subsequently passed live checks. API availability, indexed coverage, and the active metadata snapshot can change; retain response provenance with research exports. Clinical summaries are derived metadata, and some routes are bounded rather than exhaustive.

Visible TCIA Collection and Analysis Result records establish dataset publication. DataCite metadata and publication metrics enrich those records but do not establish publication by themselves. Public metadata about controlled data does not grant payload access. For public DICOM series detail and viewer URLs, use `idc-index` directly. Selection export prepares route-specific manifests for TCIA Data Retriever; it never transfers imaging payloads.

TCIA data use remains subject to the [TCIA Data Usage Policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/).
