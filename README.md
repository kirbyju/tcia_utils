# TCIA Utils 4.0

TCIA Utils is a Python client for discovering TCIA datasets, participants, clinical evidence, available data, citations, and verified analytical publications. It uses the shared TCIA V2 REST service. A researcher can start with a question about a dataset without choosing the underlying catalog or storage system.

Version `4.0.0a1` is a source candidate. The published `tcia_utils` package may still be 3.x. Build and install this candidate in a separate environment before running the examples:

```sh
python -m pip install build
python -m build
python -m pip install dist/tcia_utils-4.0.0a1-py3-none-any.whl
```

The client requires Python 3.11 or newer and `httpx`. Notebook dependencies are in [examples/requirements.txt](examples/requirements.txt). The built wheel is the package used for notebook verification; an editable checkout is not required.

Provide the URL of a TCIA V2 REST host that has a ready installed bundle. The client does not choose a host or make a request during import or construction.

```python
from tcia_utils import TCIAClient

with TCIAClient(base_url="https://<TCIA-REST-host>") as tcia:
    service = tcia.status()
    page = tcia.search_datasets(query="lung", limit=10)
    for dataset in page.items:
        print(dataset["dataset_type"], dataset["short_title"], dataset.get("doi"))
    print(page.completeness.state, service.provenance.release_fingerprint)
```

Searches return one `Page` at a time. Call `page.next_page()` or iterate `page.iter_pages()` when more results are needed. Detail calls return a `Result` containing the service's named fields and notes. Both expose observed release provenance and completeness. `partial` means more cursor pages are available; `unknown` means the route has no exhaustive cursor or contains a bounded sublist. An empty successful page is a real zero-match result. Failures raise typed `TCIAError` subclasses: for example, `ServiceUnavailableError` for a missing capability or HTTP 503, `HTTPResponseError` for HTTP 500, and `TransportError` for a connection failure.

Use [the task-oriented guide](docs/v4/user-guide.md) for methods, evidence scope, selection exports, and tool handoffs. The [migration guide](MIGRATION.md) explains the 3.x API change; [troubleshooting](docs/v4/troubleshooting.md) covers setup and response errors. The five canonical [notebooks](examples/notebooks) teach discovery, public DICOM handoff to `idc-index`, non-DICOM and annotations, controlled public metadata, and participant/clinical evidence. Their default executed cells inspect metadata only.

This source candidate is not yet release-ready. The [service compatibility review](docs/v4/design/service-compatibility.md) records required deployed capabilities. In a bounded live check on October 14, 2026, the observed host returned HTTP 500 for participant assets, public non-DICOM assets, clinical subjects, and clinical facts; see the [client smoke report](docs/v4/design/client-live-smoke.json) and [notebook validation](docs/v4/design/notebook-validation.md). Fixture notebook success does not establish hosted support for those routes.

Visible TCIA Collection and Analysis Result records establish dataset publication. DataCite metadata and publication metrics enrich those records but do not establish publication by themselves. Public metadata about controlled data does not grant payload access. For public DICOM series detail and viewer URLs, use `idc-index` directly. Selection export prepares route-specific manifests for TCIA Data Retriever; it never transfers imaging payloads.

TCIA data use remains subject to the [TCIA Data Usage Policy](https://www.cancerimagingarchive.net/data-usage-policies-and-restrictions/).
