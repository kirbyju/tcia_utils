# Moving from TCIA Utils 3.x to 4.0

The 4.0 interface is a new research client, not a compatibility release for every 3.x provider module. Pin a 3.x environment for existing scripts until you have moved each workflow deliberately. Version `4.0.0a1` is currently a source candidate; installation from the published package index may still resolve to 3.x.

| 3.x pattern | 4.0 approach |
| --- | --- |
| Import `wordpress`, `datacite`, `nbia`, `pathdb`, or `idc` from the package root | Construct `TCIAClient(base_url=...)` and use task methods for discovery and evidence |
| Query a catalog provider and infer that a DOI is a TCIA publication | Use visible `search_datasets()` / `get_dataset()` records as publication identity; use `get_citation()` for DataCite enrichment |
| Treat a short title as globally unique | Keep `(dataset_type, short_title)` for Collection versus Analysis Result; `get_dataset()` may return multiple `datasets` rows |
| Treat a dataset diagnosis or display patient ID as a patient-level fact | Inspect clinical evidence scope and use dataset-scoped participant identity and reviewed crosswalks |
| Download through package-specific helpers | Prepare an explicit selection manifest for TCIA Data Retriever; use `idc-index` directly for public DICOM detail and viewer selection |

The client needs an explicit TCIA V2 REST `base_url`. `status()` reports its observed fingerprint, profile, and capabilities. The package does not silently fall back to live WordPress, DataCite, IDC, or another host when a snapshot component is unavailable. There is no 4.0 MCP transport, native controlled download, or token helper.

Start a migration by calling `search_datasets()`, then inspect `get_dataset()` and `list_downloads()` before choosing records. Use `search_participants()` and clinical methods only where the service reports the corresponding capability. Keep each result's provenance and completeness when saving an analysis. A `Page` covers one requested page; use explicit cursor iteration for more. Bounded clinical and controlled routes report `unknown` completeness even if their returned count is below the limit.

Existing workshop notebooks under `TCIA_Notebooks` remain historical examples of older APIs. The five maintained 4.0 notebooks live under [`examples/notebooks`](examples/notebooks) in this repository and use the built 4.0 package.
