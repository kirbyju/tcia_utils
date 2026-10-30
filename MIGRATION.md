# Moving from TCIA Utils 3.x to 4.0

The 4.0 interface is a new research client, not a compatibility release for every 3.x provider module. Pin a 3.x environment for existing scripts until you have moved each workflow deliberately. Version `4.0.0a3` is an opt-in alpha. Install it explicitly with `python -m pip install "tcia_utils==4.0.0a3"`; ordinary pip installs continue selecting the stable release.

| 3.x pattern | 4.0 approach |
| --- | --- |
| Import `wordpress`, `datacite`, `nbia`, `pathdb`, or `idc` from the package root | Construct `TCIAClient(base_url=...)` and use task methods for discovery and evidence |
| Query a catalog provider and infer that a DOI is a TCIA publication | Use visible `search_datasets()` / `get_dataset()` records as publication identity; use `get_citation()` for DataCite enrichment |
| Treat a short title as globally unique | Keep `(dataset_type, short_title)` for Collection versus Analysis Result; `get_dataset()` may return multiple `datasets` rows |
| Treat a dataset diagnosis or display patient ID as a patient-level fact | Inspect clinical evidence scope and use dataset-scoped participant identity and reviewed crosswalks |
| Download through package-specific helpers | Use `download_dataset()` / `download_files()` for explicit transfers, or export a selection for Python manifest input; public DICOM delegates to `idc-index` |

The client needs an explicit TCIA V2 REST `base_url`. `status()` reports its observed fingerprint, profile, and capabilities. The package does not silently fall back to live WordPress, DataCite, IDC, or another host when a snapshot component is unavailable. The client uses REST for metadata; there is no MCP transport. Explicit download methods resolve approved controlled files through the supported API without requiring readers to handle tokens.

Start a migration by calling `search_datasets()`, then inspect `get_dataset()` and `list_downloads()` before choosing records. Use `search_participants()` and clinical methods only where the service reports the corresponding capability. Keep each result's provenance and completeness when saving an analysis. A `Page` covers one requested page; use explicit cursor iteration for more. Bounded clinical and controlled routes report `unknown` completeness even if their returned count is below the limit.

Existing workshop notebooks under `TCIA_Notebooks` remain historical examples of older APIs. The single guided 4.0 workshop lives under [TCIA_Notebooks/tcia_utils_v4](https://github.com/kirbyju/TCIA_Notebooks/tree/main/tcia_utils_v4) and use the installed 4.0 package.

The local 4.0.0a4.dev0 review build restores direct manifest path/URL convenience
through `download_manifest()`. Its dataset download plan now requires the shared
query-service endpoint; update/deploy that service before adopting the new
client's `plan_downloads()` in production. Collection search supplies categories
directly. The review wheel and service source have not been published or deployed.

The current v4 review removes external application launchers and Aspera handoff
exports. `backend`, `retriever`, `package_client`, and `data_retriever_command` are
removed. Published software requirements remain discovery metadata; inspect
`python_supported` for SDK execution support. SDK downloads require only Python
and pip-installed dependencies.
