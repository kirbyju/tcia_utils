# Local shared-download review

The local 4.0.0a4.dev0 review is being prepared as the explicitly authorized
4.0.0a4 preview release. Publishing the package does not deploy the API; the
operator must update the hosted service separately before Colab testing.
Croissant remains deferred under WORK-0047.

The query-skill branch owns compact software codes/catalogs, requirements URLs,
collection search categories, and the typed shared download plan. The client
consumes that plan and owns local files, runtime checks, credentials, transfers,
and direct manifest inputs. Notebook code uses these interfaces directly.

Validation uses the installed wheel, real REST/MCP contract adapters with fixture
SQLite data, synthetic transfers, and the public workshop recording projected
through local service code. Recorded/projection results do not establish a deployed
API or current research result. No credentialed or imaging-payload transfer is run.

When notebook review is complete: review/publish the query-skill source and normal
schema-9 metadata release, install the bundle and update/restart the service, verify
its fingerprint and bounded real queries, and then prepare the exact SDK/notebook
release pins and normal Colab installation cell. Current service code can interpret
schema-8 requirements until its next normal bundle rebuild. The new endpoint must
exist before the new SDK's shared `plan_downloads()` can work. Standalone
`download_manifest()` works without that endpoint. Publishing remains a separate
explicit step after the user's review.


The review now also supports `max_items` on all three Python download entrypoints.
Sample selection is local execution behavior and needs no additional metadata
API contract. It creates a reduced manifest before invoking existing adapters;
reports distinguish sampled scope from full-dataset completion. Package-only
routes refuse sampling instead of performing an unbounded package transfer.
The notebook demonstrates one-series/file transfers and a two-file CT/SEG pair.

The current review scopes execution to Python and pip dependencies. External
application launchers and application-specific Aspera handoffs are removed. The
metadata inventory still includes external groups and official requirements.
IDC initialization is cached per client, progress appears before setup, and a
manifest-only folder does not trigger synchronization. Controlled downloads remain
the narrow httpx authorization/streaming adapter; TDR packaging is a separate
conversation and no new packaging work is included here.

The follow-up 4.0.0a5 preview coordinates list-valued API attributes with a
warning-free display helper. REST/MCP 0.5.1 owns normalization; the SDK does not
handle older response formats. The existing metadata bundle is reused.

The 4.0.0a6 preview removes automatic public DICOM synchronization after a sample.
Normal downloads follow IDC copy behavior; resume=True explicitly requests the
existing-file scan, with a separate preparation message. This applies to manifest,
dataset and participant-image downloads. HTTP/controlled file semantics are
unchanged. No REST/MCP or artifact update is required.

The 4.0.0a7 preview delegates supported payload transfers to the published
tcia-data-retriever 0.1.0rc1 wheel. Custom token exchange and HTTP streaming have
been removed. The downloads extra is TDR; the cohorts extra retains IDC metadata.
Policy acceptance is explicit for actual downloads. Current TDR console and
credential-cache behavior are documented. Local same-object CT/MR tests cover
fresh and prepared-client cases with byte-level verification; a separate Colab
notebook is provided. No live controlled-access transfer is claimed.


## 4.0.0a8: independent IDC features

The SDK no longer installs or wraps idc-index and has no cohorts extra. The
notebook's optional IDC section directly queries its separately installed client;
TCIA non-DICOM planning and TDR-backed manifest/file downloads remain independent.
The older implementation notes above describe their dated review builds.


## 4.0.0a9: shared public image planning

Basic participant image planning now uses the TCIA service for both public DICOM
and non-DICOM. The service validates TCIA identities and reuses versioned IDC REST
metadata for DICOM. The SDK has no IDC dependency. The workshop uses the same plan
and download functions for CT series and whole-slide files. API 0.6.0 adds the
metadata-only route and read-only MCP tool; TDR still transfers files locally.
Recorded notebook validation uses projected plans from verified retained source
records and identities; deployment is checked separately. No payload transfers
were performed for this implementation.
