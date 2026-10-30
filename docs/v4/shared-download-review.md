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
