# TCIA Utils v4 service compatibility

T03 validates the accepted [phase-1 contract](api-contract.md) against the shared TCIA REST/MCP service. The local service repair is commit `99b7dbd` in `tcia-query-skill`, with skill hash manifest version `2026.10.14.2`. It is a source change only: it has not been published, installed, or deployed. The TCIA Utils client must keep source validation separate from the state of its configured host.

## Required service behavior

Phase 1 uses the existing V2 routes in `endpoint-matrix.json`; T03 adds no route. It requires the configured host to return typed, nonempty results from `/v2/participants/{participant_key}/assets`, `/v2/public-non-dicom/assets`, `/v2/clinical/{short_title}/subjects`, and `/v2/clinical/{short_title}/facts` when matching records exist. In particular, the response models must accept the producer inventory's geometry, provenance, size, granularity, and clinical quality fields. The local patch preserves these fields in both REST JSON and MCP structured output. The service still marks the public DICOM detail route as `IDC/idc-index`.

The client should inspect `/v2/ready` and `/v2/bundle` for release fingerprint, installed profile, and capabilities, then handle each data route's actual response. Readiness and OpenAPI route presence alone do not prove a route works. An older host that returns HTTP 500 on an asset or clinical route is unavailable for that operation; the client must surface the error and must not turn it into an empty result. The published metadata release fingerprint identifies installed data, not the running service code. Before claiming host compatibility, verify bounded nonempty calls for the four repaired routes on that host.

## Observed hosted state

Bounded public metadata probes on 2026-10-14 observed the hosted V2 API at version `0.4.0`, installed `research_detail` profile, and metadata release fingerprint `a74b80068740d823657379be97e704e6db5229424a731de47397aa534213ff29`. Dataset, citation, publication, participant search/detail, coverage, controlled-dataset, controlled-file, annotation-download, and an empty clinical-conflict probe returned HTTP 200. The four nonempty asset/clinical routes above returned HTTP 500. These probes are route samples, not an exhaustive validation of all filters or records. The bounded [route observations](hosted-route-observation.json) preserve request paths and statuses; the assembled [client smoke report](client-live-smoke.json) records method-level results. Hosted behavior remains unverified after the source repair until the service is deployed and probed again.

## Deliberate phase-1 limits

Participant `Asset` rows summarize holdings; they do not contain public `SeriesInstanceUID` values or retrieval locations. Public non-DICOM asset rows expose location counts but no location detail route. The client must not derive identifiers or `imageUrl` values from opaque asset IDs, `source_url`, or proximity. Exports use exact route evidence already present on an input record, including a controlled-file `drs_uri` when supplied, or externally verified public series IDs from direct `idc-index` use. Other selections remain explicit unresolved records. Controlled metadata is public inventory, not retrieval authorization.

Clinical, controlled-file, controlled-dataset, and annotation-download routes have bounded results without cursors or total counts. Their completeness remains unknown even when a returned count is below the requested limit. Dataset detail's related analyses are bounded suggestions. Cursor routes retain page count, `has_more`, `truncated`, and `next_cursor`; iteration must keep the same observed release identity. No phase-1 method may claim exhaustive results from a bounded route.

## Local validation

The service patch uses schema-faithful nonempty fixtures, directly checks the producer SQLite view columns against strict public models, and asserts the affected REST and MCP outputs retain representative fields. The pinned server environment reported MCP `2.0.0`, FastAPI `0.141.1`, Uvicorn `0.52.4`, and Pydantic `2.13.5`. `python -m unittest discover -s mcp_server/tests -p 'test_*.py'` passed 49 tests; `python scripts/tcia_skill_version.py check` passed for `2026.10.14.2`; `git diff --check` passed. These are local checks only. A `uv pip sync --offline --dry-run` inspection was unavailable because `uv` failed inside this macOS sandbox; the existing server virtual environment was used for tests.
