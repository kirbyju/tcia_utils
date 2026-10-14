# TCIA Utils 4.0 phase-1 API contract

Status: accepted by the coordinator under T02. This is a source and hosted-route contract, not evidence that a 4.0 package exists or has been published.

## Evidence and boundaries

The contract was checked against `tcia-query-skill/mcp_server/tcia_query_mcp/{rest,models,service}.py` at source revision `2ebb7a6d4adecb378a57d4dd2753e5444f7c55bf`, the installed skill's corresponding models, and hosted `/v2/openapi.json` (API version `0.4.0`, 27 paths) plus `/v2/ready` observed 2026-10-14. Hosted readiness reported `research_detail` and release fingerprint `a74b80068740d823657379be97e704e6db5229424a731de47397aa534213ff29`. These observations establish advertised routes and installed capabilities at that moment, not successful behavior of each route. A bounded live probe of /v2/participants/{participant_key}/assets?limit=2 returned HTTP 500; T03 must diagnose it before that feature is advertised as working. A client must inspect its own configured host at runtime. The source checkout and the deployed service are separate states. See `endpoint-matrix.json` for every phase-1 mapping.

Only visible WordPress Collection and Analysis Result records establish TCIA publication identity. DataCite and other providers enrich those records. A `short_title` alone is not a globally unique dataset key: keep `(dataset_type, short_title)` in rows, selections, and exports. The existing dataset detail route may return more than one `datasets` row for a short title. Keep the array and its `caveats`; do not silently choose one. Related analyses are the service's bounded, heuristic `related_analysis_results` suggestions (currently at most 25), not a complete relationship graph.

## Construction and shared types

```python
from tcia_utils import TCIAClient

with TCIAClient(base_url="https://<TCIA REST host>", timeout=15.0) as tcia:
    page = tcia.search_datasets(query="lung")
```

`base_url` is explicit and must target a TCIA V2 REST host; no import-time request or implicit fallback. The client is synchronous, reusable, and closeable. T04 supplies one small HTTP transport (prefer `httpx`), standard-library data containers and JSON, and typed errors. The supported floor is Python `>=3.11`; core dependencies are only the HTTP client and its transitive requirements. `pandas` belongs in an optional `tables` extra, with conversion via `Result.to_dataframe()` importing it lazily. Notebook tools (including `idc-index`) belong in a separate optional `examples` extra or documented notebook environment, never core import. Avoid a mandatory Pydantic runtime or a second MCP transport.

`Page[T]` retains `items`, server `count` (number returned, not total), `limit`, `has_more`, `truncated`, `next_cursor`, `query`, and `provenance`. It exposes explicit `next_page()` / `iter_pages()` using the original filters and limit. No automatic all-pages call is part of a simple search. `Result[T]` retains the response's named data, notes, `provenance`, and `completeness`. Keep service field names inside records so scope and source evidence are not flattened away. `provenance` contains observed `base_url`, `release_fingerprint`, `release_tag`, installed profile, capability flags, and observation time from `/v2/ready` or `/v2/bundle`; it is **not** a per-row server stamp. Before and after a multi-request query iteration, compare the observed fingerprint and profile. Reject changed identity, invalidated cursors, repeated cursors, and responses inconsistent with the page contract; never restart a query on a new release. The server cursor also binds filters and component generation. A single unguarded call carries only observed service identity and must be described as such. Offline selection and export use captured provenance without network access; optional explicit revalidation may compare current service identity before a new fetch or export.

`completeness` has `complete`, `partial`, `unknown`, or `unavailable`, plus a reason. Cursor pages with `has_more=True` are partial; a terminal page can complete the same unchanged iteration. `coverage_complete` describes participant linking only, not completeness of clinical or file detail. Routes with only `count` and `limit` and no cursor have `unknown` completeness even when `count < limit`; no exhaustive claim follows from that comparison. An empty successful page means zero matches in its requested scope. A missing optional component or HTTP 503 is unavailable and raises `ServiceUnavailableError` (with response code and retryable flag); it is not an empty page. Other errors are `InvalidRequestError` (422), `NotFoundError` (404), `RetiredRouteError` (410), `TransportError` (network/timeouts), and `ProtocolError` (malformed/inconsistent response). Preserve the server's RFC 9457 `code`, `detail`, `status`, `retryable` when present. Retry only idempotent reads after transport errors or retryable 5xx, with a small bounded policy; do not retry 4xx. Unknown additive fields may be retained in raw records, but missing required envelope fields or incorrect types are protocol errors.

Capability checks use `/v2/ready` and `/v2/bundle`: `participant_search`, `public_non_dicom_detail`, `controlled_access_detail`, `clinical_detail`, `datacite_metadata`, and `verified_analytical_publications`. `research_core` and `research_detail` are installed profiles, not API versions. Never infer a capability from a green liveness response. Status and detail methods make no imaging payload request.

`ServiceStatus` combines the observed readiness identity and bundle capabilities without copying a whole bundle manifest into every result. `ExportReport` identifies files written and counts of exported, deduplicated, excluded, and unresolved records by reason. The eight files under `tests/fixtures/v4/` are synthetic model-valid examples for empty, partial, unavailable, mixed-access, inferred-clinical, and bounded-detail behavior; they are not live data or proof of deployed route success.

## Flat public methods

All filter arguments below are keyword-only. List filters accept sequences of strings and are passed to the route with the same meaning. Public names describe research tasks; provider names remain in record provenance. Path identifiers are encoded as one segment except the DOI route, whose `{doi:path}` must preserve its slash while safely encoding other characters. Methods return `Page` for cursor routes, `Result` for detail/bounded routes. The HTTP query operation, response schema, capability, and owner are pinned in the matrix.

```python
status() -> Result[ServiceStatus]
search_datasets(*, query=None, short_titles=None, dataset_type="both",
    access_levels=None, modalities=None, data_types=None, download_types=None,
    file_types=None, cancer_types=None, cancer_locations=None, species=None,
    programs=None, external_resources=None, has_external_clinical_resource=None,
    doi=None, limit=25, cursor=None) -> Page[DatasetSummary]
get_dataset(short_title) -> Result[DatasetDetailResponse]
get_access(short_title) -> Result[AccessSummaryResponse]
list_downloads(short_title, *, access_levels=None, modalities=None,
    data_types=None, download_types=None, file_types=None,
    requires_annotations=False, limit=25, cursor=None) -> Page[Download]
search_citations(*, query=None, dois=None, short_titles=None,
    sort_by="doi", limit=25, cursor=None) -> Page[DataCiteDOISummary]
get_citation(doi, *, style="apa") -> Result[DataCiteDetailResponse]
search_publications(*, query=None, dataset_dois=None, short_titles=None,
    from_year=None, to_year=None, limit=25, cursor=None) -> Page[VerifiedPublication]
get_impact(short_title, *, publication_limit=25) -> Result[DatasetImpactResponse]

search_participants(*, query=None, short_titles=None, dataset_type="both",
    access_levels=None, data_categories=None, data_types=None,
    file_formats=None, geometry_statuses=None, modalities=None,
    limit=25, cursor=None) -> Page[Participant]
get_participant(participant_key) -> Result[ParticipantDetailResponse]
get_coverage(short_title, *, dataset_type=None) -> Result[CoverageResponse]
list_clinical_subjects(short_title, *, subject_ids=None,
    include_clinical_only=False, has_conflicts=None, include_inferred=True,
    limit=50) -> Result[ClinicalSubjectsResponse]
list_clinical_facts(short_title, *, subject_id=None, concepts=None,
    source_kinds=None, inferred=None, limit=100) -> Result[ClinicalFactsResponse]
list_clinical_conflicts(short_title, *, subject_id=None, concepts=None,
    limit=100) -> Result[ClinicalConflictsResponse]

list_participant_assets(participant_key, *, access_levels=None,
    data_categories=None, data_types=None, file_formats=None,
    geometry_statuses=None, data_domains=None, limit=100,
    cursor=None) -> Page[Asset]
search_public_assets(*, short_titles=None, participant_id=None,
    file_formats=None, modalities=None, geometry_statuses=None,
    media_kinds=None, object_roles=None, requires_annotations=False,
    limit=50, cursor=None) -> Page[Asset]
search_controlled_datasets(*, modalities=None, file_types=None,
    requires_annotations=False, include_mixed=True,
    limit=25) -> Result[ControlledDatasetsResponse]
list_controlled_files(short_title, *, route_systems=None, modalities=None,
    file_types=None, body_part=None, participant_id=None, patient_id=None,
    has_drs_uri=None, limit=50) -> Result[ControlledFilesResponse]
search_annotation_downloads(*, query=None, short_titles=None,
    modalities=None, access_levels=None, limit=25) -> Result[DicomAnnotationsResponse]

select(records, *, release_identity, query_scope, coverage=None,
    include_source_images=False) -> Selection
export_selection(selection, destination, *, overwrite=False) -> ExportReport
```

`select` is a local operation over explicit records returned by the client or supplied with equivalent provenance. It does not query another provider. `Selection` records dataset identity, captured source release observation, query/filter scope, record grain (dataset, participant, asset, file, series, download), coverage and completeness, and each record's provenance. `export_selection` is the only disk-writing public operation. It writes route-specific CSV manifests and a JSON companion after explicit destination/overwrite checks. The bytes are deterministic for the same captured selection and options; it does not add a fresh observation timestamp. It accounts for every input record as exported, deduplicated with reason, excluded by an explicit rule, or unresolved. It does not run Data Retriever, `idc-index`, or an imaging transfer, and does not make a network call by default.

Selection exports require exact route evidence on each record. A public IDC handoff needs a verified public `SeriesInstanceUID` from an appropriate externally supplied record; the current participant `Asset` DTO does not expose a series UID or location field, and its opaque identifiers must not be parsed into one. A notebook can carry validated TCIA dataset/DOI identity and a reviewed participant identifier into a direct `idc-index` query, then supply the resulting verified series identifiers to `select`. TCIA Utils does not itself promise series UID discovery from the current service. A Data Retriever CSV has exactly one recognized route column (`SeriesInstanceUID`, `imageUrl`, or `drs_uri`); split routes and access classes into separate files. `source_url` is not automatically an `imageUrl`; require an explicit matching delivery route. Controlled `drs_uri` is public inventory metadata and requires separate user authorization for retrieval. Aspera package/path exports use their supported distinct format only when those exact fields exist. Unresolved selections remain in the companion. No `.tcia` fabrication, credential handling, signed URL export, implicit source-image inclusion, or payload download. An annotation signal does not prove a source-image relationship. Carry unknown sizes and incomplete coverage into the companion.

## Filter and evidence rules

Filters apply only where the mapped route supports them. There is no universal patient-level diagnosis, cross-format modality, or combined public/controlled filter. Callers can combine already returned records locally only with visible provenance and `unknown` completeness where any input is bounded. The clinical `subject_id` is scoped by `short_title`; participant identity is `(dataset_type, short_title, participant_key)` and display identifiers never act as cross-dataset keys. Clinical `subject_id` and inventory `display_participant_id` may require a reviewed crosswalk; absent evidence means unresolved. Do not equate `ClinicalSubject.primary_diagnosis` or `primary_site` with an observed fact when its `*_is_inferred` flag is true. `ClinicalFact.evidence_scope`, `is_inferred`, `source_kind`, `source_priority`, `provenance`, `resolved_sources`, and conflicts survive conversion and export. Dataset-scoped inference is not an observed participant diagnosis. Clinical-only subjects require `include_clinical_only=True`.

`list_clinical_*`, `search_controlled_datasets`, `list_controlled_files`, and `search_annotation_downloads` are bounded inspection operations. A client must reject attempts to request above route maximum and must not paginate by harvesting IDs. Their full-cohort/export completeness is unsupported in phase 1. `get_dataset` has a bounded related-analysis sublist; `get_impact` has a bounded publication sublist, while `search_publications` is cursor-paged for fuller discovery. Controlled metadata may lack DICOM detail, route identifiers, sizes, or patient linkage. Public non-DICOM assets can describe packages or transformed previews as well as files; `asset_granularity` and `inventory_status` stay visible. The non-DICOM route admits narrowly reviewed IDC-missing DICOM exceptions and is not a general DICOM series API. DICOM detail/viewer selection remains a direct `idc-index` handoff with separately checked IDC coverage.

## Ownership and implementation order

| Task | Owns | Does not edit |
| --- | --- | --- |
| T04 | `pyproject.toml`, `src/tcia_utils/__init__.py`, `client.py` composition root, `_transport.py`, `_results.py`, `_errors.py`, transport tests | Feature modules |
| T05 | `src/tcia_utils/_datasets.py`, `_citations.py`; corresponding `tests/test_v4_datasets.py`, `test_v4_citations.py` | `client.py` / public exports |
| T06 | `src/tcia_utils/_participants.py`, `_clinical.py`; `tests/test_v4_participants.py`, `test_v4_clinical.py` | `client.py` / public exports |
| T07 | `src/tcia_utils/_available_data.py`; `tests/test_v4_available_data.py` | `client.py` / public exports |
| T08 | `src/tcia_utils/_selection.py`; `tests/test_v4_selection.py` and export fixtures | `client.py` / public exports |
| T09 | Final `client.py`, `__init__.py` facade wiring; `tests/test_v4_workflows.py` | Feature internals without owner review |

Feature modules expose internal functions/classes that take T04's transport and result helpers; T09 wires them as the flat `TCIAClient`. T04 can publish a minimal status client and result types first. T05–T08 do not independently change package metadata, common result types, or the composition root. Three feature workers can own T05, T06, and T07 concurrently; T08 follows as capacity allows. Contract adjustments go to the coordinator before shared edits.

## Service gaps and disposition for T03

No new server route is required for the bounded phase-1 methods above. T03 must diagnose the observed hosted HTTP 500 on participant assets and validate remaining key routes before acceptance; a route appearing in OpenAPI or readiness is insufficient. If the failure is reproducible in source, repair the narrow defect and retest it. The existing service does not expose cursors or total counts for clinical/controlled/annotation detail; that limits exhaustive workflows and export claims. The dataset detail related-analysis list uses a 25-row bound and heuristic match; present it as suggestions and use explicit searches for independent evidence. REST responses do not carry a fingerprint per row; T04 must observe and guard release identity. Missing exact `imageUrl`, public series UID, or Aspera package/path evidence makes an export record unresolved rather than a request for guessed URLs. If T03 finds a promised workflow impossible even with these bounds, reopen this contract before changing the server.
