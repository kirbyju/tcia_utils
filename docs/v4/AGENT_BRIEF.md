> Historical phase-1 brief. For the current review scope and supported Python downloads, use [the user guide](user-guide.md#download-from-python). External application launchers and Aspera handoffs are no longer part of the SDK.

# TCIA Utils 4.0 agent brief

Implement a clean Python client for TCIA research workflows. Researchers should discover datasets, participants, clinical evidence, available data, citations, and publications without selecting TCIA's underlying software. The package consumes the shared TCIA REST contract; it does not reproduce the metadata ingestion pipeline or require an LLM account.

The user has authorized local implementation with a coordinator and up to three GPT-6 Sol medium agents. The coordinator dispatches task cards after accepting prerequisites. This authorization does not include publication or deployment.

## Accepted product decisions

- Target a new 4.0 interface. Do not make repairing 3.x, merging all legacy branches, or preserving every old import a prerequisite.
- Prefer one small client with task-oriented methods. Final names and signatures are fixed by T02. Avoid public `dicom`, `files`, and `downloads` namespaces that mix formats and actions.
- DICOM/non-DICOM, modality, annotation availability, and public/controlled access are filters or result properties. Do not equate pathology with non-DICOM or noncommercial licenses with controlled access.
- Use REST for ordinary Python access. MCP remains a parallel agent interface to shared server functionality. Do not implement a second MCP transport in phase 1.
- Keep public DICOM detail queries, viewer selection, and retrieval in `idc-index`. Supply evidenced identifiers and example handoffs; do not wrap its full API or create a new DICOM downloader.
- Use the TCIA service's public controlled-data inventory for discovery. Public metadata is distinct from authorization to retrieve controlled payloads and may have less detail than public DICOM metadata.
- Prepare official route-specific manifest handoffs for TCIA Data Retriever. Do not implement credential exchange, native controlled downloads, or another transfer engine in phase 1.
- Keep provider names in provenance and expert documentation, rather than making users choose a provider to discover data.
- Defer direct provider adapters and local artifact backends. T13 assesses whether any expert adapters merit a later implementation task.

## Evidence and identity requirements

Visible TCIA Collection and Analysis Result records establish publication identity. External enrichment does not establish TCIA publication. Participant identity is dataset-scoped; never join on a bare patient identifier or a guessed dataset-name mapping. Preserve reviewed crosswalks and unresolved identities.

Clinical results must retain patient versus dataset evidence scope, inheritance, inference, conflicts, and source precedence supplied by the server. Do not invent a second clinical resolution algorithm. A dataset diagnosis label is not an observed diagnosis for every participant.

Expose release identity, coverage, truncation, unavailable detail, and unsupported filters. An empty result is not an unavailable service. Do not silently combine pages from different releases, silently switch to live upstream discovery, or describe limited results as exhaustive.

New manifests contain exactly one recognized route column: `SeriesInstanceUID`, `imageUrl`, or `drs_uri`. Split routes and public/controlled selections as needed. Aspera package/path exports use their own supported format. Preserve unresolved records in a separate inventory. Manifest creation must not fetch payloads or require credentials.

## Repository boundaries and starting state

The planning checkout is on `add-crdc-module-2661349312382329103` at `b75d8953e25bd29e249045a247c2f94fd0345f43`, observed 2026-10-14. It has pre-existing changes in `src/tcia_utils/crdc.py`, `tests/test_crdc.py`, and untracked `CRDC_API_Examples.ipynb`. Preserve them; do not stash, reset, merge, or copy them into the v4 baseline implicitly.

The earlier assessment verified GitHub main at `ef45a04` with version 3.3.4 and published 3.3.5 at `8d627da862c5c54bddd360089938146908beeb6b`. T01 must recheck these observations before choosing a baseline. This is a small workspace decision, not a 3.x repair project.

Read applicable repository AGENTS.md instructions. Use separate managed worktrees for simultaneous implementation. The coordinator owns shared integration and the backlog status table. Task agents own only the paths agreed in T02 and their focused tests. Do not modify another agent's checkout. Before starting a task, use the integration revision containing its completed prerequisites.

The query-skill checkout owns the REST/MCP service and its shared evidence rules. Server changes belong there, under its own validation instructions. Do not edit the installed Codex skill as a substitute for editing source. No push, remote issue/comment, publication, release tag, workflow dispatch, deployment, service restart, or real imaging transfer is included in these task briefs; obtain the applicable authorization separately.

## Shared files and coordination

T02 assigns module boundaries and the single owner of the client composition root. T04 owns initial package metadata, dependencies, public exports, transport, results, and error types. T05–T08 implement separate feature modules and tests. T09 integrates facade changes and cross-feature behavior. T10 owns user documentation and notebooks. T11 owns CI workflow changes and package-verification tests. Changes to another owner's shared files go through the coordinator.

Do not overengineer plugin systems, asynchronous APIs, or framework layers. A synchronous, typed, testable client with clear functions is sufficient. Choose tested dependencies and a supported Python floor explicitly; do not inherit the old dependency list by default.

## Required handoff

Return the task ID, implementation revision or exact changed files, behavior delivered, commands and results for validation, unresolved limitations, and any changes required in another repository. Include fixture/service versions where relevant. Never report mocked tests as live service verification or local checks as publication/deployment.

Task agents do not mark themselves accepted. The coordinator reviews the diff and evidence, integrates the task, then updates the board. Reopen the owning task for a failed acceptance criterion. Report blockers precisely rather than inventing endpoints or broadening scope.

## Reference material

- `docs/v4/README.md`: canonical task board and sequencing.
- `tcia-query-skill/SKILL.md`, `references/api-upgrade-notes.md`, and `mcp_server/README.md`: current source contracts and boundaries.
- `tcia-query-skill/mcp_server/tcia_query_mcp/rest.py` and `models.py`: implementation and response schemas; confirm deployed capabilities separately.
- `tcia-cohort-builder/cohort_builder_data.py`: existing participant and clinical evidence behavior to compare, not copy indiscriminately.
- `data-retriever/README.md`: manifest/CLI consumer contract.
- Official `idc-index` documentation: supported handoff APIs.

Local checkout paths are environment-specific. Resolve them when dispatching; portable task cards use repository names. The installed TCIA skill was verified current at version 2026.10.14.1 when this backlog was created.
