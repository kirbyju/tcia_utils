# T02 Define the public API and endpoint contract

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils; read tcia-query-skill  
**Depends on:** None  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Freeze a small task-oriented Python API and map every phase-1 capability to the shared REST service before implementation fans out.

## Work and boundaries

Define method signatures, stable user-facing terminology, result/page/selection types, explicit export operations, error semantics, and default versus opt-in detail. Map datasets, related analyses, participants, coverage, clinical evidence, non-DICOM assets, controlled metadata, annotations, citations, and verified publications to exact routes and schemas. Distinguish current source contracts from deployed capabilities. Identify missing pagination, filters, evidence fields, or route metadata that would prevent a promised workflow. Assign concrete module/test ownership and the client composition root to prevent conflicting edits. Specify how lightweight installs and optional DataFrames/notebook dependencies work. Record a supported Python/dependency policy for T04 to implement.

## Deliverables and ownership

`docs/v4/design/api-contract.md`, `docs/v4/design/endpoint-matrix.json`, and small sanitized contract fixtures under `tests/fixtures/v4/`. Matrix entries include method, HTTP operation/path, request/response model, paging/completeness behavior, capability requirements, owner task, and server gaps.

## Acceptance criteria

Every proposed public method has an implemented route or an explicit prerequisite in T03. No duplicated public DICOM API, credential exchange, payload downloader, local bundle installer, or MCP client is included. Scope/data identity and output provenance are specified. Cross-route queries do not promise impossible filter semantics. Coordinator accepts this contract before T04–T08 implement it.

## Validation

Review signatures against actual REST models/OpenAPI and the accepted notebook use cases. Include representative empty, partial, unavailable, mixed-access, and inferred-clinical responses. Validate fixture JSON and ensure no credentials or private routes are present.

## Delegation prompt

> Execute T02, “Define the public API and endpoint contract,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
