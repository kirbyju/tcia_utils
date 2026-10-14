# T08 Implement selection exports and tool handoffs

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils; read data-retriever and idc-index contracts  
**Depends on:** T03, T04  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Turn approved selections into reproducible exports that existing tools can consume, without implementing another query or transfer engine.

## Work and boundaries

Build export operations on the shared selection contract and T02 fixtures. Produce route-specific Data Retriever manifests, a separate Aspera export only when exact supported package/path evidence exists, and public IDC SeriesInstanceUID lists with dataset/provenance context. Include a structured companion describing query/filter scope, release identity, coverage, counts, known/unknown sizes, licenses/citations, unresolved records, and selection rules. Keep source-image inclusion explicit for annotation selections. Document a small direct idc-index handoff; never equate a TCIA series list with verified IDC coverage without checking through the upstream tool.

## Deliverables and ownership

Selection/export module, manifest/companion schemas, golden fixtures, and interoperability tests. This is an export API, not a downloads namespace or transfer runner.

## Acceptance criteria

Every input selection record is accounted for as exported, deduplicated with reason, excluded by an explicit rule, or unresolved. Each manifest has exactly one recognized route column and no mixed public/controlled route ambiguity. Identifiers preserve leading zeros and exact values. Exports are deterministic for the same selection and source generation. File writes require an explicit destination and overwrite policy. No credentials, signed URLs, reconstructed Aspera URLs, payload transfer, or new .tcia generation occurs.

## Validation

Test mixed routes/access, absent routes, repeated logical assets/series, unknown sizes, exact identifier round trips, path/overwrite handling, annotation/source selection, and manifest consumer compatibility. Use consumer fixtures or a parser-only check; do not run real downloads.

## Delegation prompt

> Execute T08, “Implement selection exports and tool handoffs,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
