# T13 Assess which expert interfaces are worth maintaining

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils; read upstream provider projects  
**Depends on:** T12  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Identify justified expert-only capabilities after the primary 4.0 workflow is working, without committing to preserve every legacy adapter.

## Work and boundaries

Inventory existing modules and compare them against the new TCIA client and current upstream packages. Evaluate IDC, CTDC/General Commons, WordPress, DataCite, PathDB, and narrowly scoped NBIA fallback needs. For each capability recommend upstream use, a small expert adapter, deprecation, or no further work. Inspect schema/query/authentication/pagination behavior and obvious correctness gaps. Metadata checks are read-only and scoped. Assess native controlled-transfer code separately from metadata adapters; it is not implied by retaining CRDC query support.

## Deliverables and ownership

`docs/v4/design/expert-interface-assessment.md` with a capability matrix, validation evidence, ownership/maintenance cost, and focused follow-on task proposals only for approved gaps.

## Acceptance criteria

Each proposed adapter enables a concrete workflow unavailable through the core service or a supported upstream API. No automatic NBIA authentication revival, duplicated IDC downloader, or blanket migration of legacy modules is recommended. Provider-specific namespaces are clearly advanced and do not change the primary research API.

## Validation

Use focused offline tests and bounded public metadata probes where appropriate. Record exact upstream versions and mark unavailable integrations unverified. Return recommendations and task briefs; do not implement new adapters in this task.

## Delegation prompt

> Execute T13, “Assess which expert interfaces are worth maintaining,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
