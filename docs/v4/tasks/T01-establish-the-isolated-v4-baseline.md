# T01 Establish the isolated v4 baseline

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** None  
**Relative size:** S — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Establish a clean, reproducible workspace for 4.0 without importing unfinished CRDC work or undertaking legacy repairs.

## Work and boundaries

Inspect current Git state, published 3.3.5 provenance, and available worktrees. Select and record an immutable base revision for v4 with a short rationale. Create or identify the isolated integration workspace, bring only the approved planning documents into it, and document how dependent agents start from the same revision. Inventory reusable license/project metadata and intentionally excluded legacy modules. Preserve the original checkout and its user changes.

## Deliverables and ownership

`docs/v4/design/baseline.md`; isolated integration workspace and starting revision; a concise inventory of inherited versus excluded material. This task owns workspace setup and baseline documentation only.

## Acceptance criteria

The original dirty files are unchanged. The baseline is reproducible from a recorded commit. Planning documents are available to subsequent agents. No published changes are lost merely because main and the published tag differ. No 3.x bug-fixing or broad branch reconciliation is required to finish.

## Validation

Compare before/after status of the original checkout. Verify selected commit/tag provenance and inspect the isolated workspace. Report workspace identity and exact baseline revision.

## Delegation prompt

> Execute T01, “Establish the isolated v4 baseline,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
