# T09 Integrate the flat public API and complete workflows

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T05, T06, T07, T08  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Integrate independently developed feature modules into the accepted simple public client and verify end-to-end metadata-to-selection behavior.

## Work and boundaries

Own facade/public-export changes and resolve cross-feature integration issues on the isolated integration baseline. Check parameter naming, return types, capability handling, scope preservation, and conversion behavior. Exercise discover → inspect dataset → select participants → inspect clinical/data evidence → cite → export. Keep the public interface small and remove accidental provider-facing concepts from primary method names. Coordinate fixes with owning tasks instead of silently changing their contracts.

## Deliverables and ownership

Integrated public API and cross-feature offline workflow tests; updated API contract only for explicitly reviewed adjustments; a reproducible example for T10.

## Acceptance criteria

The public workflow runs against realistic service fixtures from an installed wheel. Result conversions/exports retain completeness and provenance. No duplicated IDC API, eager metadata payload downloads, or hidden raw-provider fallback exists. Method behavior matches the contract and all phase-1 features are reachable without namespace knowledge.

## Validation

Run feature tests and cross-feature workflows, including mixed-access participants, absent clinical detail, unavailable server capabilities, and a release change during pagination. Report API signature/contract conformance and any reopened task.

## Delegation prompt

> Execute T09, “Integrate the flat public API and complete workflows,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
