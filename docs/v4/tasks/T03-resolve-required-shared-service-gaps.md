# T03 Resolve required shared service gaps

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia-query-skill  
**Depends on:** T02  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Make only the service changes required to support the accepted phase-1 client contract, or document that no service changes are needed.

## Work and boundaries

Use T02's gap list to implement minimal additive server changes with shared REST/MCP semantics where applicable. Prioritize complete pagination or explicit bounded-result semantics, evidence scope, coverage, and route identity needed by the approved workflows. Reuse existing shared query logic. Preserve existing consumers. Add focused server contract tests and migration notes when needed. Follow that repository's AGENTS.md and release-verification skill requirements. Do not broaden into a metadata rebuild, unrelated schema redesign, or deployment.

## Deliverables and ownership

Scoped service change set and tests in tcia-query-skill; `docs/v4/design/service-compatibility.md` in the integration workspace identifying required service versions/capabilities and source versus deployed status. If no changes are required, provide a validated no-change disposition.

## Acceptance criteria

Each T02 gap is resolved, explicitly deferred with the corresponding client feature excluded, or shown not to be a gap. Older servers receive clear client capability handling. Clinical or other limited routes cannot support a falsely exhaustive client method. Source changes are validated; no claim of deployment is made.

## Validation

Run affected server tests and required repository checks. Validate schema compatibility, paging boundaries, invalid input, unavailable components, and unchanged consumer behavior. Report required deployment separately as an external release dependency.

## Delegation prompt

> Execute T03, “Resolve required shared service gaps,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
