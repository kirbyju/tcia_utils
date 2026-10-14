# T04 Build the client foundation

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T01, T02  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Create the minimal 4.0 package foundation and shared REST transport/result behavior for feature agents to build on.

## Work and boundaries

Implement the accepted synchronous client lifecycle, explicit configurable service URL, connection reuse/cleanup, bounded timeouts, appropriate read-operation retry rules, typed errors, result envelopes, lazy paging, and release/capability reporting. Preserve release-local cursors and surface partial/unavailable states. Use one shared conversion/export foundation with no implicit disk writes. Set up the package metadata, optional dependencies, and initial public exports under the T02 file-ownership plan. Keep the core import lightweight and free of root-logging configuration or network activity.

## Deliverables and ownership

Core package modules, focused transport/result tests, updated pyproject metadata for the agreed 4.0 prerelease approach, and a short internal extension guide for T05–T08. T04 owns the initial pyproject, shared error/result modules, and client composition root until handoff to T09.

## Acceptance criteria

Client creation and import perform no requests or downloads. Failures differ from empty results. Pagination does not silently cross releases or alter filters. Unknown additive response fields are handled according to the contract. No adjacent checkout, installed skill, LLM account, IDC index download, or legacy eager import is required.

## Validation

Run offline tests using controlled HTTP responses for timeout, connection failure, retryable/nonretryable statuses, malformed responses, repeated/invalid cursors, release changes, and cleanup. Verify import behavior and install the built package in an isolated environment.

## Delegation prompt

> Execute T04, “Build the client foundation,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
