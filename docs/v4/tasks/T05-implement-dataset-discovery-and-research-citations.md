# T05 Implement dataset discovery and research citations

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T03, T04  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Deliver dataset discovery/detail, related analyses, access information, citations, verified publications, and impact methods using the shared REST client.

## Work and boundaries

Implement only T02-assigned feature modules and tests. Preserve Collection versus Analysis Result identity, download-level mixed access/license evidence, related analysis links, and explicit citation styles. Keep verified analytical use distinct from mentions, DOI event counts, and download statistics. Support the agreed paging/completeness behavior without live WordPress or DataCite fallback. Return structured results suitable for tables and citation exports.

## Deliverables and ownership

Dataset and citation/publication feature modules, fixtures/tests, and examples for T09 to wire into the facade. Do not edit the shared composition root or introduce raw provider clients.

## Acceptance criteria

Search/detail and citation workflows work against fixtures matching the service contract. Mixed-access datasets are not collapsed to a misleading single permission. DOI identifiers survive URL encoding. Verified-use and other impact signals retain separate provenance. Empty and unavailable states remain distinguishable.

## Validation

Test related analyses, mixed licenses/access, DOI path encoding, supported citation formats, pagination, no matches, missing records, and unavailable artifacts. Check that query methods do not write files or make unrequested downstream calls.

## Delegation prompt

> Execute T05, “Implement dataset discovery and research citations,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
