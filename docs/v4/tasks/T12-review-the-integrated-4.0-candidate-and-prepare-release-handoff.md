# T12 Review the integrated 4.0 candidate and prepare release handoff

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils; read service and consumer evidence  
**Depends on:** T09, T10, T11  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Independently assess whether the integrated candidate delivers the approved workflows and is ready for an authorized prerelease.

## Work and boundaries

Review the integrated code, public interface, package artifacts, notebooks, and task evidence. Check dependency duplication, identity/provenance boundaries, pagination/completeness, server compatibility, manifest accounting, and the simplicity of the primary workflow. Verify required service features are actually deployed before treating a hosted tutorial as release-ready. Reopen defects under their owning task. Prepare a release checklist, release notes, supported-server policy, rollback/pinning guidance, and concrete publication commands for later authorization.

## Deliverables and ownership

`docs/v4/release-readiness.md`, reviewed release notes/migration guide, validation evidence, and any precise blocker tasks. Do not create a release tag, publish to PyPI/GitHub, deploy the service, or claim release completion.

## Acceptance criteria

All phase-1 acceptance criteria have evidence or explicit blocking dispositions. There are no unresolved correctness failures in advertised workflows. Package, source, fixture, and service versions are identified. Hosted examples either work against the verified deployment or remain a documented release blocker. Reviewer separates locally validated, remotely tested, deployed, and published states.

## Validation

Run the integrated release validation from a clean install, review deterministic and bounded live metadata results, and perform a parser-only manifest interoperability check. Return a go/no-go recommendation with reasons; mark this review done only when the review deliverables are complete, retaining any publication blockers explicitly.

## Delegation prompt

> Execute T12, “Review the integrated 4.0 candidate and prepare release handoff,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
