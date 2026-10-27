# T06 Implement participant and clinical evidence access

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T03, T04  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Expose participants, participant detail, coverage, clinical subjects/facts/conflicts, and supported evidence-aware selection without recreating clinical resolution logic.

## Work and boundaries

Use the accepted API and shared result types. Preserve dataset-scoped identity, reviewed cross-dataset links, patient/inherited/dataset evidence scope, inference flags, conflicts, and missing-crosswalk states. Implement only selection/filter semantics supported by the service contract. Reuse server decisions rather than importing the Explorer or copying its clinical precedence rules. Retain raw evidence and resolved values as distinct fields when supplied.

## Deliverables and ownership

Participant and clinical feature modules with focused tests and fixtures. Document any intentionally bounded operation and the approved mechanism for complete selection.

## Acceptance criteria

The same bare patient identifier in two datasets is never merged implicitly. Dataset-only diagnosis inference does not become observed patient evidence. Missing clinical data is distinguishable from negative findings and absent service components. Shared fixtures agree with the service/Explorer on eligibility and evidence scope. No client-side all-patient harvest is introduced to bypass missing server filters.

## Validation

Test duplicate IDs across datasets, reviewed inheritance, unknown relationships, conflicting facts, inferred-only subjects, empty evidence, clinical-only subjects, and pagination/truncation. Use a small shared parity fixture and report any producer/consumer disagreement.

## Delegation prompt

> Execute T06, “Implement participant and clinical evidence access,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
