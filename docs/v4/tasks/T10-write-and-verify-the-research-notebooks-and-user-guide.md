# T10 Write and verify the research notebooks and user guide

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils; coordinate TCIA_Notebooks  
**Depends on:** T09  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Teach the new client through small, readable research workflows and explicit handoffs to maintained tools.

## Work and boundaries

Create five notebooks: dataset discovery/citations; selecting public DICOM identifiers and using idc-index directly for detail/preview; non-DICOM imaging and annotations; controlled public metadata with manifest preparation; participant/clinical/imaging availability selection. Use small public examples and explain source scope, missing evidence, and access limits when they affect choices. All default executable cells remain metadata-only; real transfer examples are nonexecuting or explicitly opt-in. Add a quickstart, installation guide, task-oriented API documentation, migration guidance for 3.x users, and troubleshooting. Per the 2026-10-15 user decision, maintain canonical notebooks in TCIA_Notebooks/tcia_utils_v4 and link from the package repository. Keep the package CI bound to an immutable tutorial revision.

## Deliverables and ownership

Notebooks under TCIA_Notebooks/tcia_utils_v4, updated primary README/user docs, pinned or constrained reproducible example environment, and a notebook validation report. Coordinate any TCIA_Notebooks change separately; do not alter historical workshop notebooks wholesale.

## Acceptance criteria

A beginner can complete discovery through manifest export without choosing WordPress/NBIA/CTDC/Gen Commons. Public DICOM detail explicitly uses the real idc-index API. Controlled metadata examples require no credentials. The guide distinguishes metadata access from transfer and makes the live service requirement clear. Notebook code uses the built v4 package rather than an editable adjacent checkout.

## Validation

Execute deterministic notebook variants with recorded fixtures and a separately identified bounded live metadata smoke run when connectivity permits. Record package/service/IDC versions and label fixture outputs. Check notebook output for credentials/private paths and verify no payload transfer runs by default. Report live checks as unverified if unavailable.

## Delegation prompt

> Execute T10, “Write and verify the research notebooks and user guide,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
