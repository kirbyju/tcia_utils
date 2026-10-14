# T11 Add package and CI validation gates

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T04  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Make v4 builds, tests, and example validation repeatable before any package publication.

## Work and boundaries

Implement CI for the agreed supported Python/dependency combinations, offline tests, wheel/sdist build checks, clean-install/import smoke tests, and notebook validation hooks. Network integration checks must be separately selected and bounded. Gate the existing publishing workflow on the appropriate validation and ensure ordinary pushes cannot publish. Keep credentials and real downloads out of default tests. Coordinate needed package metadata changes with the foundation/integration owner. Retain the existing trusted-publishing approach rather than introducing secrets.

## Deliverables and ownership

CI workflows, build/install smoke tests, test markers/configuration, and documented local verification commands. Use local validation where workflow execution is not authorized.

## Acceptance criteria

The default test command is offline and does not delete arbitrary cwd files or download images. The core package installs without notebook/IDC extras. Optional example dependencies install and import in their own check. A publish job cannot bypass validation. Workflow edits do not dispatch a run or publish a release during this task.

## Validation

Run local equivalents of applicable jobs, inspect workflow trigger/dependency logic, and verify artifact contents and dependency metadata. After T09/T10 land, rerun the integrated validation once; distinguish locally checked YAML/jobs from remotely executed CI.

## Delegation prompt

> Execute T11, “Add package and CI validation gates,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
