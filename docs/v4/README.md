# TCIA Utils 4.0 implementation backlog

Create a focused Python client for the shared TCIA REST service, with reliable selection exports and notebooks that hand off to existing retrieval/imaging tools. This backlog records the scope agreed on 2026-10-14. It contains 12 phase-1 tasks and one deferred phase-2 assessment. Implementation is now authorized and coordinated in the originating Codex chat. All implementation agents are explicitly configured as GPT-6 Sol with medium reasoning.

The product brief is [AGENT_BRIEF.md](AGENT_BRIEF.md). Each task card includes scope, ownership, dependencies, acceptance criteria, validation, and a reusable delegation prompt. These files are repository documentation and should travel with the implementation workspace.

## Canonical status board

Only the coordinator updates this table. Task cards intentionally do not duplicate status. Set an owner and workspace/revision in the evidence column at dispatch; replace or extend it with review/test evidence at completion.

| Task | Deliverable | Status | Dependencies | Owner | Workspace and evidence |
|---|---|---|---|---|---|
| [T01](tasks/T01-establish-the-isolated-v4-baseline.md) | Establish the isolated v4 baseline | Done | — | worker1 | Accepted baseline `bde9c995`; original CRDC checkout preserved. |
| [T02](tasks/T02-define-the-public-api-and-endpoint-contract.md) | Define the public API and endpoint contract | Done | — | worker2 | Contract accepted and integrated as `314de23`; 21 HTTP mappings and eight synthetic fixtures checked. |
| [T03](tasks/T03-resolve-required-shared-service-gaps.md) | Resolve required shared service gaps | Active | T02 | worker3 | Isolated query-skill service repair at `7b49c1e`; both asset response schemas under review. |
| [T04](tasks/T04-build-the-client-foundation.md) | Build the client foundation | Active | T01, T02 | worker1 | Worker1 at `314de23`; shared HTTP/result foundation. |
| [T05](tasks/T05-implement-dataset-discovery-and-research-citations.md) | Implement dataset discovery and research citations | Waiting | T03, T04 | — | — |
| [T06](tasks/T06-implement-participant-and-clinical-evidence-access.md) | Implement participant and clinical evidence access | Waiting | T03, T04 | — | — |
| [T07](tasks/T07-implement-available-data-and-controlled-metadata-discovery.md) | Implement available data and controlled metadata discovery | Waiting | T03, T04 | — | — |
| [T08](tasks/T08-implement-selection-exports-and-tool-handoffs.md) | Implement selection exports and tool handoffs | Waiting | T03, T04 | — | — |
| [T09](tasks/T09-integrate-the-flat-public-api-and-complete-workflows.md) | Integrate the flat public API and complete workflows | Waiting | T05, T06, T07, T08 | — | — |
| [T10](tasks/T10-write-and-verify-the-research-notebooks-and-user-guide.md) | Write and verify the research notebooks and user guide | Waiting | T09 | — | — |
| [T11](tasks/T11-add-package-and-ci-validation-gates.md) | Add package and CI validation gates | Active | T04 | worker2 | Preparing independent CI/packaging gates; integrated validation waits for T04. |
| [T12](tasks/T12-review-the-integrated-4.0-candidate-and-prepare-release-handoff.md) | Review the integrated 4.0 candidate and prepare release handoff | Waiting | T09, T10, T11 | — | — |
| [T13](tasks/T13-assess-which-expert-interfaces-are-worth-maintaining.md) | Assess which expert interfaces are worth maintaining | Deferred | T12 | — | — |

Statuses: **Ready** (dependencies accepted), **Waiting** (prerequisites outstanding), **Active**, **Review**, **Done**, **Blocked** (state the concrete blocker), and **Deferred**. Waiting is not a failure. A task marked Done has accepted deliverables; this never implies package publication or service deployment.

T03 may finish with a validated no-change disposition. It is a required decision gate, not a requirement to modify the server. T12 can finish a no-go review while release blockers remain visible in its report and the board. T13 remains deferred until phase 1 is accepted and prioritized; finishing T12 alone does not automatically dispatch it.

## Execution waves

1. **Start T01 and T02 independently.** One establishes the isolated baseline; the other fixes the API and endpoint contract. Review T02 before writing the shared foundation.
2. **Run T03 and T04.** Service-gap work and the package foundation can proceed separately against the accepted contract. Review their shared contract changes before the next wave.
3. **Run T05–T08 and T11 as capacity permits.** These have separate feature ownership. With three workers, start T05, T06, and T07; then T08 and T11. Prefer finishing and reviewing work over starting all five simultaneously.
4. **Run T09**, integrating feature modules and complete workflows. T11 may continue in parallel when it stays within CI/test ownership.
5. **Run T10** against the integrated API; finish T11's integrated checks.
6. **Assign T12 to an independent reviewer.** Reopen defects under their owning task. Publication/deployment remain separate authorized actions after readiness.
7. **Consider T13 after phase-1 feedback.** Turn only justified expert capabilities into new implementation cards.

## Managing delegation

Use one coordinator chat for assignments, contract decisions, integration, and this board. Give each implementation agent one card plus the shared brief, the exact starting integration revision, repository/worktree path, and accepted prerequisite evidence. A fresh agent should not need the original conversation.

Use a separate managed worktree per simultaneous implementation agent. Do not run multiple writers in the currently dirty CRDC checkout. T01 prepares the baseline and ensures these planning documents are available to later worktrees. T02 and T04 fix shared file ownership; T09 owns final facade integration. Cross-cutting changes return to that owner instead of being edited independently by several agents.

Each card is suitable for a local subagent or a dedicated Codex chat. Creating this backlog does not create sidebar chats, start agents, or send GitHub messages. The coordinator can later convert cards into GitHub issues if desired; keep this board canonical until deliberately transferring status ownership to an issue tracker. Avoid maintaining two competing status systems.

Use the delegation prompt in the task card, appending the actual accepted revision and workspace path. Require the standard handoff in AGENT_BRIEF.md. Review evidence before marking Done; run focused checks after integration and broader checks only when warranted by the combined change.

## Scope exclusions

Phase 1 does not repair the 3.x release, preserve every old import, recreate idc-index, implement native controlled transfers, add an MCP client, automatically install local metadata bundles, or revive tool-specific expert modules. It also does not require a new public REST proxy for IDC. The primary handoff is a trustworthy selection of evidenced public series identifiers that users can pass to idc-index directly.

## Release gates

- Public methods match the accepted service contract and expose incomplete/unavailable states.
- Participant and clinical evidence retain identity and provenance; missing data is not treated as a negative finding.
- Every selected record is accounted for in exported, explicitly excluded/deduplicated, or unresolved inventories.
- Five beginner-readable notebooks use the built package and existing tool handoffs, with no default payload transfers.
- Default tests are offline; package installation and optional example dependencies are verified independently.
- Required hosted service capabilities are verified before the hosted notebooks are advertised as working.
- An independent review distinguishes validated source/builds from deployed services and published packages.

## Current planning checkpoint

The installed TCIA query skill was updated and live-verified as 2026.10.14.1 earlier in this chat. The planning checkout still has pre-existing CRDC edits and is not the v4 integration baseline. T01 verified the published baseline and preserved the original checkout. Implementation is tracked below.

## Active coordination checkpoint

The canonical implementation board is this file in the isolated integration worktree. The original checkout retains the planning snapshot; its source edits remain untouched. T01 was accepted after review of baseline provenance, the documentation-only commit, and the worker preservation checks. Local commits support integration; push, publication, deployment, and imaging transfers remain outside the execution authorization.
