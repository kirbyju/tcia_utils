# T07 Implement available data and controlled metadata discovery

Read [the shared agent brief](../AGENT_BRIEF.md) and [the task board](../README.md) before starting. The board owns status and assignment; do not start until dependencies are accepted and the coordinator dispatches the task.

**Repository scope:** tcia_utils  
**Depends on:** T03, T04  
**Relative size:** M — S is a small setup/decision task; M is a focused implementation or review task, not a time guarantee.

## Objective

Expose available participant/dataset data across formats and access levels, including public metadata for controlled files, without adding a public DICOM wrapper.

## Work and boundaries

Implement the agreed data discovery methods over participant assets, public non-DICOM assets, controlled metadata, and download-level annotation signals. Preserve record grain: package, file, series, and download-level descriptions are not interchangeable. Keep modality, format, access, license, geometry status, and annotations independent. Preserve alternate delivery locations without multiplying logical assets. Carry explicit unknown/missing route and relationship evidence to T08 selections.

## Deliverables and ownership

Data inventory feature modules, route-bearing result fixtures, and focused tests. Record the supported public/controlled capabilities and missing-field semantics for notebooks.

## Acceptance criteria

Metadata-only discovery requires no user download key. Controlled file availability does not generate public viewer/download links or claim all DICOM tags are available. Download-level annotation signals do not invent source-series links. Combined results preserve provenance and grain. Unsupported universal filters are rejected or disclosed according to T02, not silently ignored.

## Validation

Test mixed access, DICOM/non-DICOM records, transformed pathology previews versus source packages, missing sizes, unknown geometry, duplicate delivery locations, inaccessible detail components, and annotations without proven source relationships. Assert zero payload requests.

## Delegation prompt

> Execute T07, “Implement available data and controlled metadata discovery,” using this task card and `docs/v4/AGENT_BRIEF.md` as the scope. Confirm the accepted dependency revisions and your isolated workspace before editing. Complete the deliverables and validation above, preserve unrelated work, and return the required handoff. Report concrete blockers and contract changes to the coordinator. Do not publish, deploy, transfer imaging payloads, or expand into another task.
