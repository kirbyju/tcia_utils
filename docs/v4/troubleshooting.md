# TCIA Utils 4.0 troubleshooting

**Import still exposes the old provider modules.** Check `importlib.metadata.version("tcia_utils")` and `tcia_utils.__file__`. The package index may still serve 3.x; install the built 4.0.0a1 wheel into a fresh environment. Avoid an editable checkout or a `PYTHONPATH` that points to another source tree when verifying notebooks.

**Construction fails.** Supply an explicit HTTP(S) service root as `base_url`, without a `/v2` suffix. The client does not infer a default host. The timeout must be greater than zero and at most 120 seconds.

**`ServiceUnavailableError`.** A required snapshot or optional component is unavailable. Inspect the error `code`, `status`, and `retryable` fields and call `status()` when readiness works. HTTP 503 or a false capability is not a zero-match result. A liveness response alone does not prove readiness.

**`InvalidRequestError`.** Check route-specific filters and limit bounds. Empty sequence filters are rejected because they could widen a query silently. Citation styles are `apa`, `vancouver`, `chicago-author-date`, `bibtex`, and `ris`. Bounded clinical and controlled routes cannot be expanded into exhaustive paging by increasing the limit above the advertised maximum.

**`ProtocolError`.** The server returned malformed or inconsistent JSON, a cursor repeated, or its release/profile changed during paging. Restart the entire query deliberately after checking the new service identity; the client never silently combines pages from different releases. Preserve the error and observed fingerprint when reporting a service issue.

**An empty result looks surprising.** Confirm the dataset type, spelling, and filter scope. Empty success covers only the requested scope and page. It does not show that a separate clinical, controlled, or IDC service has no matching data.

**Clinical IDs do not line up.** Participant keys are dataset-scoped and clinical subject IDs may use a different namespace. Do not join on a bare display patient ID or guessed dataset-name mapping. Carry unresolved identities until a reviewed crosswalk is available.

**A manifest row is unresolved.** Check for an exact recognized route field and its access class. An opaque asset ID, `source_url`, missing `SeriesInstanceUID`, or generic annotation flag is insufficient for a Data Retriever route. Review the JSON companion for the reason; do not substitute a guessed URL.

**A notebook fixture passes but live mode fails.** Fixture mode uses synthetic, model-shaped data and makes no claim about deployed routes. Inspect the notebook's live status and route-specific error. Public DICOM detail depends on `idc-index` and its own coverage; the TCIA service does not supply every series UID.
