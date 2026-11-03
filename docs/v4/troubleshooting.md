# TCIA Utils 4.0 troubleshooting

**Import still exposes the old provider modules.** Check `importlib.metadata.version("tcia_utils")` and `tcia_utils.__file__`. The package index may still serve 3.x; install the explicit 4.0.0a15 preview into a fresh environment. Avoid an editable checkout or a `PYTHONPATH` that points to another source tree when verifying notebooks.

**Construction fails.** Supply an explicit HTTP(S) service root as `base_url`, without a `/v2` suffix. The client does not infer a default host. The timeout must be greater than zero and at most 120 seconds.

**`ServiceUnavailableError`.** A required snapshot or optional component is unavailable. Inspect the error `code`, `status`, and `retryable` fields and call `status()` when readiness works. HTTP 503 or a false capability is not a zero-match result. A liveness response alone does not prove readiness.

**`InvalidRequestError`.** Check route-specific filters and limit bounds. Empty sequence filters are rejected because they could widen a query silently. Citation styles are `apa`, `vancouver`, `chicago-author-date`, `bibtex`, and `ris`. Bounded clinical and controlled routes cannot be expanded into exhaustive paging by increasing the limit above the advertised maximum.

**`ProtocolError`.** The server returned malformed or inconsistent JSON, a cursor repeated, or its release/profile changed during paging. Restart the entire query deliberately after checking the new service identity; the client never silently combines pages from different releases. Preserve the error and observed fingerprint when reporting a service issue.

**An empty result looks surprising.** Confirm the dataset type, spelling, and filter scope. Empty success covers only the requested scope and page. It does not show that a separate clinical, controlled, or IDC service has no matching data.

**Clinical IDs do not line up.** Participant keys are dataset-scoped and clinical subject IDs may use a different namespace. Do not join on a bare display patient ID or guessed dataset-name mapping. Carry unresolved identities until a reviewed crosswalk is available.

**A manifest row is unresolved.** Check for an exact recognized route field and its access class. An opaque asset ID, `source_url`, missing `SeriesInstanceUID`, or generic annotation flag is insufficient for a Data Retriever route. Review the JSON companion for the reason; do not substitute a guessed URL.

**A notebook fixture passes but live mode fails.** The workshop replays recorded public metadata. This does not prove the current deployment. Inspect the live status and route-specific error. Participant image planning requires query API 0.6.0+ and `POST /v2/participants/download-plan`; public DICOM planning also requires the upstream metadata service to be available. An unavailable provider raises an error rather than returning zero matches. No IDC Python installation is needed.

## Controlled ZIP reports a direct-file size mismatch

Upgrade to `tcia_utils[downloads]==4.0.0a15` and restart the Colab/Jupyter runtime.
Earlier previews could forward the inventory's uncompressed DICOM-series size as
the expected ZIP byte count. The fix separates reported sizes from exact payload
bytes and preserves TDR's response-length and checksum checks. A rounded manifest
label such as `19.71 KB` is not an exact byte count either.

This error does not by itself indicate a rejected API key. `metadata.csv` is
written during preparation. The preceding warning about missing public-series
cache columns is not the size-validation failure. No TDR upgrade or credential
replacement is required for this fix.
