# 4.0.0a7 preview

Supported public DICOM, public non-DICOM and approved controlled-file transfers
now delegate to `tcia-data-retriever==0.1.0rc1`. The downloads extra installs its
platform wheel and bundled headless executable. Custom SDK token exchange, DRS
resolution and HTTP payload streaming are removed. The SDK retains dataset and
participant discovery, exact selection, manifest path/URL convenience, sampling,
route validation and report accounting. Aspera remains an external-only route.

IDC is no longer the transfer backend. The new cohorts extra retains idc-index
for public DICOM cohort metadata. The examples extra includes both dependencies;
core imports and metadata queries do not launch the transfer executable.

Actual downloads now require `accept_data_policy=True` after reviewing TCIA's
usage policy. Dry runs need no acceptance and perform no transfer/authentication.
`resume=True` maps to TDR skip-existing; it is not a byte-range guarantee. TDR owns
retries, checksums, file layout and archive extraction. Regular copy may download
existing files again; do not combine resume and overwrite. Similarly named files
are kept in separate engine batches when their job keys would collide.

Compatibility notes: TDR 0.1.0rc1 always inherits console progress. progress=False
silences SDK messages only and produces a warning about the engine limit. TDR may
retain a private copy of a supplied controlled key in its application cache; use
an isolated/temporary environment if that persistence is inappropriate. Neither
SDK metadata nor reports contain keys, tokens or temporary storage URLs. Failure
reports use conservative batch accounting rather than invent per-file success.

The guided notebook pins this preview, requests policy acceptance explicitly,
and retains one-item public examples. Transfer performance comparisons and dated
results are maintained separately in the [SDK development tools](../../tools/benchmarks/transfers/README.md).
No live controlled-transfer or Colab-performance result is claimed. This needs
no new REST/MCP routes or bundle.

When upgrading from earlier transfer backends, use a fresh destination. TDR owns
its file layout and completion metadata; resume=True does not migrate an earlier
IDC or SDK HTTP output tree. Keep prior downloads separate until you have chosen
the layout for your workflow.
