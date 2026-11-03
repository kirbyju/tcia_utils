# 4.0.0a15 preview

Fix controlled CT/SEG archive downloads that failed with a direct-file size
mismatch. Earlier previews could treat the inventory's uncompressed DICOM-series
size as the exact byte count of the compressed ZIP. The adapter now separates
reported inventory sizes from payload validation sizes.

Explicit download_size_bytes values and payload checksums are still forwarded to
TDR, and response-length validation remains enabled. Raw manifests with explicit
payload byte sizes retain that validation. Reported metadata stays available for
description; rounded size labels are not exact validation assertions.

REST/MCP server 0.7.1 exposes the size basis, exact payload byte count when known,
published manifest size label and available payload hashes from existing artifact
provenance. This is a source-only server update and needs no bundle rebuild. The
SDK also safely handles controlled inventory records without exact size metadata.
The workshop no longer describes reported series-size totals as download bytes.

TDR remains pinned to 0.1.0rc2; authentication, file layout and summary counters are
unchanged. The warning about missing public-series cache columns is separate from
the size mismatch. Preparation metadata is not evidence of transfer success.

Validation: 330 SDK tests, 102 shared-server tests, and 20 notebook checks. New
regressions distinguish rounded labels and uncompressed series sizes from exact
payload bytes while retaining explicit byte counts and checksums. No real
controlled payloads or user credentials are used by these checks.

The released TDR engine was also exercised with tiny synthetic ZIPs: a reported
series size and an unknown payload size completed, a correct exact payload size
completed, and deliberately wrong exact sizes/checksums still failed. The complete
notebook passed offline and against public hosted metadata with transfers blocked.
