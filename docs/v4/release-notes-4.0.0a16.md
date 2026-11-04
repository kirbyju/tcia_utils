# 4.0.0a16 preview

The downloads/examples extras now pin tcia-data-retriever 0.1.0rc3, a release
candidate containing the existing-file counter change proposed in TDR PR #13.
The TDR review PR remains open; this preview makes the candidate available for
testing before formal review is complete.

Files accepted by TDR's existing-file checks now count as Skipped in ordinary
runs and explicit resume runs. They remain completed items. Downloaded counts new
non-sync transfers; Synced continues to describe public DICOM sync jobs, which
may transfer missing files. The SDK already accepts all three completion counts.
No new download option is required.

The workshop installs this preview and explains the corrected counters. Earlier
controlled ZIP size-validation fixes are retained. Downloads continue to use the
same packaged TDR engine, output paths, approved credentials and integrity checks.

Validation: 332 SDK tests, including all-skipped and mixed downloaded/skipped
completion cases; 20 notebook checks; and complete offline notebook execution
with RC3 and the built SDK wheel. The published RC3 engine was tested with tiny
local sample/full/repeat downloads; reused files caused no payload requests and
were correctly reported as Skipped. No TCIA imaging payloads or user credentials
were used. No REST/MCP server update is required for this dependency change.
