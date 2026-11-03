# 4.0.0a12 preview

All transfer entry points pass the caller's destination directly to TDR. The SDK
no longer adds dataset, manifest-hash, files or batch directory layers around
TDR's collection/participant/study/series layout. Sample, full and resume calls
use the same root. Normalized transfer lists remain under destination/manifests/.

File-target collision checks now cover the entire selected inventory before any
job starts, including separate manifest and open/controlled groups. Separate TDR
calls still avoid colliding file job keys while sharing the same output root.

Existing downloads are not moved. To resume an earlier tree, pass the previous TDR
root (the directory directly above the collection folder) with resume=True.

The workshop shows download_method alongside download_requirements, explains the
manifest filter before using it, and supplies a sample root without repeating the
dataset name. No server, artifact-bundle or TDR dependency update is required.
