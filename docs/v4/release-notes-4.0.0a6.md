# 4.0.0a6 preview

Public DICOM downloads now follow IDC normal copy behavior by default. Existing
images in the destination no longer automatically trigger a synchronization scan
before the progress bar appears. This removes the extra pre-transfer scan when
expanding a one-series sample to a full manifest. Normal copy mode may copy
existing files again.

To continue an interrupted public DICOM download, pass `resume=True` to
`download_manifest()`, `download_dataset()`, or `download_images()` and reuse the
same destination. The explicit synchronization check can take time before
transfer progress appears; its status message now explains that stage. A folder
containing only manifests still does not trigger synchronization. Reports record
whether public DICOM resume was requested.

HTTP and controlled-file downloads retain their existing size/checksum,
overwrite, and authorization behavior; this option does not add partial-file
resume for those routes. No API/server update or metadata rebuild is needed.
The notebook pins this preview and explains the default and opt-in resume.
