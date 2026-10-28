# TCIA Utils 4.0.0a2

This testing release moves recurring workshop code into the SDK and adds explicit Python downloads. Stable 3.x remains the default pip selection.

- `plan_downloads()` shows published download categories, access levels, routes, and requirements.
- `save_manifests()` saves original official manifests and hashes; `data_retriever_command()` retains an optional CLI handoff.
- `download_dataset()` / `download_files()` support public HTTPS files, public DICOM via `idc-index`, and approved controlled DRS downloads through the service API. Original slide packages delegate to separately configured Aspera tools. All selected groups are accounted for; missing prerequisites cannot silently drop data.
- `Page.to_dataframe(all_pages=True)` collects cursor pages; `Result.to_dataframe(columns=...)` supports explicit empty schemas. `pin_snapshot=True` protects a multi-call session from release changes.
- `clinical_for_participants()` batches exact Collection-scoped IDs, retains unmatched records and full source evidence, and provides flat resolved-value rows for tables.
- The canonical workshop has no custom function definitions, displays download categories, uses Python downloads as its main path, and keeps Data Retriever optional. Prerelease-installation background was removed from its lesson text.

See the [user guide](user-guide.md#download-from-python) for supported routes and completion semantics. Download tests use synthetic bytes and fake credentials; metadata-only notebook execution does not verify live controlled payload transfers or a configured Aspera installation. Detailed public DICOM queries remain in `idc-index`. The package's REST discovery API is unchanged from a1.
