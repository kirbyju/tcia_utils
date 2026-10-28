# TCIA Utils 4.0.0a3

Collection search results can now be enriched with `describe_datasets()`, which adds published data categories without mixing them with modalities, formats, or external resources. The helper preserves dataset-type identity and checks snapshot consistency.

Download prerequisite errors now explain the specific missing tool or authorization requirement and include actionable `message`, `next_step`, and `documentation_url` fields in reports. A missing Aspera CLI for public pathology no longer produces a generic suggestion to install unrelated dependencies or supply credentials. The workshop includes a setup guide, executable-path setting, and dry-run check before the optional pathology transfer.

The notebook pins this release and displays collection data categories in its initial search table. Native payload routes and transfer completion semantics are unchanged from a2. Validation uses synthetic transfer tests and live public metadata; no credentialed or imaging-payload transfer is performed.
