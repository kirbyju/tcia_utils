# 4.0.0a18 preview

The downloads and examples extras now pin TDR 0.1.0rc4, built from desktop
release 1.23.1-20261119 (8d0d394ef63d94ead01bcdae835c7b61ceb6c196).
It includes Croissant reader fixes, license/provenance preservation and dataset
reference exports.

The client accepts both completion-report schemas 1 and 2, retaining strict
nonnegative count, success and full-selection checks. Schema 2 aggregates
completed work in the retained `downloaded` field, including accepted existing
files and synchronization jobs; it does not measure new payload transfers.

The previous pinned TDR 0.1.0rc3 remains compatible with schema-1 accounting.
No REST/MCP deployment is required for this client compatibility update.
