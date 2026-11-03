# 4.0.0a14 preview

Participant searches now combine imaging availability with `diagnosis`,
`primary_site`, and `include_inferred` in one request. The service filters before
pagination using exact Collection-scoped subject IDs. Text matches literal,
case-insensitive substrings; disease site is distinct from scan anatomy.

Rows preserve compact `clinical_match` evidence, including separate inference
flags and source scopes. The response confirms the applied `clinical_query` and
snapshot on every page. Missing or inconsistent confirmation raises an error.
Full clinical enrichment remains available after selecting a cohort. These
operations support ordinary Python records and JSON exports without pandas.

Requires shared REST/MCP server 0.7.0 for clinical-filter requests. This is a
source-only server update using existing artifacts; no bundle rebuild is needed.
The updated workshop places all criteria before its first participant query and
explores clinical details only for the matches. Download behavior is unchanged.

Validation: 315 SDK tests; 95 shared-server tests, including 14 clinical-search
cases; a local API-to-SDK check using recorded public metadata reproduced 322
combined matches (5 CMB-LCA and 317 NSCLC-Radiomics), or 5 with recorded-only
diagnosis/site evidence. These are local results, not deployment verification.
No imaging payloads were transferred.
