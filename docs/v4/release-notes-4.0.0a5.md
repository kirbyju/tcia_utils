# 4.0.0a5 preview

Multi-value dataset and download attributes use one list-valued contract across
search, details, access summaries and download plans. Singleton values remain
one-item lists; missing values are empty lists. Scalar identifiers, titles, URLs
and narrative text remain strings. The canonical normalization is in REST/MCP
server 0.5.1; no legacy-server compatibility layer is added to the SDK.

Download-category aliases and report entries also retain lists. A new optional
`format_table()` helper produces display-only missing labels without changing raw
query/export data, preserving lists, zeroes, booleans and provenance. It avoids
pandas implicit-downcasting warnings and is tested with pandas 2.2 and 3.1.
The notebook uses this helper for every download table and pins this preview.

The formatting contract requires updating the hosted API to 0.5.1. It reuses the
existing metadata bundle; no snapshot rebuild is required for this change. Image
download behavior, authorization, max_items and transfer scope are unchanged.
