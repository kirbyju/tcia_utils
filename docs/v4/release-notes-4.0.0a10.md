# 4.0.0a10 preview

Public results now provide `to_dict()` and `to_json(indent=2)` without pandas.
Search pages and detail results preserve records, scope, provenance, notes and
completeness. Image/download plans, selections and reports use the same helpers;
download reports also include their selection completion flag.

Exports copy nested values, turn tuples into arrays and local paths into strings,
and reject unsupported values or nonfinite numbers. Exporting a page never fetches
another page or serializes its internal callback. Use explicit `iter_pages()` to
collect a paged query. The package guide includes a complete pandas-free example.

No dependency, REST/MCP, artifact-bundle or notebook changes are required. The
existing a9 workshop remains compatible and retains its tested package pin.
