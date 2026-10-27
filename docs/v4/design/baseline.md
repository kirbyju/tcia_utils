# TCIA Utils 4.0 baseline

Established 2026-10-14 for the isolated 4.0 implementation. The source revision is the immutable commit `8d627da862c5c54bddd360089938146908beeb6b`, also identified locally by `v3.3.5`. The integration checkout adds only the `docs/v4` planning tree and this record to that source. The local baseline commit containing these documents is the starting revision for all initial worker checkouts; record its full SHA from `git rev-parse HEAD` after this document is committed.

## Why this source revision

The [PyPI 3.3.5 release](https://pypi.org/project/tcia-utils/3.3.5/) lists the source distribution and wheel uploaded on 2026-07-28. PyPI's verified publish attestations for both files identify `github.com/kirbyju/tcia_utils` commit `8d627da862c5c54bddd360089938146908beeb6b` and `.github/workflows/publish.yml`. The local `v3.3.5` tag resolves to the same commit, whose `pyproject.toml` declares version `3.3.5`. Local `origin/main` resolves to `ef45a046d63f7787d8f0cb627d4d6741156b41b4` and version `3.3.4`. The published commit contains subsequent WordPress API v2 work; starting at `main` would omit it. This selection preserves published source while leaving the current CRDC branch and its uncommitted changes outside 4.0.

This provenance establishes the published package's source commit. It does not validate current behavior or require preserving 3.x public APIs in 4.0.

## Inherited material and deliberate exclusions

- Retain `LICENSE` (Apache 2.0) and project identity, author, homepage, and bug tracker from `pyproject.toml` as metadata to review for 4.0.
- Retain `README.md` and `MIGRATION.md` as historical documentation, and the 3.3.5 source and tests as reference material. T04 chooses the 4.0 Python floor and dependencies; it must not copy the 3.x dependency list by default.
- Existing `nbia.py`, `nbia_v4.py`, `idc.py`, `pathdb.py`, `wordpress.py`, `datacite.py`, and `utils.py` are legacy provider-oriented modules. Their code is available in the source revision for comparison, but none is designated as a 4.0 public interface or required implementation.
- The current checkout's `crdc.py` and `test_crdc.py` edits, `CRDC_API_Examples.ipynb`, and unrelated CRDC branch changes are excluded. No stash, reset, merge, or copy from that checkout is part of this baseline.

## Workspace rule

The integration checkout is `/private/tmp/tcia-utils-v4-integration`. Initial worker checkouts are `/private/tmp/tcia-utils-v4-worker1`, `/private/tmp/tcia-utils-v4-worker2`, and `/private/tmp/tcia-utils-v4-worker3`. Each starts at the local baseline commit containing this file and the planning tree. Subsequent task work starts from the integration revision containing its accepted prerequisites. The coordinator owns integration and board updates; workers edit only their assigned paths.

No commit in this setup has been pushed or published.
