# Coordinated GitHub and PyPI publication plan

Prepared 2026-10-15. The recommended first release is `4.0.0a1`. PyPI currently
serves stable `3.3.5`, and neither PyPI nor GitHub tags contain a v4 release.
Normal pip installs continue to prefer stable releases; researchers can opt in
with `python -m pip install tcia_utils==4.0.0a1` after publication. Before a later
stable 4.0 release, review older tutorial dependency pins and migration guidance.

## Prepared local changes

- Package: branch `codex/tcia-utils-v4`, integrated with current GitHub main.
  The original CRDC checkout and its uncommitted files were preserved.
- Tutorials: branch `codex/tcia-utils-v4-notebooks` in TCIA_Notebooks, based on
  its current main. The original notebook checkout and downloads were preserved.
- Canonical tutorials: `TCIA_Notebooks/tcia_utils_v4`, including helper,
  synthetic fixture data, requirements, generator, and five notebooks.
- Package CI reads `examples/notebooks.json` and checks out tutorial commit
  `4f93716eb3bd9de97df0974255826664e0eed2bc`. Maintained copies were removed from
  the package. The tutorial commit must be pushed before package CI can fetch it.
- Tutorial execution passed 5/5 fixtures and 5/5 bounded live variants from the
  relocated folder. All 101 package tests, build/archive checks, pin/version
  checks and workflow YAML parsing passed locally. The rebuilt wheel's runtime
  module bytes match the previously clean-install-tested candidate.
- The four required hosted routes and MCP equivalents are verified. The earlier
  deployment blocker is resolved. This preparation does not publish the package.

## Publication sequence

1. Push the tutorial branch and open its focused PR. Publish the package branch
   and open its v4 PR. No release tag is created in this step. Package CI can
   then retrieve the exact tutorial commit even while its PR is pending.
2. Require the package's hosted Python 3.11–3.13 checks, installed wheel/sdist
   checks, and five notebook fixture executions to pass. Review the API break,
   migration notes, cross-repository pin, package contents, and preview version.
   Confirm the PyPI Trusted Publisher still matches owner `kirbyju`, repository
   `tcia_utils`, workflow `publish.yml`, and environment `pypi`.
3. Merge the reviewed package PR. Create the GitHub **prerelease** and annotated
   tag `v4.0.0a1` at its exact merged commit after publication is authorized.
   Pushing that tag runs `publish.yml`: validation, checked wheel/sdist build,
   then the existing PyPI OIDC publishing job. A GitHub branch push or PR is
   separate from this tag-triggered upload. The `pypi` environment currently has
   no required-reviewer rule, so do not use the tag as a trial run.
4. Verify the PyPI release files, hashes, provenance/attestations, and a clean
   install of the exact preview. Repeat a small live client check and run the
   tutorials against the package installed from PyPI. Attach the checked build
   artifacts and release/migration notes to the GitHub prerelease.
5. Merge/promote the tutorial PR with installation instructions for the now
   available preview. Remove its temporary unpublished-preview notice. If that
   changes notebook/support code, advance the package's tutorial pin before the
   next release. Keep links between both repositories current.

## Tutorial usability follow-up

The prepared notebooks run from the complete folder. They are not yet validated
as independently downloadable Colab notebooks because they import `_support.py`
and load local fixture files. Before advertising single-notebook Colab links,
add and test a versioned bootstrap or simplify the public notebooks so they can
run independently. Existing tutorials and pending IDC/CRDC work are outside this
relocation; they have not been rewritten.

## Publishing configuration evidence

The existing trusted-publishing workflow successfully published 3.3.5 in run
[30392500278](https://github.com/kirbyju/tcia_utils/actions/runs/30392500278).
The v4 workflow retains its publishing identity and adds validation gates. PyPI
publisher registration is account-side state and was not independently read.
See [PyPI Trusted Publisher setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
and [pip prerelease selection](https://pip.pypa.io/en/stable/cli/pip_install/#pre-release-versions).

No package/notebook branch push, PR, merge to GitHub main, release tag, GitHub
Release, or PyPI upload has occurred during this preparation.
