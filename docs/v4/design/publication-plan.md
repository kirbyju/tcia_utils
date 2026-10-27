# Coordinated GitHub and PyPI publication plan

Updated 2026-10-27. The recommended first release is `4.0.0a1`. PyPI currently
serves stable `3.3.5`, and neither PyPI nor GitHub tags contain a v4 release.
Normal pip installs continue to prefer stable releases; researchers can opt in
with `python -m pip install tcia_utils==4.0.0a1` after publication. Before a later
stable 4.0 release, review older tutorial dependency pins and migration guidance.

## Prepared local changes

- Package: branch `codex/tcia-utils-v4`, integrated with current GitHub main.
  The original CRDC checkout and its uncommitted files were preserved.
- Tutorials: branch `codex/tcia-utils-v4-notebooks` in TCIA_Notebooks, based on
  its current main. The original notebook checkout and downloads were preserved.
- Canonical workshop: `TCIA_Notebooks/tcia_utils_v4`, one standalone notebook,
  requirements, generator, recorded public-metadata regression fixture, and
  maintainer-only validation tools. The old five drafts have been superseded.
- Package CI reads `examples/notebooks.json` and checks out tutorial commit
  `ea2cdf81a65f15ad84feb2d0663da683c68bc08c`. Maintained copies were removed from
  the package. The tutorial commit must be pushed before package CI can fetch it.
- The single workshop passed live public-metadata execution and offline replay
  against the installed candidate wheel on Python 3.13. Nine focused notebook
  tests and all 101 installed-wheel package tests passed locally. The workflow
  still requires hosted checks before publication.
- The four required hosted routes and MCP equivalents are verified. The earlier
  deployment blocker is resolved. This preparation does not publish the package.

## Publication sequence

1. Push the tutorial branch and open its focused PR. Publish the package branch
   and open its v4 PR. No release tag is created in this step. Package CI can
   then retrieve the exact tutorial commit even while its PR is pending.
2. Require the package's hosted Python 3.11–3.13 checks, installed wheel/sdist
   checks, and the single workshop replay plus its focused example tests to pass. Review the API break,
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
   changes notebook code, advance the package's tutorial pin before the
   next release. Keep links between both repositories current.

## Workshop usability and remaining platform limits

As of 2026-10-25 the five drafts are replaced by one standalone, conversational
workshop. It has no learner-side helper/fixture dependency, uses real public
metadata by default, and includes explicit opt-in transfer cells. Local Jupyter
execution is verified; a hosted Colab runtime remains untested. The preview is
still unpublished on PyPI (stable 3.3.5; no 4.x versions observed 2026-10-25).

CMB-GEC has three imaging download groups. Two have official Data Retriever
manifests. Its public tissue-slide package still needs the collection download
link; the CLI cannot complete that group. All 251 public DICOM manifest series
are present in the Data Retriever main-branch metadata checked at
`5474a701d2d9cc09aa95cacdecc30a7b8c0cfc71`. This is metadata coverage evidence,
not proof of a released binary or a successful image transfer.

The NIfTI API exposes indexed files and geometry status but no per-file transfer
route; inspected detailed geometry objects are empty. The workshop includes
published package links and an optional local NIfTI header comparison. Clinical
coverage and exact CT/SEG references remain explicit review boundaries. These
limitations must stay visible in preview documentation. No image payloads or
controlled credentials were used in validation.

## Publishing configuration evidence

The existing trusted-publishing workflow successfully published 3.3.5 in run
[30392500278](https://github.com/kirbyju/tcia_utils/actions/runs/30392500278).
The v4 workflow retains its publishing identity and adds validation gates. PyPI
publisher registration is account-side state and was not independently read.
See [PyPI Trusted Publisher setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/)
and [pip prerelease selection](https://pip.pypa.io/en/stable/cli/pip_install/#pre-release-versions).

No package/notebook branch push, PR, merge to GitHub main, release tag, GitHub
Release, or PyPI upload has occurred during this preparation.

The October 27 notebook revision adds a Colab badge targeting the preview branch
and explains explicit alpha installation. Its executable cells are unchanged;
nine notebook checks and offline replay passed. The badge requires the notebook
branch to be pushed, and its PyPI installation requires publishing `4.0.0a1`.
Neither action was performed while adding the badge.
