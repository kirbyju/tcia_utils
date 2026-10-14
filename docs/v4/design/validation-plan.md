# TCIA Utils 4.0 validation plan

This plan defines T11's required checks. It does not claim a hosted CI run, a published package, or successful live service behavior. The existing 3.x tests are deliberately outside the v4 default command: some contact imaging endpoints or delete files in the current working directory, and the v4 core will no longer depend on their provider packages.

## Local checks after T04

Run from the v4 integration checkout with Python 3.11, 3.12, or 3.13:

```bash
python -m pip install -e '.[test]'
python -m pytest -q tests/test_v4_*.py
python -m pip install build
python -m build
python tools/check_v4_distribution.py dist
```

For the clean core installation, create disposable virtual environments outside the source tree or in a disposable worktree, install `dist/*.whl` and separately `dist/*.tar.gz`, then import `TCIAClient`. The wheel/sdist checker verifies the v4 entrypoint, version, Python floor, `httpx` as the only core declared dependency, the `test`/`tables`/`examples` extras, and exclusion of legacy provider modules. It inspects archive bytes without installing or accessing a network service. T04 owns corrections to `pyproject.toml` and package contents if these checks fail.

Install optional examples independently from the built wheel:

```bash
python -m venv examples-venv
examples-venv/bin/python -m pip install 'dist/tcia_utils-4.0.0a1-py3-none-any.whl[examples]'
examples-venv/bin/python -c 'import idc_index, nbformat, pandas'
examples-venv/bin/python tools/validate_v4_notebooks.py examples/notebooks
examples-venv/bin/python tools/run_v4_notebooks_offline.py examples/notebooks
```

The structure hook never executes cells. Before T10, a missing `examples/notebooks` directory is reported as pending on branch/PR checks; once it exists, the hook requires at least five valid notebooks with code cells. CI invokes `tools/run_v4_notebooks_offline.py` when T10 supplies it, and fails if the notebook directory exists without the runner. That runner must execute deterministic fixture variants and assert zero payload calls; T10 owns it and the separately labeled, bounded metadata-only live smoke check. The publishing workflow calls validation with `require_notebooks=true`, so a release tag fails if the five notebooks or offline runner are absent. Transfer cells must remain opt-in. After T09/T10 integration, rerun all commands and inspect the notebook report for package/service/IDC versions, credentials, private paths, and unintended payload calls.

## GitHub Actions gates

`.github/workflows/validate-v4.yml` runs offline v4 tests on Python 3.11–3.13, then builds and checks both distributions and reruns the offline tests against clean wheel/sdist installations from outside the source tree. The separate examples job builds and installs the wheel with its `examples` extra before validating and executing notebooks. It runs on branch pushes and pull requests and can be called by the publishing workflow. The commands select only `tests/test_v4_*.py`, whose HTTP interactions must use fixtures or mocks; no live integration test or imaging transfer is part of the default gate.

`.github/workflows/publish.yml` remains a tag-triggered trusted PyPI publisher using OIDC. Only `v4.*` tags trigger it. Its build requires the reusable validation workflow to succeed, and its publish job requires that validated build. Ordinary branch pushes cannot publish. Repository environment protection for `pypi` remains in place. T11 does not push a tag, trigger a workflow, or publish an artifact.

Hosted service probes are separate, explicitly selected checks of metadata routes. The observed `/v2/ready` and OpenAPI describe installed capabilities and advertised routes, not proof that each endpoint succeeds; T03 is investigating a hosted participant-assets HTTP 500. Tests based on synthetic fixtures are offline contract checks and should never be described as deployed verification.

## Acceptance evidence to record

Record the integration commit, Python versions, exact local commands/results, built artifact names and metadata, wheel/sdist file inventories, CI run URLs if a later authorized push triggers CI, notebook validation report, and any bounded live service fingerprint. Distinguish local validation, remotely executed CI, publication, and deployed service state.
