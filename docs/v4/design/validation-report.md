# TCIA Utils v4 local validation evidence

This report records local checks of integration commit `f0a27ca` on 2026-10-14. It is not a hosted CI result, package publication, or deployed-service check.

| Check | Local result |
| --- | --- |
| Python 3.13.7 source tree, `python -m pytest -q` | 101 passed; v4 tests are the default collection |
| `python -m build --no-isolation` | Wheel and source archive built |
| `tools/check_v4_distribution.py` | Both archives passed metadata and legacy-exclusion checks |
| Fresh wheel installs with resolved runtime dependencies on Python 3.11, 3.12, and 3.13, test invocation from outside source tree | 101 passed on each version at the core baseline `f62f256`; final integrated wheel rerun on 3.11 passed 101 |
| Fresh source archive installs with isolated build and resolved runtime dependencies on Python 3.11, 3.12, and 3.13, test invocation from outside source tree | 101 passed on each version at the core baseline `f62f256`; final integrated source archive rerun on 3.11 passed 101 |
| Clean `examples` extra install | Final wheel installed into a checkout-local Python 3.11 virtual environment; `idc-index` 0.13.0, `nbformat` 5.11.1, `nbclient` 0.11.0, `ipykernel` 7.4.0, and `pandas` 3.0.6 imported with no broken requirements |
| Notebook structure and offline execution | Five valid notebooks; exact CI command `examples-venv/bin/python tools/run_v4_notebooks_offline.py examples/notebooks` passed all five fixture variants against the installed wheel |

The final built artifacts were `tcia_utils-4.0.0a1-py3-none-any.whl` (SHA-256 `847334ecea2585540490772b9e2c94399d10a6aca35680fd020f70c82700e60f`) and `tcia_utils-4.0.0a1.tar.gz` (SHA-256 `fdfc26cd973ceb6b72b9e73e89a7d7a890cc8b71122abd4366452470517a4b40`). The full installed-artifact test invocation used absolute test paths while the working directory was outside the checkout, so source imports could not mask the installed package. The notebook runner separately verified that `tcia_utils.__file__` was an installed `site-packages` path and rejected editable source imports.

Fresh artifact installs resolved `httpx` 0.28.1 and its dependencies; the source archive also resolved its isolated build backend. The local source build used an existing QA environment with `--no-isolation`, followed by clean isolated source-archive installs. The final integrated package changed example dependencies and notebooks after the three-version core matrix, so the final artifacts were rerun on Python 3.11 only. The five notebook fixture variants ran through local Jupyter kernels; they made no live service or payload calls. Hosted GitHub Actions is not yet verified. CI is configured to exercise Python 3.11–3.13 and fresh installs when run.

The v4 CI workflow runs the offline tests against each installed archive from `/tmp`, in addition to testing the editable source tree. The separate examples job builds a wheel, installs its `examples` extra, validates five notebooks, and runs their deterministic fixture variants. Publication remains gated on those checks. T10's checked-in [notebook fixture report](notebook-fixture-validation.json) records its own execution; the T11 checkout-local CI-layout rerun also passed five of five.

The final handoff adds release-review and task-board documentation after these checks. Hashes above identify the tested `f0a27ca` archives; any later local handoff build records its exact commit and hashes separately in `dist/artifact-provenance.json`.
