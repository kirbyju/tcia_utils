# TCIA Utils v4 local validation evidence

This report records local checks of the integration baseline through `f62f256` (worker checkout `90e5253`) on 2026-10-14. It is not a hosted CI result, package publication, or deployed-service check.

| Check | Local result |
| --- | --- |
| Python 3.13.7 source tree, `python -m pytest -q` | 101 passed; v4 tests are the default collection |
| `python -m build --no-isolation` | Wheel and source archive built |
| `tools/check_v4_distribution.py` | Both archives passed metadata and legacy-exclusion checks |
| Fresh wheel installs with resolved runtime dependencies on Python 3.11, 3.12, and 3.13, test invocation from outside source tree | 101 passed on each version; `tcia_utils.__file__` resolved inside the wheel virtual environment |
| Fresh source archive installs with isolated build and resolved runtime dependencies on Python 3.11, 3.12, and 3.13, test invocation from outside source tree | 101 passed on each version; `tcia_utils.__file__` resolved inside the source-archive virtual environment |
| Clean `examples` extra install | Resolved and imported `idc_index` 0.13.0, `nbformat` 5.11.1, and `pandas` 3.0.6 from an independent virtual environment |
| Notebook structure and offline execution | Pending T10 integration; no notebook directory yet on this baseline |

The built artifacts were `tcia_utils-4.0.0a1-py3-none-any.whl` (SHA-256 `4480a129dc7560b85a2650c4aeab37249e20a5c638be325634331cbb34a91bbe`) and `tcia_utils-4.0.0a1.tar.gz` (SHA-256 `935800c21dea6a596f5092337b77aa3f07bc4840ed455025b6b6909fcd107f25`). The full installed-artifact test invocation used absolute test paths while the working directory was outside the checkout, so source imports could not mask the installed package.

Fresh artifact installs resolved `httpx` 0.28.1 and its dependencies; the source archive also resolved its isolated build backend. The local source build used an existing QA environment with `--no-isolation`, followed by clean isolated source-archive installs. The wheel's `examples` extra resolved separately and imported all three declared example dependencies. Notebook execution and hosted GitHub Actions are not yet verified locally. CI is configured to exercise Python 3.11–3.13 and fresh installs when run.

The v4 CI workflow now runs the offline tests against each installed archive from `/tmp`, in addition to testing the editable source tree. Publication remains gated on five valid notebooks and their deterministic offline runner. Record the final notebook report here after T10 is integrated.
