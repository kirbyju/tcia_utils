"""Run the pinned canonical workshop's installed-wheel validation harness."""
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
notebooks = (Path(sys.argv[1]).resolve() if len(sys.argv) > 1 and not sys.argv[1].startswith("--")
             else ROOT / "notebook-examples" / "tcia_utils_v4")
runner = notebooks.parent / "tools" / "run_tcia_workshop.py"
if not runner.is_file():
    raise SystemExit("Missing the pinned TCIA_Notebooks validation runner; check out the full canonical repository.")
subprocess.run([sys.executable, str(runner), *sys.argv[1:]], check=True)
