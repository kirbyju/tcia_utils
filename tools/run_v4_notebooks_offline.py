"""Execute the five v4 notebooks using the installed wheel.

Fixture mode is the offline default; live mode makes bounded metadata calls.
Run with the notebook examples environment's Python, not an editable SDK install.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from importlib.metadata import distribution, version
import json
import os
from pathlib import Path
import re
import sys
import tempfile

import nbformat
from nbclient import NotebookClient


ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = ROOT / "notebook-examples" / "tcia_utils_v4"
EXPECTED = (
    "01_discover_datasets_citations.ipynb",
    "02_public_dicom_idc.ipynb",
    "03_non_dicom_annotations.ipynb",
    "04_controlled_metadata_manifest.ipynb",
    "05_participants_clinical_availability.ipynb",
)


def _kernel_spec(directory: Path) -> None:
    kernels = directory / "kernels" / "tcia-v4-examples"
    kernels.mkdir(parents=True)
    (kernels / "kernel.json").write_text(json.dumps({
        "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
        "display_name": "TCIA v4 examples", "language": "python",
    }))
    os.environ["JUPYTER_PATH"] = str(directory) + os.pathsep + os.environ.get("JUPYTER_PATH", "")


def run(mode: str, notebooks: Path = NOTEBOOKS) -> list[dict[str, object]]:
    if mode not in {"fixture", "live"}:
        raise ValueError("mode must be fixture or live")
    import tcia_utils
    location = Path(tcia_utils.__file__).resolve()
    dist = distribution("tcia_utils")
    direct_url = dist.read_text("direct_url.json")
    editable = bool(direct_url and json.loads(direct_url).get("dir_info", {}).get("editable"))
    if location.is_relative_to(ROOT / "src") or editable or "site-packages" not in location.parts:
        raise RuntimeError("Notebook validation must import an installed wheel, not this source tree")
    results = []
    with tempfile.TemporaryDirectory(prefix="tcia-v4-notebook-check-") as temporary:
        _kernel_spec(Path(temporary))
        os.environ["TCIA_EXAMPLE_MODE"] = mode
        for name in EXPECTED:
            source = notebooks / name
            nb = nbformat.read(source, as_version=4)
            with tempfile.TemporaryDirectory(prefix="tcia-v4-manifest-") as output:
                os.environ["TCIA_EXAMPLE_OUTPUT_DIR"] = output
                executed = NotebookClient(
                    nb, timeout=90, kernel_name="tcia-v4-examples",
                    resources={"metadata": {"path": str(notebooks)}},
                ).execute()
                outputs = "\n".join(
                    str(item.get("text", ""))
                    for cell in executed.cells if cell.cell_type == "code"
                    for item in cell.get("outputs", [])
                    if item.output_type == "stream"
                )
                if mode == "fixture" and "SYNTHETIC FIXTURE MODE" not in outputs:
                    raise AssertionError(f"{name} did not label synthetic output")
                if re.search(r"/Users/|/private/|Bearer\s+|BEGIN PRIVATE KEY", outputs, re.I):
                    raise AssertionError(f"{name} emitted private path or credential-shaped output")
                if any(item.output_type == "error" for cell in executed.cells if cell.cell_type == "code"
                       for item in cell.get("outputs", [])):
                    raise AssertionError(f"{name} emitted a notebook error")
                unavailable = [line for line in outputs.splitlines()
                               if "unavailable on this service" in line]
                release = re.search(r"release ([0-9a-f]{64}|synthetic-fixture-v1) profile", outputs)
                idc_release = re.search(r"IDC index version (v[0-9]+)", outputs)
                results.append({"notebook": name, "mode": mode, "ok": True,
                                "service_unavailable_routes": unavailable,
                                "observed_service_fingerprint": release.group(1) if release else None,
                                "observed_idc_index_release": idc_release.group(1) if idc_release else None})
                print(f"PASS {name} ({mode}); unavailable routes: {len(unavailable)}")
    print("installed tcia_utils", version("tcia_utils"), "idc-index", version("idc-index"))
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs="?", type=Path, default=NOTEBOOKS,
                        help="directory containing the five canonical notebooks")
    parser.add_argument("--mode", choices=("fixture", "live"), default="fixture")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    findings = run(args.mode, args.directory.resolve())
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps({
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
            "package_version": version("tcia_utils"),
            "idc_index_version": version("idc-index"),
            "idc_index_data_version": version("idc-index-data"),
            "service_base_url": "synthetic fixture" if args.mode == "fixture" else
                                os.environ.get("TCIA_V2_BASE_URL", "https://tcia.duckdns.org"),
            "notebooks": findings,
        }, indent=2) + "\n")
