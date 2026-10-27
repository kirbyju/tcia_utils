"""Check that built v4 artifacts contain only the client and light core dependencies.

Run after ``python -m build``. This reads archives; it never installs packages or
contacts a service.
"""

from __future__ import annotations

import argparse
from email.parser import Parser
from pathlib import Path
import re
import tarfile
import zipfile


LEGACY_MODULES = {
    "crdc.py",
    "datacite.py",
    "idc.py",
    "nbia.py",
    "nbia_v4.py",
    "pathdb.py",
    "utils.py",
    "wordpress.py",
}


def check_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as wheel:
        names = set(wheel.namelist())
        assert "tcia_utils/__init__.py" in names, "wheel lacks package entrypoint"
        assert "tcia_utils/client.py" in names, "wheel lacks v4 client"
        for module in LEGACY_MODULES:
            assert f"tcia_utils/{module}" not in names, f"legacy module in wheel: {module}"
        metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
        assert len(metadata_paths) == 1, "wheel must contain one METADATA file"
        metadata = Parser().parsestr(wheel.read(metadata_paths[0]).decode("utf-8"))
        assert metadata["Name"].replace("-", "_").lower() == "tcia_utils"
        assert metadata["Version"].startswith("4."), "expected v4 package version"
        assert metadata["Requires-Python"] and ">=3.11" in metadata["Requires-Python"]
        requirements = metadata.get_all("Requires-Dist", [])
        core = [req for req in requirements if "extra ==" not in req and 'extra ==' not in req.lower()]
        core_names = {re.split(r"[ ;<>=!~\[]", req, maxsplit=1)[0].lower() for req in core}
        assert core_names == {"httpx"}, f"unexpected core dependencies: {sorted(core_names)}"
        extras = set(metadata.get_all("Provides-Extra", []))
        assert {"test", "tables", "examples"}.issubset(extras), f"missing extras: {extras}"


def check_sdist(path: Path) -> None:
    with tarfile.open(path, "r:gz") as archive:
        names = archive.getnames()
    assert any(name.endswith("/pyproject.toml") for name in names), "sdist lacks pyproject"
    assert any(name.endswith("/src/tcia_utils/client.py") for name in names), "sdist lacks v4 client"
    for module in LEGACY_MODULES:
        assert not any(name.endswith(f"/src/tcia_utils/{module}") for name in names), (
            f"legacy module in sdist: {module}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    wheels = sorted(args.directory.glob("*.whl"))
    sdists = sorted(args.directory.glob("*.tar.gz"))
    assert len(wheels) == 1 and len(sdists) == 1, "expected exactly one wheel and one sdist"
    check_wheel(wheels[0])
    check_sdist(sdists[0])
    print(f"validated {wheels[0].name} and {sdists[0].name}")


if __name__ == "__main__":
    main()
