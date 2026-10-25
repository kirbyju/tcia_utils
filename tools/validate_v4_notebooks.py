"""Validate v4 notebook structure without executing cells or fetching data."""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--require-count", type=int, default=0)
    args = parser.parse_args()
    if not args.directory.exists():
        assert args.require_count == 0, f"missing required notebooks: {args.directory}"
        print(f"notebooks pending T10: {args.directory}")
        return

    import nbformat  # Provided by the optional examples extra, never core.

    notebooks = sorted(args.directory.glob("*.ipynb"))
    assert len(notebooks) == (args.require_count or 1), (
        "v4 examples directory must contain exactly one guided workshop notebook"
    )
    assert notebooks[0].name == "TCIA_Dataset_and_Participant_Queries.ipynb"
    for path in notebooks:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        assert any(cell.cell_type == "code" for cell in notebook.cells), f"no code cells: {path}"
        print(f"validated {path}")


if __name__ == "__main__":
    main()
