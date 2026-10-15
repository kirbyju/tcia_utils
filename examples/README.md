# TCIA Utils tutorials

The canonical notebooks and their example environment are maintained in
[TCIA_Notebooks/tcia_utils_v4](https://github.com/kirbyju/TCIA_Notebooks/tree/main/tcia_utils_v4).
The immutable revision used for this package release is recorded in
[notebooks.json](notebooks.json). Clone that repository at the pinned revision
and pass its `tcia_utils_v4` directory to this package's notebook validators.

Package CI checks out the exact revision, installs this package's built wheel,
and runs all five fixture notebooks. Updating the tutorials for a release
requires deliberately advancing this pin. No maintained notebook copies are
kept in the package repository.
