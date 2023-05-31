# Overview
The [tcia_utils](https://pypi.org/project/tcia-utils/) package contains functions to simplify common tasks one might perform when interacting with The Cancer Imaging Archive (TCIA) via Jupyter/Python. Learn more about TCIA and its open-access datasets at https://www.cancerimagingarchive.net/.  Please be sure to comply with the [TCIA Data Usage Policy](https://wiki.cancerimagingarchive.net/x/c4hF).

# Installation
```
pip install tcia_utils
```

# Usage

To import functions related to the NBIA software, which holds TCIA's DICOM radiology data:
```
from tcia_utils import nbia
```

To import functions related to the PathDB software, which holds TCIA's histopathology data:
```
from tcia_utils import pathdb
```

To import functions related to Datacite, which holds metadata and Digital Object Identifiers (DOIs) for TCIA datasets:
```
from tcia_utils import datacite
```

Example notebooks demonstrating tcia_utils functionality can be found at https://github.com/kirbyju/TCIA_Notebooks.
