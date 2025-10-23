import warnings
from .nbia import *

warnings.warn(
    "tcia_utils.nbia_v4 is deprecated; import tcia_utils.nbia instead. "
    "This shim will be removed in the next major release.",
    DeprecationWarning,
    stacklevel=2,
)