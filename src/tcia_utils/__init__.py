"""TCIA research client. Importing this package performs no network work."""

from ._errors import (
    HTTPResponseError,
    InvalidRequestError,
    NotFoundError,
    ProtocolError,
    RetiredRouteError,
    ServiceUnavailableError,
    TCIAError,
    TransportError,
)
from ._results import Completeness, Page, Provenance, Result, ServiceStatus
from .client import TCIAClient
from ._selection import Selection, ExportReport

__all__ = [
    "TCIAClient", "Selection", "ExportReport", "Result", "Page", "Provenance", "Completeness", "ServiceStatus",
    "TCIAError", "InvalidRequestError", "NotFoundError", "RetiredRouteError",
    "ServiceUnavailableError", "TransportError", "HTTPResponseError", "ProtocolError",
]
