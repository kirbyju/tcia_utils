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
from ._downloads import DownloadPlan, DownloadReport, ManifestReport, DownloadError

__all__ = [
    "DownloadPlan", "DownloadReport", "ManifestReport", "DownloadError", "ParticipantImagePlan",
    "TCIAClient", "Selection", "ExportReport", "Result", "Page", "Provenance", "Completeness", "ServiceStatus",
    "TCIAError", "InvalidRequestError", "NotFoundError", "RetiredRouteError",
    "ServiceUnavailableError", "TransportError", "HTTPResponseError", "ProtocolError",
]

from ._images import ParticipantImagePlan
