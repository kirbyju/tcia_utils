"""Typed failures from the TCIA V2 REST service."""

from __future__ import annotations


class TCIAError(Exception):
    """Base error retaining the server's problem details when available."""

    def __init__(
        self,
        detail: str,
        *,
        code: str | None = None,
        status: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.code = code
        self.status = status
        self.retryable = retryable


class InvalidRequestError(TCIAError):
    """A request is locally invalid or was rejected with HTTP 422."""


class NotFoundError(TCIAError):
    """A requested entity was not found (HTTP 404)."""


class RetiredRouteError(TCIAError):
    """The requested route has been retired (HTTP 410)."""


class ServiceUnavailableError(TCIAError):
    """A required service or optional component is unavailable."""


class TransportError(TCIAError):
    """A bounded HTTP request could not be completed."""


class HTTPResponseError(TCIAError):
    """An HTTP failure not covered by a more specific error type."""


class ProtocolError(TCIAError):
    """A service response violates its declared contract."""
