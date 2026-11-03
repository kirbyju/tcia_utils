"""Typed failures from the TCIA V2 REST service."""

from __future__ import annotations


class TCIAError(Exception):
    """Base exception for TCIA client and service failures.

    Attributes:
        detail: Human-readable error description.
        code: Machine-readable problem code when available.
        status: HTTP status when a response is available.
        retryable: Whether retry may succeed; not a guarantee.

    Concrete subclasses distinguish invalid inputs, missing records, unavailable
    components, transport failures and inconsistent protocol responses. A missing
    component is an error, not a successful zero-match result."""

    def __init__(
        self,
        detail: str,
        *,
        code: str | None = None,
        status: int | None = None,
        retryable: bool = False,
    ) -> None:
        """Create a typed error retaining service problem details when available.

        Args:
            detail: Human-readable error description.
            code: Optional machine-readable problem code.
            status: Optional HTTP status; None for failures without a response.
            retryable: Whether retry may succeed (default False); not a guarantee.

        Returns:
            None. Initializes the detail, code, status and retryable attributes."""
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
