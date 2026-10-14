"""Bounded synchronous JSON transport for idempotent TCIA reads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

import httpx

from ._errors import (
    HTTPResponseError,
    InvalidRequestError,
    NotFoundError,
    ProtocolError,
    RetiredRouteError,
    ServiceUnavailableError,
    TransportError,
)


class Transport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 15.0,
        retries: int = 2,
        client: httpx.Client | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
            raise InvalidRequestError("base_url must be an HTTP(S) TCIA V2 service root")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0 or timeout > 120:
            raise InvalidRequestError("timeout must be greater than 0 and at most 120 seconds")
        if not isinstance(retries, int) or isinstance(retries, bool) or not 0 <= retries <= 3:
            raise InvalidRequestError("retries must be between 0 and 3")
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self._client = client or httpx.Client(base_url=self.base_url, timeout=timeout)
        self._closed = False

    def close(self) -> None:
        if not self._closed:
            self._client.close()
            self._closed = True

    def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise TransportError("Client is closed")
        method = method.upper()
        if method not in {"GET", "POST"} or not path.startswith("/v2/") or "?" in path or "#" in path:
            raise InvalidRequestError("Only explicit TCIA V2 read routes are supported")
        if method == "GET" and json_body is not None:
            raise InvalidRequestError("GET routes do not accept a JSON body")
        if method == "POST" and (params is not None or json_body is None):
            raise InvalidRequestError("POST searches require a JSON body and no URL parameters")
        # The currently mapped POST routes are searches. No transfer/write route is accepted.
        if method == "POST" and not path.endswith("/search"):
            raise InvalidRequestError("POST is supported only for V2 searches")
        for attempt in range(self.retries + 1):
            try:
                response = self._client.request(method, self.base_url + path, params=params, json=json_body)
            except httpx.RequestError as exc:
                if attempt < self.retries:
                    continue
                raise TransportError(str(exc), retryable=True) from exc
            if 300 <= response.status_code < 400:
                raise ProtocolError("Unexpected redirect from TCIA V2 service", status=response.status_code)
            if response.status_code >= 400:
                problem = _problem(response)
                if response.status_code >= 500 and problem["retryable"] and attempt < self.retries:
                    continue
                error_type = {404: NotFoundError, 410: RetiredRouteError, 422: InvalidRequestError, 503: ServiceUnavailableError}.get(response.status_code, HTTPResponseError)
                raise error_type(problem["detail"], code=problem["code"], status=response.status_code, retryable=problem["retryable"])
            try:
                payload = response.json()
            except ValueError as exc:
                raise ProtocolError("Service returned invalid JSON", status=response.status_code) from exc
            if not isinstance(payload, dict):
                raise ProtocolError("Service returned a non-object JSON response", status=response.status_code)
            return payload
        raise AssertionError("bounded retry loop exhausted")


def _problem(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {}
    code = body.get("code")
    detail = body.get("detail")
    retryable = body.get("retryable")
    return {
        "code": code if isinstance(code, str) else None,
        "detail": detail if isinstance(detail, str) else f"HTTP {response.status_code}",
        "retryable": retryable if isinstance(retryable, bool) else response.status_code in {502, 503, 504},
    }
