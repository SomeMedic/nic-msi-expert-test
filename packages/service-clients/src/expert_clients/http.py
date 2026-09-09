"""Bounded internal JSON transport. Callers own business-aware retry policy."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
import math
import re
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
from pydantic import BaseModel, SecretStr, ValidationError


class DependencyError(RuntimeError):
    def __init__(self, code: str, *, retryable: bool, status_code: int | None = None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code


class ServiceClient:
    def __init__(self, base_url: str, token: SecretStr, *, connect_timeout: float = 3,
                 pool_timeout: float = 5, max_connections: int = 8,
                 max_response_bytes: int = 16 * 1024 * 1024,
                 trace_headers: Callable[[], Mapping[str, str]] | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        try:
            parsed = urlsplit(base_url)
            clean_url = parsed.scheme in {"http", "https"} and parsed.hostname and not any((parsed.username, parsed.password, parsed.query, parsed.fragment))
        except ValueError:
            clean_url = False
        if not clean_url:
            raise ValueError("Service URL must be HTTP(S) without credentials, query or fragment")
        if not all(math.isfinite(value) and value > 0 for value in (connect_timeout, pool_timeout)):
            raise ValueError("Transport timeouts must be finite and positive")
        if max_connections < 1 or max_response_bytes < 1:
            raise ValueError("Transport limits must be positive")
        self._connect_timeout = connect_timeout
        self._pool_timeout = pool_timeout
        self._max_response_bytes = max_response_bytes
        self._trace_headers = trace_headers
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/", trust_env=False,
            headers={"Authorization": f"Bearer {token.get_secret_value()}"},
            timeout=httpx.Timeout(30, connect=connect_timeout, pool=pool_timeout),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections),
            follow_redirects=False, transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get[T: BaseModel](self, path: str, response_model: type[T],
                               *, request_id: UUID | None = None, timeout_seconds: float = 30,
                               expected_response_headers: Mapping[str, str] | None = None) -> T:
        return await self._request("GET", path, response_model, request_id=request_id,
                                   timeout_seconds=timeout_seconds,
                                   expected_response_headers=expected_response_headers)

    async def post[T: BaseModel](self, path: str, payload: BaseModel, response_model: type[T],
                                *, request_id: UUID | None = None, timeout_seconds: float = 30,
                                expected_response_headers: Mapping[str, str] | None = None) -> T:
        return await self._request("POST", path, response_model, json=payload.model_dump(mode="json"),
                                   request_id=request_id, timeout_seconds=timeout_seconds,
                                   expected_response_headers=expected_response_headers)

    async def _request[T: BaseModel](self, method: str, path: str, response_model: type[T], *,
                                    request_id: UUID | None, timeout_seconds: float,
                                    expected_response_headers: Mapping[str, str] | None = None,
                                    json: dict | None = None) -> T:
        if not path.startswith("/") or path.startswith("//") or any(character in path for character in (":", "?", "#", "\\", "%")) or ".." in path.split("/"):
            raise ValueError("Service path must be a fixed relative endpoint")
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Timeout must be finite and positive")
        correlation = str(request_id or uuid4())
        headers = {"X-Request-ID": correlation}
        # Optional framework-neutral propagation; never forward arbitrary
        # headers, baggage, credentials or untrusted caller correlation.
        if self._trace_headers is not None:
            try:
                parent = self._trace_headers().get("traceparent", "")
            except Exception:
                parent = ""  # Optional telemetry cannot prevent a business call.
            if (isinstance(parent, str) and re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]", parent)
                    and int(parent[3:35], 16) and int(parent[36:52], 16)):
                headers["traceparent"] = parent
        try:
            async with asyncio.timeout(timeout_seconds):
                async with self._client.stream(
                    method, path.lstrip("/"), json=json,
                    headers=headers,
                    timeout=httpx.Timeout(timeout_seconds, connect=self._connect_timeout, pool=self._pool_timeout),
                ) as response:
                    if response.status_code < 200 or response.status_code >= 300:
                        raise DependencyError(
                            "DEPENDENCY_REJECTED", retryable=response.status_code in {429, 502, 503, 504},
                            status_code=response.status_code,
                        )
                    for name, expected in (expected_response_headers or {}).items():
                        if response.headers.get(name) != expected:
                            raise DependencyError("DEPENDENCY_CONTRACT_VIOLATION", retryable=False)
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > self._max_response_bytes:
                            raise DependencyError("DEPENDENCY_RESPONSE_TOO_LARGE", retryable=False)
                        body.extend(chunk)
        except (httpx.TimeoutException, TimeoutError):
            raise DependencyError("DEPENDENCY_TIMEOUT", retryable=True) from None
        except httpx.HTTPError:
            raise DependencyError("DEPENDENCY_UNAVAILABLE", retryable=True) from None
        try:
            return response_model.model_validate_json(body)
        except (ValidationError, ValueError):
            raise DependencyError("DEPENDENCY_CONTRACT_VIOLATION", retryable=False) from None
