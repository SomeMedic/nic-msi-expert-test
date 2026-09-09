"""Opt-in capture uses registry authority before and after bounded object I/O."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from collections.abc import Callable
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Protocol, TypeVar
from urllib.parse import unquote, urlsplit
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import SecretStr, ValidationError
from starlette.responses import Response

from expert_api.auth import Principal
from expert_api.errors import ApiError, DATABASE_CODES
from expert_api.source_viewer import drain_blocking
from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole
from expert_contracts.debug import DebugCaptureList
from expert_contracts.debug_capture import (
    CAPTURE_ENVELOPE_MAX_BYTES, DebugCaptureReceipt, DebugCaptureSubmission,
    DebugObjectRef, capture_object_bytes, capture_registry_metadata,
)
from expert_contracts.errors import ErrorCode

T = TypeVar("T")
CAPTURE_TIMEOUT_SECONDS = 2.0
DOWNLOAD_TIMEOUT_SECONDS = 10.0
CHUNK_BYTES = 64 * 1024


class CaptureStorage(Protocol):
    def put_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@contextmanager
def capture_database_errors():
    """The SQL exception body and private validation input never become messages."""
    try:
        yield
    except psycopg.Error as error:
        capture_codes = {
            "DEBUG_CAPTURE_DISABLED": ErrorCode.FORBIDDEN,
            "DEBUG_CAPTURE_EXPIRED": ErrorCode.FORBIDDEN,
            "STALE_EXECUTION": ErrorCode.TERMINAL_CONFLICT,
            "RUN_NOT_RUNNING": ErrorCode.TERMINAL_CONFLICT,
            "RUN_CANCELLED": ErrorCode.RUN_CANCELLED,
            "INVALID_DEBUG_ARGUMENT": ErrorCode.INVALID_REQUEST,
            "DEBUG_OBJECT_MISMATCH": ErrorCode.SOURCE_UNAVAILABLE,
            "STALE_DEBUG_CLEANUP": ErrorCode.TERMINAL_CONFLICT,
        }
        name = error.diag.message_primary or ""
        code = capture_codes.get(name, DATABASE_CODES.get(name))
        if code is None:
            code = ErrorCode.DEPENDENCY_UNAVAILABLE
        raise ApiError(code) from None


def unavailable(*, oversized: bool = False) -> DebugCaptureReceipt:
    return DebugCaptureReceipt(state="oversized" if oversized else "unavailable", part_id=None, expires_at=None)


def _object_ref(value: object) -> DebugObjectRef:
    try:
        return DebugObjectRef.model_validate(value)
    except (ValidationError, ValueError, TypeError):
        raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None


def known_credentials(settings: Settings) -> tuple[bytes, ...]:
    """Inspect only configured secrets; no environment/header/exception capture."""
    values: set[bytes] = set()
    for name in type(settings).model_fields:
        secret = getattr(settings, name)
        if not isinstance(secret, SecretStr):
            continue
        value = secret.get_secret_value()
        if value:
            values.add(value.encode("utf-8"))
            values.add(json.dumps(value, ensure_ascii=True)[1:-1].encode("ascii"))
        if name in {"database_dsn", "redis_url"}:
            try:
                password = urlsplit(value).password
                if password:
                    values.add(unquote(password).encode("utf-8"))
            except ValueError:
                pass
    return tuple(values)


def validate_request_bytes(raw: bytes) -> None:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError()
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if not isinstance(value, dict) or json.dumps(value, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False).encode("utf-8") != raw:
            raise ValueError()
    except (ValueError, UnicodeError, RecursionError):
        raise ApiError(ErrorCode.VALIDATION_ERROR) from None


class DebugCaptureService:
    def __init__(self, settings: Settings, pool, storage: CaptureStorage):
        self.settings, self.pool, self.storage = settings, pool, storage
        self._secrets = known_credentials(settings)
        self._receive = asyncio.Semaphore(2)
        self._read = asyncio.Semaphore(2)
        self._inflight: set[asyncio.Task[Any]] = set()
        self._operations: set[asyncio.Task[Any]] = set()
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        # Root closes storage only after this service has drained bounded I/O.
        cancelled = False
        while self._inflight or self._operations:
            drain = asyncio.gather(*tuple(self._inflight | self._operations), return_exceptions=True)
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    cancelled = True
        if cancelled:
            raise asyncio.CancelledError

    @asynccontextmanager
    async def _operation(self):
        if self._closed:
            raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
        task = asyncio.current_task()
        if task is None:
            raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
        self._operations.add(task)
        try:
            yield
        finally:
            self._operations.discard(task)

    async def _call(self, operation: Callable[[], T]) -> T:
        task = asyncio.create_task(drain_blocking(operation))
        self._inflight.add(task)
        try:
            return await task
        finally:
            self._inflight.discard(task)

    async def _one(self, query: str, parameters: tuple) -> Any:
        with capture_database_errors():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout = '1000ms'")
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(query, parameters)
                        row = await cursor.fetchone()
                        if row is None:
                            raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
                        return row["value"]

    async def _failure(self, submission: DebugCaptureSubmission, reason: str) -> None:
        context = submission.context
        try:
            await self._one("SELECT agent.mark_debug_part_unavailable(%s,%s,%s,%s,%s,%s) AS value",
                            (context.run_id, context.owner, context.execution_epoch,
                             context.call_id, submission.part.part, reason))
        except ApiError:
            pass  # A best-effort marker never masks the original capture outcome.

    async def submit(self, run_id: UUID, submission: DebugCaptureSubmission) -> DebugCaptureReceipt:
        if run_id != submission.context.run_id:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        if not self.settings.debug_capture_allowed or self._closed:
            return unavailable()
        try:
            async with self._operation(), asyncio.timeout(CAPTURE_TIMEOUT_SECONDS), self._receive:
                if self._closed:
                    return unavailable()
                raw = submission.part.payload_bytes()
                wire = submission.model_dump_json().encode("utf-8")
                if any(secret in raw or secret in wire for secret in self._secrets):
                    await self._failure(submission, "credential_detected")
                    return unavailable()
                if submission.part.part == "request":
                    validate_request_bytes(raw)
                return await self._submit(submission)
        except TimeoutError:
            return unavailable()
        except (BotoCoreError, ClientError):
            return unavailable()

    async def listing(self, run_id: UUID, principal: Principal) -> DebugCaptureList:
        principal.require(ApplicationRole.OPERATOR)
        try:
            async with self._operation(), asyncio.timeout(DOWNLOAD_TIMEOUT_SECONDS):
                value = await self._one("SELECT agent.list_debug_parts(%s,%s) AS value", (run_id, principal.subject))
            if value is None:
                raise ApiError(ErrorCode.NOT_FOUND)
            listing = DebugCaptureList.model_validate(value)
            if listing.run_id != run_id:
                raise ValueError()
            if not self.settings.debug_capture_allowed:
                # Preserve immutable consent/expiry; deployment policy only closes availability.
                items = [item.model_copy(update={"download_url": None}) for item in listing.items]
                status = "unavailable" if listing.enabled and listing.status != "expired" else listing.status
                listing = DebugCaptureList.model_validate(listing.model_copy(update={"items": items, "status": status}).model_dump())
            return listing
        except (ValidationError, ValueError, TypeError):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None
        except TimeoutError:
            raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None

    async def _submit(self, submission: DebugCaptureSubmission) -> DebugCaptureReceipt:
        context, part = submission.context, submission.part
        body = capture_object_bytes(submission)
        sha = hashlib.sha256(body).hexdigest()
        reference = _object_ref(await self._one(
            "SELECT agent.reserve_debug_part(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) AS value",
            (context.run_id, context.owner, context.execution_epoch, context.call_id, part.part,
             part.role, context.schema_attempt, part.request_sha256, part.payload_sha256, sha,
             len(body), Jsonb(capture_registry_metadata(submission))),
        ))
        expected_key = (f"runs/{context.run_id}/debug/{context.execution_epoch}/{context.call_id}/"
                        f"{part.part}/{reference.object_id}/{sha}.json")
        if (reference.bucket != self.settings.s3_bucket_debug or reference.object_key != expected_key
                or reference.sha256 != sha or reference.size_bytes != len(body)
                or reference.state not in {"reserved", "attached"}):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        # The reservation is committed by _one before the first network write.
        version = await self._call(lambda: self._put(reference, body))
        attached = _object_ref(await self._one(
            "SELECT agent.attach_debug_part(%s,%s,%s,%s,%s,%s) AS value",
            (reference.part_id, context.owner, context.execution_epoch, sha, len(body), version),
        ))
        if (attached.state != "attached" or attached.part_id != reference.part_id
                or attached.object_id != reference.object_id or attached.object_key != reference.object_key
                or attached.sha256 != sha or attached.size_bytes != len(body)
                or attached.expires_at != reference.expires_at or attached.object_version_id != version):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return DebugCaptureReceipt(state="attached", part_id=attached.part_id, expires_at=attached.expires_at)

    def _put(self, reference: DebugObjectRef, body: bytes) -> str | None:
        if reference.state == "attached":
            if not hmac.compare_digest(self._fetch(reference), body):
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            return reference.object_version_id
        try:
            response = self.storage.put_object(
                Bucket=reference.bucket, Key=reference.object_key, Body=body,
                ContentType="application/json", IfNoneMatch="*",
                Metadata={"sha256": reference.sha256},
                ChecksumSHA256=base64.b64encode(bytes.fromhex(reference.sha256)).decode("ascii"),
            )
        except ClientError as error:
            if error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 412:
                raise
            # Lost PUT acknowledgement: verify exact bytes, never overwrite.
            data, version = self._fetch_version(reference)
            if not hmac.compare_digest(data, body):
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            return version
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") not in {200, 201}:
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return self._version(response.get("VersionId"))

    @staticmethod
    def _version(value: Any) -> str | None:
        if value is not None and (not isinstance(value, str) or not 1 <= len(value) <= 1000
                                  or any(ord(char) < 32 for char in value)):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return value

    def _fetch(self, reference: DebugObjectRef) -> bytes:
        return self._fetch_version(reference)[0]

    def _fetch_version(self, reference: DebugObjectRef) -> tuple[bytes, str | None]:
        arguments = {"Bucket": reference.bucket, "Key": reference.object_key, "ChecksumMode": "ENABLED"}
        if reference.object_version_id is not None:
            arguments["VersionId"] = reference.object_version_id
        response = self.storage.get_object(**arguments)
        stream = response.get("Body")
        try:
            if stream is None:
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            version = self._version(response.get("VersionId"))
            if (response.get("ContentLength") != reference.size_bytes
                    or response.get("ContentType") != "application/json"
                    or response.get("Metadata", {}).get("sha256") != reference.sha256
                    or response.get("DeleteMarker", False)
                    or (reference.object_version_id is not None and version != reference.object_version_id)):
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            data = bytearray()
            while True:
                chunk = stream.read(min(CHUNK_BYTES, reference.size_bytes + 1 - len(data)))
                if not chunk:
                    break
                data.extend(chunk)
                if len(data) > reference.size_bytes or len(data) > CAPTURE_ENVELOPE_MAX_BYTES:
                    raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            if len(data) != reference.size_bytes or hashlib.sha256(data).hexdigest() != reference.sha256:
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            return bytes(data), version
        finally:
            if stream is not None:
                stream.close()

    async def _authorize(self, run_id: UUID, part_id: UUID, principal: Principal) -> DebugObjectRef:
        value = await self._one("SELECT agent.authorize_debug_part(%s,%s,%s) AS value",
                                (run_id, part_id, principal.subject))
        if value is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        reference = _object_ref(value)
        if (reference.bucket != self.settings.s3_bucket_debug or reference.part_id != part_id or reference.state != "attached"
                or reference.object_key.split("/")[1] != str(run_id)):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return reference

    async def download(self, run_id: UUID, part_id: UUID, principal: Principal) -> Response:
        principal.require(ApplicationRole.OPERATOR)
        if not self.settings.debug_capture_allowed or self._closed:
            raise ApiError(ErrorCode.FORBIDDEN)
        try:
            async with self._operation(), asyncio.timeout(DOWNLOAD_TIMEOUT_SECONDS), self._read:
                reference = await self._authorize(run_id, part_id, principal)
                body = await self._call(lambda: self._fetch(reference))
                # Consent, principal, expiry and current revoke are rechecked after I/O.
                if await self._authorize(run_id, part_id, principal) != reference:
                    raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                return Response(body, media_type="application/json", headers={
                    "Cache-Control": "no-store, private", "Pragma": "no-cache",
                    "Content-Disposition": f'attachment; filename="debug-{part_id}.json"',
                    "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
                    "Content-Security-Policy": "sandbox; default-src 'none'",
                    "Cross-Origin-Resource-Policy": "same-origin",
                })
        except TimeoutError:
            raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
        except (BotoCoreError, ClientError):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None
