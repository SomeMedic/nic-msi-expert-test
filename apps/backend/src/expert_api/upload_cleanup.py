"""Clean exact immutable upload keys after PostgreSQL makes them unattachable.

All writers must use reserved server identities and conditional IfNoneMatch PUT;
this service is not a cleaner for arbitrary mutable or externally shared keys.
The pinned MinIO ignores DELETE IfMatch, so that header is not the deletion
authority. PostgreSQL claim/reference checks and canonical immutable-key ownership
provide the boundary. SQL schedules repeated audits of cleaned intents to remove
late native PUT completions; an asyncio timeout alone cannot rule those out.
All S3 calls run after claim commit and finish before service resources close.
"""
from __future__ import annotations

import asyncio
import base64
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
import logging
import re
import time
from typing import Any, Protocol, TypeVar
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from expert_clients.dependencies import DependencyHealth
from expert_clients.settings import Settings

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CleanupStorage(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> dict[str, Any]: ...


class CleanupDeferred(Exception):
    """An unresolved object or expired ownership remains visible in PostgreSQL."""


@dataclass(frozen=True)
class CleanupClaim:
    intent_id: UUID
    token: UUID
    bucket: str
    key: str
    sha256: str
    size_bytes: int
    version_id: str | None
    deadline: float


async def _drain(task: asyncio.Task[Any]) -> None:
    # Cancelling to_thread does not cancel the native SDK call. Do not let the
    # caller close the client while that thread can still use it.
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            continue
        except Exception:
            break
    if not task.cancelled():
        task.exception()  # Retrieve a provider failure without logging its body.


class UploadCleanupService:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, storage: CleanupStorage,
                 *, owner: UUID | None = None):
        self.settings = settings
        self.pool = pool
        self.storage = storage
        self.owner = owner or uuid4()
        self._stop = asyncio.Event()
        self._iteration = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._healthy = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Upload cleanup is already started")
        self._stop = asyncio.Event()
        self._task = asyncio.create_task(self._supervise(), name="upload-cleanup")

    async def close(self) -> None:
        self._stop.set()
        drain = asyncio.create_task(self._close())
        try:
            await asyncio.shield(drain)
        except asyncio.CancelledError:
            await _drain(drain)
            raise

    async def _close(self) -> None:
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        # Also drain a directly invoked cleanup_once before releasing resources.
        async with self._iteration:
            self._task = None
            self._healthy = False

    def health(self) -> DependencyHealth:
        running = self._task is not None and not self._task.done()
        return DependencyHealth("upload_cleanup", running and self._healthy)

    async def _supervise(self) -> None:
        while not self._stop.is_set():
            await self.cleanup_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.upload_cleanup_poll_seconds)
            except TimeoutError:
                pass

    async def cleanup_once(self) -> bool:
        async with self._iteration:
            if self._stop.is_set():
                return False
            try:
                self._healthy = await self._cleanup()
            except asyncio.CancelledError:
                self._healthy = False
                raise
            except Exception:
                self._healthy = False
                logger.error("dependency.failed", extra={"safe_fields": {"error_code": "DEPENDENCY_UNAVAILABLE"}})
            return self._healthy

    async def _claim(self) -> tuple[Mapping[str, Any] | None, bool, float]:
        started = time.monotonic()
        async with asyncio.timeout(self.settings.upload_cleanup_operation_timeout_seconds):
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(
                            "SELECT u.*, clock_timestamp() AS observed_at "
                            "FROM app.claim_upload_cleanup(%s, 1, %s) AS u",
                            (self.owner, self.settings.upload_cleanup_lease_seconds),
                        )
                        rows = await cursor.fetchall()
                        if len(rows) > 1:
                            raise CleanupDeferred()
                        if rows:
                            return rows[0], False, started
                        await cursor.execute(
                            "SELECT EXISTS(SELECT 1 FROM app.upload_intents "
                            "WHERE state='cleanup_pending') AS pending"
                        )
                        status = await cursor.fetchone()
                        if status is None:
                            raise CleanupDeferred()
                        return None, bool(status["pending"]), started
        # Transaction and connection context exits commit the claim before return.

    def _validate_claim(self, row: Mapping[str, Any], started: float) -> CleanupClaim:
        intent_id, token = UUID(str(row["id"])), UUID(str(row["cleanup_token"]))
        document, version = UUID(str(row["document_id"])), UUID(str(row["version_id"]))
        sha = row["source_sha256"]
        if not isinstance(sha, str) or re.fullmatch(r"[0-9a-f]{64}", sha) is None:
            raise CleanupDeferred()
        key = f"originals/{document}/{version}/{sha}.pdf"
        if row["state"] != "cleanup_pending" or UUID(str(row["cleanup_owner"])) != self.owner:
            raise CleanupDeferred()
        if row["bucket"] != self.settings.s3_bucket_originals or row["object_key"] != key:
            raise CleanupDeferred()
        size = row["size_bytes"]
        if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= self.settings.upload_max_bytes:
            raise CleanupDeferred()
        observed, expires, until = row["observed_at"], row["expires_at"], row["cleanup_until"]
        if any(not isinstance(value, datetime) or value.tzinfo is None for value in (observed, expires, until)):
            raise CleanupDeferred()
        remaining = (until - observed).total_seconds()
        if (observed - expires).total_seconds() < 3600 or not 0 < remaining <= self.settings.upload_cleanup_lease_seconds:
            raise CleanupDeferred()
        object_version = row["object_version_id"]
        if object_version is not None and (
            not isinstance(object_version, str) or not 1 <= len(object_version) <= 1024
            or any(ord(char) < 32 for char in object_version)
        ):
            raise CleanupDeferred()
        return CleanupClaim(intent_id, token, row["bucket"], key, sha, size, object_version,
                            started + remaining)

    def _live(self, claim: CleanupClaim) -> None:
        if self._stop.is_set() or time.monotonic() >= claim.deadline:
            raise CleanupDeferred()

    async def _storage_call(self, operation: Callable[..., T], **kwargs: Any) -> T:
        task = asyncio.create_task(asyncio.to_thread(operation, **kwargs))
        try:
            async with asyncio.timeout(self.settings.upload_cleanup_operation_timeout_seconds):
                return await asyncio.shield(task)
        except BaseException:
            await _drain(task)
            raise

    @staticmethod
    def _absent(error: ClientError) -> bool:
        return (error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                and error.response.get("Error", {}).get("Code") in {"404", "NotFound", "NoSuchKey", "NoSuchVersion"})

    async def _head(self, claim: CleanupClaim, version: str | None) -> dict[str, Any] | None:
        self._live(claim)
        request: dict[str, Any] = {"Bucket": claim.bucket, "Key": claim.key, "ChecksumMode": "ENABLED"}
        if version is not None:
            request["VersionId"] = version
        try:
            return await self._storage_call(self.storage.head_object, **request)
        except ClientError as error:
            if self._absent(error):
                return None
            raise

    def _verified_delete(self, claim: CleanupClaim, head: Mapping[str, Any]) -> dict[str, Any]:
        if (head.get("Metadata", {}).get("sha256") != claim.sha256
                or head.get("ContentLength") != claim.size_bytes
                or head.get("ContentType") != "application/pdf" or head.get("DeleteMarker", False)):
            raise CleanupDeferred()
        checksum = head.get("ChecksumSHA256")
        if checksum is not None and checksum != base64.b64encode(bytes.fromhex(claim.sha256)).decode("ascii"):
            raise CleanupDeferred()
        version = head.get("VersionId")
        if version is not None and (not isinstance(version, str) or not 1 <= len(version) <= 1024):
            raise CleanupDeferred()
        if claim.version_id is not None and version != claim.version_id:
            raise CleanupDeferred()
        etag = head.get("ETag")
        # Keep a concrete precondition for servers that implement it; the pinned
        # MinIO does not. Never retry a rejected precondition without this header.
        if not isinstance(etag, str) or re.fullmatch(r'"[0-9a-fA-F]{32}(?:-[1-9][0-9]*)?"', etag) is None:
            raise CleanupDeferred()
        request = {"Bucket": claim.bucket, "Key": claim.key, "IfMatch": etag}
        if version is not None:
            request["VersionId"] = version
        return request

    async def _mark(self, claim: CleanupClaim) -> bool:
        self._live(claim)
        async with asyncio.timeout(min(self.settings.upload_cleanup_operation_timeout_seconds,
                                       claim.deadline - time.monotonic())):
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute("SELECT app.mark_upload_cleaned(%s,%s,%s) AS cleaned",
                                             (claim.intent_id, self.owner, claim.token))
                        row = await cursor.fetchone()
                        return bool(row and row["cleaned"] is True)

    async def _cleanup(self) -> bool:
        row, pending, started = await self._claim()
        if row is None:
            return not pending
        claim = self._validate_claim(row, started)
        head = await self._head(claim, claim.version_id)
        if head is not None:
            request = self._verified_delete(claim, head)
            self._live(claim)
            deleted = await self._storage_call(self.storage.delete_object, **request)
            if deleted.get("ResponseMetadata", {}).get("HTTPStatusCode") not in {200, 204}:
                raise CleanupDeferred()
            if await self._head(claim, request.get("VersionId")) is not None:
                raise CleanupDeferred()
        # Definitive absence also reconciles DELETE-success / mark-failure retries.
        return await self._mark(claim)
