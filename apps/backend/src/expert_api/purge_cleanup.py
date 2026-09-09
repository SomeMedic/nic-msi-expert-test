"""Delete only exact objects in a confirmed, fenced PostgreSQL purge manifest.

SQL owns references, acceptance, retry exhaustion, finalization and late-PUT
audits. Storage I/O never holds a transaction. DELETE IfMatch is only an extra
precondition: the pinned MinIO does not enforce it, so immutable manifest
identity and fresh SQL authorization are the deletion authority.
"""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime
import logging
import math
import re
import time
from typing import Any, Protocol
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from psycopg.rows import dict_row

from expert_api.errors import database_failures
from expert_api.source_viewer import drain_blocking
from expert_clients.dependencies import DependencyHealth
from expert_clients.settings import Settings
from expert_contracts.purge import PurgeFailureCode, PurgeObjectClaim

logger = logging.getLogger(__name__)


class PurgeCleanupStorage(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> dict[str, Any]: ...


class PurgeDeferred(Exception):
    """The object remains durably pending; no provider text is retained."""
    def __init__(self, code: PurgeFailureCode | None = None):
        self.code = code
        super().__init__(code.value if code is not None else "PURGE_AUTHORITY_UNAVAILABLE")


class PurgeCleanupService:
    def __init__(self, settings: Settings, pool, storage: PurgeCleanupStorage,
                 *, owner: UUID | None = None):
        self.settings, self.pool, self.storage = settings, pool, storage
        self.owner = owner or uuid4()
        self._stop = asyncio.Event()
        self._iteration = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._healthy = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Purge cleanup already started")
        self._stop.clear()
        self._task = asyncio.create_task(self._supervise(), name="purge-cleanup")

    async def close(self) -> None:
        self._stop.set()
        task = asyncio.create_task(self._close())
        interrupted = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                interrupted = True
        task.result()
        if interrupted:
            raise asyncio.CancelledError

    async def _close(self) -> None:
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        # Directly invoked cleanup_once also drains before shared transports close.
        async with self._iteration:
            self._task = None
            self._healthy = False

    def health(self) -> DependencyHealth:
        return DependencyHealth("purge_cleanup", self._task is not None
                                and not self._task.done() and self._healthy)

    async def _supervise(self) -> None:
        while not self._stop.is_set():
            await self.cleanup_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.purge_cleanup_poll_seconds)
            except TimeoutError:
                pass

    async def _rows(self, query: str, parameters: tuple, *, deadline: float | None = None) -> list[dict[str, Any]]:
        timeout = self.settings.purge_cleanup_operation_timeout_seconds
        if deadline is not None:
            self._live(deadline)
            timeout = min(timeout, deadline - time.monotonic())
        with database_failures():
            async with asyncio.timeout(timeout):
                async with self.pool.connection() as connection, connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout = '1000ms'")
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute(query, parameters)
                        return list(await cursor.fetchall())

    def _validate_claim(self, value: object) -> PurgeObjectClaim:
        claim = PurgeObjectClaim.model_validate(value)
        expected_bucket = (self.settings.s3_bucket_originals if claim.kind == "original"
                           else self.settings.s3_bucket_artifacts)
        if (claim.bucket != expected_bucket
                or (claim.kind == "original" and claim.size_bytes > self.settings.upload_max_bytes)):
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)
        version = claim.object_version_id
        if version is not None and (not 1 <= len(version) <= 1024 or any(ord(c) < 32 for c in version)):
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)
        return claim

    async def _claim(self) -> tuple[PurgeObjectClaim, float] | None:
        started = time.monotonic()
        rows = await self._rows(
            "SELECT value, clock_timestamp() AS observed_at FROM app.claim_purge_objects(%s,1,%s) AS value",
            (self.owner, self.settings.purge_cleanup_lease_seconds),
        )
        if not rows:
            return None
        if len(rows) != 1:
            raise PurgeDeferred()
        claim = self._validate_claim(rows[0]["value"])
        observed = rows[0]["observed_at"]
        if not isinstance(observed, datetime) or observed.tzinfo is None:
            raise PurgeDeferred()
        remaining = (claim.claim_until - observed).total_seconds()
        if not 0 < remaining <= self.settings.purge_cleanup_lease_seconds:
            raise PurgeDeferred()
        # PG lease time is authoritative; local elapsed time consumes its budget.
        return claim, started + remaining  # Transaction commits before returning.

    def _live(self, deadline: float) -> None:
        if self._stop.is_set() or time.monotonic() >= deadline:
            raise PurgeDeferred()

    @staticmethod
    def _identity(claim: PurgeObjectClaim, owner: UUID) -> tuple:
        return claim.plan_id, claim.object_id, owner, claim.claim_token

    async def _authorize(self, claim: PurgeObjectClaim, deadline: float) -> None:
        rows = await self._rows("SELECT app.authorize_purge_delete(%s,%s,%s,%s) AS value",
                                self._identity(claim, self.owner), deadline=deadline)
        if len(rows) != 1 or rows[0]["value"] is None:
            raise PurgeDeferred()
        current = self._validate_claim(rows[0]["value"])
        if current != claim:
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)

    async def _head(self, claim: PurgeObjectClaim, deadline: float) -> dict[str, Any] | None:
        self._live(deadline)
        parameters: dict[str, Any] = {
            "Bucket": claim.bucket, "Key": claim.object_key, "ChecksumMode": "ENABLED",
        }
        if claim.object_version_id is not None:
            parameters["VersionId"] = claim.object_version_id
        try:
            async with asyncio.timeout(min(self.settings.purge_cleanup_operation_timeout_seconds,
                                           deadline - time.monotonic())):
                return await drain_blocking(lambda: self.storage.head_object(**parameters))
        except ClientError as error:
            if (error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                    and error.response.get("Error", {}).get("Code") in {"404", "NotFound", "NoSuchKey", "NoSuchVersion"}):
                return None
            raise

    @staticmethod
    def _delete_request(claim: PurgeObjectClaim, head: dict[str, Any]) -> dict[str, Any]:
        if (head.get("ContentLength") != claim.size_bytes or head.get("ContentType") != claim.media_type
                or head.get("Metadata", {}).get("sha256") != claim.sha256 or head.get("DeleteMarker", False)
                or head.get("VersionId") != claim.object_version_id):
            # Never resolve an unrecorded storage version outside the manifest.
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)
        checksum = head.get("ChecksumSHA256")
        if checksum is not None and checksum != base64.b64encode(bytes.fromhex(claim.sha256)).decode("ascii"):
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)
        etag = head.get("ETag")
        if not isinstance(etag, str) or re.fullmatch(r'"[0-9a-fA-F]{32}(?:-[1-9][0-9]*)?"', etag) is None:
            raise PurgeDeferred(PurgeFailureCode.OBJECT_MISMATCH)
        request = {"Bucket": claim.bucket, "Key": claim.object_key, "IfMatch": etag}
        if claim.object_version_id is not None:
            request["VersionId"] = claim.object_version_id
        return request

    async def _clean(self, claim: PurgeObjectClaim, deadline: float) -> None:
        head = await self._head(claim, deadline)
        request = self._delete_request(claim, head) if head is not None else None
        # Recheck durable references/fence immediately before DELETE, after HEAD.
        # Missing-object replay also requires authority before it can be marked.
        await self._authorize(claim, deadline)
        if request is not None:
            self._live(deadline)
            async with asyncio.timeout(min(self.settings.purge_cleanup_operation_timeout_seconds,
                                           deadline - time.monotonic())):
                result = await drain_blocking(lambda: self.storage.delete_object(**request))
            if result.get("ResponseMetadata", {}).get("HTTPStatusCode") not in {200, 204}:
                raise PurgeDeferred(PurgeFailureCode.DELETE_UNVERIFIED)
            if await self._head(claim, deadline) is not None:
                raise PurgeDeferred(PurgeFailureCode.DELETE_UNVERIFIED)
        self._live(deadline)
        rows = await self._rows("SELECT app.mark_purge_object_deleted(%s,%s,%s,%s) AS value",
                                self._identity(claim, self.owner), deadline=deadline)
        if len(rows) != 1 or rows[0]["value"] is not True:
            raise PurgeDeferred()
        # The final mark atomically calls SQL finalization when all objects are
        # confirmed. Empty manifests finalize during acceptance. There is no
        # non-atomic Python "last object -> finalize" gap or early finalizer call.

    async def _reschedule(self, claim: PurgeObjectClaim, deadline: float, code: PurgeFailureCode) -> None:
        self._live(deadline)
        delay = min(3600, max(1, math.ceil(self.settings.purge_cleanup_poll_seconds * 2 ** (claim.attempt - 1))))
        if claim.attempt == 8:
            code = PurgeFailureCode.ATTEMPTS_EXHAUSTED
        rows = await self._rows("SELECT app.reschedule_purge_object(%s,%s,%s,%s,%s,%s) AS value",
            (*self._identity(claim, self.owner), code.value, delay), deadline=deadline)
        if len(rows) != 1 or rows[0]["value"] is not True:
            raise PurgeDeferred()

    async def cleanup_once(self) -> bool:
        async with self._iteration:
            if self._stop.is_set():
                return False
            healthy = True
            try:
                for _ in range(self.settings.purge_cleanup_batch_size):
                    if self._stop.is_set():
                        break
                    selected = await self._claim()
                    if selected is None:
                        break
                    claim, deadline = selected
                    try:
                        await self._clean(claim, deadline)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        healthy = False
                        code = error.code if isinstance(error, PurgeDeferred) else PurgeFailureCode.STORAGE_UNAVAILABLE
                        if code is not None and not self._stop.is_set() and time.monotonic() < deadline:
                            await self._reschedule(claim, deadline, code)
            except asyncio.CancelledError:
                self._healthy = False
                raise
            except Exception:
                healthy = False
            self._healthy = healthy and not self._stop.is_set()
            if not self._healthy:
                logger.error("dependency.failed", extra={"safe_fields": {"error_code": "DEPENDENCY_UNAVAILABLE"}})
            return self._healthy
