"""Bounded retention cleanup, confined to exact SQL-claimed immutable debug keys."""
from __future__ import annotations

import asyncio
import base64
from datetime import datetime
import logging
import re
import time
from typing import Any, Protocol
from uuid import UUID, uuid4

from botocore.exceptions import ClientError
from psycopg.rows import dict_row

from expert_api.debug_capture import capture_database_errors
from expert_api.source_viewer import drain_blocking
from expert_clients.dependencies import DependencyHealth
from expert_clients.settings import Settings
from expert_contracts.debug_capture import DebugCleanupClaim

logger = logging.getLogger(__name__)
POLL_SECONDS = 60.0
BATCH_LIMIT = 20
LEASE_SECONDS = 30


class DebugCleanupStorage(Protocol):
    def head_object(self, **kwargs: Any) -> dict[str, Any]: ...
    def delete_object(self, **kwargs: Any) -> dict[str, Any]: ...


class CleanupDeferred(Exception):
    """Identity/authority has not been proved; the claimed object is not deleted."""


class DebugCleanupService:
    def __init__(self, settings: Settings, pool, storage: DebugCleanupStorage, *, owner: UUID | None = None):
        self.settings, self.pool, self.storage = settings, pool, storage
        self.owner = owner or uuid4()
        self._stop = asyncio.Event()
        self._iteration = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None
        self._healthy = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("Debug cleanup already started")
        self._stop.clear()
        self._task = asyncio.create_task(self._supervise(), name="debug-cleanup")

    async def close(self) -> None:
        self._stop.set()
        task = asyncio.create_task(self._close())
        cancelled = False
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                cancelled = True
        task.result()
        if cancelled:
            raise asyncio.CancelledError

    async def _close(self) -> None:
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        async with self._iteration:
            self._task = None
            self._healthy = False

    def health(self) -> DependencyHealth:
        return DependencyHealth("debug_cleanup", self._task is not None and not self._task.done() and self._healthy)

    async def _supervise(self) -> None:
        while not self._stop.is_set():
            await self.cleanup_once()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=POLL_SECONDS)
            except TimeoutError:
                pass

    async def _rows(self, query: str, parameters: tuple) -> list[dict[str, Any]]:
        with capture_database_errors():
            async with asyncio.timeout(2):
                async with self.pool.connection() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout = '1000ms'")
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute(query, parameters)
                            return list(await cursor.fetchall())

    async def _claim(self) -> tuple[DebugCleanupClaim, float] | None:
        started = time.monotonic()
        rows = await self._rows(
            "SELECT value, clock_timestamp() AS observed_at "
            "FROM agent.claim_debug_cleanup(%s,1,%s) AS value", (self.owner, LEASE_SECONDS))
        if not rows:
            return None
        if len(rows) != 1:
            raise CleanupDeferred()
        claim = DebugCleanupClaim.model_validate(rows[0]["value"])
        observed = rows[0]["observed_at"]
        if not isinstance(observed, datetime) or observed.tzinfo is None:
            raise CleanupDeferred()
        remaining = (claim.claim_until - observed).total_seconds()
        if claim.bucket != self.settings.s3_bucket_debug or not 0 < remaining <= LEASE_SECONDS:
            raise CleanupDeferred()
        return claim, started + remaining  # Claim transaction committed before S3.

    def _live(self, deadline: float) -> None:
        if self._stop.is_set() or time.monotonic() >= deadline:
            raise CleanupDeferred()

    async def _head(self, claim: DebugCleanupClaim, version: str | None, deadline: float) -> dict[str, Any] | None:
        self._live(deadline)
        request: dict[str, Any] = {"Bucket": claim.bucket, "Key": claim.object_key, "ChecksumMode": "ENABLED"}
        if version is not None:
            request["VersionId"] = version
        try:
            async with asyncio.timeout(min(3, deadline - time.monotonic())):
                return await drain_blocking(lambda: self.storage.head_object(**request))
        except ClientError as error:
            if (error.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404
                    and error.response.get("Error", {}).get("Code") in {"404", "NotFound", "NoSuchKey", "NoSuchVersion"}):
                return None
            raise

    @staticmethod
    def _delete_request(claim: DebugCleanupClaim, head: dict[str, Any]) -> dict[str, Any]:
        if (head.get("ContentLength") != claim.size_bytes or head.get("ContentType") != "application/json"
                or head.get("Metadata", {}).get("sha256") != claim.sha256 or head.get("DeleteMarker", False)):
            raise CleanupDeferred()
        checksum = head.get("ChecksumSHA256")
        if checksum is not None and checksum != base64.b64encode(bytes.fromhex(claim.sha256)).decode("ascii"):
            raise CleanupDeferred()
        version = head.get("VersionId")
        if version is not None and (not isinstance(version, str) or not 1 <= len(version) <= 1000
                                    or any(ord(char) < 32 for char in version)):
            raise CleanupDeferred()
        if claim.object_version_id is not None and claim.object_version_id != version:
            raise CleanupDeferred()
        etag = head.get("ETag")
        if not isinstance(etag, str) or not re.fullmatch(r'"[0-9a-fA-F]{32}(?:-[1-9][0-9]*)?"', etag):
            raise CleanupDeferred()
        # Pinned MinIO ignores DELETE IfMatch. Authority comes from the SQL claim
        # and immutable generated key; no mutable/shared/arbitrary object can enter.
        request = {"Bucket": claim.bucket, "Key": claim.object_key, "IfMatch": etag}
        if version is not None:
            request["VersionId"] = version
        return request

    async def _clean(self, claim: DebugCleanupClaim, deadline: float) -> None:
        head = await self._head(claim, claim.object_version_id, deadline)
        if head is not None:
            request = self._delete_request(claim, head)
            self._live(deadline)
            async with asyncio.timeout(min(3, deadline - time.monotonic())):
                result = await drain_blocking(lambda: self.storage.delete_object(**request))
            if result.get("ResponseMetadata", {}).get("HTTPStatusCode") not in {200, 204}:
                raise CleanupDeferred()
            if await self._head(claim, request.get("VersionId"), deadline) is not None:
                raise CleanupDeferred()
        self._live(deadline)
        rows = await self._rows("SELECT agent.confirm_debug_deleted(%s,%s,%s) AS value",
                                (claim.part_id, self.owner, claim.claim_token))
        if len(rows) != 1 or rows[0]["value"] is not True:
            raise CleanupDeferred()

    async def cleanup_once(self) -> bool:
        async with self._iteration:
            if self._stop.is_set():
                return False
            try:
                # SQL alone selects terminal, expired, fully projected run payloads.
                # Disabled new capture does not disable existing retention duties.
                rows = await self._rows("SELECT agent.expire_private_run_payloads(%s) AS value", (BATCH_LIMIT,))
                if len(rows) != 1 or not isinstance(rows[0]["value"], dict):
                    raise CleanupDeferred()
                for _ in range(BATCH_LIMIT):
                    if self._stop.is_set():
                        break
                    claim = await self._claim()
                    if claim is None:
                        break
                    await self._clean(*claim)
                self._healthy = True
            except asyncio.CancelledError:
                self._healthy = False
                raise
            except Exception:
                self._healthy = False
                logger.error("dependency.failed", extra={"safe_fields": {"error_code": "DEPENDENCY_UNAVAILABLE"}})
            return self._healthy
