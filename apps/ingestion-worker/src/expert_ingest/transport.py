"""Redis delivery hints and PostgreSQL-authoritative ingestion transitions."""
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import asyncio
from typing import Any, Mapping, cast
from uuid import UUID, uuid4

from psycopg import Error as PostgresError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from expert_ingest.source import SourceFailure, SourceReference


ACKABLE_STATUSES = frozenset({"completed", "failed", "cancelled", "retry_wait"})
LEASE_ERRORS = frozenset({"STALE_INGESTION", "CANCEL_REQUESTED", "INGESTION_NOT_RUNNING", "LEASE_EXPIRED"})
_SAFE_DB_ERRORS = LEASE_ERRORS | {
    "INGESTION_NOT_FOUND", "SOURCE_REVOKED", "VERSION_CONFLICT", "GENERATION_NOT_READY",
    "GENERATION_INVALID", "INVALID_FAILURE", "INVALID_STAGE_EVENT", "STAGE_NOT_STARTED",
    "STAGE_ALREADY_COMPLETED", "PROGRESS_REGRESSION", "EXECUTION_BINDING_IMMUTABLE",
}


class JobRejected(Exception):
    def __init__(self, code: str):
        self.code = code if code in _SAFE_DB_ERRORS else "DEPENDENCY_UNAVAILABLE"
        super().__init__(self.code)


@dataclass(frozen=True)
class Command:
    event_id: UUID
    job_id: UUID

    @classmethod
    def parse(cls, fields: Mapping[str, Any]) -> Command:
        if (set(fields) != {"schema_version", "event_id", "job_id"}
                or fields["schema_version"] != "1"
                or any(not isinstance(v, str) or len(v) > 64 for v in fields.values())):
            raise ValueError("Invalid ingestion command")
        try:
            return cls(UUID(fields["event_id"]), UUID(fields["job_id"]))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("Invalid ingestion command") from None


@dataclass(frozen=True)
class Delivery:
    message_id: str
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class Acquisition:
    outcome: str
    job_id: UUID
    lease_epoch: int
    status: str
    attempt: int
    lease_until: datetime | None


class RedisIngestionStream:
    def __init__(self, redis: Redis, *, stream: str = "expert:ingestion.jobs.v1",
                 group: str = "ingestion-workers", consumer: str | None = None):
        if not stream.startswith("expert:") or not group:
            raise ValueError("Invalid ingestion stream configuration")
        self.redis = redis
        self.stream = stream
        self.group = group
        self.consumer = consumer or f"ingestion-{uuid4()}"
        self._claim_cursor = "0-0"

    async def ensure_group(self) -> None:
        try:
            # Start at the beginning so pre-existing outbox deliveries are not skipped.
            await self.redis.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise

    async def read(self, *, count: int, block_ms: int = 1000) -> list[Delivery]:
        if not 1 <= count <= 100 or not 1 <= block_ms <= 2000:
            raise ValueError("Invalid stream read bounds")
        try:
            result = await self.redis.xreadgroup(
                self.group, self.consumer, {self.stream: ">"}, count=count, block=block_ms,
            )
        except ResponseError as error:
            if str(error).startswith("NOGROUP"):
                await self.ensure_group()
                return []
            raise
        rows = cast(list[tuple[str, list[tuple[str, Mapping[str, Any]]]]], result)
        return [Delivery(message_id, fields) for _, entries in rows for message_id, fields in entries]

    async def reclaim(self, *, count: int, idle_ms: int) -> list[Delivery]:
        if not 1 <= count <= 100 or idle_ms < 1:
            raise ValueError("Invalid stream reclaim bounds")
        try:
            result = await self.redis.xautoclaim(
                self.stream, self.group, self.consumer, min_idle_time=idle_ms,
                start_id=self._claim_cursor, count=count,
            )
        except ResponseError as error:
            if str(error).startswith("NOGROUP"):
                await self.ensure_group()
                self._claim_cursor = "0-0"
                return []
            raise
        self._claim_cursor = result[0]
        # Claiming delivery ownership never grants the right to execute a PG job.
        return [Delivery(message_id, fields) for message_id, fields in result[1]]

    async def ack(self, message_id: str) -> None:
        await self.redis.xack(self.stream, self.group, message_id)


class PostgresIngestionJobs:
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def _query(self, query: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        for attempt in range(3):
            try:
                async with self.pool.connection() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='5s'")
                        await connection.execute("SET LOCAL lock_timeout='2s'")
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute(query, params)
                            row = await cursor.fetchone()
                    # Commit completes before a caller may ACK the delivery.
                    return row
            except PostgresError as error:
                if error.sqlstate in {"40001", "40P01"} and attempt < 2:
                    await asyncio.sleep(0.05 * (attempt + 1))
                    continue
                raise JobRejected(error.diag.message_primary or "DEPENDENCY_UNAVAILABLE") from None
        raise AssertionError("Unreachable transaction retry")

    async def acquire(self, job_id: UUID, owner: UUID, lease_seconds: int) -> Acquisition:
        row = await self._query("SELECT * FROM app.acquire_ingestion(%s,%s,%s)",
                                (job_id, owner, lease_seconds))
        if row is None:
            raise JobRejected("INGESTION_NOT_FOUND")
        return Acquisition(**row)

    async def status(self, job_id: UUID) -> str | None:
        row = await self._query("SELECT status FROM app.ingestion_jobs WHERE id=%s", (job_id,))
        return row["status"] if row else None

    async def heartbeat(self, job_id: UUID, owner: UUID, epoch: int, lease_seconds: int) -> None:
        await self._query("SELECT app.heartbeat_ingestion(%s,%s,%s,%s)",
                          (job_id, owner, epoch, lease_seconds))

    async def guard(self, job_id: UUID, owner: UUID, epoch: int) -> None:
        await self._query("SELECT app.guard_ingestion_write(%s,%s,%s)", (job_id, owner, epoch))

    async def trace_id(self, job_id: UUID, owner: UUID, epoch: int) -> str | None:
        async with self.guarded_transaction(job_id, owner, epoch) as connection:
            row = await (await connection.execute(
                "SELECT trace_id FROM app.ingestion_jobs WHERE id=%s", (job_id,))).fetchone()
            if row is None:
                raise JobRejected("INGESTION_NOT_FOUND")
            return row[0]

    async def bind_trace(self, job_id: UUID, owner: UUID, epoch: int, trace_id: str) -> None:
        await self._query("SELECT app.bind_ingestion_trace(%s,%s,%s,%s)", (job_id, owner, epoch, trace_id))

    @asynccontextmanager
    async def guarded_transaction(self, job_id: UUID, owner: UUID, epoch: int):
        """Pipeline generation writes must use this same connection and transaction.

        This scope must contain only short database work, never S3/model/parser calls.
        Caller retries the whole deterministic DB unit after serialization/deadlock.
        Lease authority is checked at admission and again before transaction commit.
        """
        try:
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='5s'")
                    await connection.execute("SET LOCAL lock_timeout='2s'")
                    await connection.execute("SELECT app.guard_ingestion_write(%s,%s,%s)",
                                             (job_id, owner, epoch))
                    yield connection
                    # Locks prevent takeover during this unit, but clock time still
                    # advances: an admitted write must roll back if its lease expired.
                    await connection.execute("SELECT app.guard_ingestion_write(%s,%s,%s)",
                                             (job_id, owner, epoch))
        except PostgresError as error:
            raise JobRejected(error.diag.message_primary or "DEPENDENCY_UNAVAILABLE") from None

    async def advance(self, job_id: UUID, owner: UUID, epoch: int, stage: str,
                      event_type: str, processed: int = 0, total: int | None = None) -> None:
        await self._query("SELECT * FROM app.advance_ingestion(%s,%s,%s,%s,%s,%s,%s)",
                          (job_id, owner, epoch, stage, event_type, processed, total))

    async def fail(self, job_id: UUID, owner: UUID, epoch: int, code: str, safe_message: str,
                   retryable: bool, retry_delay_seconds: int) -> str:
        row = await self._query("SELECT * FROM app.fail_ingestion(%s,%s,%s,%s,%s,%s,%s)",
                                (job_id, owner, epoch, code, safe_message, retryable, retry_delay_seconds))
        if row is None:
            raise JobRejected("INGESTION_NOT_FOUND")
        return row["status"]

    async def complete(self, job_id: UUID, owner: UUID, epoch: int,
                       index_generation_id: UUID, operation_id: UUID) -> str:
        row = await self._query("SELECT * FROM app.complete_ingestion(%s,%s,%s,%s,%s)",
                                (job_id, owner, epoch, index_generation_id, operation_id))
        if row is None:
            raise JobRejected("INGESTION_NOT_FOUND")
        return row["status"]

    async def reconcile(self, *, limit: int = 100, min_interval_seconds: int = 60) -> int:
        row = await self._query("SELECT app.reconcile_ingestion(%s,%s) AS dispatched",
                                (limit, min_interval_seconds))
        return row["dispatched"] if row else 0

    async def source(self, job_id: UUID) -> SourceReference:
        row = await self._query("""
            SELECT j.id AS job_id, v.logical_document_id AS document_id, v.id AS version_id,
                   o.id AS object_id, o.bucket, o.object_key AS key, o.object_version_id,
                   o.sha256::text, o.size_bytes
              FROM app.ingestion_jobs j
              JOIN app.document_versions v ON v.id=j.version_id
              JOIN app.stored_objects o ON o.id=v.source_object_id
              JOIN app.logical_documents d ON d.id=v.logical_document_id
             WHERE j.id=%s AND o.kind='original' AND o.state='attached'
               AND o.sha256=v.source_sha256 AND o.size_bytes=v.content_size
               AND o.media_type='application/pdf' AND d.security_revoked_at IS NULL
        """, (job_id,))
        if row is None:
            raise SourceFailure("SOURCE_UNAVAILABLE", "Original source is unavailable")
        return SourceReference(**row)
