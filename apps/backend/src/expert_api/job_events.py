"""Bounded ingestion SSE replay from PostgreSQL; Redis supplies broadcast hints."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from psycopg.rows import dict_row
from pydantic import ValidationError
from redis.exceptions import RedisError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.jobs import public_job_error_message
from expert_clients.settings import Settings
from expert_contracts.errors import ErrorCode
from expert_contracts.events import INGESTION_EVENT_ADAPTER, PublicIngestionEvent

TERMINAL = frozenset({"completed", "failed", "cancelled"})
_FAILURE_CODES = frozenset({
    "PDF_INVALID", "EXTRACTION_QUALITY_FAILED", "GENERATION_INVALID", "DEPENDENCY_UNAVAILABLE",
    "MODEL_UNAVAILABLE", "MODEL_TIMEOUT", "TOKEN_LIMIT_EXCEEDED", "VERSION_CONFLICT",
    "SOURCE_REVOKED", "SOURCE_UNAVAILABLE", "INTERNAL_ERROR", "DEADLINE_EXCEEDED", "SIZE_LIMIT_EXCEEDED",
})


def event_from_row(row: dict, job_id: UUID, version_id: UUID) -> PublicIngestionEvent:
    try:
        event = INGESTION_EVENT_ADAPTER.validate_python({
            "schema_version": row["schema_version"], "event_id": row["event_id"], "job_id": row["job_id"],
            "sequence": row["sequence"], "type": row["event_type"], "stage": row["stage"],
            "attempt": row["attempt"], "execution_epoch": row["execution_epoch"],
            "occurred_at": row["created_at"], "data": row["public_payload"],
        })
        if event.job_id != job_id:
            raise ValueError()
        if event.type in {"ingestion.ready_to_publish", "ingestion.completed"}:
            if event.data.version_id != version_id:
                raise ValueError()
        if event.type == "ingestion.failed":
            error = event.data.error
            if error.code.value not in _FAILURE_CODES or error.retryable or error.details:
                raise ValueError()
            # The SQL writer accepts bounded free text; it is not a privacy guarantee.
            # Public replay uses code-controlled text, retaining the durable error identity.
            message = public_job_error_message(error.code)
            event = event.model_copy(update={"data": event.data.model_copy(update={
                "error": error.model_copy(update={"message": message}),
            })})
        return event
    except (ValidationError, ValueError, TypeError, KeyError):
        raise ApiError(ErrorCode.INTERNAL_ERROR) from None


def encode_event(event: PublicIngestionEvent) -> bytes:
    return (f"id: {event.sequence}\nevent: {event.type}\ndata: {event.model_dump_json()}\n\n").encode("utf-8")


@dataclass(frozen=True)
class EventBatch:
    events: tuple[PublicIngestionEvent, ...]
    last_sequence: int
    terminal: bool


@dataclass(frozen=True)
class PreparedReplay:
    job_id: UUID
    principal: Principal
    cursor: int
    redis_cursor: str
    initial: EventBatch


class JobEventService:
    """No background tasks or owned transports; request cancellation closes its reader only."""
    def __init__(self, settings: Settings, pool, redis_client):
        self.settings, self.pool, self.redis = settings, pool, redis_client
        self.batch_size = min(settings.sse_batch_size, settings.sse_buffer_max_events)

    def stream_key(self, job_id: UUID) -> str:
        return self.settings.redis_notification_prefix + "ingestion.events:" + str(job_id)

    async def _assert_visible(self, job_id: UUID) -> None:
        with database_failures():
            async with self.pool.connection() as connection:
                row = await (await connection.execute(
                    "SELECT d.security_revoked_at FROM app.ingestion_jobs j "
                    "JOIN app.document_versions v ON v.id=j.version_id "
                    "JOIN app.logical_documents d ON d.id=v.logical_document_id WHERE j.id=%s", (job_id,)
                )).fetchone()
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        if row[0] is not None:
            raise ApiError(ErrorCode.SOURCE_REVOKED)

    async def _tail(self, job_id: UUID) -> str:
        try:
            if self.redis is not None:
                async with asyncio.timeout(min(self.settings.sse_pg_poll_seconds, 1.0)):
                    rows = await self.redis.xrevrange(self.stream_key(job_id), count=1)
                if rows:
                    value = rows[0][0]
                    return value.decode("ascii") if isinstance(value, bytes) else value
        except (RedisError, OSError, TimeoutError, UnicodeError):
            pass
        return "0-0"

    async def read_batch(self, job_id: UUID, principal: Principal, cursor: int) -> EventBatch:
        # Library job visibility matches JobService.get, not principal-owned run history.
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                await connection.execute("SET LOCAL statement_timeout='5s'")
                async with connection.cursor(row_factory=dict_row) as db_cursor:
                    await db_cursor.execute(
                        "SELECT j.status,j.last_event_sequence,j.version_id,d.security_revoked_at "
                        "FROM app.ingestion_jobs j JOIN app.document_versions v ON v.id=j.version_id "
                        "JOIN app.logical_documents d ON d.id=v.logical_document_id WHERE j.id=%s", (job_id,))
                    job = await db_cursor.fetchone()
                    if job is None:
                        raise ApiError(ErrorCode.NOT_FOUND)
                    if job["security_revoked_at"] is not None:
                        raise ApiError(ErrorCode.SOURCE_REVOKED)
                    last = job["last_event_sequence"]
                    if type(cursor) is not int or cursor < 0 or cursor > last:
                        raise ApiError(ErrorCode.EVENT_CURSOR_INVALID, details={"last_sequence": last})
                    await db_cursor.execute(
                        "SELECT * FROM app.ingestion_events WHERE job_id=%s AND sequence>%s "
                        "ORDER BY sequence LIMIT %s", (job_id, cursor, self.batch_size))
                    rows = await db_cursor.fetchall()
        expected = cursor + 1
        for row in rows:
            if row["sequence"] != expected or row["sequence"] > last:
                raise self._expired(job_id, last)
            expected += 1
        if expected <= last and len(rows) < self.batch_size:
            raise self._expired(job_id, last)
        return EventBatch(tuple(event_from_row(row, job_id, job["version_id"]) for row in rows),
                          last, job["status"] in TERMINAL)

    @staticmethod
    def _expired(job_id: UUID, last: int) -> ApiError:
        return ApiError(ErrorCode.EVENT_HISTORY_EXPIRED, details={
            "snapshot_url": f"/api/v1/ingestion-jobs/{job_id}", "last_sequence": last,
        })

    async def prepare(self, job_id: UUID, principal: Principal, cursor: int) -> PreparedReplay:
        await self._assert_visible(job_id)
        # Auth runs at the HTTP boundary. Observe Redis before PG drain so a commit
        # between drain and wait is visible; absent/lost hints still resolve by polling.
        redis_cursor = await self._tail(job_id)
        initial = await self.read_batch(job_id, principal, cursor)
        return PreparedReplay(job_id, principal, cursor, redis_cursor, initial)

    async def _wait(self, job_id: UUID, redis_cursor: str, seconds: float) -> str:
        started = asyncio.get_running_loop().time()
        try:
            if self.redis is not None:
                async with asyncio.timeout(seconds):
                    hints = await self.redis.xread({self.stream_key(job_id): redis_cursor},
                        count=self.batch_size, block=max(1, int(seconds * 1000)))
                if hints and hints[0][1]:
                    value = hints[0][1][-1][0]
                    return value.decode("ascii") if isinstance(value, bytes) else value
        except (RedisError, OSError, TimeoutError, UnicodeError):
            pass
        remaining = seconds - (asyncio.get_running_loop().time() - started)
        if remaining > 0:
            await asyncio.sleep(remaining)
        return redis_cursor

    async def iterate(self, prepared: PreparedReplay, *, is_disconnected: Callable[[], Awaitable[bool]],
                      authorize: Callable[[], Awaitable[Principal]] | None = None) -> AsyncIterator[bytes]:
        cursor, redis_cursor, batch = prepared.cursor, prepared.redis_cursor, prepared.initial
        loop = asyncio.get_running_loop()
        heartbeat_at = loop.time() + self.settings.sse_heartbeat_seconds
        try:
            while True:
                if await is_disconnected():
                    return
                if (prepared.principal.expires_at is not None
                        and prepared.principal.expires_at <= datetime.now(timezone.utc)):
                    return
                if authorize is not None and (await authorize()).subject != prepared.principal.subject:
                    return
                for event in batch.events:
                    yield encode_event(event)
                    cursor = event.sequence
                if batch.terminal and cursor == batch.last_sequence:
                    return
                if cursor < batch.last_sequence:
                    batch = await self.read_batch(prepared.job_id, prepared.principal, cursor)
                    continue
                if loop.time() >= heartbeat_at:
                    yield b": heartbeat\n\n"
                    heartbeat_at = loop.time() + self.settings.sse_heartbeat_seconds
                seconds = min(self.settings.sse_pg_poll_seconds, max(0.001, heartbeat_at - loop.time()))
                redis_cursor = await self._wait(prepared.job_id, redis_cursor, seconds)
                batch = await self.read_batch(prepared.job_id, prepared.principal, cursor)
        except ApiError:
            # Headers are committed: close, and let reconnect report a truthful JSON error.
            return
