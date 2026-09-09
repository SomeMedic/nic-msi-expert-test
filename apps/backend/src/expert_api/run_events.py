"""Bounded broadcast SSE replay: PostgreSQL owns events; Redis only wakes readers."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from uuid import UUID

from psycopg.rows import dict_row
from pydantic import ValidationError
from redis.exceptions import RedisError
from starlette.requests import ClientDisconnect
from starlette.responses import StreamingResponse

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.runs import TERMINAL, validated_terminal_error
from expert_clients.settings import Settings
from expert_contracts.errors import ErrorCode
from expert_contracts.events import PublicRunEvent, RUN_EVENT_ADAPTER

SEND_TIMEOUT_SECONDS = 5.0
_MAX_SEQUENCE = 2**63 - 1


def parse_cursor(last_event_id: str | None, after: str | None) -> int:
    # An ignored query parameter cannot invalidate a valid Last-Event-ID.
    value = last_event_id if last_event_id is not None else after
    if value is None:
        return 0
    if re.fullmatch(r"(?:0|[1-9][0-9]{0,18})", value) is None or int(value) > _MAX_SEQUENCE:
        raise ApiError(ErrorCode.EVENT_CURSOR_INVALID)
    return int(value)


def event_from_row(row: dict, run_id: UUID) -> PublicRunEvent:
    try:
        event = RUN_EVENT_ADAPTER.validate_python({
            "schema_version": row["schema_version"], "event_id": row["event_id"],
            "run_id": row["run_id"], "sequence": row["sequence"], "type": row["event_type"],
            "stage": row["stage"], "attempt": row["attempt"], "execution_epoch": row["execution_epoch"],
            "occurred_at": row["created_at"], "data": row["public_payload"],
        })
        if event.run_id != run_id:
            raise ValueError()
        if event.type == "run.failed":
            validated_terminal_error(event.data.error, run_id)
        if (event.type == "stage.started" and event.data.message_code is not None
                and event.data.message_code != "RUN_" + event.stage.value.upper()):
            raise ValueError()
        return event
    except (ValidationError, ValueError, TypeError, KeyError):
        raise ApiError(ErrorCode.INTERNAL_ERROR) from None


def encode_event(event: PublicRunEvent) -> bytes:
    return (f"id: {event.sequence}\nevent: {event.type}\ndata: {event.model_dump_json()}\n\n").encode("utf-8")


@dataclass(frozen=True)
class EventBatch:
    events: tuple[PublicRunEvent, ...]
    last_sequence: int
    terminal: bool


@dataclass(frozen=True)
class PreparedReplay:
    run_id: UUID
    principal: Principal
    cursor: int
    redis_cursor: str
    initial: EventBatch


class RunEventService:
    def __init__(self, settings: Settings, pool, redis_client):
        self.settings, self.pool, self.redis = settings, pool, redis_client
        self.batch_size = min(settings.sse_batch_size, settings.sse_buffer_max_events)

    def stream_key(self, run_id: UUID) -> str:
        return self.settings.redis_notification_prefix + "run.events:" + str(run_id)

    async def _assert_owner(self, run_id: UUID, principal: Principal) -> None:
        with database_failures():
            async with self.pool.connection() as connection:
                row = await (await connection.execute(
                    "SELECT id FROM agent.runs WHERE id=%s AND principal_id=%s",
                    (run_id, principal.subject),
                )).fetchone()
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)

    async def _tail(self, run_id: UUID) -> str:
        try:
            if self.redis is not None:
                async with asyncio.timeout(min(self.settings.sse_pg_poll_seconds, 1.0)):
                    tail = await self.redis.xrevrange(self.stream_key(run_id), count=1)
                if tail:
                    value = tail[0][0]
                    return value.decode("ascii") if isinstance(value, bytes) else value
        except (RedisError, OSError, TimeoutError, UnicodeError):
            pass
        return "0-0"

    async def read_batch(self, run_id: UUID, principal: Principal, cursor: int) -> EventBatch:
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                # State, retention boundary and batch are one consistent view.
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                async with connection.cursor(row_factory=dict_row) as db_cursor:
                    await db_cursor.execute(
                        "SELECT status,last_event_sequence FROM agent.runs WHERE id=%s AND principal_id=%s",
                        (run_id, principal.subject),
                    )
                    run = await db_cursor.fetchone()
                    if run is None:
                        raise ApiError(ErrorCode.NOT_FOUND)
                    last = run["last_event_sequence"]
                    if type(cursor) is not int or cursor < 0 or cursor > last:
                        raise ApiError(ErrorCode.EVENT_CURSOR_INVALID, details={"last_sequence": last})
                    await db_cursor.execute(
                        "SELECT e.* FROM agent.run_events e JOIN agent.runs r ON r.id=e.run_id "
                        "WHERE e.run_id=%s AND r.principal_id=%s AND e.sequence>%s "
                        "ORDER BY e.sequence LIMIT %s", (run_id, principal.subject, cursor, self.batch_size),
                    )
                    rows = await db_cursor.fetchall()
        expected = cursor + 1
        for row in rows:
            if row["sequence"] != expected or row["sequence"] > last:
                raise self._expired(run_id, last)
            expected += 1
        if expected <= last and len(rows) < self.batch_size:
            raise self._expired(run_id, last)
        return EventBatch(tuple(event_from_row(row, run_id) for row in rows), last, run["status"] in TERMINAL)

    @staticmethod
    def _expired(run_id: UUID, last: int) -> ApiError:
        return ApiError(ErrorCode.EVENT_HISTORY_EXPIRED, details={
            "snapshot_url": f"/api/v1/runs/{run_id}", "last_sequence": last,
        })

    async def prepare(self, run_id: UUID, principal: Principal, cursor: int) -> PreparedReplay:
        await self._assert_owner(run_id, principal)
        # Read the broadcast tail before draining PG, so a commit between the
        # drain and XREAD remains visible. Lost hints still resolve on each poll.
        redis_cursor = await self._tail(run_id)
        initial = await self.read_batch(run_id, principal, cursor)
        return PreparedReplay(run_id, principal, cursor, redis_cursor, initial)

    async def _wait(self, run_id: UUID, redis_cursor: str, seconds: float) -> str:
        started = asyncio.get_running_loop().time()
        try:
            if self.redis is not None:
                async with asyncio.timeout(seconds):
                    hints = await self.redis.xread({self.stream_key(run_id): redis_cursor},
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

    async def iterate(self, prepared: PreparedReplay, *,
                      is_disconnected: Callable[[], Awaitable[bool]],
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
                if authorize is not None:
                    current = await authorize()
                    if current.subject != prepared.principal.subject:
                        return
                for event in batch.events:
                    yield encode_event(event)
                    cursor = event.sequence
                if batch.terminal and cursor == batch.last_sequence:
                    return
                if cursor < batch.last_sequence:
                    batch = await self.read_batch(prepared.run_id, prepared.principal, cursor)
                    continue
                if loop.time() >= heartbeat_at:
                    yield b": heartbeat\n\n"
                    heartbeat_at = loop.time() + self.settings.sse_heartbeat_seconds
                seconds = min(self.settings.sse_pg_poll_seconds, max(0.001, heartbeat_at - loop.time()))
                redis_cursor = await self._wait(prepared.run_id, redis_cursor, seconds)
                batch = await self.read_batch(prepared.run_id, prepared.principal, cursor)
        except ApiError:
            # Once headers are sent a JSON error envelope would corrupt SSE.
            # Close; a reconnect performs the same checks before sending headers.
            return


class BoundedEventResponse(StreamingResponse):
    """Disconnect a stalled recipient instead of accumulating replay events."""
    async def __call__(self, scope, receive, send) -> None:
        async def bounded_send(message):
            async with asyncio.timeout(SEND_TIMEOUT_SECONDS):
                await send(message)
        try:
            await super().__call__(scope, receive, bounded_send)
        except (TimeoutError, ClientDisconnect):
            pass
        finally:
            close = getattr(self.body_iterator, "aclose", None)
            if close is not None:
                await close()
