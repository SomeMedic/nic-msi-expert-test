"""Short PostgreSQL transactions, completed before any Redis operation."""
from __future__ import annotations

from dataclasses import replace
import re
from typing import Any, Protocol
from uuid import UUID

from psycopg.rows import dict_row

from expert_clients.dependencies import Dependencies

from .transport import OutboxEvent


TERMINAL_CODES = ("OUTBOX_INVALID_TOPIC", "OUTBOX_INVALID_PAYLOAD", "OUTBOX_ATTEMPTS_EXHAUSTED")


class OutboxStore(Protocol):
    async def claim(self, owner: UUID, limit: int, lease_seconds: int) -> list[OutboxEvent]: ...
    async def mark(self, event_id: UUID, owner: UUID, token: UUID) -> bool: ...
    async def reschedule(self, event_id: UUID, owner: UUID, token: UUID, code: str, delay: int) -> bool: ...
    async def reconcile(self, limit: int, minimum_interval: int) -> int: ...
    async def attention_required(self, max_attempts: int) -> bool: ...


class PostgresOutboxStore:
    def __init__(self, dependencies: Dependencies):
        self.dependencies = dependencies

    def _pool(self):
        pool = self.dependencies.pool
        if pool is None:
            raise RuntimeError("Outbox database transport is not initialized")
        return pool

    async def claim(self, owner: UUID, limit: int, lease_seconds: int) -> list[OutboxEvent]:
        async with self._pool().connection() as connection:
            async with connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("SELECT * FROM app.claim_outbox(%s,%s,%s)", (owner, limit, lease_seconds))
                    rows = await cursor.fetchall()
            # The claim is committed here. Leaving the pool context precedes return.
        events = [OutboxEvent(**{name: row[name] for name in OutboxEvent.__dataclass_fields__
                                if name != "trace_id"}) for row in rows]
        result = []
        for event in events:
            if event.aggregate_type == "ingestion_job":
                trace_id = await self._scalar("SELECT app.get_claimed_outbox_trace(%s,%s,%s)",
                                              (event.event_id, owner, event.claim_token))
                if trace_id is not None and (not isinstance(trace_id, str)
                        or not re.fullmatch(r"[0-9a-f]{32}", trace_id) or not int(trace_id, 16)):
                    raise RuntimeError("Outbox trace binding is invalid")
                event = replace(event, trace_id=trace_id)
            result.append(event)
        return result

    async def _scalar(self, query: str, parameters: tuple[Any, ...]) -> Any:
        async with self._pool().connection() as connection:
            async with connection.transaction():
                cursor = await connection.execute(query, parameters)
                row = await cursor.fetchone()
                if row is None:
                    raise RuntimeError("Outbox routine returned no result")
                value = row[0]
        return value

    async def mark(self, event_id: UUID, owner: UUID, token: UUID) -> bool:
        return bool(await self._scalar("SELECT app.mark_outbox_published(%s,%s,%s)", (event_id, owner, token)))

    async def reschedule(self, event_id: UUID, owner: UUID, token: UUID, code: str, delay: int) -> bool:
        return bool(await self._scalar(
            "SELECT app.reschedule_outbox(%s,%s,%s,%s,%s)", (event_id, owner, token, code, delay),
        ))

    async def reconcile(self, limit: int, minimum_interval: int) -> int:
        return int(await self._scalar("SELECT app.reconcile_ingestion(%s,%s)", (limit, minimum_interval)))

    async def attention_required(self, max_attempts: int) -> bool:
        return bool(await self._scalar(
            "SELECT EXISTS (SELECT 1 FROM app.outbox_events WHERE published_at IS NULL "
            "AND (last_error_code = ANY(%s) OR publish_attempts >= %s))",
            (list(TERMINAL_CODES), max_attempts),
        ))
