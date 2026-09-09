"""Supervised at-least-once delivery and independent PostgreSQL reconciliation.

All timing/concurrency values are engineering configuration, not source SLOs.
Cancellation leaves an unmarked immutable command for its lease successor.
"""
from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import Protocol, TypeVar
from uuid import UUID, uuid4

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.settings import Settings
from expert_observability.tracing import correlation_context, restored_run_trace, safe_span
from redis.typing import EncodableT, FieldT

from .store import OutboxStore
from .transport import InvalidOutboxMessage, OutboxEvent, RedisMessage, prepare_message

logger = logging.getLogger(__name__)
T = TypeVar("T")
# Hints are replayed from PG, so bounded trimming is safe. Job commands are not trimmed.
NOTIFICATION_MAX_LENGTH = 1000


class RedisSink(Protocol):
    async def append(self, message: RedisMessage) -> None: ...


class RedisOutboxSink:
    def __init__(self, dependencies: Dependencies):
        self.dependencies = dependencies

    async def append(self, message: RedisMessage) -> None:
        redis = self.dependencies.redis
        if redis is None:
            raise RuntimeError("Outbox Redis transport is not initialized")
        # Automatic Redis IDs permit retries after ambiguous ACKs. The stable
        # business event_id in fields is the consumer's deduplication identity.
        fields: dict[FieldT, EncodableT] = {key: value for key, value in message.fields.items()}
        if message.notification:
            await redis.xadd(message.stream, fields, maxlen=NOTIFICATION_MAX_LENGTH, approximate=True)
        else:
            await redis.xadd(message.stream, fields)


class OutboxPublisher:
    def __init__(self, settings: Settings, store: OutboxStore, sink: RedisSink, *, owner: UUID | None = None):
        self.settings = settings
        self.store = store
        self.sink = sink
        self.owner = owner or uuid4()
        self.publish_healthy = False
        self.reconcile_healthy = False
        self._stop = asyncio.Event()
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        # Reserve lease time for claim, delivery, mark and a failed-attempt write.
        self.operation_timeout = min(settings.outbox_operation_timeout_seconds, settings.outbox_claim_seconds / 4)

    async def _bounded(self, operation: Awaitable[T]) -> T:
        async with asyncio.timeout(self.operation_timeout):
            return await operation

    def _log_failure(self, *, permanent: bool = False, attempt: int = 0) -> None:
        logger.error("dependency.failed", extra={"safe_fields": {
            "error_code": "INTERNAL_ERROR" if permanent else "DEPENDENCY_UNAVAILABLE", "attempt": attempt,
        }})

    def start(self) -> None:
        if self._tasks:
            raise RuntimeError("Outbox publisher is already started")
        self._stop = asyncio.Event()
        self._tasks = (
            asyncio.create_task(self._supervise(self.publish_once, self.settings.outbox_poll_seconds, False)),
            asyncio.create_task(self._supervise(self.reconcile_once, self.settings.outbox_reconcile_seconds, True)),
        )

    async def close(self) -> None:
        self._stop.set()  # No further claims; allow already-started delivery to drain.
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=self.settings.outbox_shutdown_seconds)
            for task in pending:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = ()
        self.publish_healthy = self.reconcile_healthy = False

    def health(self) -> DependencyHealth:
        running = bool(self._tasks) and all(not task.done() for task in self._tasks)
        return DependencyHealth("outbox_delivery", running and self.publish_healthy and self.reconcile_healthy)

    async def _supervise(self, operation: Callable[[], Awaitable[None]], interval: float, reconciler: bool) -> None:
        while not self._stop.is_set():
            try:
                await operation()
            except Exception:
                # CancelledError is a BaseException and is never translated into retry.
                if reconciler:
                    self.reconcile_healthy = False
                else:
                    self.publish_healthy = False
                self._log_failure()
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=interval)
            except TimeoutError:
                pass

    async def reconcile_once(self) -> None:
        await self._bounded(self.store.reconcile(
            self.settings.outbox_reconcile_batch_size, math.ceil(self.settings.outbox_reconcile_seconds),
        ))
        self.reconcile_healthy = True

    async def publish_once(self) -> None:
        limit = min(self.settings.outbox_batch_size, self.settings.outbox_publish_concurrency)
        events = await self._bounded(self.store.claim(self.owner, limit, self.settings.outbox_claim_seconds))
        if len(events) > limit:
            raise RuntimeError("Outbox claim exceeded its bound")
        tasks: list[asyncio.Task[bool]] = []
        async with asyncio.TaskGroup() as group:
            for event in events:
                tasks.append(group.create_task(self._deliver(event)))
        attention = await self._bounded(self.store.attention_required(self.settings.outbox_max_attempts))
        self.publish_healthy = all(task.result() for task in tasks) and not attention
        if attention:
            self._log_failure(permanent=True)

    def _backoff(self, attempt: int) -> int:
        return min(self.settings.outbox_retry_max_seconds, math.ceil(
            self.settings.outbox_retry_base_seconds * 2 ** min(max(attempt - 1, 0), 16),
        ))

    async def _reschedule(self, event: OutboxEvent, code: str, delay: int) -> None:
        try:
            await self._bounded(self.store.reschedule(event.event_id, self.owner, event.claim_token, code, delay))
        except Exception:
            # If this write fails, the persisted claim will expire and be reclaimed.
            self._log_failure()

    async def _deliver(self, event: OutboxEvent) -> bool:
        identity = {"run_id": event.aggregate_id} if event.aggregate_type == "run" else (
            {"job_id": event.aggregate_id} if event.aggregate_type == "ingestion_job" else {})
        with (restored_run_trace(event.trace_id) if event.trace_id else nullcontext(),
              correlation_context(**identity, attempt=event.publish_attempts)):
            with safe_span("outbox.publish") as span:
                result = await self._deliver_once(event)
                if result:
                    span.set_attributes(status="completed", count=1)
                else:
                    span.fail("DEPENDENCY_UNAVAILABLE")
                return result

    async def _deliver_once(self, event: OutboxEvent) -> bool:
        if not 1 <= event.publish_attempts <= self.settings.outbox_max_attempts:
            await self._reschedule(event, "OUTBOX_ATTEMPTS_EXHAUSTED", 0)
            self._log_failure(permanent=True)
            return False
        try:
            message = prepare_message(event, self.settings.redis_stream_ingestion, self.settings.redis_notification_prefix)
        except InvalidOutboxMessage as error:
            await self._reschedule(event, error.code, 0)
            self._log_failure(permanent=True, attempt=event.publish_attempts)
            return False
        try:
            await self._bounded(self.sink.append(message))
            # SQL checks exact token AND live lease. A false result is ownership
            # loss, not permission to overwrite or clear a successor's claim.
            marked = await self._bounded(self.store.mark(event.event_id, self.owner, event.claim_token))
            if not marked:
                self._log_failure(attempt=event.publish_attempts)
            return marked
        except Exception:
            exhausted = event.publish_attempts >= self.settings.outbox_max_attempts
            code = "OUTBOX_ATTEMPTS_EXHAUSTED" if exhausted else "DEPENDENCY_UNAVAILABLE"
            await self._reschedule(event, code, 0 if exhausted else self._backoff(event.publish_attempts))
            self._log_failure(permanent=exhausted, attempt=event.publish_attempts)
            return False
