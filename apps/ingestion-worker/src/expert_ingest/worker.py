"""Bounded ingestion executor. A real pipeline is required at construction."""
from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import AbstractAsyncContextManager, nullcontext, suppress
from dataclasses import dataclass
import inspect
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID, NAMESPACE_URL, uuid4, uuid5

from expert_observability.tracing import correlation_context, restored_run_trace, safe_span

from expert_ingest.source import LocalSource, OriginalSource, SourceFailure, SourceReference
from expert_ingest.transport import (
    ACKABLE_STATUSES, LEASE_ERRORS, Acquisition, Command, Delivery, JobRejected, RedisIngestionStream,
)


class Jobs(Protocol):
    async def acquire(self, job_id: UUID, owner: UUID, lease_seconds: int) -> Acquisition: ...
    async def status(self, job_id: UUID) -> str | None: ...
    async def heartbeat(self, job_id: UUID, owner: UUID, epoch: int, lease_seconds: int) -> None: ...
    async def guard(self, job_id: UUID, owner: UUID, epoch: int) -> None: ...
    async def trace_id(self, job_id: UUID, owner: UUID, epoch: int) -> str | None: ...
    async def bind_trace(self, job_id: UUID, owner: UUID, epoch: int, trace_id: str) -> None: ...
    def guarded_transaction(self, job_id: UUID, owner: UUID, epoch: int) -> AbstractAsyncContextManager[Any]: ...
    async def advance(self, job_id: UUID, owner: UUID, epoch: int, stage: str,
                      event_type: str, processed: int = 0, total: int | None = None) -> None: ...
    async def fail(self, job_id: UUID, owner: UUID, epoch: int, code: str, safe_message: str,
                   retryable: bool, retry_delay_seconds: int) -> str: ...
    async def complete(self, job_id: UUID, owner: UUID, epoch: int,
                       index_generation_id: UUID, operation_id: UUID) -> str: ...
    async def reconcile(self, *, limit: int = 100, min_interval_seconds: int = 60) -> int: ...
    async def source(self, job_id: UUID) -> SourceReference: ...


@dataclass(frozen=True)
class WorkerOptions:
    concurrency: int = 1
    lease_seconds: int = 60
    heartbeat_seconds: float = 10
    job_timeout_seconds: float = 600
    reclaim_idle_seconds: float = 60
    reconcile_seconds: float = 30
    reconcile_min_interval_seconds: int = 60
    retry_delay_seconds: int = 5
    transport_backoff_seconds: float = 1
    janitor_age_seconds: float = 3600

    def __post_init__(self) -> None:
        if (not 1 <= self.concurrency <= 4 or not 1 <= self.lease_seconds <= 300
                or not 0 < self.heartbeat_seconds < self.lease_seconds / 2
                or not 0 < self.job_timeout_seconds <= 3600
                or not 0 < self.reclaim_idle_seconds <= 3600
                or not 0 < self.reconcile_seconds <= 3600
                or not 1 <= self.reconcile_min_interval_seconds <= 3600
                or not 0 <= self.retry_delay_seconds <= 3600
                or not 0 < self.transport_backoff_seconds <= 60
                or self.janitor_age_seconds < 600):
            raise ValueError("Invalid ingestion worker limits")


@dataclass(frozen=True)
class Execution:
    job_id: UUID
    owner: UUID
    epoch: int
    attempt: int
    jobs: Jobs

    async def guard(self) -> None:
        """Preflight only. Use transaction() for fenced DB generation writes."""
        await self.jobs.guard(self.job_id, self.owner, self.epoch)

    def transaction(self) -> AbstractAsyncContextManager[Any]:
        return self.jobs.guarded_transaction(self.job_id, self.owner, self.epoch)

    async def advance(self, stage: str, event_type: str, *, processed: int = 0,
                      total: int | None = None) -> None:
        await self.jobs.advance(self.job_id, self.owner, self.epoch, stage, event_type, processed, total)


# Implementations must cooperate with cancellation and keep parser CPU work in a
# bounded subprocess. Transaction scopes contain DB work only; no network awaits.
Pipeline = Callable[[Execution, LocalSource], Awaitable[UUID]]

_ERROR_MESSAGES = {
    "PDF_INVALID": "Original source is not a supported PDF",
    "EXTRACTION_QUALITY_FAILED": "Document extraction quality is insufficient",
    "GENERATION_INVALID": "Index generation is invalid",
    "GENERATION_NOT_READY": "Index generation is not ready",
    "DEPENDENCY_UNAVAILABLE": "An ingestion dependency is unavailable",
    "MODEL_UNAVAILABLE": "The required model is unavailable",
    "MODEL_TIMEOUT": "The model request timed out",
    "TOKEN_LIMIT_EXCEEDED": "Document content exceeds the model token limit",
    "VERSION_CONFLICT": "The current document publication has changed",
    "SOURCE_REVOKED": "Access to the document source was revoked",
    "SOURCE_UNAVAILABLE": "The immutable original could not be verified",
    "SIZE_LIMIT_EXCEEDED": "Original source exceeds its allowed size",
    "DEADLINE_EXCEEDED": "Document processing timed out",
    "INTERNAL_ERROR": "Document processing failed",
}


class PipelineFailure(Exception):
    def __init__(self, code: str, *, retryable: bool = False):
        self.code = code if code in _ERROR_MESSAGES else "INTERNAL_ERROR"
        self.retryable = retryable
        super().__init__(_ERROR_MESSAGES[self.code])


class _HeartbeatLost(Exception):
    pass


class IngestionWorker:
    def __init__(self, stream: RedisIngestionStream, jobs: Jobs, sources: OriginalSource,
                 pipeline: Pipeline, *, options: WorkerOptions | None = None, owner: UUID | None = None):
        if not (inspect.iscoroutinefunction(pipeline)
                or inspect.iscoroutinefunction(getattr(pipeline, "__call__", None))):
            raise TypeError("An asynchronous ingestion pipeline is required")
        self.stream = stream
        self.jobs = jobs
        self.sources = sources
        self.pipeline = pipeline
        self.options = options or WorkerOptions()
        self.owner = owner or uuid4()
        # Counters are safe diagnostics: never record stream fields or raw exceptions.
        self.counters: Counter[str] = Counter()

    async def _ack_if_durable(self, delivery: Delivery, job_id: UUID) -> bool:
        if await self.jobs.status(job_id) in ACKABLE_STATUSES:
            await self.stream.ack(delivery.message_id)
            self.counters["acknowledged"] += 1
            return True
        return False

    async def _heartbeat(self, execution: Execution) -> None:
        while True:
            await asyncio.sleep(self.options.heartbeat_seconds)
            try:
                await self.jobs.heartbeat(execution.job_id, execution.owner, execution.epoch,
                                          self.options.lease_seconds)
                self.counters["heartbeats"] += 1
            except Exception:
                raise _HeartbeatLost() from None

    async def _work(self, execution: Execution) -> None:
        async with asyncio.timeout(self.options.job_timeout_seconds):
            await execution.guard()
            trace_id = await self.jobs.trace_id(execution.job_id, execution.owner, execution.epoch)
            with (restored_run_trace(trace_id) if trace_id else nullcontext(),
                  correlation_context(job_id=execution.job_id, execution_epoch=execution.epoch,
                                      attempt=execution.attempt),
                  safe_span("ingestion.execute") as span):
                if span.trace_id is not None:
                    await self.jobs.bind_trace(execution.job_id, execution.owner, execution.epoch, span.trace_id)
                await self._execute(execution)
                span.set_attributes(status="completed")

    async def _execute(self, execution: Execution) -> None:
        reference = await self.jobs.source(execution.job_id)
        async with self.sources.open(reference, execution.owner, execution.epoch) as source:
            await execution.guard()
            generation = await self.pipeline(execution, source)
            if not isinstance(generation, UUID):
                raise PipelineFailure("GENERATION_INVALID")
            await execution.guard()
            # This is the publication gate; manual-publish jobs only become ready.
            # No source/value or fabricated publication count is attached to its span.
            with safe_span("ingestion.publish", stage="ready_to_publish", version_id=reference.version_id):
                operation = uuid5(NAMESPACE_URL, f"expert:ingestion:{execution.job_id}:{execution.epoch}")
                status = await self.jobs.complete(execution.job_id, execution.owner, execution.epoch,
                                                  generation, operation)
                if status != "completed":
                    raise PipelineFailure("GENERATION_INVALID")

    async def _with_heartbeat(self, execution: Execution) -> None:
        work = asyncio.create_task(self._work(execution))
        heartbeat = asyncio.create_task(self._heartbeat(execution))
        try:
            done, _ = await asyncio.wait({work, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
            if work in done:
                await work
            else:
                await heartbeat
        finally:
            for task in (work, heartbeat):
                if not task.done():
                    task.cancel()
            # Joining work ensures parser cancellation and source cleanup finish first.
            await asyncio.gather(work, heartbeat, return_exceptions=True)

    async def process(self, delivery: Delivery) -> bool:
        """Process one delivery; False leaves it pending for controlled recovery."""
        try:
            command = Command.parse(delivery.fields)
        except ValueError:
            self.counters["invalid_commands"] += 1
            # There is no trustworthy job identity with which to make a durable decision.
            # Keep it visible in PEL; periodic bounded reclaim is the retry/attention path.
            return False
        try:
            acquired = await self.jobs.acquire(command.job_id, self.owner, self.options.lease_seconds)
            if acquired.outcome != "acquired":
                return await self._ack_if_durable(delivery, command.job_id)
            if acquired.job_id != command.job_id or acquired.lease_epoch < 1:
                raise JobRejected("STALE_INGESTION")
            execution = Execution(command.job_id, self.owner, acquired.lease_epoch, acquired.attempt, self.jobs)
            try:
                await self._with_heartbeat(execution)
            except (_HeartbeatLost, asyncio.CancelledError):
                # Shutdown or loss of lease certainty must not manufacture a retry/failure.
                raise
            except Exception as error:
                if isinstance(error, JobRejected) and error.code in LEASE_ERRORS:
                    return await self._ack_if_durable(delivery, command.job_id)
                code = "INTERNAL_ERROR"
                retryable = False
                if isinstance(error, (SourceFailure, PipelineFailure)):
                    code, retryable = error.code, error.retryable
                elif isinstance(error, TimeoutError):
                    code, retryable = "DEADLINE_EXCEEDED", True
                elif isinstance(error, JobRejected):
                    code = error.code
                    retryable = code == "DEPENDENCY_UNAVAILABLE"
                if code not in _ERROR_MESSAGES:
                    code = "INTERNAL_ERROR"
                if code == "GENERATION_NOT_READY":
                    code = "GENERATION_INVALID"
                await self.jobs.fail(execution.job_id, execution.owner, execution.epoch,
                                     code, _ERROR_MESSAGES[code], retryable,
                                     min(3600, self.options.retry_delay_seconds * 2 ** min(execution.attempt - 1, 8)))
            return await self._ack_if_durable(delivery, command.job_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.counters["processing_interruptions"] += 1
            # Handles cancellation, committed-but-response-lost transitions, and takeover.
            try:
                return await self._ack_if_durable(delivery, command.job_id)
            except Exception:
                self.counters["transport_errors"] += 1
                return False

    async def _reconcile(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                dispatched = await self.jobs.reconcile(
                    limit=100, min_interval_seconds=self.options.reconcile_min_interval_seconds,
                )
                self.counters["reconciled"] += dispatched
            except Exception:
                self.counters["reconciliation_errors"] += 1
            await self._pause(stop, self.options.reconcile_seconds)

    @staticmethod
    async def _pause(stop: asyncio.Event, seconds: float) -> None:
        with suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=seconds)

    async def run(self, stop: asyncio.Event) -> None:
        """Run until shutdown, with PG recovery independent of Redis availability."""
        self.counters["temp_directories_removed"] += await asyncio.to_thread(
            self.sources.janitor, older_than_seconds=self.options.janitor_age_seconds,
        )
        tasks: set[asyncio.Task[bool]] = set()
        reconciler = asyncio.create_task(self._reconcile(stop))
        next_claim = 0.0
        try:
            while not stop.is_set():
                tasks = {task for task in tasks if not task.done()}
                capacity = self.options.concurrency - len(tasks)
                if capacity == 0:
                    await self._pause(stop, 0.05)
                    continue
                try:
                    now = asyncio.get_running_loop().time()
                    deliveries: list[Delivery] = []
                    if now >= next_claim:
                        deliveries = await self.stream.reclaim(
                            count=capacity, idle_ms=max(1, int(self.options.reclaim_idle_seconds * 1000)),
                        )
                        next_claim = now + min(self.options.reclaim_idle_seconds, 10)
                    if not deliveries:
                        deliveries = await self.stream.read(count=capacity)
                    for delivery in deliveries[:capacity]:
                        tasks.add(asyncio.create_task(self.process(delivery)))
                except Exception:
                    self.counters["transport_errors"] += 1
                    await self._pause(stop, self.options.transport_backoff_seconds)
        finally:
            reconciler.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(reconciler, *tasks, return_exceptions=True)
