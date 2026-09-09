"""One native inference owner with bounded admission and cancellation-safe capacity."""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from contextvars import copy_context
from typing import Any, Callable

from expert_clients.settings import Settings
from expert_observability.tracing import (
    correlation_context, correlation_fields, inject_trace_headers,
    internal_trace_context, safe_attributes, safe_span,
)

from .adapters import LocalModels
from .errors import ModelError
from .profile import RuntimeProfile

logger = logging.getLogger(__name__)


@dataclass
class Work:
    operation: str
    request: Any
    result: asyncio.Future
    admitted_at: float
    deadline: float
    trace_headers: dict[str, str]
    correlation: dict[str, str | int | float]
    outcome_code: str | None = None


class ModelRuntime:
    def __init__(self, settings: Settings, *, factory: Callable[[Settings], Any] = LocalModels):
        self.settings = settings
        self._factory = factory
        self._models: Any = None
        self._executor: ThreadPoolExecutor | None = None
        self._task: asyncio.Task | None = None
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=settings.ml_admission_slots)
        self._sequence = itertools.count()
        self._admitted = 0
        self._closing = False
        self.ready = False
        self.startup_finished = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None or self._closing:
            raise RuntimeError("Model runtime cannot be restarted")
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="expert-inference")
        self._task = asyncio.create_task(self._serve(), name="expert-model-runtime")

    @property
    def profile(self) -> RuntimeProfile:
        if not self.ready:
            raise ModelError("MODEL_UNAVAILABLE")
        return self._models.profile

    @property
    def admitted(self) -> int:
        return self._admitted

    async def invoke(self, operation: str, request: Any) -> tuple[Any, dict]:
        if not self.ready or self._closing:
            raise ModelError("MODEL_UNAVAILABLE")
        if operation not in {"query", "documents", "rerank"}:
            raise ModelError("INVALID_REQUEST")
        if self._admitted >= self.settings.ml_admission_slots:
            raise ModelError("CAPACITY_EXCEEDED", {"retry_after_seconds": 2})
        # No await between admission and enqueue: one event loop owns the bounded counter.
        self._admitted += 1
        now = time.monotonic()
        future = asyncio.get_running_loop().create_future()
        work = Work(operation, request, future, now, now + self.settings.ml_request_timeout_seconds,
                    inject_trace_headers(), safe_attributes(correlation_fields()))
        self._queue.put_nowait((0 if operation == "query" else 1, next(self._sequence), work))
        try:
            async with asyncio.timeout_at(asyncio.get_running_loop().time() + self.settings.ml_request_timeout_seconds):
                return await asyncio.shield(future)
        except TimeoutError:
            work.outcome_code = "MODEL_TIMEOUT"
            future.cancel()
            raise ModelError("MODEL_TIMEOUT") from None
        except asyncio.CancelledError:
            work.outcome_code = "RUN_CANCELLED"
            future.cancel()
            raise
        # Capacity is released by _serve, never by a cancelled/timed-out HTTP request.

    async def _serve(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            self._models = self._factory(self.settings)
            await loop.run_in_executor(self._executor, self._models.load)
            self.ready = not self._closing
        except Exception:
            logger.error("dependency.failed", extra={"safe_fields": {"error_code": "MODEL_UNAVAILABLE"}})
        finally:
            self.startup_finished.set()
        try:
            while self.ready and not self._closing:
                _, _, work = await self._queue.get()
                if work is None:
                    break
                try:
                    if work.result.cancelled():
                        continue
                    if time.monotonic() >= work.deadline:
                        raise ModelError("MODEL_TIMEOUT")
                    queue_ms = (time.monotonic() - work.admitted_at) * 1000
                    response, metrics = await self._execute(work)
                    if time.monotonic() >= work.deadline:
                        raise ModelError("MODEL_TIMEOUT")
                    if not work.result.done():
                        work.result.set_result((response, {**metrics, "queue_ms": queue_ms}))
                except ModelError as error:
                    if error.code in {"MODEL_UNAVAILABLE", "OUTPUT_SCHEMA_INVALID"}:
                        self.ready = False
                    if not work.result.done():
                        work.result.set_exception(error)
                except Exception:
                    self.ready = False
                    if not work.result.done():
                        work.result.set_exception(ModelError("MODEL_UNAVAILABLE"))
                    logger.error("dependency.failed", extra={"safe_fields": {"error_code": "MODEL_UNAVAILABLE"}})
                finally:
                    self._admitted -= 1
                    self._queue.task_done()
        finally:
            self.ready = False
            while not self._queue.empty():
                _, _, pending = self._queue.get_nowait()
                if pending is not None:
                    self._admitted -= 1
                    if not pending.result.done():
                        pending.result.set_exception(ModelError("MODEL_UNAVAILABLE"))
                self._queue.task_done()
            if self._models is not None:
                await loop.run_in_executor(self._executor, self._models.close)

    async def _execute(self, work: Work) -> tuple[Any, dict]:
        recipe = self._models.profile.reranker_recipe if work.operation == "rerank" else self._models.profile.embedding_recipe
        # Queued work runs in the startup task, so preserve only explicit safe
        # propagation and IDs at admission, then restore/clear them per native call.
        with internal_trace_context(work.trace_headers), correlation_context(**work.correlation):
            with safe_span("model.rerank" if work.operation == "rerank" else "model.embed",
                           model=recipe.model, model_revision=recipe.revision,
                           profile_fingerprint=recipe.runtime_fingerprint) as span:
                try:
                    response, metrics = await asyncio.get_running_loop().run_in_executor(
                        self._executor, copy_context().run, self._models.execute, work.operation, work.request)
                    span.set_attributes(input_tokens=metrics.get("input_tokens"))
                    if work.outcome_code == "MODEL_TIMEOUT":
                        span.fail("MODEL_TIMEOUT")
                    elif work.result.cancelled():
                        span.set_attributes(status="cancelled", error_code="RUN_CANCELLED")
                    else:
                        span.set_attributes(status="ok")
                    return response, metrics
                except ModelError as error:
                    span.fail(error.code)
                    raise

    async def close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.ready = False
        if self._task is not None and not self._task.done():
            # Wake an idle worker. A running native call is joined, never cancelled or replaced.
            if not self._queue.full():
                self._queue.put_nowait((-1, next(self._sequence), None))
            await asyncio.shield(self._task)
        if self._executor is not None:
            await asyncio.to_thread(self._executor.shutdown, wait=True, cancel_futures=False)
