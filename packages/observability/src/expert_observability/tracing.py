"""Explicit local tracing. No payload, automatic instrumentation or environment transport.

The SDK owns span/context semantics; the pinned OTLP package owns protobuf encoding.
Our small processor owns the bounded queue and lifecycle because SDK 1.44's batch
force_flush explicitly ignores its timeout. Only this module's safe facade is public.
"""
from __future__ import annotations

import asyncio
import http.client
import math
import re
import threading
import time
from collections import deque
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Protocol
from urllib.parse import urlsplit
from uuid import UUID

from opentelemetry import context, trace
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanLimits, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.sdk.util.instrumentation import InstrumentationScope
from opentelemetry.trace import NonRecordingSpan, SpanContext, Status, StatusCode, TraceFlags, TraceState

from expert_contracts.common import IngestionStage, RunStage
from expert_contracts.errors import ErrorCode

SERVICES = frozenset({"backend", "agent-runtime", "ingestion-worker", "retrieval-ml", "outbox-publisher"})
SPAN_NAMES = frozenset({
    "http.request", "run.create", "run.start", "run.execute", "snapshot.capture",
    "router.catalog_search", "router.classify", "retrieval.dense", "retrieval.lexical",
    "retrieval.fusion", "retrieval.prepare", "retrieval.search", "rerank", "context.build",
    "drafter.generate", "citations.check", "critic.validate", "repair.generate", "finalizer.commit",
    "model.embed", "model.rerank", "model.generate", "ingestion.execute", "ingestion.parse",
    "ingestion.embed", "ingestion.index", "ingestion.publish", "outbox.publish", "lease.recover",
})
EVENT_NAMES = frozenset({"stage.retry_scheduled", "lease.lost", "dependency.failed", "artifact.reused"})
ID_KEYS = frozenset({"request_id", "run_id", "job_id", "snapshot_id", "version_id", "parse_generation_id"})
COUNT_KEYS = frozenset({
    "execution_epoch", "attempt", "count", "dense_count", "lexical_count", "fused_count",
    "reranked_count", "evidence_count", "claim_count", "supported_count", "page_count", "chunk_count",
    "input_tokens", "output_tokens", "total_tokens", "schema_attempt", "repair_attempt", "status_code",
})
REVISION_KEYS = frozenset({"model_revision", "prompt_revision", "configuration_fingerprint", "profile_fingerprint"})
STAGES = frozenset(RunStage) | frozenset(IngestionStage)
STATUSES = frozenset({"ok", "error", "cancelled", "completed", "refused", "failed", "reused", "retrying"})
MODELS = frozenset({"ai-forever/FRIDA", "Qwen/Qwen3-Reranker-0.6B", "Qwen/Qwen3-14B-AWQ"})
SCHEMA_VERSION = "p11.safe.v1"
QUEUE_SIZE = 256
BATCH_SIZE = 32
EXPORT_TIMEOUT_SECONDS = 2.0

_correlation: ContextVar[dict[str, str | int | float]] = ContextVar("safe_correlation", default={})
_active: ContextVar[TracingHandle | None] = ContextVar("safe_tracing", default=None)
_default: TracingHandle | None = None
_restored_trace: ContextVar[int | None] = ContextVar("restored_run_trace", default=None)


class _RunIdGenerator(RandomIdGenerator):
    def generate_trace_id(self) -> int:
        return _restored_trace.get() or super().generate_trace_id()


class TracingSettings(Protocol):
    @property
    def service_name(self) -> str: ...

    @property
    def otel_service_name(self) -> str | None: ...

    @property
    def otel_exporter_otlp_endpoint(self) -> object | None: ...


def safe_attributes(values: Mapping[str, object]) -> dict[str, str | int | float]:
    result: dict[str, str | int | float] = {}
    for key, value in values.items():
        if key in ID_KEYS and isinstance(value, (str, UUID)):
            try:
                parsed = UUID(str(value))
                if parsed.int:
                    result[key] = str(parsed)
            except ValueError:
                pass
        elif key in COUNT_KEYS and type(value) is int and 0 <= value <= 2**31 - 1:
            if key != "status_code" or 100 <= value <= 599:
                result[key] = value
        elif key == "duration_ms" and type(value) in (int, float):
            if 0 <= value <= 604_800_000 and math.isfinite(value):  # type: ignore[arg-type,operator]
                result[key] = round(value, 3)  # type: ignore[call-overload]
        elif key in REVISION_KEYS and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
            result[key] = value
        elif isinstance(value, str) and (
            (key == "stage" and value in STAGES) or (key == "status" and value in STATUSES)
            or (key == "model" and value in MODELS) or (key == "error_code" and value in ErrorCode)
        ):
            result[key] = value
    return result


def _clean_context(value: SpanContext | None) -> SpanContext | None:
    if value is None or not value.is_valid:
        return None
    return SpanContext(value.trace_id, value.span_id, value.is_remote,
                       TraceFlags(int(value.trace_flags) & 1), TraceState())


def _clean_span(span: ReadableSpan, service: str) -> ReadableSpan | None:
    if span.name not in SPAN_NAMES:
        return None
    return ReadableSpan(
        name=span.name, context=_clean_context(span.context), parent=_clean_context(span.parent),
        resource=Resource({"service.name": service, "telemetry.schema.version": SCHEMA_VERSION}),
        attributes=safe_attributes(span.attributes or {}),
        events=tuple(Event(event.name, safe_attributes(event.attributes or {}), event.timestamp)
                     for event in span.events[:8] if event.name in EVENT_NAMES),
        links=(), kind=span.kind, status=Status(span.status.status_code),
        start_time=span.start_time, end_time=span.end_time,
        instrumentation_scope=InstrumentationScope("expert.safe", SCHEMA_VERSION),
    )


def _endpoint(value: object) -> tuple[str, int, str]:
    try:
        parsed = urlsplit(str(value))
        if (parsed.scheme != "http" or parsed.hostname not in {"otel-collector", "127.0.0.1", "localhost", "::1"}
                or parsed.username or parsed.password or parsed.query or parsed.fragment
                or parsed.path not in {"", "/", "/v1/traces"}):
            raise ValueError()
        return parsed.hostname, parsed.port or 4318, "/v1/traces"
    except (ValueError, TypeError):
        raise ValueError("TRACING_CONFIGURATION_INVALID") from None


class LocalOTLPExporter(SpanExporter):
    """Single request, no redirects/proxies/netrc/env/response-body logging or retry."""

    def __init__(self, endpoint: object):
        self._host, self._port, self._path = _endpoint(endpoint)
        self._closed = threading.Event()

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        if self._closed.is_set():
            return SpanExportResult.FAILURE
        connection = http.client.HTTPConnection(self._host, self._port, timeout=EXPORT_TIMEOUT_SECONDS)
        try:
            payload = encode_spans(spans).SerializeToString()
            if len(payload) > 512 * 1024:
                return SpanExportResult.FAILURE
            connection.request("POST", self._path, body=payload,
                               headers={"Content-Type": "application/x-protobuf", "Connection": "close"})
            response = connection.getresponse()
            # Even error bodies/reasons are never read or formatted. Redirects fail.
            return SpanExportResult.SUCCESS if 200 <= response.status < 300 else SpanExportResult.FAILURE
        except Exception:
            return SpanExportResult.FAILURE
        finally:
            connection.close()

    def shutdown(self) -> None:
        self._closed.set()


class _BoundedProcessor(SpanProcessor):
    def __init__(self, exporter: SpanExporter, service: str):
        self._exporter, self._service = exporter, service
        self._condition = threading.Condition()
        self._queue: deque[ReadableSpan] = deque()
        self._accepted = self._finished = 0
        self._closed = self._stop = False
        self.dropped = self.failed_batches = self.delivered = 0
        self._thread = threading.Thread(target=self._work, name="expert-safe-otel", daemon=True)
        self._thread.start()

    def on_start(self, span: Span, parent_context: context.Context | None = None) -> None:
        pass

    def on_end(self, span: ReadableSpan) -> None:
        cleaned = _clean_span(span, self._service)
        if cleaned is None:
            return
        with self._condition:
            if self._closed or len(self._queue) >= QUEUE_SIZE:
                self.dropped += 1
                return
            self._queue.append(cleaned)
            self._accepted += 1
            self._condition.notify_all()

    def _work(self) -> None:
        while True:
            with self._condition:
                self._condition.wait_for(lambda: self._queue or self._stop)
                if self._stop:
                    return
                batch = [self._queue.popleft() for _ in range(min(BATCH_SIZE, len(self._queue)))]
            try:
                success = self._exporter.export(batch) == SpanExportResult.SUCCESS
            except Exception:
                success = False  # Untrusted exporter exceptions never reach SDK logging.
            with self._condition:
                self._finished += len(batch)
                self.delivered += len(batch) if success else 0
                self.failed_batches += 0 if success else 1
                self._condition.notify_all()

    def force_flush(self, timeout_millis: int = 3000) -> bool:
        timeout = min(max(timeout_millis, 0), 5000) / 1000
        with self._condition:
            target = self._accepted
            failures = self.failed_batches
            complete = self._condition.wait_for(lambda: self._finished >= target, timeout)
            return complete and self.failed_batches == failures

    def shutdown(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
        self.force_flush(3000)
        with self._condition:
            self._stop = True
            self.dropped += len(self._queue)
            self._queue.clear()
            self._condition.notify_all()
        # Exporter has no flush/network action in shutdown. A stuck remote read can
        # occupy this one daemon thread, never keep product shutdown alive.
        try:
            self._exporter.shutdown()
        except Exception:
            pass
        self._thread.join(timeout=0.05)


class TracingHandle:
    def __init__(self, service: str, exporter: SpanExporter | None):
        self.enabled = exporter is not None
        self._processor = _BoundedProcessor(exporter, service) if exporter is not None else None
        self._provider = TracerProvider(
            resource=Resource({"service.name": service}), sampler=ALWAYS_ON, shutdown_on_exit=False,
            id_generator=_RunIdGenerator(),
            span_limits=SpanLimits(max_attributes=48, max_events=8, max_links=0, max_attribute_length=128),
        )
        if self._processor is not None:
            self._provider.add_span_processor(self._processor)
        self._tracer = self._provider.get_tracer("expert.safe", SCHEMA_VERSION)

    def force_flush(self, timeout_millis: int = 3000) -> bool:
        return self._processor.force_flush(timeout_millis) if self._processor else True

    def shutdown(self) -> None:
        self.enabled = False
        self._provider.shutdown()

    def delivery_status(self) -> dict[str, int | bool]:
        processor = self._processor
        return {"enabled": self.enabled, "delivered_spans": processor.delivered if processor else 0,
                "dropped_spans": processor.dropped if processor else 0,
                "failed_batches": processor.failed_batches if processor else 0}


def configure_tracing(settings: TracingSettings, *, exporter: SpanExporter | None = None,
                      activate: bool = True) -> TracingHandle:
    """Application startup once; injected exporters are for explicit bounded tests."""
    global _default
    service = settings.otel_service_name or settings.service_name
    if service not in SERVICES or service != settings.service_name:
        raise ValueError("TRACING_CONFIGURATION_INVALID")
    if exporter is None and settings.otel_exporter_otlp_endpoint is not None:
        exporter = LocalOTLPExporter(settings.otel_exporter_otlp_endpoint)
    handle = TracingHandle(service, exporter)
    if activate:
        if _default is not None:
            _default.shutdown()
        _default = handle
    return handle


@contextmanager
def use_tracing(handle: TracingHandle) -> Iterator[None]:
    token = _active.set(handle)
    try:
        yield
    finally:
        _active.reset(token)


@contextmanager
def correlation_context(**metadata: object) -> Iterator[None]:
    token = _correlation.set({**_correlation.get(), **safe_attributes(metadata)})
    try:
        yield
    finally:
        _correlation.reset(token)


def correlation_fields() -> dict[str, str | int | float]:
    result = dict(_correlation.get())
    current = trace.get_current_span().get_span_context()
    if current.is_valid:
        result.update(trace_id=format(current.trace_id, "032x"), span_id=format(current.span_id, "016x"))
    return result


def _remote_parent(headers: Mapping[str, str]) -> SpanContext | None:
    value = headers.get("traceparent", "")
    if not isinstance(value, str) or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]", value):
        return None
    _, trace_id, span_id, flags = value.split("-")
    parent = SpanContext(int(trace_id, 16), int(span_id, 16), True, TraceFlags(int(flags, 16)), TraceState())
    return parent if parent.is_valid else None


@contextmanager
def internal_trace_context(headers: Mapping[str, str]) -> Iterator[None]:
    """Caller MUST authenticate internal service first. No baggage or tracestate."""
    parent = _remote_parent(headers)
    fresh = context.Context()
    token = context.attach(trace.set_span_in_context(NonRecordingSpan(parent), fresh) if parent else fresh)
    try:
        yield
    finally:
        context.detach(token)


def inject_trace_headers() -> dict[str, str]:
    current = trace.get_current_span().get_span_context()
    if not current.is_valid:
        return {}
    return {"traceparent": f"00-{current.trace_id:032x}-{current.span_id:016x}-{int(current.trace_flags) & 1:02x}"}


@contextmanager
def restored_run_trace(trace_id: str) -> Iterator[None]:
    """Trusted PG binding only: a new root in the existing trace, no invented parent."""
    if not isinstance(trace_id, str) or not re.fullmatch(r"[0-9a-f]{32}", trace_id) or not int(trace_id, 16):
        raise ValueError("TRACING_CONTEXT_INVALID")
    token = _restored_trace.set(int(trace_id, 16))
    try:
        with internal_trace_context({}):
            yield
    finally:
        _restored_trace.reset(token)


class SafeSpan:
    __slots__ = ("_span", "_failed")

    def __init__(self, span: trace.Span):
        self._span = span
        self._failed = False

    @property
    def trace_id(self) -> str | None:
        value = self._span.get_span_context()
        return f"{value.trace_id:032x}" if value.is_valid else None

    @property
    def span_id(self) -> str | None:
        value = self._span.get_span_context()
        return f"{value.span_id:016x}" if value.is_valid else None

    def set_attributes(self, **metadata: object) -> None:
        self._span.set_attributes(safe_attributes(metadata))

    def event(self, name: str, **metadata: object) -> None:
        if name in EVENT_NAMES:
            self._span.add_event(name, safe_attributes(metadata))

    def fail(self, error_code: str = "INTERNAL_ERROR") -> None:
        self._failed = True
        self.set_attributes(error_code=error_code if error_code in ErrorCode else "INTERNAL_ERROR", status="error")
        self._span.set_status(Status(StatusCode.ERROR))


@contextmanager
def safe_span(name: str, **metadata: object) -> Iterator[SafeSpan]:
    if name not in SPAN_NAMES:
        raise ValueError("TRACING_SPAN_UNKNOWN")
    handle = _active.get() or _default
    span = (handle._tracer.start_span(name, attributes=safe_attributes({**_correlation.get(), **metadata}))
            if handle is not None and handle.enabled else trace.INVALID_SPAN)
    with trace.use_span(span, end_on_exit=True, record_exception=False, set_status_on_exception=False):
        wrapper = SafeSpan(span)
        begin = time.monotonic()
        try:
            yield wrapper
        except asyncio.CancelledError:
            wrapper.set_attributes(status="cancelled", error_code="RUN_CANCELLED")
            raise
        except BaseException:
            if not wrapper._failed:
                wrapper.fail()
            raise
        finally:
            wrapper.set_attributes(duration_ms=(time.monotonic() - begin) * 1000)
