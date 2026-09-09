from __future__ import annotations

import json
import logging
import math
import re
from contextvars import ContextVar
from datetime import datetime, timezone
from uuid import UUID

from expert_contracts.common import IngestionStage, RunStage
from expert_contracts.errors import ErrorCode
from expert_observability.tracing import correlation_fields

request_id_context: ContextVar[str | None] = ContextVar("request_id", default=None)
SAFE_FIELDS = frozenset({
    "request_id", "run_id", "job_id", "trace_id", "span_id", "execution_epoch",
    "status_code", "duration_ms", "attempt", "count", "error_code", "stage",
})
SAFE_STRING = re.compile(r"^[a-zA-Z0-9_.:-]{1,128}$")
SAFE_EVENTS = frozenset({"http.request", "http.unhandled", "service.start", "service.stop", "dependency.failed"})
NUMERIC_FIELDS = frozenset({"execution_epoch", "status_code", "duration_ms", "attempt", "count"})
UUID_FIELDS = frozenset({"request_id", "run_id", "job_id"})
STAGES = frozenset(RunStage) | frozenset(IngestionStage)


def safe_field(key: str, value):
    if key in NUMERIC_FIELDS:
        if isinstance(value, int) and not isinstance(value, bool):
            return value if 0 <= value <= 2**63 - 1 else None
        if isinstance(value, float) and math.isfinite(value) and value >= 0:
            return round(value, 3)
    elif key in UUID_FIELDS:
        try:
            return str(UUID(str(value)))
        except ValueError:
            return None
    elif key in {"trace_id", "span_id"}:
        length = 32 if key == "trace_id" else 16
        if isinstance(value, str) and re.fullmatch(f"[0-9a-f]{{{length}}}", value) and int(value, 16):
            return value
    elif key == "stage" and isinstance(value, str) and value in STAGES:
        return value
    elif key == "error_code" and isinstance(value, str) and value in ErrorCode:
        return value
    return None


class SafeJsonFormatter(logging.Formatter):
    """External messages/exception bodies are discarded, including exc_info."""

    service_name: str = "unknown"

    def format(self, record: logging.LogRecord) -> str:
        event = record.msg if isinstance(record.msg, str) and record.msg in SAFE_EVENTS and not record.args else "external_log"
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": getattr(self, "service_name", "unknown"),
            "event": event,
        }
        correlation = request_id_context.get()
        cleaned_request = safe_field("request_id", correlation) if correlation else None
        if cleaned_request:
            payload["request_id"] = cleaned_request
        supplied = getattr(record, "safe_fields", {})
        if not isinstance(supplied, dict):
            supplied = {}
        for key, value in {**correlation_fields(), **supplied}.items():
            if key not in SAFE_FIELDS:
                continue
            cleaned = safe_field(key, value)
            if cleaned is not None:
                payload[key] = cleaned
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def configure_logging(service_name: str, level: str = "INFO") -> None:
    formatter = SafeJsonFormatter()
    formatter.service_name = service_name
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
