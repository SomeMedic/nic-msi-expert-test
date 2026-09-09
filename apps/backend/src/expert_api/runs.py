"""Principal-scoped run commands and projections of committed public state."""
from __future__ import annotations

import base64
from datetime import datetime
import hashlib
import json
import re
from typing import Final, Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from psycopg.rows import dict_row
from pydantic import ValidationError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.uploads import fingerprint, idempotency_key
from expert_clients.http import DependencyError, ServiceClient
from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole
from expert_contracts.common import RunLinks
from expert_contracts.errors import ErrorCode, ErrorInfo
from expert_contracts.internal import StartRunRequest, StartRunResponse
from expert_contracts.runs import (
    CreateRunRequest, DebugStep, PublicRun, RunAccepted, RunCancelAccepted,
    RunDebug, RunList, RunSummary,
)

TERMINAL = frozenset({"completed", "refused", "failed", "cancelled"})
START_TIMEOUT_SECONDS = 3.0
_READ = """
 SELECT r.id AS run_id,r.status,r.current_stage,r.current_stage_attempt AS stage_attempt,
        r.created_at,r.started_at,r.finished_at,r.cancel_requested_at IS NOT NULL AS cancel_requested,
        r.last_event_sequence AS last_sequence,r.terminal_error AS error,
        CASE WHEN s.id IS NULL THEN NULL ELSE jsonb_build_object(
          'id',s.id,'captured_at',s.captured_at,'version_count',
          (SELECT count(*) FROM agent.kb_snapshot_items si WHERE si.snapshot_id=s.id)) END AS snapshot,
        rr.public_result AS result
 FROM agent.runs r LEFT JOIN agent.kb_snapshots s ON s.id=r.snapshot_id
 LEFT JOIN agent.run_results rr ON rr.id=r.result_id AND rr.run_id=r.id
 WHERE r.id=%s AND r.principal_id=%s
"""
_TECHNICAL_CODES = frozenset({
    "DEPENDENCY_UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL_ERROR", "MODEL_UNAVAILABLE",
    "MODEL_TIMEOUT", "OUTPUT_SCHEMA_INVALID", "TOKEN_LIMIT_EXCEEDED", "SOURCE_REVOKED",
    "SOURCE_UNAVAILABLE", "GENERATION_INVALID", "CAPACITY_EXCEEDED",
})
_DEBUG_METADATA_FIELDS = frozenset({
    "run_id", "status", "duration_ms", "snapshot", "configuration_fingerprint", "trace_id",
    "model_calls", "capture", "private_payload_expires_at", "payloads_purged_at",
})

DebugStepStatus = Literal["started", "completed", "failed", "retry_scheduled"]
_DEBUG_STEP_STATUSES: Final[dict[str, DebugStepStatus]] = {
    "stage.started": "started",
    "stage.completed": "completed",
    "stage.retry_scheduled": "retry_scheduled",
}


def validated_terminal_error(value: object, run_id: UUID) -> ErrorInfo:
    """Stored error text must match the controlled SQL public error vocabulary."""
    try:
        error = ErrorInfo.model_validate(value)
        expected = {
            "DEADLINE_EXCEEDED": "Превышено время выполнения запроса.",
            "SOURCE_REVOKED": "Доступ к источнику отозван.",
            "OUTPUT_SCHEMA_INVALID": "Ответ модели не соответствует обязательной схеме.",
        }.get(error.code.value, "Не удалось завершить обработку запроса.")
        if (error.code.value not in _TECHNICAL_CODES or error.message != expected
                or error.request_id != run_id or error.retryable or error.details):
            raise ValueError()
        return error
    except (ValidationError, ValueError, TypeError):
        raise ApiError(ErrorCode.INTERNAL_ERROR) from None


def public_run(row: dict) -> PublicRun:
    try:
        if row.get("error") is not None:
            row = dict(row, error=validated_terminal_error(row["error"], row["run_id"]))
        return PublicRun.model_validate(row)
    except (ValidationError, ValueError, TypeError):
        raise ApiError(ErrorCode.INTERNAL_ERROR) from None


def _encode_cursor(row: dict, principal: Principal) -> str:
    payload = [1, row["created_at"].isoformat(), str(row["run_id"]),
               hashlib.sha256(principal.subject.encode()).hexdigest()]
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def _decode_cursor(value: str, principal: Principal) -> tuple[datetime, UUID]:
    try:
        if len(value) > 500 or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError()
        payload = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if (not isinstance(payload, list) or len(payload) != 4 or type(payload[0]) is not int
                or payload[0] != 1 or payload[3] != hashlib.sha256(principal.subject.encode()).hexdigest()):
            raise ValueError()
        created_at = datetime.fromisoformat(payload[1])
        if created_at.tzinfo is None or created_at.utcoffset() is None:
            raise ValueError()
        return created_at, UUID(payload[2])
    except (ValueError, TypeError, UnicodeError, KeyError):
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "cursor"}) from None


class RunService:
    def __init__(self, settings: Settings, pool, *, runtime_client: ServiceClient,
                 configuration_fingerprint: str):
        if re.fullmatch(r"[0-9a-f]{64}", configuration_fingerprint) is None:
            raise ValueError("RUN_CONFIGURATION_FINGERPRINT_INVALID")
        self.settings, self.pool = settings, pool
        self.runtime_client = runtime_client
        self.configuration_fingerprint = configuration_fingerprint

    async def create(self, principal: Principal, key: str, payload: CreateRunRequest,
                     request_id: UUID) -> RunAccepted:
        principal.require(ApplicationRole.OPERATOR)
        key = idempotency_key(key)
        if payload.debug_capture and not self.settings.debug_capture_allowed:
            raise ApiError(ErrorCode.FORBIDDEN)
        # The lock protects admission across backend replicas, while SQL owns
        # idempotency and durable creation. A retry consumes no extra slot.
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                                         ("expert:run-admission:" + principal.subject,))
                    await cursor.execute(
                        "SELECT id FROM agent.runs WHERE principal_id=%s AND idempotency_key=%s",
                        (principal.subject, key),
                    )
                    existing = await cursor.fetchone()
                    if existing is None:
                        await cursor.execute(
                            "SELECT count(*) AS active FROM agent.runs WHERE principal_id=%s "
                            "AND status IN ('created','running','cancelling')", (principal.subject,),
                        )
                        active = await cursor.fetchone()
                        if active["active"] >= self.settings.run_max_active_per_operator:
                            raise ApiError(ErrorCode.CAPACITY_EXCEEDED, details={"retry_after_seconds": 1})
                    await cursor.execute(
                        "SELECT id FROM agent.create_run(%s,%s,%s,%s,%s,"
                        "clock_timestamp()+make_interval(secs=>%s),%s,%s,%s,%s,%s)",
                        (uuid4(), principal.subject, payload.question, key,
                         fingerprint(payload.model_dump(mode="json")), self.settings.run_deadline_seconds,
                         self.configuration_fingerprint, payload.debug_capture,
                         self.settings.run_max_recovery_attempts, self.settings.debug_capture_ttl_hours,
                         self.settings.private_step_artifact_retention_hours),
                    )
                    row = await cursor.fetchone()
                    run_id = row["id"]
        # No caller-controlled question, model options or ownership claims cross
        # the private start boundary. The runtime reads authoritative PG state.
        try:
            await self.runtime_client.post(
                f"/v1/runs/{run_id}/start",
                StartRunRequest(request_id=request_id,
                                execution_request_id=uuid5(NAMESPACE_URL, f"expert:run-start:{run_id}")),
                StartRunResponse, request_id=request_id, timeout_seconds=START_TIMEOUT_SECONDS,
            )
        except DependencyError:
            # A committed run remains accepted; reconciliation retries start.
            pass
        return RunAccepted(run_id=run_id, links=RunLinks(
            self=f"/api/v1/runs/{run_id}", events=f"/api/v1/runs/{run_id}/events",
        ))

    async def get(self, run_id: UUID, principal: Principal) -> PublicRun:
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(_READ, (run_id, principal.subject))
                    row = await cursor.fetchone()
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        return public_run(row)

    async def list(self, principal: Principal, *, limit: int = 50, cursor: str | None = None) -> RunList:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "limit"})
        boundary = _decode_cursor(cursor, principal) if cursor is not None else None
        query = """SELECT id AS run_id,left(question,500) AS question_excerpt,status,created_at,
          CASE WHEN finished_at IS NULL THEN NULL ELSE
          greatest(0,floor(extract(epoch FROM (finished_at-created_at))*1000))::bigint END AS duration_ms
          FROM agent.runs WHERE principal_id=%s"""
        parameters: list = [principal.subject]
        if boundary is not None:
            query += " AND (created_at,id)<(%s,%s)"
            parameters.extend(boundary)
        query += " ORDER BY created_at DESC,id DESC LIMIT %s"
        parameters.append(limit + 1)
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.cursor(row_factory=dict_row) as db_cursor:
                    await db_cursor.execute(query, parameters)
                    rows = await db_cursor.fetchall()
        try:
            return RunList(items=[RunSummary.model_validate(row) for row in rows[:limit]],
                           next_cursor=_encode_cursor(rows[limit - 1], principal) if len(rows) > limit else None)
        except (ValidationError, ValueError, TypeError):
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def cancel(self, run_id: UUID, principal: Principal) -> PublicRun | RunCancelAccepted:
        principal.require(ApplicationRole.OPERATOR)
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                await connection.execute("SELECT id FROM agent.request_cancel(%s,%s)",
                                         (run_id, principal.subject))
        current = await self.get(run_id, principal)
        if current.status in TERMINAL:
            return current
        if current.status != "cancelling":
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return RunCancelAccepted(run_id=run_id, last_sequence=current.last_sequence)

    async def debug(self, run_id: UUID, principal: Principal) -> RunDebug:
        principal.require(ApplicationRole.OPERATOR)
        # The definer routine exposes a closed metadata projection; the backend
        # has no general SELECT grant on private artifacts or evidence packs.
        from expert_api.run_events import event_from_row

        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("SELECT agent.get_run_debug_metadata(%s,%s) AS metadata",
                                         (run_id, principal.subject))
                    row = await cursor.fetchone()
                    if row is None or row["metadata"] is None:
                        raise ApiError(ErrorCode.NOT_FOUND)
                    metadata = row["metadata"]
                    await cursor.execute(
                        "SELECT e.* FROM agent.run_events e JOIN agent.runs r ON r.id=e.run_id "
                        "WHERE e.run_id=%s AND r.principal_id=%s AND e.stage IS NOT NULL "
                        "ORDER BY e.sequence LIMIT 501", (run_id, principal.subject),
                    )
                    rows = await cursor.fetchall()
        if len(rows) > 500:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        steps = []
        for row in rows:
            event = event_from_row(row, run_id)
            if event.type not in {"stage.started", "stage.completed", "stage.retry_scheduled"}:
                continue
            status = _DEBUG_STEP_STATUSES[event.type]
            steps.append(DebugStep(stage=event.stage, attempt=event.attempt,
                                   status=status, occurred_at=event.occurred_at,
                                   duration_ms=getattr(event.data, "duration_ms", None),
                                   candidate_count=getattr(event.data, "candidate_count", None)))
        try:
            if not isinstance(metadata, dict) or metadata.keys() != _DEBUG_METADATA_FIELDS:
                raise ValueError("RUN_DEBUG_PROJECTION_INVALID")
            debug = RunDebug.model_validate({**metadata, "steps": steps})
            if debug.run_id != run_id:
                raise ValueError("RUN_DEBUG_BINDING_INVALID")
            trace_url = (str(self.settings.jaeger_public_base_url).rstrip("/") + "/trace/" + debug.trace_id
                         if debug.trace_id is not None else None)
            # Revalidate the generated link and all safe fields; model_copy(update)
            # would bypass the shared DTO's trace URL and payload guards.
            return RunDebug.model_validate({**debug.model_dump(), "trace_url": trace_url})
        except (ValidationError, ValueError, TypeError):
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None
