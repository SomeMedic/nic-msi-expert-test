"""Authenticated ingestion job reads and operator commands."""
from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg.rows import dict_row

from expert_clients.settings import Settings
from expert_contracts.common import IngestionStage, JobStatus
from expert_contracts.documents import IngestionJob, JobCommandAccepted, JobProgress, ReindexRequest
from expert_contracts.errors import ErrorCode, ErrorInfo
from expert_observability.web import ERRORS
from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.uploads import bind_queued_trace, fingerprint


def public_job_error_message(code: ErrorCode) -> str:
    """SQL's free safe_message field is not itself a public privacy boundary."""
    return ERRORS.get(code.value, ERRORS["INTERNAL_ERROR"])[1]


def _job_from_row(row: dict) -> IngestionJob:
    error = None
    if row.get("error_code") is not None:
        try:
            code = ErrorCode(row["error_code"])
        except ValueError:
            code = ErrorCode.INTERNAL_ERROR
        error = ErrorInfo(
            code=code,
            message=public_job_error_message(code),
            retryable=bool(row.get("last_failure_retryable")),
            request_id=uuid5(NAMESPACE_URL, f"ingestion-job:{row['id']}:{row['last_event_sequence']}:{code}"),
            details={},
        )
    progress = None
    if row.get("progress_unit") is not None:
        progress = JobProgress(
            processed_units=row["processed_units"],
            total_units=row["total_units"],
            unit=row["progress_unit"],
        )
    return IngestionJob(
        job_id=row["id"],
        version_id=row["version_id"],
        status=JobStatus(row["status"]),
        stage=IngestionStage(row["stage"]) if row.get("stage") is not None else None,
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        progress=progress,
        cancel_requested=row.get("cancel_requested_at") is not None,
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        available_at=row["available_at"],
        last_sequence=row["last_event_sequence"],
        error=error,
        trace_id=row.get("trace_id"),
    )


def _accepted(row: dict) -> JobCommandAccepted:
    return JobCommandAccepted(
        job_id=row["id"],
        status=JobStatus(row["status"]),
        last_sequence=row["last_event_sequence"],
    )


class JobService:
    def __init__(self, settings: Settings, pool):
        self.settings = settings
        self.pool = pool

    async def _one(self, statement: str, parameters: tuple, *, trace_principal: str | None = None) -> dict | None:
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        row = await (await cursor.execute(statement, parameters)).fetchone()
                        if row is not None and trace_principal is not None:
                            await bind_queued_trace(cursor, row["id"], trace_principal)
                        return row

    async def get(self, job_id: UUID, principal: Principal) -> IngestionJob:
        row = await self._one(
            """
            SELECT j.id,j.version_id,j.status,j.stage,j.attempt,j.max_attempts,j.cancel_requested_at,
                   j.created_at,j.started_at,j.finished_at,j.available_at,j.last_event_sequence,
                   j.processed_units,j.total_units,j.progress_unit,j.error_code,j.safe_error_message,
                   j.last_failure_retryable,j.trace_id,d.security_revoked_at
            FROM app.ingestion_jobs j
            JOIN app.document_versions v ON v.id=j.version_id
            JOIN app.logical_documents d ON d.id=v.logical_document_id
            WHERE j.id=%s
            """,
            (job_id,),
        )
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        if row["security_revoked_at"] is not None:
            raise ApiError(ErrorCode.SOURCE_REVOKED)
        return _job_from_row(row)

    async def cancel(self, job_id: UUID, principal: Principal) -> JobCommandAccepted:
        row = await self._command("SELECT * FROM app.cancel_ingestion(%s,%s)", (job_id, principal.subject))
        return _accepted(row)

    async def retry(self, job_id: UUID, principal: Principal) -> JobCommandAccepted:
        row = await self._command(
            "SELECT * FROM app.request_ingestion_retry(%s,%s,%s)",
            (job_id, principal.subject, self.settings.ingestion_max_queued),
        )
        return _accepted(row)

    async def reindex(
        self,
        version_id: UUID,
        principal: Principal,
        key: str,
        request: ReindexRequest,
    ) -> JobCommandAccepted:
        pipeline = self.settings.ingestion_pipeline_fingerprint
        if request.pipeline_config_alias != pipeline:
            raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "pipeline_config_alias"})
        request_hash = fingerprint({
            "command": "reindex",
            "version_id": str(version_id),
            "pipeline_config_alias": request.pipeline_config_alias,
            "expected_current_publication_id": (
                str(request.expected_current_publication_id) if request.expected_current_publication_id else None
            ),
            "auto_publish": request.auto_publish,
        })
        row = await self._command(
            "SELECT * FROM app.request_reindex(%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                version_id,
                principal.subject,
                key,
                request_hash,
                pipeline,
                request.expected_current_publication_id,
                request.auto_publish,
                self.settings.ingestion_max_queued,
            ),
            trace_principal=principal.subject,
        )
        return _accepted(row)

    async def _command(self, statement: str, parameters: tuple, *, trace_principal: str | None = None) -> dict:
        row = await self._one(statement, parameters, trace_principal=trace_principal)
        if row is None:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return row
