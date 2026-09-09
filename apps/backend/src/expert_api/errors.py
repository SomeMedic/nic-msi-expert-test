"""Translate only allowlisted database outcomes into public errors."""
from contextlib import contextmanager

from fastapi import FastAPI, Request
import psycopg

from expert_contracts.errors import ErrorCode, Scalar
from expert_observability.web import error_response


class ApiError(Exception):
    def __init__(self, code: ErrorCode, *, details: dict[str, Scalar] | None = None):
        super().__init__(code.value)
        self.code = code
        self.details = details or {}


DATABASE_CODES = {
    "IDEMPOTENCY_CONFLICT": ErrorCode.IDEMPOTENCY_CONFLICT,
    "VERSION_CONFLICT": ErrorCode.VERSION_CONFLICT,
    "SOURCE_REVOKED": ErrorCode.SOURCE_REVOKED,
    "GENERATION_NOT_READY": ErrorCode.GENERATION_INVALID,
    "CAPACITY_EXCEEDED": ErrorCode.CAPACITY_EXCEEDED,
    "DEADLINE_EXCEEDED": ErrorCode.DEADLINE_EXCEEDED,
    "CANCEL_REQUESTED": ErrorCode.TERMINAL_CONFLICT,
    "STALE_INGESTION": ErrorCode.TERMINAL_CONFLICT,
    "TERMINAL_CONFLICT": ErrorCode.TERMINAL_CONFLICT,
    "UPLOAD_EXPIRED": ErrorCode.TERMINAL_CONFLICT,
    "UPLOAD_NOT_STORED": ErrorCode.SOURCE_UNAVAILABLE,
    "OBJECT_VERIFICATION_FAILED": ErrorCode.SOURCE_UNAVAILABLE,
    "UPLOAD_NOT_ATTACHABLE": ErrorCode.TERMINAL_CONFLICT,
    "INGESTION_NOT_RUNNING": ErrorCode.TERMINAL_CONFLICT,
    "INGESTION_NOT_RETRYABLE": ErrorCode.TERMINAL_CONFLICT,
    "INGESTION_ATTEMPTS_EXHAUSTED": ErrorCode.TERMINAL_CONFLICT,
    "LEASE_EXPIRED": ErrorCode.TERMINAL_CONFLICT,
    "INVALID_PURGE_ARGUMENT": ErrorCode.INVALID_REQUEST,
    "PURGE_PLAN_CHANGED": ErrorCode.VERSION_CONFLICT,
    "PURGE_BLOCKED": ErrorCode.VERSION_CONFLICT,
    "PURGE_ALREADY_PENDING": ErrorCode.VERSION_CONFLICT,
    "PURGE_PLAN_EXPIRED": ErrorCode.TERMINAL_CONFLICT,
    "DOCUMENT_PURGE_PENDING": ErrorCode.SOURCE_UNAVAILABLE,
    "DOCUMENT_PURGED": ErrorCode.SOURCE_UNAVAILABLE,
}
for _name in ("DOCUMENT_NOT_FOUND", "VERSION_NOT_FOUND", "JOB_NOT_FOUND", "RUN_NOT_FOUND", "UPLOAD_NOT_FOUND", "INGESTION_NOT_FOUND", "PURGE_PLAN_NOT_FOUND"):
    DATABASE_CODES[_name] = ErrorCode.NOT_FOUND


@contextmanager
def database_failures():
    try:
        yield
    except psycopg.Error as error:
        code = DATABASE_CODES.get(error.diag.message_primary or "")
        if code is None:
            code = ErrorCode.DEPENDENCY_UNAVAILABLE if isinstance(error, psycopg.OperationalError) else ErrorCode.INTERNAL_ERROR
        raise ApiError(code) from None


def install_api_errors(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(request: Request, error: ApiError):
        return error_response(error.code.value, request.state.request_id, details=error.details)
