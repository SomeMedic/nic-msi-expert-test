"""Stable public error and refusal codes; no exception objects in transport."""
from enum import StrEnum
from typing import Annotated
from uuid import UUID

from pydantic import Field, StrictBool, StrictFloat, StrictInt, StrictStr, model_validator

from .common import NonBlank, StrictDTO


class ErrorCode(StrEnum):
    INVALID_REQUEST = "INVALID_REQUEST"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    TERMINAL_CONFLICT = "TERMINAL_CONFLICT"
    SIZE_LIMIT_EXCEEDED = "SIZE_LIMIT_EXCEEDED"
    UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE"
    CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    MODEL_UNAVAILABLE = "MODEL_UNAVAILABLE"
    MODEL_TIMEOUT = "MODEL_TIMEOUT"
    OUTPUT_SCHEMA_INVALID = "OUTPUT_SCHEMA_INVALID"
    TOKEN_LIMIT_EXCEEDED = "TOKEN_LIMIT_EXCEEDED"
    EVENT_HISTORY_EXPIRED = "EVENT_HISTORY_EXPIRED"
    EVENT_CURSOR_INVALID = "EVENT_CURSOR_INVALID"
    SOURCE_REVOKED = "SOURCE_REVOKED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    PDF_INVALID = "PDF_INVALID"
    EXTRACTION_QUALITY_FAILED = "EXTRACTION_QUALITY_FAILED"
    GENERATION_INVALID = "GENERATION_INVALID"
    RUN_CANCELLED = "RUN_CANCELLED"
    NO_RELEVANT_CONTEXT = "NO_RELEVANT_CONTEXT"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"


class RefusalCode(StrEnum):
    NO_RELEVANT_CONTEXT = "NO_RELEVANT_CONTEXT"
    VERIFICATION_FAILED = "VERIFICATION_FAILED"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"


NO_RELEVANT_CONTEXT_MESSAGE = (
    "Извините, по вашему запросу не найдено информации в действующих нормативных документах. "
    "Пожалуйста, уточните запрос"
)
VERIFICATION_FAILED_MESSAGE = "Извините, система не смогла верифицировать ответ. Пожалуйста, уточните запрос"
OUT_OF_SCOPE_MESSAGE = "Извините, запрос не относится к предметной области загруженных нормативных документов. Пожалуйста, уточните запрос"
REFUSAL_MESSAGES = {
    RefusalCode.NO_RELEVANT_CONTEXT: NO_RELEVANT_CONTEXT_MESSAGE,
    RefusalCode.VERIFICATION_FAILED: VERIFICATION_FAILED_MESSAGE,
    RefusalCode.OUT_OF_SCOPE: OUT_OF_SCOPE_MESSAGE,
}

Scalar = StrictStr | StrictInt | StrictFloat | StrictBool | None
_DETAIL_KEYS = {
    ErrorCode.INVALID_REQUEST: {"field", "reason"},
    ErrorCode.VALIDATION_ERROR: {"field", "reason", "limit"},
    ErrorCode.SIZE_LIMIT_EXCEEDED: {"max_bytes", "max_chars"},
    ErrorCode.UNSUPPORTED_MEDIA_TYPE: {"media_type"},
    ErrorCode.TOKEN_LIMIT_EXCEEDED: {"max_tokens", "input_tokens"},
    ErrorCode.CAPACITY_EXCEEDED: {"retry_after_seconds"},
    ErrorCode.VERSION_CONFLICT: {"expected_publication_id", "current_publication_id"},
    ErrorCode.EVENT_HISTORY_EXPIRED: {"snapshot_url", "last_sequence"},
    ErrorCode.EVENT_CURSOR_INVALID: {"last_sequence"},
}


class ErrorInfo(StrictDTO):
    code: ErrorCode
    message: Annotated[NonBlank, Field(max_length=1000)]
    retryable: bool
    request_id: UUID
    details: dict[str, Scalar] = Field(default_factory=dict, max_length=8)

    @model_validator(mode="after")
    def allowlisted_details(self):
        if not self.details.keys() <= _DETAIL_KEYS.get(self.code, set()):
            raise ValueError("details keys are not allowed for this error code")
        return self


class ErrorEnvelope(StrictDTO):
    error: ErrorInfo
