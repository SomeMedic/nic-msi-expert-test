from __future__ import annotations

import hmac
import logging
import time
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from expert_contracts.errors import ErrorCode, ErrorEnvelope, ErrorInfo, Scalar
from expert_observability.logging import request_id_context
from expert_observability.tracing import correlation_context, internal_trace_context, safe_span

logger = logging.getLogger(__name__)
ERRORS = {
    "INVALID_REQUEST": (400, "Некорректный запрос", False),
    "VALIDATION_ERROR": (422, "Проверьте поля запроса", False),
    "UNAUTHENTICATED": (401, "Требуется авторизация", False),
    "FORBIDDEN": (403, "Доступ запрещён", False),
    "NOT_FOUND": (404, "Объект не найден", False),
    "CAPACITY_EXCEEDED": (429, "Система занята. Повторите запрос позже", True),
    "DEPENDENCY_UNAVAILABLE": (503, "Сервис временно недоступен", True),
    "DEADLINE_EXCEEDED": (504, "Превышено время выполнения", True),
    "INTERNAL_ERROR": (500, "Не удалось выполнить запрос", False),
    "VERSION_CONFLICT": (409, "Текущая публикация изменилась. Обновите документ", False),
    "IDEMPOTENCY_CONFLICT": (409, "Этот ключ уже использован для другого запроса", False),
    "TERMINAL_CONFLICT": (409, "Операция недоступна в текущем состоянии", False),
    "SIZE_LIMIT_EXCEEDED": (413, "Превышен допустимый размер файла", False),
    "UNSUPPORTED_MEDIA_TYPE": (415, "Поддерживаются только PDF-файлы", False),
    "PDF_INVALID": (422, "Не удалось прочитать PDF-файл", False),
    "GENERATION_INVALID": (409, "Поколение документа ещё не готово к публикации", False),
    "SOURCE_REVOKED": (403, "Доступ к источнику отозван", False),
    "SOURCE_UNAVAILABLE": (503, "Источник временно недоступен", True),
    "EVENT_HISTORY_EXPIRED": (410, "История событий больше недоступна. Обновите состояние запроса", False),
    "EVENT_CURSOR_INVALID": (400, "Некорректный указатель события", False),
}


def error_response(code: str, request_id: UUID, *, status_code: int | None = None,
                   details: dict[str, Scalar] | None = None) -> JSONResponse:
    default_status, message, retryable = ERRORS[code]
    envelope = ErrorEnvelope(error=ErrorInfo(
        code=ErrorCode(code), message=message, retryable=retryable, request_id=request_id, details=details or {},
    ))
    headers = {"X-Request-ID": str(request_id)}
    if code == "CAPACITY_EXCEEDED":
        delay = (details or {}).get("retry_after_seconds", 2)
        headers["Retry-After"] = str(delay if type(delay) is int and delay > 0 else 2)
    return JSONResponse(
        status_code=status_code or default_status, content=envelope.model_dump(mode="json"),
        headers=headers,
    )


class HttpBoundary:
    def __init__(self, app, *, internal_token: bytes | None = None):
        self.app = app
        self.internal_token = internal_token

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        try:
            request_id = UUID(headers.get(b"x-request-id", b"").decode("ascii"))
        except (ValueError, UnicodeError):
            request_id = uuid4()
        scope.setdefault("state", {})["request_id"] = request_id
        token = request_id_context.set(str(request_id))
        started = False
        status = 500
        begin = time.monotonic()
        trace_fields = {}

        async def send_with_correlation(message):
            nonlocal started, status
            if message["type"] == "http.response.start":
                started = True
                status = message["status"]
                response_headers = [(key, value) for key, value in message.get("headers", []) if key.lower() != b"x-request-id"]
                response_headers.append((b"x-request-id", str(request_id).encode("ascii")))
                message = {**message, "headers": response_headers}
            await send(message)

        try:
            if self.internal_token is not None and scope.get("path", "").startswith("/v1/"):
                expected = b"Bearer " + self.internal_token
                if not hmac.compare_digest(headers.get(b"authorization", b""), expected):
                    await error_response("UNAUTHENTICATED", request_id)(scope, receive, send_with_correlation)
                    return
            # The internal parent is admitted only after the Bearer check above.
            # Public input cannot join or suppress a service trace via headers.
            trusted = self.internal_token is not None and scope.get("path", "").startswith("/v1/")
            parent = {}
            if trusted:
                value = headers.get(b"traceparent", b"")
                if len(value) == 55:
                    try:
                        parent["traceparent"] = value.decode("ascii")
                    except UnicodeError:
                        pass
            with internal_trace_context(parent), correlation_context(request_id=request_id):
                with safe_span("http.request") as span:
                    trace_fields = {"trace_id": span.trace_id, "span_id": span.span_id}
                    await self.app(scope, receive, send_with_correlation)
                    span.set_attributes(status_code=status)
                    if status >= 500:
                        span.fail()
        except Exception:
            logger.error("http.unhandled", extra={"safe_fields": {**trace_fields, "error_code": "INTERNAL_ERROR"}})
            if started:
                raise
            await error_response("INTERNAL_ERROR", request_id)(scope, receive, send_with_correlation)
        finally:
            logger.info("http.request", extra={"safe_fields": {
                **trace_fields, "status_code": status, "duration_ms": (time.monotonic() - begin) * 1000,
            }})
            request_id_context.reset(token)


def install_http_boundary(app: FastAPI, *, internal_token: bytes | None = None) -> None:
    app.add_middleware(HttpBoundary, internal_token=internal_token)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, _error: RequestValidationError):
        return error_response("VALIDATION_ERROR", request.state.request_id)

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException):
        code = {400: "INVALID_REQUEST", 401: "UNAUTHENTICATED", 403: "FORBIDDEN", 404: "NOT_FOUND", 409: "TERMINAL_CONFLICT", 413: "SIZE_LIMIT_EXCEEDED", 415: "UNSUPPORTED_MEDIA_TYPE", 422: "VALIDATION_ERROR", 429: "CAPACITY_EXCEEDED", 500: "INTERNAL_ERROR", 503: "DEPENDENCY_UNAVAILABLE", 504: "DEADLINE_EXCEEDED"}.get(error.status_code, "INVALID_REQUEST")
        return error_response(code, request.state.request_id, status_code=error.status_code)
