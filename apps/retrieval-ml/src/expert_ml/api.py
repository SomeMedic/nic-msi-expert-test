"""Internal inference HTTP routes; all result IDs and numerical outputs use shared DTOs."""
from __future__ import annotations

import asyncio
from contextlib import suppress

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse

from expert_contracts.errors import ErrorCode, ErrorEnvelope, ErrorInfo
from expert_contracts.inference import (
    CapabilitiesResponse, DocumentEmbeddingRequest, DocumentEmbeddingResponse,
    QueryEmbeddingRequest, QueryEmbeddingResponse, RerankRequest, RerankResponse,
)
from expert_observability.web import error_response

from .errors import ModelError
from .profile import RuntimeProfile
from .runtime import ModelRuntime

router = APIRouter(prefix="/v1")
MAX_BODY_BYTES = 2 * 1024 * 1024
ML_ERRORS = {
    "MODEL_UNAVAILABLE": (503, "Локальные модели временно недоступны", True),
    "MODEL_TIMEOUT": (504, "Превышено время обработки моделью", True),
    "OUTPUT_SCHEMA_INVALID": (502, "Модель вернула некорректный результат", False),
    "TOKEN_LIMIT_EXCEEDED": (422, "Превышен допустимый лимит токенов", False),
}


class BodyLimit:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] != "POST":
            await self.app(scope, receive, send)
            return
        body = bytearray()
        try:
            async with asyncio.timeout(10):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > MAX_BODY_BYTES:
                        await error_response("SIZE_LIMIT_EXCEEDED", scope["state"]["request_id"],
                                             details={"max_bytes": MAX_BODY_BYTES})(scope, receive, send)
                        return
                    body.extend(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            await error_response("DEADLINE_EXCEEDED", scope["state"]["request_id"])(scope, receive, send)
            return
        delivered = False

        async def replay():
            nonlocal delivered
            if delivered:
                return await receive()
            delivered = True
            return {"type": "http.request", "body": bytes(body), "more_body": False}

        await self.app(scope, replay, send)


def runtime(request: Request) -> ModelRuntime:
    return request.app.state.model_runtime


async def infer(operation: str, payload, request: Request, response: Response):
    owner = runtime(request)
    async def disconnected():
        while (await request.receive())["type"] != "http.disconnect":
            pass

    pending = asyncio.create_task(owner.invoke(operation, payload))
    disconnect = asyncio.create_task(disconnected())
    try:
        done, _ = await asyncio.wait({pending, disconnect}, return_when=asyncio.FIRST_COMPLETED)
        if pending not in done:
            pending.cancel()
            with suppress(asyncio.CancelledError):
                await pending
            raise ModelError("MODEL_TIMEOUT")
        result, metrics = await pending
    finally:
        disconnect.cancel()
        if not pending.done():
            pending.cancel()
        with suppress(asyncio.CancelledError):
            await disconnect
        with suppress(asyncio.CancelledError, ModelError):
            await pending
    profile = owner.profile
    recipe = profile.reranker_recipe if operation == "rerank" else profile.embedding_recipe
    response.headers["X-Model-Fingerprint"] = recipe.model_fingerprint
    response.headers["X-Runtime-Fingerprint"] = recipe.runtime_fingerprint
    response.headers["X-Input-Tokens"] = str(metrics["input_tokens"])
    response.headers["Server-Timing"] = f'queue;dur={metrics["queue_ms"]:.3f}, inference;dur={metrics["inference_ms"]:.3f}'
    return result


@router.post("/embeddings/query", response_model=QueryEmbeddingResponse)
async def query(payload: QueryEmbeddingRequest, request: Request, response: Response):
    return await infer("query", payload, request, response)


@router.post("/embeddings/documents", response_model=DocumentEmbeddingResponse)
async def documents(payload: DocumentEmbeddingRequest, request: Request, response: Response):
    return await infer("documents", payload, request, response)


@router.post("/rerank", response_model=RerankResponse)
async def rerank(payload: RerankRequest, request: Request, response: Response):
    return await infer("rerank", payload, request, response)


@router.get("/capabilities", response_model=CapabilitiesResponse)
async def capabilities(request: Request):
    return runtime(request).profile.capabilities


@router.get("/profile", response_model=RuntimeProfile)
async def profile(request: Request):
    return runtime(request).profile


def install_model_errors(app: FastAPI) -> None:
    @app.exception_handler(ModelError)
    async def model_error(request: Request, error: ModelError):
        if error.code not in ML_ERRORS:
            return error_response(error.code, request.state.request_id, details=error.details)
        status, message, retryable = ML_ERRORS[error.code]
        envelope = ErrorEnvelope(error=ErrorInfo(code=ErrorCode(error.code), message=message,
            retryable=retryable, request_id=request.state.request_id, details=error.details))
        return JSONResponse(status_code=status, content=envelope.model_dump(mode="json"))
