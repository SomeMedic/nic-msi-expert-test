"""Authenticated run commands, public projections and reconnectable SSE."""
from __future__ import annotations

import asyncio
import json
import re
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response
from pydantic import ValidationError

from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.run_events import BoundedEventResponse, RunEventService, parse_cursor
from expert_api.runs import RunService
from expert_api.uploads import idempotency_key
from expert_contracts.auth import ApplicationRole
from expert_contracts.errors import ErrorCode
from expert_contracts.runs import (
    CreateRunRequest, PublicRun, RunAccepted, RunCancelAccepted, RunDebug, RunList,
)
from expert_observability.tracing import safe_span

router = APIRouter(prefix="/api/v1", tags=["Runs"])
BODY_MAX_BYTES = 32768
BODY_TIMEOUT_SECONDS = 5.0


def _service(request: Request) -> RunService:
    service = getattr(request.app.state, "runs", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


def _events(request: Request) -> RunEventService:
    service = getattr(request.app.state, "run_events", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


async def _body(request: Request) -> CreateRunRequest:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Content-Type"})
    body = bytearray()
    try:
        async with asyncio.timeout(BODY_TIMEOUT_SECONDS):
            async for chunk in request.stream():
                if len(body) + len(chunk) > BODY_MAX_BYTES:
                    raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": BODY_MAX_BYTES})
                body.extend(chunk)
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if isinstance(payload, dict) and "debug_capture" in payload and type(payload["debug_capture"]) is not bool:
            raise ValueError()
        return CreateRunRequest.model_validate(payload)
    except TimeoutError:
        raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
    except (UnicodeError, ValueError, ValidationError, RecursionError):
        raise ApiError(ErrorCode.VALIDATION_ERROR) from None


@router.post("/runs", response_model=RunAccepted, status_code=202, operation_id="create_run",
             openapi_extra={"parameters": [{"name": "Idempotency-Key", "in": "header", "required": True,
                 "schema": {"type": "string", "minLength": 1, "maxLength": 128}}],
                 "requestBody": {"required": True, "content": {
                 "application/json": {"schema": CreateRunRequest.model_json_schema()}}}})
async def create_run(request: Request, response: Response,
                     principal: Annotated[Principal, Depends(current_principal)]) -> RunAccepted:
    principal.require(ApplicationRole.OPERATOR)
    values = request.headers.getlist("idempotency-key")
    key = idempotency_key(values[0] if len(values) == 1 else None)
    payload = await _body(request)
    with safe_span("run.create", request_id=request.state.request_id) as span:
        accepted = await _service(request).create(principal, key, payload, request.state.request_id)
        span.set_attributes(run_id=accepted.run_id)
    response.headers["Location"] = accepted.links.self
    response.headers["Cache-Control"] = "no-store"
    return accepted


@router.get("/runs", response_model=RunList, operation_id="list_runs")
async def list_runs(request: Request, response: Response,
                    principal: Annotated[Principal, Depends(current_principal)],
                    limit: Annotated[int, Query(ge=1, le=100)] = 50,
                    cursor: Annotated[str | None, Query(max_length=500)] = None) -> RunList:
    for key in request.query_params:
        if key not in {"limit", "cursor"} or len(request.query_params.getlist(key)) != 1:
            raise ApiError(ErrorCode.INVALID_REQUEST)
    if "limit" in request.query_params and re.fullmatch(r"[1-9][0-9]{0,2}", request.query_params["limit"]) is None:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "limit"})
    response.headers["Cache-Control"] = "no-store"
    return await _service(request).list(principal, limit=limit, cursor=cursor)


@router.get("/runs/{run_id}", response_model=PublicRun, operation_id="get_run")
async def get_run(run_id: UUID, request: Request, response: Response,
                  principal: Annotated[Principal, Depends(current_principal)]) -> PublicRun:
    response.headers["Cache-Control"] = "no-store"
    return await _service(request).get(run_id, principal)


@router.post("/runs/{run_id}/cancel", response_model=RunCancelAccepted | PublicRun,
             operation_id="cancel_run", responses={202: {"model": RunCancelAccepted}})
async def cancel_run(run_id: UUID, request: Request, response: Response,
                     principal: Annotated[Principal, Depends(current_principal)]) -> RunCancelAccepted | PublicRun:
    principal.require(ApplicationRole.OPERATOR)
    result = await _service(request).cancel(run_id, principal)
    response.status_code = 202 if isinstance(result, RunCancelAccepted) else 200
    response.headers["Cache-Control"] = "no-store"
    return result


@router.get("/runs/{run_id}/debug", response_model=RunDebug, operation_id="get_run_debug")
async def get_run_debug(run_id: UUID, request: Request, response: Response,
                        principal: Annotated[Principal, Depends(current_principal)]) -> RunDebug:
    principal.require(ApplicationRole.OPERATOR)
    response.headers["Cache-Control"] = "no-store"
    return await _service(request).debug(run_id, principal)


@router.get("/runs/{run_id}/events", response_class=BoundedEventResponse, operation_id="stream_run_events",
            responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}},
                       410: {"description": "Event history expired", "content": {"application/json": {
                           "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}}}},
            openapi_extra={"parameters": [{"name": "Last-Event-ID", "in": "header", "required": False,
                "schema": {"type": "string", "pattern": "^(0|[1-9][0-9]{0,18})$"}}]})
async def stream_run_events(run_id: UUID, request: Request,
                            principal: Annotated[Principal, Depends(current_principal)],
                            after: Annotated[str | None, Query()] = None) -> BoundedEventResponse:
    headers = request.headers.getlist("last-event-id")
    if len(headers) > 1 or len(request.query_params.getlist("after")) > 1:
        raise ApiError(ErrorCode.EVENT_CURSOR_INVALID)
    if any(key != "after" for key in request.query_params):
        raise ApiError(ErrorCode.INVALID_REQUEST)
    cursor = parse_cursor(headers[0] if headers else None, after)
    service = _events(request)
    prepared = await service.prepare(run_id, principal, cursor)

    async def authorize() -> Principal:
        return await current_principal(request)

    return BoundedEventResponse(
        service.iterate(prepared, is_disconnected=request.is_disconnected, authorize=authorize),
        media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
