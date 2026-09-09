"""Small authenticated commands; PostgreSQL is the execution authority."""
from __future__ import annotations

import asyncio
import json
from uuid import UUID

from fastapi import APIRouter, Request, Response
from pydantic import ValidationError

from expert_contracts.internal import ExecutionStatus, StartRunRequest, StartRunResponse
from expert_observability.web import error_response

from ..checkpoint import ExecutionError
from ..run_manager import RunManager

router = APIRouter(prefix="/v1/runs", tags=["Internal execution"])
BODY_MAX_BYTES = 4096


def _manager(request: Request) -> RunManager:
    manager = getattr(request.app.state, "run_manager", None)
    if manager is None:
        raise ExecutionError("DEPENDENCY_UNAVAILABLE")
    return manager


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


async def _body(request: Request) -> StartRunRequest:
    if request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
        raise ExecutionError("INVALID_REQUEST")
    body = bytearray()
    try:
        async with asyncio.timeout(5):
            async for chunk in request.stream():
                if len(body) + len(chunk) > BODY_MAX_BYTES:
                    raise ExecutionError("SIZE_LIMIT_EXCEEDED")
                body.extend(chunk)
        # Reject duplicate keys/NaN before Pydantic's closed command validation.
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        if isinstance(payload, dict) and "schema_version" in payload and type(payload["schema_version"]) is not int:
            raise ValueError()
        return StartRunRequest.model_validate_json(json.dumps(payload), strict=True)
    except TimeoutError:
        raise ExecutionError("DEADLINE_EXCEEDED") from None
    except (UnicodeError, ValueError, ValidationError, RecursionError):
        raise ExecutionError("VALIDATION_ERROR") from None


def _error(request: Request, error: ExecutionError) -> Response:
    code = "NOT_FOUND" if error.code == "RUN_NOT_FOUND" else error.code
    if code not in {"NOT_FOUND", "CAPACITY_EXCEEDED", "DEPENDENCY_UNAVAILABLE", "INTERNAL_ERROR",
                    "INVALID_REQUEST", "VALIDATION_ERROR", "DEADLINE_EXCEEDED", "SIZE_LIMIT_EXCEEDED"}:
        code = "INTERNAL_ERROR"
    return error_response(code, request.state.request_id)


@router.post("/{run_id}/start", response_model=StartRunResponse, status_code=202,
             openapi_extra={"requestBody": {"required": True, "content": {
                 "application/json": {"schema": StartRunRequest.model_json_schema()}}}})
async def start_run(run_id: UUID, request: Request, response: Response):
    try:
        if request.query_params:
            raise ExecutionError("INVALID_REQUEST")
        command = await _body(request)
        result = await _manager(request).start(run_id, command)
        response.status_code = 202 if result.status == "accepted" else 200
        response.headers["Cache-Control"] = "no-store"
        return result
    except ExecutionError as error:
        return _error(request, error)


@router.get("/{run_id}/execution", response_model=ExecutionStatus)
async def execution_status(run_id: UUID, request: Request, response: Response):
    try:
        if request.query_params:
            raise ExecutionError("INVALID_REQUEST")
        result = await _manager(request).execution(run_id)
        response.headers["Cache-Control"] = "no-store"
        return result
    except ExecutionError as error:
        return _error(request, error)
