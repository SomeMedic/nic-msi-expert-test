"""Separate safe public metadata/download from bounded private model bytes."""
from __future__ import annotations

import asyncio
import hmac
import json
import re
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from fastapi.routing import APIRoute
from pydantic import ValidationError

from expert_api.auth import Principal, current_principal
from expert_api.debug_capture import CAPTURE_TIMEOUT_SECONDS, DebugCaptureService
from expert_api.errors import ApiError
from expert_contracts.debug import DebugCaptureList
from expert_contracts.debug_capture import (
    CAPTURE_ENVELOPE_MAX_BYTES, DebugCaptureReceipt, DebugCaptureSubmission,
)
from expert_contracts.errors import ErrorCode


def _service(request: Request) -> DebugCaptureService:
    service: DebugCaptureService | None = getattr(request.app.state, "debug_capture", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


def _internal_auth(request: Request) -> None:
    settings = _service(request).settings
    expected = b"Bearer " + settings.require_secret("agent_runtime_token").get_secret_value().encode("utf-8")
    values = request.headers.getlist("authorization")
    if len(values) != 1 or not hmac.compare_digest(values[0].encode("utf-8", "surrogatepass"), expected):
        raise ApiError(ErrorCode.UNAUTHENTICATED)


class CaptureRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def bounded(request: Request):
            _internal_auth(request)
            try:
                async with asyncio.timeout(CAPTURE_TIMEOUT_SECONDS):
                    return await original(request)
            except TimeoutError:
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
        return bounded


router = APIRouter(prefix="/api/v1", tags=["Debug Capture"])
internal_router = APIRouter(prefix="/internal/v1", route_class=CaptureRoute, include_in_schema=False)


def _pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError()
        value[key] = item
    return value


async def _submission(request: Request) -> DebugCaptureSubmission:
    types = request.headers.getlist("content-type")
    if len(types) != 1 or types[0].split(";", 1)[0].strip().lower() != "application/json":
        raise ApiError(ErrorCode.INVALID_REQUEST)
    lengths = request.headers.getlist("content-length")
    declared = None
    if lengths:
        if len(lengths) != 1 or not re.fullmatch(r"[0-9]{1,10}", lengths[0]):
            raise ApiError(ErrorCode.INVALID_REQUEST)
        declared = int(lengths[0])
        if declared > CAPTURE_ENVELOPE_MAX_BYTES:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": CAPTURE_ENVELOPE_MAX_BYTES})
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > CAPTURE_ENVELOPE_MAX_BYTES:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": CAPTURE_ENVELOPE_MAX_BYTES})
        data.extend(chunk)
    if declared is not None and len(data) != declared:
        raise ApiError(ErrorCode.INVALID_REQUEST)
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        return DebugCaptureSubmission.model_validate(value)
    except (ValidationError, ValueError, UnicodeError, TypeError, RecursionError):
        # Never delegate raw model bytes to FastAPI's validation-error renderer.
        raise ApiError(ErrorCode.VALIDATION_ERROR) from None


@internal_router.post("/runs/{run_id}/debug-parts", response_model=DebugCaptureReceipt)
async def receive_debug_part(run_id: UUID, request: Request, response: Response) -> DebugCaptureReceipt:
    submission = await _submission(request)
    response.headers["Cache-Control"] = "no-store"
    return await _service(request).submit(run_id, submission)


@router.get("/runs/{run_id}/debug/captures", response_model=DebugCaptureList, operation_id="list_debug_captures")
async def list_debug_captures(run_id: UUID, request: Request, response: Response,
                              principal: Principal = Depends(current_principal)) -> DebugCaptureList:
    response.headers["Cache-Control"] = "no-store, private"
    return await _service(request).listing(run_id, principal)


@router.get("/runs/{run_id}/debug/captures/{part_id}", response_class=Response,
             operation_id="download_debug_capture", responses={200: {"content": {
                 "application/json": {"schema": {"type": "string", "format": "binary"}}}}})
async def download_debug_capture(run_id: UUID, part_id: UUID, request: Request,
                                  principal: Principal = Depends(current_principal)) -> Response:
    return await _service(request).download(run_id, part_id, principal)
