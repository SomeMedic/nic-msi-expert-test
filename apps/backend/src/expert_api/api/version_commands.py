"""Authenticate before consuming bounded literal JSON command bodies."""
from __future__ import annotations

import asyncio
import json
import re
from typing import TypeVar
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from fastapi.routing import APIRoute
from pydantic import BaseModel, ValidationError

from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.version_commands import VersionCommandService
from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import (
    ArchiveDocumentRequest, DeactivateRequest, DocumentSummary, PublicationInfo, PublishRequest, VersionSummary,
)
from expert_contracts.errors import ErrorCode

BODY_MAX_BYTES = 8192
REQUEST_TIMEOUT_SECONDS = 10.0
M = TypeVar("M", bound=BaseModel)


class VersionCommandRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()
        async def bounded(request: Request):
            try:
                async with asyncio.timeout(REQUEST_TIMEOUT_SECONDS):
                    return await original(request)
            except TimeoutError:
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
        return bounded


router = APIRouter(prefix="/api/v1", tags=["Document Commands"], route_class=VersionCommandRoute)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError()
        result[key] = value
    return result


async def command_body(request: Request, model: type[M]) -> M:
    media_types = request.headers.getlist("content-type")
    if len(media_types) != 1 or media_types[0].split(";", 1)[0].strip().lower() != "application/json":
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Content-Type"})
    lengths = request.headers.getlist("content-length")
    declared = None
    if lengths:
        if len(lengths) != 1 or re.fullmatch(r"[0-9]{1,10}", lengths[0]) is None:
            raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Content-Length"})
        declared = int(lengths[0])
        if declared > BODY_MAX_BYTES:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": BODY_MAX_BYTES})
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > BODY_MAX_BYTES:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": BODY_MAX_BYTES})
        body.extend(chunk)
    if declared is not None and declared != len(body):
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Content-Length"})
    try:
        data = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        return model.model_validate(data)
    except (ValueError, UnicodeError, ValidationError, RecursionError):
        raise ApiError(ErrorCode.VALIDATION_ERROR) from None


def _service(request: Request) -> VersionCommandService:
    service: VersionCommandService | None = getattr(request.app.state, "version_commands", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


@router.post("/versions/{version_id}/publish", response_model=PublicationInfo, operation_id="publish_version",
             openapi_extra={"requestBody": {"required": True, "content": {
                 "application/json": {"schema": PublishRequest.model_json_schema()}}}})
async def publish_version(version_id: UUID, request: Request, response: Response,
                           principal: Principal = Depends(current_principal)) -> PublicationInfo:
    principal.require(ApplicationRole.OPERATOR)
    payload = await command_body(request, PublishRequest)
    value = await _service(request).publish(version_id, principal, payload)
    response.headers["Cache-Control"] = "no-store"
    return value


@router.post("/versions/{version_id}/deactivate", response_model=VersionSummary, operation_id="deactivate_version",
             openapi_extra={"requestBody": {"required": True, "content": {
                 "application/json": {"schema": DeactivateRequest.model_json_schema()}}}})
async def deactivate_version(version_id: UUID, request: Request, response: Response,
                              principal: Principal = Depends(current_principal)) -> VersionSummary:
    principal.require(ApplicationRole.OPERATOR)
    payload = await command_body(request, DeactivateRequest)
    value = await _service(request).deactivate(version_id, principal, payload)
    response.headers["Cache-Control"] = "no-store"
    return value


@router.post("/documents/{document_id}/archive", response_model=DocumentSummary, operation_id="archive_document",
             openapi_extra={"requestBody": {"required": True, "content": {
                 "application/json": {"schema": ArchiveDocumentRequest.model_json_schema()}}}})
async def archive_document(document_id: UUID, request: Request, response: Response,
                           principal: Principal = Depends(current_principal)) -> DocumentSummary:
    principal.require(ApplicationRole.OPERATOR)
    payload = await command_body(request, ArchiveDocumentRequest)
    value = await _service(request).archive(document_id, principal, payload)
    response.headers["Cache-Control"] = "no-store"
    return value
