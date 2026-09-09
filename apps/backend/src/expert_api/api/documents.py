"""Upload admission authenticates before reading or spooling any PDF bytes."""
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import UploadAccepted
from expert_contracts.errors import ErrorCode
from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.upload_parser import parse_upload
from expert_api.uploads import UploadService, idempotency_key

router = APIRouter(prefix="/api/v1", tags=["Documents"])
UPLOAD_BODY = {"requestBody": {"required": True, "content": {"multipart/form-data": {"schema": {
    "type": "object", "required": ["file", "options"], "additionalProperties": False,
    "properties": {
        "file": {"type": "string", "format": "binary", "description": "One PDF, maximum 50 MiB"},
        "options": {"type": "string", "description": "JSON-encoded VersionUploadOptions"},
    },
}, "encoding": {"file": {"contentType": "application/pdf"}, "options": {"contentType": "application/json"}}}}}}


async def _upload(request: Request, response: Response, principal: Principal, document_id: UUID | None) -> UploadAccepted:
    principal.require(ApplicationRole.OPERATOR)
    if len(request.headers.getlist("idempotency-key")) != 1:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Idempotency-Key"})
    key = idempotency_key(request.headers.get("idempotency-key"))
    service: UploadService | None = getattr(request.app.state, "uploads", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    async with service.admission():
        async with parse_upload(request, service.settings) as upload:
            accepted = await service.accept(principal, key, document_id, upload)
    response.headers["Location"] = accepted.links.job
    response.headers["Cache-Control"] = "no-store"
    return accepted


@router.post("/documents", response_model=UploadAccepted, status_code=202,
             operation_id="upload_document", openapi_extra=UPLOAD_BODY)
async def upload_document(request: Request, response: Response,
                          principal: Annotated[Principal, Depends(current_principal)]) -> UploadAccepted:
    return await _upload(request, response, principal, None)


@router.post("/documents/{document_id}/versions", response_model=UploadAccepted, status_code=202,
             operation_id="upload_document_version", openapi_extra=UPLOAD_BODY)
async def upload_version(document_id: UUID, request: Request, response: Response,
                         principal: Annotated[Principal, Depends(current_principal)]) -> UploadAccepted:
    return await _upload(request, response, principal, document_id)
