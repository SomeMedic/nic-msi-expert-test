"""Admin dry-run, explicit plan confirmation and durable purge progress."""
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from expert_api.api.version_commands import VersionCommandRoute, command_body
from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.purge import PurgeService
from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import PurgeAccepted, PurgePlan, PurgeRequest
from expert_contracts.errors import ErrorCode
from expert_contracts.purge import PurgeStatus

router = APIRouter(prefix="/api/v1", tags=["Document Purge"], route_class=VersionCommandRoute)


def _service(request: Request) -> PurgeService:
    service: PurgeService | None = getattr(request.app.state, "purge", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    if request.query_params:
        raise ApiError(ErrorCode.INVALID_REQUEST)
    return service


@router.post("/documents/{document_id}/purge-plan", response_model=PurgePlan, operation_id="plan_document_purge")
async def plan_document_purge(document_id: UUID, request: Request, response: Response,
                              principal: Principal = Depends(current_principal)) -> PurgePlan:
    principal.require(ApplicationRole.ADMIN)
    # There is no client-controlled retention override or plan input.
    async for chunk in request.stream():
        if chunk:
            raise ApiError(ErrorCode.INVALID_REQUEST)
    value = await _service(request).plan(document_id, principal)
    response.headers["Cache-Control"] = "no-store"
    return value


@router.post("/documents/{document_id}/purge", response_model=PurgeAccepted, status_code=202,
             operation_id="accept_document_purge", openapi_extra={"requestBody": {"required": True, "content": {
                 "application/json": {"schema": PurgeRequest.model_json_schema()}}}})
async def accept_document_purge(document_id: UUID, request: Request, response: Response,
                                principal: Principal = Depends(current_principal)) -> PurgeAccepted:
    principal.require(ApplicationRole.ADMIN)
    payload = await command_body(request, PurgeRequest)
    value = await _service(request).accept(document_id, principal, payload)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Location"] = f"/api/v1/documents/{document_id}/purge-plans/{value.plan_id}"
    return value


@router.get("/documents/{document_id}/purge-plans/{plan_id}", response_model=PurgeStatus,
            operation_id="get_document_purge")
async def get_document_purge(document_id: UUID, plan_id: UUID, request: Request, response: Response,
                             principal: Principal = Depends(current_principal)) -> PurgeStatus:
    principal.require(ApplicationRole.ADMIN)
    value = await _service(request).status(document_id, plan_id, principal)
    response.headers["Cache-Control"] = "no-store"
    return value
