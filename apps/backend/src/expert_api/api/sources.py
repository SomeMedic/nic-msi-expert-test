"""Public source access never accepts an object address or a private artifact ID."""
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.source_viewer import SourceViewerService, UnsatisfiableRange
from expert_contracts.errors import ErrorCode
from expert_contracts.sources import PublicEvidence
from expert_observability.web import error_response

router = APIRouter(prefix="/api/v1", tags=["Sources"])


def _service(request: Request) -> SourceViewerService:
    service: SourceViewerService | None = getattr(request.app.state, "source_viewer", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


@router.get("/versions/{version_id}/source", response_class=Response, operation_id="get_version_source",
    responses={200: {"content": {"application/pdf": {"schema": {"type": "string", "format": "binary"}}}},
               206: {"description": "Verified PDF byte range", "content": {"application/pdf": {}}},
               416: {"description": "Unsatisfiable or unsupported byte range"}})
async def get_version_source(version_id: UUID, request: Request,
                             principal: Principal = Depends(current_principal)) -> Response:
    ranges = request.headers.getlist("range")
    try:
        return await _service(request).original(version_id, principal,
            range_header=",".join(ranges) if ranges else None, if_range=request.headers.get("if-range"))
    except UnsatisfiableRange as error:
        response = error_response(ErrorCode.INVALID_REQUEST.value, request.state.request_id, details={"field": "Range"})
        response.status_code = 416
        response.headers.update({"Content-Range": f"bytes */{error.size}", "Accept-Ranges": "bytes",
                                 "Cache-Control": "no-store, private"})
        return response


@router.get("/runs/{run_id}/sources/{evidence_id}", response_model=PublicEvidence, operation_id="get_run_source")
async def get_run_source(run_id: UUID, evidence_id: str, request: Request, response: Response,
                         principal: Principal = Depends(current_principal)) -> PublicEvidence:
    response.headers["Cache-Control"] = "no-store, private"
    return await _service(request).evidence(run_id, evidence_id, principal)
