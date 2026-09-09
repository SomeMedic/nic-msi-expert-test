"""Explicit generation pins for lazy canonical structure and extraction diagnostics."""
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response

from expert_api.auth import Principal, current_principal
from expert_api.document_tree import DocumentTreeService
from expert_api.errors import ApiError
from expert_contracts.errors import ErrorCode
from expert_contracts.structure import CanonicalTreePage, ParseQualityPage, StructuredTablePage

router = APIRouter(prefix="/api/v1", tags=["Document Structure"])


def _service(request: Request, response: Response) -> DocumentTreeService:
    response.headers["Cache-Control"] = "no-store"
    service: DocumentTreeService | None = getattr(request.app.state, "document_tree", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


@router.get("/versions/{version_id}/tree", response_model=CanonicalTreePage, operation_id="get_canonical_tree")
async def get_canonical_tree(version_id: UUID, parse_generation_id: UUID, request: Request, response: Response,
                             principal: Principal = Depends(current_principal), parent_id: UUID | None = None,
                             cursor: str | None = Query(default=None, max_length=500),
                             limit: int = Query(default=50, ge=1, le=100)) -> CanonicalTreePage:
    return await _service(request, response).tree(version_id, parse_generation_id, principal,
        parent_id=parent_id, cursor=cursor, limit=limit)


@router.get("/versions/{version_id}/quality", response_model=ParseQualityPage, operation_id="get_parse_quality")
async def get_parse_quality(version_id: UUID, parse_generation_id: UUID, request: Request, response: Response,
                            principal: Principal = Depends(current_principal),
                            cursor: str | None = Query(default=None, max_length=500),
                            limit: int = Query(default=50, ge=1, le=100)) -> ParseQualityPage:
    return await _service(request, response).quality(version_id, parse_generation_id, principal, cursor=cursor, limit=limit)


@router.get("/versions/{version_id}/tables/{node_id}", response_model=StructuredTablePage, operation_id="get_structured_table")
async def get_structured_table(version_id: UUID, node_id: UUID, parse_generation_id: UUID, request: Request, response: Response,
                               principal: Principal = Depends(current_principal),
                               cursor: str | None = Query(default=None, max_length=500),
                               limit: int = Query(default=25, ge=1, le=50)) -> StructuredTablePage:
    return await _service(request, response).table(version_id, parse_generation_id, node_id, principal, cursor=cursor, limit=limit)
