"""Authenticated public library reads; lifecycle writes use separate controlled commands."""
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request, Response

from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.library import LibraryService, LibraryStatus
from expert_contracts.documents import DocumentDetail, DocumentList, LibraryProfile, VersionDetail
from expert_contracts.errors import ErrorCode

router = APIRouter(prefix="/api/v1", tags=["Documents"])


def _service(request: Request, response: Response) -> LibraryService:
    response.headers["Cache-Control"] = "no-store"
    service: LibraryService | None = getattr(request.app.state, "library", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


@router.get("/library/profile", response_model=LibraryProfile, operation_id="get_library_profile")
async def library_profile(request: Request, response: Response,
                          principal: Principal = Depends(current_principal)) -> LibraryProfile:
    return await _service(request, response).profile(principal)


@router.get("/documents", response_model=DocumentList, operation_id="list_documents")
async def list_documents(request: Request, response: Response, principal: Principal = Depends(current_principal),
                         cursor: str | None = Query(default=None, max_length=500), limit: int = Query(default=50, ge=1, le=100),
                         q: str = Query(default="", max_length=500), status: LibraryStatus = "all") -> DocumentList:
    return await _service(request, response).list(principal, cursor=cursor, limit=limit, q=q, status=status)


@router.get("/documents/{document_id}", response_model=DocumentDetail, operation_id="get_document")
async def get_document(document_id: UUID, request: Request, response: Response,
                       principal: Principal = Depends(current_principal),
                       versions_cursor: str | None = Query(default=None, max_length=500),
                       versions_limit: int = Query(default=50, ge=1, le=100)) -> DocumentDetail:
    return await _service(request, response).document(document_id, principal,
        versions_cursor=versions_cursor, versions_limit=versions_limit)


@router.get("/documents/{document_id}/versions/{version_id}", response_model=VersionDetail, operation_id="get_document_version")
async def get_document_version(document_id: UUID, version_id: UUID, request: Request, response: Response,
                               principal: Principal = Depends(current_principal)) -> VersionDetail:
    return await _service(request, response).version(version_id, principal, document_id=document_id)


@router.get("/versions/{version_id}", response_model=VersionDetail, operation_id="get_version")
async def get_version(version_id: UUID, request: Request, response: Response,
                      principal: Principal = Depends(current_principal)) -> VersionDetail:
    # Existing UploadAccepted.links.version points to this canonical alias.
    return await _service(request, response).version(version_id, principal)
