"""HTTP job commands; SSE is implemented in its own P09 surface."""
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response

from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import IngestionJob, JobCommandAccepted, ReindexRequest
from expert_contracts.errors import ErrorCode
from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.jobs import JobService
from expert_api.uploads import idempotency_key

router = APIRouter(prefix="/api/v1", tags=["Ingestion Jobs"])


def _service(request: Request) -> JobService:
    service: JobService | None = getattr(request.app.state, "jobs", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


def _single_idempotency_key(request: Request) -> str:
    if len(request.headers.getlist("idempotency-key")) != 1:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Idempotency-Key"})
    return idempotency_key(request.headers.get("idempotency-key"))


def _accepted(response: Response, job: JobCommandAccepted) -> JobCommandAccepted:
    response.headers["Location"] = f"/api/v1/ingestion-jobs/{job.job_id}"
    response.headers["Cache-Control"] = "no-store"
    return job


@router.get("/ingestion-jobs/{job_id}", response_model=IngestionJob, operation_id="get_ingestion_job")
async def get_ingestion_job(
    job_id: UUID,
    request: Request,
    principal: Annotated[Principal, Depends(current_principal)],
) -> IngestionJob:
    return await _service(request).get(job_id, principal)


@router.post(
    "/ingestion-jobs/{job_id}/cancel",
    response_model=JobCommandAccepted,
    status_code=202,
    operation_id="cancel_ingestion_job",
)
async def cancel_ingestion_job(
    job_id: UUID,
    request: Request,
    response: Response,
    principal: Annotated[Principal, Depends(current_principal)],
) -> JobCommandAccepted:
    principal.require(ApplicationRole.OPERATOR)
    return _accepted(response, await _service(request).cancel(job_id, principal))


@router.post(
    "/ingestion-jobs/{job_id}/retry",
    response_model=JobCommandAccepted,
    status_code=202,
    operation_id="retry_ingestion_job",
)
async def retry_ingestion_job(
    job_id: UUID,
    request: Request,
    response: Response,
    principal: Annotated[Principal, Depends(current_principal)],
) -> JobCommandAccepted:
    principal.require(ApplicationRole.OPERATOR)
    return _accepted(response, await _service(request).retry(job_id, principal))


@router.post(
    "/versions/{version_id}/reindex",
    response_model=JobCommandAccepted,
    status_code=202,
    operation_id="reindex_version",
)
async def reindex_version(
    version_id: UUID,
    payload: ReindexRequest,
    request: Request,
    response: Response,
    principal: Annotated[Principal, Depends(current_principal)],
) -> JobCommandAccepted:
    principal.require(ApplicationRole.OPERATOR)
    key = _single_idempotency_key(request)
    return _accepted(response, await _service(request).reindex(version_id, principal, key, payload))
