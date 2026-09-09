"""Authenticated ingestion progress broadcast; disconnect is not cancellation."""
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request

from expert_api.auth import Principal, current_principal
from expert_api.errors import ApiError
from expert_api.job_events import JobEventService
from expert_api.run_events import BoundedEventResponse, parse_cursor
from expert_contracts.errors import ErrorCode

router = APIRouter(prefix="/api/v1", tags=["Ingestion Jobs"])


@router.get("/ingestion-jobs/{job_id}/events", response_class=BoundedEventResponse,
            operation_id="stream_ingestion_events",
            responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}},
                       410: {"description": "Event history expired", "content": {"application/json": {
                           "schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}}}},
            openapi_extra={"parameters": [{"name": "Last-Event-ID", "in": "header", "required": False,
                "schema": {"type": "string", "pattern": "^(0|[1-9][0-9]{0,18})$"}}]})
async def stream_ingestion_events(job_id: UUID, request: Request,
                                 principal: Annotated[Principal, Depends(current_principal)],
                                 after: Annotated[str | None, Query()] = None) -> BoundedEventResponse:
    headers = request.headers.getlist("last-event-id")
    if len(headers) > 1 or len(request.query_params.getlist("after")) > 1:
        raise ApiError(ErrorCode.EVENT_CURSOR_INVALID)
    if any(key != "after" for key in request.query_params):
        raise ApiError(ErrorCode.INVALID_REQUEST)
    cursor = parse_cursor(headers[0] if headers else None, after)
    service: JobEventService | None = getattr(request.app.state, "job_events", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    prepared = await service.prepare(job_id, principal, cursor)

    async def authorize() -> Principal:
        return await current_principal(request)

    return BoundedEventResponse(
        service.iterate(prepared, is_disconnected=request.is_disconnected, authorize=authorize),
        media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
