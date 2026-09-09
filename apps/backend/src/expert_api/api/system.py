from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request, Response

from expert_api.auth import Principal, current_principal
from expert_contracts.auth import ApplicationRole
from expert_contracts.debug import DebugCapturePolicy
from expert_contracts.errors import ErrorCode
from expert_contracts.system import ComponentStatus, IngestionCapabilities, SystemStatus

router = APIRouter(prefix="/api/v1", tags=["system"])


@router.get("/system/debug-capture-policy", response_model=DebugCapturePolicy,
            operation_id="get_debug_capture_policy")
async def debug_capture_policy(request: Request, response: Response,
                               principal: Principal = Depends(current_principal)) -> DebugCapturePolicy:
    settings = request.app.state.settings
    response.headers["Cache-Control"] = "no-store"
    return DebugCapturePolicy(
        debug_capture_allowed=(settings.debug_capture_allowed
                               and principal.role in {ApplicationRole.OPERATOR, ApplicationRole.ADMIN}),
        debug_capture_ttl_hours=settings.debug_capture_ttl_hours,
    )


@router.get("/system/ingestion-capabilities", response_model=IngestionCapabilities,
            operation_id="get_ingestion_capabilities")
async def ingestion_capabilities(request: Request, response: Response,
                                 principal: Principal = Depends(current_principal)) -> IngestionCapabilities:
    principal.require(ApplicationRole.OPERATOR)
    response.headers["Cache-Control"] = "no-store"
    return IngestionCapabilities(pipeline_config_alias=request.app.state.settings.ingestion_pipeline_fingerprint)


@router.get("/system/status", response_model=SystemStatus, operation_id="get_system_status")
async def system_status(request: Request) -> SystemStatus:
    """Availability of backend dependencies, without endpoints or credentials."""
    probe = getattr(request.app.state, "system_checks", request.app.state.dependencies.check)
    checks = await probe()
    components = [ComponentStatus(name="api", status="ready")]
    components.extend(ComponentStatus(
        name=check.name,
        status="ready" if check.ready else "unavailable",
        error_code=None if check.ready else ErrorCode.DEPENDENCY_UNAVAILABLE,
    ) for check in checks)
    return SystemStatus(
        status="ready" if all(check.ready for check in checks) else "degraded",
        checked_at=datetime.now(timezone.utc), components=components,
    )
