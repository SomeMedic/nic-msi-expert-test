from typing import Literal

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from expert_contracts.system import HealthStatus


def install_health(app: FastAPI, service_name: str, check_dependencies) -> None:
    @app.get("/health/live", response_model=HealthStatus, operation_id="get_liveness")
    async def live():
        return HealthStatus(status="ok", service=service_name)

    @app.get("/health/ready", response_model=HealthStatus, responses={503: {"model": HealthStatus}}, operation_id="get_readiness")
    async def ready():
        checks = await check_dependencies()
        status: Literal["ok", "not_ready"] = "ok" if all(item.ready for item in checks) else "not_ready"
        return JSONResponse(
            content=HealthStatus(status=status, service=service_name).model_dump(mode="json"),
            status_code=200 if status == "ok" else 503,
        )
