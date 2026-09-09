from typing import Annotated

from fastapi import APIRouter, Depends, Request, Response

from expert_contracts.auth import SessionInfo, SessionRequest
from expert_api.auth import COOKIE_NAME, Principal, auth_service, current_principal

router = APIRouter(prefix="/api/v1/auth", tags=["Authentication"])


@router.post("/session", response_model=SessionInfo, operation_id="create_auth_session")
async def login(request: Request, body: SessionRequest, response: Response) -> SessionInfo:
    service = auth_service(request)
    service.validate_origin(request, required=False)
    token, principal = await service.create_session(body.access_key.get_secret_value())
    response.set_cookie(COOKIE_NAME, token, max_age=service.settings.auth_session_seconds,
                        httponly=True, secure=service.settings.public_base_url.scheme == "https",
                        samesite="strict", path="/api/v1")
    response.headers["Cache-Control"] = "no-store"
    return principal.public()


@router.get("/session", response_model=SessionInfo, operation_id="get_auth_session")
async def whoami(response: Response, principal: Annotated[Principal, Depends(current_principal)]) -> SessionInfo:
    response.headers["Cache-Control"] = "no-store"
    return principal.public()


@router.delete("/session", status_code=204, operation_id="delete_auth_session")
async def logout(request: Request) -> Response:
    await auth_service(request).revoke_session(request)
    response = Response(status_code=204, headers={"Cache-Control": "no-store"})
    response.delete_cookie(COOKIE_NAME, path="/api/v1", httponly=True, samesite="strict")
    return response
