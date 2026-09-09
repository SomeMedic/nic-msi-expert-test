"""Admin transport controls; source deletion and SQL fencing use separate fixtures."""
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
import pytest

from expert_api.api.purge import router
from expert_api.auth import AuthService
from expert_api.errors import ApiError, install_api_errors
from expert_clients.settings import Settings
from expert_contracts.documents import PurgeAccepted, PurgePlan, PurgeReferenceCounts
from expert_contracts.errors import ErrorCode
from expert_contracts.purge import PurgeStatus
from expert_observability.web import install_http_boundary

ADMIN = "purge-admin-" + "a" * 40
OPERATOR = "purge-operator-" + "o" * 40
VIEWER = "purge-viewer-" + "v" * 40
HEADERS = {"Authorization": "Bearer " + ADMIN}
NOW = datetime.now(timezone.utc)


class PurgeStub:
    def __init__(self):
        self.calls = []
        self.error = None
        self.plan_id = uuid4()

    async def plan(self, document_id, principal):
        self.calls.append(("plan", document_id, principal.role))
        if self.error:
            raise ApiError(self.error)
        return PurgePlan(plan_id=self.plan_id, document_id=document_id, plan_version=2,
            created_at=NOW, expires_at=NOW + timedelta(minutes=15), eligible_after=NOW,
            allowed=False, blockers=["PROTECTED_REFERENCES"], references=PurgeReferenceCounts(
                active_runs=0, snapshots=1, results=1, checkpoints=0, objects=2))

    async def accept(self, document_id, principal, payload):
        self.calls.append(("accept", document_id, principal.role, payload))
        if self.error:
            raise ApiError(self.error)
        return PurgeAccepted(plan_id=payload.plan_id, document_id=document_id)

    async def status(self, document_id, plan_id, principal):
        self.calls.append(("status", document_id, principal.role, plan_id))
        return PurgeStatus(plan_id=plan_id, document_id=document_id, plan_version=2,
            status="purge_pending", created_at=NOW, expires_at=NOW + timedelta(minutes=15),
            accepted_at=NOW, completed_at=None, total_object_count=2, deleted_object_count=1, error_code=None)


@pytest.fixture
def app():
    result = FastAPI()
    result.state.purge = PurgeStub()
    result.state.auth = AuthService(Settings(service_name="backend", admin_access_key=ADMIN,
        operator_access_key=OPERATOR, viewer_access_key=VIEWER), None)
    install_http_boundary(result, internal_token=None)
    install_api_errors(result)
    result.include_router(router)
    return result


@pytest.mark.parametrize("suffix", ["purge-plan", "purge"])
async def test_only_admin_can_consume_command_body(app, suffix):
    async def body():
        pytest.fail("unauthorized body was consumed")
        yield b"private"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://testserver") as client:
        for key, expected in ((None, 401), (VIEWER, 403), (OPERATOR, 403)):
            headers = {"Content-Type": "application/json"}
            if key:
                headers["Authorization"] = "Bearer " + key
            response = await client.post(f"/api/v1/documents/{uuid4()}/{suffix}", headers=headers, content=body())
            assert response.status_code == expected
    assert app.state.purge.calls == []


def test_plan_reports_blockers_and_confirmed_plan_has_durable_location(app):
    document = uuid4()
    with TestClient(app) as client:
        result = client.post(f"/api/v1/documents/{document}/purge-plan", headers=HEADERS)
        assert result.status_code == 200
        assert result.headers["cache-control"] == "no-store"
        plan = result.json()
        assert plan["allowed"] is False and plan["references"]["results"] == 1
        result = client.post(f"/api/v1/documents/{document}/purge", headers=HEADERS,
                             json={"plan_id": plan["plan_id"], "plan_version": 2})
        assert result.status_code == 202
        assert result.headers["cache-control"] == "no-store"
        expected = f"/api/v1/documents/{document}/purge-plans/{plan['plan_id']}"
        assert result.headers["location"] == expected
        state = client.get(expected, headers=HEADERS)
        assert state.status_code == 200 and state.json()["deleted_object_count"] == 1
        assert state.headers["cache-control"] == "no-store"
        for key in (VIEWER, OPERATOR):
            assert client.get(expected, headers={"Authorization": "Bearer " + key}).status_code == 403
    # The stub only verifies wire binding; SQL must reject the blocked plan above.
    assert app.state.purge.calls[1][3].plan_id == app.state.purge.plan_id
    assert app.state.purge.calls[1][3].plan_version == 2


@pytest.mark.parametrize("body", [b"{}", b'{"retention_seconds":0}', b" " * 9000])
def test_plan_accepts_no_client_input(app, body):
    with TestClient(app) as client:
        response = client.post(f"/api/v1/documents/{uuid4()}/purge-plan", headers=HEADERS, content=body)
    assert response.status_code == 400
    assert not app.state.purge.calls


@pytest.mark.parametrize("body", [
    '{"plan_id":"%s","plan_version":1,"plan_version":2}',
    '{"plan_id":"%s","plan_version":1,"force":true}',
    '{"plan_id":"%s","plan_version":0}',
])
def test_confirmation_rejects_ambiguous_or_unrecognized_body(app, body):
    with TestClient(app) as client:
        result = client.post(f"/api/v1/documents/{uuid4()}/purge", content=body % uuid4(),
            headers={**HEADERS, "Content-Type": "application/json"})
    assert result.status_code == 422
    assert not app.state.purge.calls


@pytest.mark.parametrize("code,status", [(ErrorCode.VERSION_CONFLICT, 409), (ErrorCode.NOT_FOUND, 404),
                                        (ErrorCode.DEPENDENCY_UNAVAILABLE, 503)])
def test_errors_are_closed_public_envelopes(app, code, status):
    app.state.purge.error = code
    with TestClient(app) as client:
        result = client.post(f"/api/v1/documents/{uuid4()}/purge-plan", headers=HEADERS)
    assert result.status_code == status and result.json()["error"]["code"] == code.value


def test_policy_bounds_cannot_disable_quiet_retention():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        Settings(service_name="backend", source_purge_retention_seconds=3599)
    with pytest.raises(ValidationError):
        Settings(service_name="backend", purge_cleanup_lease_seconds=31)
