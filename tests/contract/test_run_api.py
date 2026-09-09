"""P09 HTTP/privacy and bounded stream contracts, with explicit domain stubs."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from expert_api.api.runs import router
from expert_api.auth import AuthService, Principal
from expert_api.errors import ApiError, install_api_errors
from expert_api.openapi import public_openapi
from expert_api.run_events import (
    BoundedEventResponse, EventBatch, PreparedReplay, RunEventService,
    encode_event, event_from_row, parse_cursor,
)
from expert_api.runs import public_run, validated_terminal_error
from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole
from expert_contracts.errors import ErrorCode
from expert_contracts.runs import PublicRun, RunAccepted, RunCancelAccepted, RunDebug, RunList
from expert_observability.web import install_http_boundary

KEYS = {role: role + "-" + role[0] * 40 for role in ("viewer", "operator", "admin")}
NOW = datetime.now(timezone.utc)
PRIVATE = "PRIVATE_DRAFT_AND_PASSWORD_DO_NOT_DISCLOSE"


def event_row(run_id, sequence=1, event_type="run.created", payload=None, **extra):
    return dict(schema_version=1, event_id=uuid4(), run_id=run_id, sequence=sequence,
                event_type=event_type, stage=None, attempt=1, execution_epoch=0,
                created_at=NOW, public_payload={} if payload is None else payload, **extra)


def state(run_id, status="created", **extra):
    return dict(run_id=run_id, status=status, current_stage=None, stage_attempt=1,
                created_at=NOW, started_at=None, finished_at=None, cancel_requested=False,
                snapshot=None, last_sequence=1, result=None, error=None) | extra


def headers(role="operator", **extra):
    return {"Authorization": "Bearer " + KEYS[role], "Idempotency-Key": "run-contract", **extra}


class StubRuns:
    def __init__(self):
        self.run_id, self.calls, self.terminal = uuid4(), [], False

    async def create(self, principal, key, payload, request_id):
        self.calls.append((principal.subject, key, payload, request_id))
        return RunAccepted(run_id=self.run_id, links={"self": f"/api/v1/runs/{self.run_id}",
                                                    "events": f"/api/v1/runs/{self.run_id}/events"})

    async def get(self, run_id, principal):
        self.calls.append(("get", principal.subject))
        if run_id != self.run_id:
            raise ApiError(ErrorCode.NOT_FOUND)
        return PublicRun(**state(run_id))

    async def list(self, principal, **parameters):
        self.calls.append(("list", principal.subject, parameters))
        return RunList(items=[], next_cursor=None)

    async def cancel(self, run_id, principal):
        if self.terminal:
            return PublicRun(**state(run_id, "cancelled", finished_at=NOW, cancel_requested=True))
        return RunCancelAccepted(run_id=run_id, last_sequence=2)

    async def debug(self, run_id, principal):
        return RunDebug(run_id=run_id, steps=[])


class StubEvents(RunEventService):
    def __init__(self, config):
        super().__init__(config, None, None)
        self.cursors, self.error = [], None

    async def prepare(self, run_id, principal, cursor):
        self.cursors.append(cursor)
        if self.error:
            raise self.error
        event = event_from_row(event_row(run_id), run_id)
        return PreparedReplay(run_id, principal, cursor, "0-0",
                              EventBatch((event,) if cursor == 0 else (), max(1, cursor), True))


@pytest.fixture
def application(monkeypatch):
    import os
    for name in list(os.environ):
        if name.startswith("EXPERT_"):
            monkeypatch.delenv(name)
    config = Settings(service_name="backend", public_base_url="http://localhost:8080",
                      **{f"{role}_access_key": key for role, key in KEYS.items()})
    app = FastAPI()
    app.state.auth, app.state.runs = AuthService(config, None), StubRuns()
    app.state.run_events = StubEvents(config)
    install_http_boundary(app, internal_token=None)
    install_api_errors(app)
    app.include_router(router)
    return app


def test_create_contract_location_identity_and_closed_request(application):
    with TestClient(application) as client:
        response = client.post("/api/v1/runs", headers=headers(), json={"question": "Synthetic question"})
    assert response.status_code == 202
    assert response.headers["location"] == response.json()["links"]["self"]
    assert response.json()["status"] == "created" and response.json()["last_sequence"] == 1
    call = application.state.runs.calls[0]
    assert call[0] == "local-operator" and call[2].debug_capture is False
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.parametrize("role,status", [(None, 401), ("viewer", 403)])
def test_create_auth_before_body(application, role, status):
    def unread():
        raise AssertionError("Unauthorized body was read")
        yield b""
    with TestClient(application) as client:
        response = client.post("/api/v1/runs", headers=headers(role) if role else {}, content=unread())
    assert response.status_code == status and not application.state.runs.calls


@pytest.mark.parametrize("raw", [
    b'{"question":"x","principal_id":"hacker"}', b'{"question":"x","debug_capture":"true"}',
    b'{"question":"x","question":"y"}', b'{"question":"x","model":"remote"}',
    b'{"question":"x","debug_capture":NaN}', b'{"question":"\xff"}',
    b'{"question":""}', b'[]', b'{"question":"x","private_draft":"' + PRIVATE.encode() + b'"}',
])
def test_bad_bodies_do_not_echo_input(application, raw, caplog):
    with TestClient(application) as client:
        response = client.post("/api/v1/runs", headers=headers(**{"Content-Type": "application/json"}), content=raw)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"
    assert PRIVATE not in response.text + caplog.text and not application.state.runs.calls


def test_body_bound_and_duplicate_idempotency(application):
    with TestClient(application) as client:
        large = client.post("/api/v1/runs", headers=headers(**{"Content-Type": "application/json"}), content=b" " * 32769)
        duplicate = client.post("/api/v1/runs", headers=list(headers().items()) + [("Idempotency-Key", "other")], json={"question": "x"})
    assert large.status_code == 413 and duplicate.status_code == 400
    assert not application.state.runs.calls


def test_read_cancel_debug_and_invalid_pagination(application):
    run_id = application.state.runs.run_id
    with TestClient(application) as client:
        assert client.get(f"/api/v1/runs/{run_id}", headers=headers()).status_code == 200
        assert client.get(f"/api/v1/runs/{uuid4()}", headers=headers()).status_code == 404
        assert client.post(f"/api/v1/runs/{run_id}/cancel", headers=headers()).status_code == 202
        application.state.runs.terminal = True
        assert client.post(f"/api/v1/runs/{run_id}/cancel", headers=headers()).status_code == 200
        assert client.get(f"/api/v1/runs/{run_id}/debug", headers=headers("viewer")).status_code == 403
        assert client.get(f"/api/v1/runs/{run_id}/debug", headers=headers()).json()["steps"] == []
        for query in ("limit=1.0", "limit=2&limit=3", "private=true"):
            assert client.get("/api/v1/runs?" + query, headers=headers()).status_code == 400
        assert client.get("/api/v1/runs?limit=101", headers=headers()).status_code == 422


def test_sse_wire_precedence_and_history_envelope(application):
    path = f"/api/v1/runs/{uuid4()}/events"
    with TestClient(application) as client:
        response = client.get(path, headers=headers())
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert response.headers["x-accel-buffering"] == "no"
        assert response.headers["cache-control"] == "no-cache"
        assert response.text.startswith("id: 1\nevent: run.created\ndata: {") and response.text.endswith("\n\n")
        assert client.get(path + "?after=" + "x" * 100, headers=headers(**{"Last-Event-ID": "1"})).text == ""
        assert application.state.run_events.cursors == [0, 1]
        assert client.get(path, headers=headers(**{"Last-Event-ID": "-1"})).status_code == 400
        application.state.run_events.error = ApiError(ErrorCode.EVENT_HISTORY_EXPIRED,
                                                    details={"last_sequence": 4, "snapshot_url": path[:-7]})
        response = client.get(path, headers=headers())
        assert response.status_code == 410 and response.json()["error"]["details"]["last_sequence"] == 4


@pytest.mark.parametrize("value", ["", "-1", "+1", " 1", "01", "1.0", "1\n", "9" * 10000, str(2**63)])
def test_cursor_is_bounded_unsigned_integer(value):
    with pytest.raises(ApiError) as error:
        parse_cursor(value, None)
    assert error.value.code == ErrorCode.EVENT_CURSOR_INVALID


def test_public_schema_has_only_shared_run_dtos(application):
    schema = public_openapi(application)
    create = schema["paths"]["/api/v1/runs"]["post"]
    assert create["requestBody"]["content"]["application/json"]["schema"]["additionalProperties"] is False
    assert any(parameter["name"] == "Idempotency-Key" for parameter in create["parameters"])
    assert "410" in schema["paths"]["/api/v1/runs/{run_id}/events"]["get"]["responses"]
    assert "StartRunRequest" not in schema["components"]["schemas"]


def test_public_projections_fail_closed_on_private_payload_or_error(caplog):
    run_id = uuid4()
    for bad in (state(run_id, result={"private_draft": PRIVATE}), state(run_id, private_json=PRIVATE)):
        with pytest.raises(ApiError) as error:
            public_run(bad)
        assert str(error.value) == "INTERNAL_ERROR"
    with pytest.raises(ApiError):
        event_from_row(event_row(run_id, payload={"private_draft": PRIVATE}), run_id)
    with pytest.raises(ApiError):
        validated_terminal_error({"code": "INTERNAL_ERROR", "message": PRIVATE,
                                  "request_id": run_id, "retryable": False, "details": {}}, run_id)
    assert PRIVATE not in caplog.text


async def test_heartbeat_expiry_disconnect_and_no_run_cancellation():
    config = Settings(service_name="backend", sse_heartbeat_seconds=0.01, sse_pg_poll_seconds=0.005)
    service = StubEvents(config)
    principal = Principal("synthetic", ApplicationRole.OPERATOR)
    run_id = uuid4()
    prepared = PreparedReplay(run_id, principal, 1, "0-0", EventBatch((), 1, False))
    reads = []

    async def batch(*args):
        reads.append(args)
        return EventBatch((), 1, False)

    async def connected():
        return False

    service.read_batch = batch
    iterator = service.iterate(prepared, is_disconnected=connected)
    assert await asyncio.wait_for(anext(iterator), 0.5) == b": heartbeat\n\n"
    await iterator.aclose()
    assert 0 < len(reads) < 10
    expired = Principal("synthetic", ApplicationRole.OPERATOR, NOW - timedelta(seconds=1))
    iterator = service.iterate(PreparedReplay(run_id, expired, 1, "0-0", EventBatch((), 1, False)),
                               is_disconnected=connected)
    with pytest.raises(StopAsyncIteration):
        await anext(iterator)


@pytest.mark.parametrize("spec_version", ["2.0", "2.4"])
async def test_slow_recipient_is_closed_without_unbounded_prefetch(monkeypatch, spec_version):
    import expert_api.run_events as module
    monkeypatch.setattr(module, "SEND_TIMEOUT_SECONDS", 0.01)
    yielded, closed = [], []

    async def body():
        try:
            for number in range(100):
                yielded.append(number)
                yield b"data: {}\n\n"
        finally:
            closed.append(True)

    async def stalled(message):
        if message["type"] == "http.response.body":
            await asyncio.sleep(1)

    async def receive():
        await asyncio.sleep(1)

    await asyncio.wait_for(BoundedEventResponse(body())({"type": "http", "asgi": {"spec_version": spec_version}}, receive, stalled), 0.5)
    assert yielded == [0] and closed == [True]


def test_event_json_cannot_inject_sse_fields():
    run_id = uuid4()
    event = event_from_row(event_row(run_id), run_id)
    wire = encode_event(event).decode()
    envelope = json.loads(wire.split("data: ", 1)[1])
    assert envelope["run_id"] == str(run_id) and wire.count("\nid: ") == 0
