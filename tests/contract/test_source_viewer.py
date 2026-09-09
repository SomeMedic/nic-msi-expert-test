"""Source transport faults with explicit PG/storage substitutes; no corpus-quality claim."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import hashlib
import io
import threading
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from expert_api.api.sources import router
from expert_api.auth import AuthService, Principal
from expert_api.errors import ApiError, install_api_errors
from expert_api.source_viewer import (
    MAX_FRAGMENT_BYTES, SourceReference, SourceViewerService, UnsatisfiableRange, byte_range,
    public_evidence, source_headers,
)
from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole
from expert_contracts.errors import ErrorCode
from expert_observability.web import install_http_boundary
from starlette.requests import ClientDisconnect

PDF = b"%PDF-1.7\nSynthetic transport fixture only\n%%EOF\n"
KEY = "source-viewer-" + "v" * 40
PRINCIPAL = Principal("local-viewer", ApplicationRole.VIEWER, None)


class StorageStub:
    def __init__(self, *, body=PDF, close_error=False, **metadata):
        self.body, self.metadata, self.streams, self.calls = body, metadata, [], []
        self.close_error = close_error

    def get_object(self, **address):
        self.calls.append(address)
        stream = CloseFailBody(self.body) if self.close_error else io.BytesIO(self.body)
        self.streams.append(stream)
        return {"Body": stream, "ContentLength": len(PDF), "ContentType": "application/pdf",
                "Metadata": {"sha256": hashlib.sha256(PDF).hexdigest()}, "VersionId": "pinned-version", **self.metadata}


class CloseFailBody(io.BytesIO):
    def close(self):
        super().close()
        raise OSError("PRIVATE_CLOSE_ERROR")


class ViewerStub(SourceViewerService):
    def __init__(self, storage=None, **kwargs):
        super().__init__(Settings(service_name="backend", viewer_access_key=KEY,
            operator_access_key="operator-" + "o" * 40, admin_access_key="admin-" + "a" * 40), None, storage or StorageStub(), **kwargs)
        document, version = uuid4(), uuid4()
        self.ref = SourceReference(version, document, uuid4(), "originals",
            f"originals/{document}/{version}/{hashlib.sha256(PDF).hexdigest()}.pdf",
            "pinned-version", hashlib.sha256(PDF).hexdigest(), len(PDF), "Точный источник.pdf")
        self.revoked = False
        self.reads = 0

    async def reference(self, version_id, principal):
        self.reads += 1
        if self.revoked:
            raise ApiError(ErrorCode.SOURCE_REVOKED)
        if version_id != self.ref.version_id:
            raise ApiError(ErrorCode.NOT_FOUND)
        return self.ref


def application(viewer):
    app = FastAPI()
    app.state.source_viewer = viewer
    app.state.auth = AuthService(viewer.settings, None)
    install_http_boundary(app, internal_token=None)
    install_api_errors(app)
    app.include_router(router)
    return app


@pytest.mark.parametrize("header,expected", [(None, (0, 9, False)), ("bytes=2-4", (2, 4, True)),
    ("bytes=8-", (8, 9, True)), ("bytes=-3", (7, 9, True)), ("bytes=-100", (0, 9, True)), ("bytes=8-100", (8, 9, True))])
def test_bounded_ranges(header, expected):
    assert byte_range(header, 10) == expected


@pytest.mark.parametrize("value", ["bytes=", "bytes=-", "bytes=-0", "bytes=10-", "bytes=5-2", "bytes=0-1,3-4",
                                  "items=1-3", "bytes= 1-2", "bytes=" + "1" * 30 + "-"])
def test_bad_ranges_are_416_candidates(value):
    with pytest.raises(UnsatisfiableRange) as error:
        byte_range(value, 10)
    assert error.value.size == 10


def test_http_pdf_range_safe_headers_and_authentication():
    service = ViewerStub()
    path = f"/api/v1/versions/{service.ref.version_id}/source"
    with TestClient(application(service)) as client:
        assert client.get(path).status_code == 401 and not service.storage.calls
        headers = {"Authorization": "Bearer " + KEY, "Range": "bytes=0-4"}
        response = client.get(path, headers=headers)
        assert response.status_code == 206 and response.content == b"%PDF-"
        assert response.headers["Content-Range"] == f"bytes 0-4/{len(PDF)}"
        assert response.headers["Content-Type"] == "application/pdf"
        assert response.headers["Cache-Control"] == "no-store, private"
        assert "sandbox" in response.headers["Content-Security-Policy"]
        assert "filename*=UTF-8''" in response.headers["Content-Disposition"]
        assert service.storage.calls[0]["VersionId"] == "pinned-version"
        complete = client.get(path, headers={**headers, "If-Range": '"different-sha"'})
        assert complete.status_code == 200 and complete.content == PDF
        invalid = client.get(path, headers={**headers, "Range": "bytes=999999-"})
        assert invalid.status_code == 416 and invalid.headers["Content-Range"] == f"bytes */{len(PDF)}"
        assert invalid.json()["error"]["request_id"] == invalid.headers["X-Request-ID"]
        service.revoked = True
        revoked = client.get(path, headers=headers)
        assert revoked.status_code == 403 and revoked.json()["error"]["code"] == "SOURCE_REVOKED"
    assert service._active == 0 and all(stream.closed for stream in service.storage.streams)


@pytest.mark.parametrize("storage", [StorageStub(body=PDF[:-1]), StorageStub(body=PDF + b"extra"),
    StorageStub(body=PDF.replace(b"fixture", b"changed")), StorageStub(ContentLength=1),
    StorageStub(ContentType="text/html"), StorageStub(VersionId="different"), StorageStub(Metadata={})])
async def test_no_partial_bytes_escape_unverified_storage(storage):
    service = ViewerStub(storage)
    with pytest.raises(ApiError) as error:
        await service.original(service.ref.version_id, PRINCIPAL, range_header="bytes=0-4")
    assert error.value.code == ErrorCode.SOURCE_UNAVAILABLE
    assert service._active == 0 and storage.streams[0].closed


async def test_source_viewer_primary_validation_error_survives_close_failure():
    service = ViewerStub(StorageStub(body=PDF + b"tamper", close_error=True))
    with pytest.raises(ApiError) as error:
        await service.original(service.ref.version_id, PRINCIPAL)
    assert error.value.code == ErrorCode.SOURCE_UNAVAILABLE
    assert service._active == 0 and service.storage.streams[0].closed


async def test_source_viewer_close_failure_without_primary_is_unavailable():
    service = ViewerStub(StorageStub(close_error=True))
    with pytest.raises(ApiError) as error:
        await service.original(service.ref.version_id, PRINCIPAL)
    assert error.value.code == ErrorCode.SOURCE_UNAVAILABLE
    assert service._active == 0 and service.storage.streams[0].closed


async def test_revoke_after_download_and_before_headers_closes_owned_temp():
    service = ViewerStub()
    response = await service.original(service.ref.version_id, PRINCIPAL)
    service.revoked = True
    sent = []
    async def send(message):
        sent.append(message)
    async def receive():
        return {"type": "http.disconnect"}
    with pytest.raises(ApiError) as error:
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert error.value.code == ErrorCode.SOURCE_REVOKED and sent == []
    assert service._active == 0 and response.file.closed


async def test_cancel_drains_storage_before_releasing_file_and_admission():
    entered, release = threading.Event(), threading.Event()
    service = ViewerStub()
    original = service._download
    files = []
    def delayed(reference, file, deadline):
        files.append(file)
        entered.set()
        assert release.wait(3)
        original(reference, file, deadline)
    service._download = delayed
    task = asyncio.create_task(service.original(service.ref.version_id, PRINCIPAL))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done() and not files[0].closed and service._active == 1
    with pytest.raises(ApiError) as error:
        await service.original(service.ref.version_id, PRINCIPAL)
    assert error.value.code == ErrorCode.CAPACITY_EXCEEDED
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert files[0].closed and service._active == 0


def test_filename_cannot_inject_headers_or_paths():
    reference = replace(ViewerStub().ref, filename='../../secret\\x\r\nX-Evil: yes".pdf')
    headers = source_headers(reference)
    assert all("\n" not in value and "\r" not in value for value in headers.values())
    assert "secret" not in headers["Content-Disposition"] and '%22' in headers["Content-Disposition"]


async def test_client_disconnect_releases_verified_file_and_slot():
    service = ViewerStub()
    response = await service.original(service.ref.version_id, PRINCIPAL)
    async def send(message):
        if message["type"] == "http.response.body":
            raise OSError("synthetic disconnected socket")
    async def receive():
        return {"type": "http.disconnect"}
    with pytest.raises(ClientDisconnect):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert service._active == 0 and response.file.closed


async def test_midstream_revoke_stops_before_next_chunk():
    body = PDF + b"x" * 100_000
    sha = hashlib.sha256(body).hexdigest()
    service = ViewerStub(StorageStub(body=body, ContentLength=len(body), Metadata={"sha256": sha}))
    service.ref = replace(service.ref, size=len(body), sha256=sha)
    response = await service.original(service.ref.version_id, PRINCIPAL)
    bodies = []
    async def send(message):
        if message["type"] == "http.response.body":
            bodies.append(message["body"])
            service.revoked = True
    async def receive():
        return {"type": "http.disconnect"}
    with pytest.raises(ApiError) as error:
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
    assert error.value.code == ErrorCode.SOURCE_REVOKED
    assert len(bodies) == 1 and len(bodies[0]) == 64 * 1024 < len(body)
    assert service._active == 0 and response.file.closed


@pytest.mark.parametrize("change", ["run", "source_url", "private_draft", "oversized_path"])
def test_public_fragment_rejects_identity_private_fields_and_oversized_complete_path(change):
    run, version = uuid4(), uuid4()
    value = {"evidence_id": "E001", "run_id": str(run), "document_version_id": str(version),
             "document_title": "Synthetic source", "version_label": None, "structural_path": ["Section"],
             "excerpt": "Exact", "source_spans": [{"pdf_page": 1, "block_id": "b1", "start_offset": 0, "end_offset": 5}],
             "source_url": f"/api/v1/versions/{version}/source"}
    assert public_evidence(value, run, "E001").excerpt == "Exact"
    if change == "run":
        value["run_id"] = str(uuid4())
    elif change == "source_url":
        value["source_url"] = "/api/v1/versions/another/source"
    elif change == "private_draft":
        value["private_draft"] = "PRIVATE DRAFT NEVER DISCLOSED"
    else:
        value["structural_path"] = ["x" * MAX_FRAGMENT_BYTES]
    with pytest.raises(ApiError) as error:
        public_evidence(value, run, "E001")
    assert error.value.code == ErrorCode.GENERATION_INVALID
