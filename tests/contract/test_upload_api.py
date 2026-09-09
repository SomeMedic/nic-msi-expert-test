"""HTTP/parser fault contracts; accept stub does not prove S3 or ingestion."""
from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from expert_api.api.auth import router as auth_router
from expert_api.api.documents import router
from expert_api.auth import AuthService
from expert_api.errors import install_api_errors
from expert_api.uploads import UploadService, finish_blocking
from expert_clients.settings import Settings
from expert_observability.web import install_http_boundary

KEYS = {role: role + "-" + letter * 40 for role, letter in (("viewer", "v"), ("operator", "o"), ("admin", "a"))}
PDF = b"%PDF-1.7\nSynthetic transport test only\n%%EOF\n"
OPTIONS = {"metadata": {"title": "Synthetic", "legal_status": "active", "approved_at": "2026-01-01"},
           "expected_current_publication_id": None, "auto_publish": False}


def settings(**overrides):
    return Settings(service_name="backend", public_base_url="http://localhost:8080",
                    **{f"{role}_access_key": key for role, key in KEYS.items()}, **overrides)


class AcceptedStub(UploadService):
    def __init__(self, config):
        super().__init__(config, None, None)
        self.files, self.calls = [], []

    async def accept(self, principal, key, document_id, upload):
        self.files.append(upload.file.file)
        self.calls.append((principal, key, document_id, upload))
        assert not upload.file.file.closed
        return self._accepted({"document_id": document_id or uuid4(), "version_id": uuid4(), "job_id": uuid4()})


@pytest.fixture
def application():
    app = FastAPI()
    config = settings(upload_max_bytes=1024, upload_body_max_bytes=2048, upload_metadata_max_bytes=1024)
    app.state.auth = AuthService(config, None)
    app.state.uploads = AcceptedStub(config)
    install_http_boundary(app, internal_token=None)
    install_api_errors(app)
    app.include_router(router)
    app.include_router(auth_router)
    return app


def multipart(pdf=PDF, options=OPTIONS, *, filename="synthetic.pdf", media_type="application/pdf"):
    return [("file", (filename, pdf, media_type)), ("options", (None, json.dumps(options), "application/json"))]


def headers(role="operator", **extra):
    return {"Authorization": "Bearer " + KEYS[role], "Idempotency-Key": "synthetic-1", **extra}


def error_code(response, code):
    body = response.json()
    assert body["error"]["code"] == code
    assert body["error"]["request_id"] == response.headers["x-request-id"]
    assert "Synthetic transport" not in response.text
    assert all(key not in response.text for key in KEYS.values())


def test_actual_multipart_hash_identity_and_cleanup(application):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", files=multipart(), headers=headers())
    assert response.status_code == 202
    assert response.headers["Location"] == response.json()["links"]["job"]
    assert response.headers["Cache-Control"] == "no-store"
    service = application.state.uploads
    assert len(service.calls) == 1 and service.active == 0
    upload = service.calls[0][3]
    assert upload.sha256 == hashlib.sha256(PDF).hexdigest() and upload.size == len(PDF)
    assert all(file.closed for file in service.files)


@pytest.mark.parametrize("role,status,code", [(None, 401, "UNAUTHENTICATED"), ("viewer", 403, "FORBIDDEN")])
def test_authority_rejected_before_body_is_read(application, role, status, code):
    def forbidden_body():
        raise AssertionError("unauthorized body consumed")
        yield b""
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", content=forbidden_body(), headers=headers(role) if role else {})
    assert response.status_code == status
    error_code(response, code)
    assert not application.state.uploads.calls


@pytest.mark.parametrize("filename", ["../private.pdf", "C:\\private.pdf", "no-extension", "private.txt", " padded.pdf"])
def test_filename_is_metadata_only_and_rejects_paths(application, filename):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", files=multipart(filename=filename), headers=headers())
    assert response.status_code == 400
    assert not application.state.uploads.calls


@pytest.mark.parametrize("parts,status,code", [
    (multipart(media_type="text/plain"), 415, "UNSUPPORTED_MEDIA_TYPE"),
    (multipart(pdf=b"PRIVATE_NOT_PDF"), 422, "PDF_INVALID"),
    (multipart(pdf=b""), 422, "PDF_INVALID"),
    (multipart(pdf=b"%PDF-" + b"x" * 1024), 413, "SIZE_LIMIT_EXCEEDED"),
    (multipart(options={"metadata": {"title": "PRIVATE"}}), 422, "VALIDATION_ERROR"),
    (multipart() + [("extra", (None, "PRIVATE"))], 400, "INVALID_REQUEST"),
    (multipart() + [("file", ("extra.pdf", PDF, "application/pdf"))], 400, "INVALID_REQUEST"),
])
def test_bad_multipart_never_reaches_accept(application, parts, status, code):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", files=parts, headers=headers())
    assert response.status_code == status
    error_code(response, code)
    assert "PRIVATE" not in response.text
    assert not application.state.uploads.calls
    assert application.state.uploads.active == 0


def raw_multipart(pdf=PDF, *, suffix=True, extra_header=b""):
    body = (b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="synthetic.pdf"\r\n'
            b'Content-Type: application/pdf\r\n' + extra_header + b'\r\n' + pdf + b'\r\n'
            b'--boundary\r\nContent-Disposition: form-data; name="options"\r\n\r\n' + json.dumps(OPTIONS).encode())
    return body + (b"\r\n--boundary--\r\n" if suffix else b"")


@pytest.mark.parametrize("body,status,code", [
    (raw_multipart(pdf=b"%PDF-" + b"x" * 3000), 413, "SIZE_LIMIT_EXCEEDED"),
    (raw_multipart(suffix=False), 400, "INVALID_REQUEST"),
    (raw_multipart(extra_header=b"Content-Type: text/plain\r\n"), 400, "INVALID_REQUEST"),
])
def test_chunked_actual_bytes_and_truncated_multipart(application, body, status, code):
    chunks = (body[index:index + 17] for index in range(0, len(body), 17))
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", content=chunks,
                               headers=headers(**{"Content-Type": "multipart/form-data; boundary=boundary"}))
    assert response.status_code == status
    error_code(response, code)
    assert not application.state.uploads.calls


def test_metadata_headers_are_bounded_independently_of_body(application):
    application.state.uploads.settings = settings(upload_max_bytes=20_000, upload_body_max_bytes=40_000)
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", content=raw_multipart(extra_header=b"X-Noise: " + b"x" * 17000 + b"\r\n"),
                               headers=headers(**{"Content-Type": "multipart/form-data; boundary=boundary"}))
    assert response.status_code == 400
    assert not application.state.uploads.calls


def test_raw_control_in_filename_and_excessive_length_integer_are_safe(application):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", content=raw_multipart().replace(b"synthetic.pdf", b"bad\x00.pdf"),
                               headers=headers(**{"Content-Type": "multipart/form-data; boundary=boundary"}))
        assert response.status_code == 400
        response = client.post("/api/v1/documents", content=b"",
                               headers=headers(**{"Content-Type": "multipart/form-data; boundary=boundary", "Content-Length": "9" * 4301}))
        assert response.status_code == 413
        error_code(response, "SIZE_LIMIT_EXCEEDED")


@pytest.mark.parametrize("body", [
    raw_multipart().replace(b"Synthetic", b"Invalid\xff"),
    raw_multipart().replace(b"synthetic.pdf", b"invalid\xff.pdf"),
])
def test_invalid_utf8_cannot_be_silently_reinterpreted_as_latin1(application, body):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", content=body,
                               headers=headers(**{"Content-Type": "multipart/form-data; boundary=boundary"}))
    assert response.status_code == 400
    assert not application.state.uploads.calls


@pytest.mark.parametrize("origin,expected", [("http://localhost:8080", 202), ("http://127.0.0.1:8080", 202),
    ("http://attacker.invalid", 403), ("http://localhost:8081", 403), ("null", 403),
    ("http://localhost:8080/path", 403), ("http://localhost:8080@attacker.invalid", 403)])
def test_browser_origin_uses_host_scheme_and_port(application, origin, expected):
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", files=multipart(), headers=headers(Origin=origin))
    assert response.status_code == expected


def test_upload_capacity_rejects_without_consuming_body(application):
    application.state.uploads.active = 2
    with TestClient(application) as client:
        response = client.post("/api/v1/documents", headers=headers())
    assert response.status_code == 429
    error_code(response, "CAPACITY_EXCEEDED")


def test_secret_input_is_redacted_from_invalid_login(application):
    with TestClient(application) as client:
        response = client.post("/api/v1/auth/session", json={"access_key": "PRIVATE", "role": "admin"})
    assert response.status_code == 422
    assert "PRIVATE" not in response.text
    error_code(response, "VALIDATION_ERROR")


@pytest.mark.asyncio
async def test_cancelled_request_waits_for_spool_user_before_closing():
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    def operation():
        entered.set()
        assert release.wait(3)
        finished.set()
    task = asyncio.create_task(finish_blocking(operation))
    assert await asyncio.to_thread(entered.wait, 3)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()
