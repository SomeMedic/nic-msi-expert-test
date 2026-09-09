"""Actual HTTP → PostgreSQL → S3 upload. PDFs are synthetic transport fixtures."""
from __future__ import annotations

import hashlib
import io
import json
from uuid import UUID

from botocore.exceptions import EndpointConnectionError
from fastapi import FastAPI
import httpx
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pypdf import PdfWriter
import pytest
import pytest_asyncio

from expert_api.api.documents import router
from expert_api.auth import AuthService
from expert_api.errors import ApiError, install_api_errors
from expert_api.uploads import UploadService
from expert_clients.settings import Settings
from expert_contracts.errors import ErrorCode
from expert_observability.web import install_http_boundary
from scripts.migrate import run_migrations
from p03_resources import p03_resources as resource_fixture

p03_resources = resource_fixture
KEY = "synthetic-operator-" + "o" * 40
OPTIONS = {"metadata": {"title": "Синтетический PDF", "document_type": "Приказ", "document_number": "TEST",
                         "authority": "Fixture only", "legal_status": "active", "approved_at": "2026-01-01"},
           "expected_current_publication_id": None, "auto_publish": False}


def pdf_bytes():
    writer, buffer = PdfWriter(), io.BytesIO()
    writer.add_blank_page(width=100, height=100)
    writer.add_metadata({"/Title": "Synthetic P03 HTTP fixture; not corpus"})
    writer.write(buffer)
    return buffer.getvalue()


class LedgerUploadService(UploadService):
    """Only adds fixture ownership registration and explicit test fault points."""
    fail_attach = False

    def __init__(self, config, pool, resource):
        super().__init__(config, pool, resource.s3_backend)
        self.resource = resource

    def _store(self, intent, upload):
        self.resource.register_original(intent["document_id"], intent["version_id"], upload.sha256, upload.size)
        return super()._store(intent, upload)

    async def _command(self, statement, parameters, **kwargs):
        if self.fail_attach and "app.attach_upload" in statement:
            raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
        return await super()._command(statement, parameters, **kwargs)


@pytest_asyncio.fixture
async def api(p03_resources):
    db = p03_resources.database
    run_migrations(db.migrate_dsn.get_secret_value())
    pool = AsyncConnectionPool(db.role_dsns["backend"].get_secret_value(), min_size=1, max_size=3,
                               open=False, timeout=3, kwargs={"autocommit": True})
    await pool.open(wait=True)
    config = Settings(service_name="backend", app_env="test", operator_access_key=KEY,
                      viewer_access_key="synthetic-viewer-" + "v" * 40, admin_access_key="synthetic-admin-" + "a" * 40)
    app = FastAPI()
    app.state.auth = AuthService(config, pool)
    app.state.uploads = LedgerUploadService(config, pool, p03_resources)
    install_http_boundary(app, internal_token=None)
    install_api_errors(app)
    app.include_router(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://localhost:8080",
                                    headers={"Authorization": "Bearer " + KEY}) as client:
            yield client, app.state.uploads, p03_resources
    finally:
        await pool.close()


async def upload(client, *, key="http-upload-1", content=None, options=None, document_id=None):
    return await client.post(f"/api/v1/documents/{document_id}/versions" if document_id else "/api/v1/documents",
        headers={"Idempotency-Key": key},
        files=[("file", ("синтетический.pdf", content or pdf_bytes(), "application/pdf")),
               ("options", (None, json.dumps(options or OPTIONS, ensure_ascii=False), "application/json"))])


@pytest.mark.asyncio
async def test_real_upload_attaches_source_version_job_event_and_outbox_once(api):
    client, service, resources = api
    response = await upload(client)
    assert response.status_code == 202, response.text
    result = response.json()
    repeated = await upload(client)
    assert repeated.status_code == 202 and repeated.json() == result
    with resources.database.connect() as connection:
        connection.row_factory = dict_row
        version = connection.execute("SELECT * FROM app.document_versions WHERE id=%s", (UUID(result["version_id"]),)).fetchone()
        intent = connection.execute("SELECT * FROM app.upload_intents WHERE version_id=%s", (version["id"],)).fetchone()
        assert intent["state"] == "attached" and version["source_metadata"]["authority"] == "Fixture only"
        assert version["source_sha256"] == hashlib.sha256(pdf_bytes()).hexdigest()
        assert connection.execute("SELECT count(*) n FROM app.ingestion_jobs").fetchone()["n"] == 1
        assert connection.execute("SELECT count(*) n FROM app.ingestion_events").fetchone()["n"] == 1
        assert connection.execute("SELECT count(*) n FROM app.outbox_events WHERE topic='ingestion.jobs.v1'").fetchone()["n"] == 1
        assert connection.execute("SELECT count(*) n FROM knowledge.parse_generations").fetchone()["n"] == 0
    source = resources.s3_ingest.get_object(Bucket="originals", Key=intent["object_key"])
    try:
        assert source["Body"].read() == pdf_bytes()
    finally:
        source["Body"].close()
    assert service.active == 0


@pytest.mark.asyncio
async def test_real_idempotency_conflict_and_explicit_new_version(api):
    client, _service, resources = api
    first = await upload(client)
    assert first.status_code == 202, first.text
    changed = await upload(client, content=pdf_bytes() + b"\n% Different bytes\n")
    assert changed.status_code == 409 and changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    new_version = await upload(client, document_id=first.json()["document_id"])
    assert new_version.status_code == 202, new_version.text
    assert new_version.json()["document_id"] == first.json()["document_id"]
    assert new_version.json()["version_id"] != first.json()["version_id"]
    with resources.database.connect() as connection:
        assert connection.execute("SELECT count(*) FROM app.document_versions").fetchone()[0] == 2


@pytest.mark.asyncio
async def test_s3_success_attach_failure_retains_exact_recoverable_intent(api):
    client, service, resources = api
    service.fail_attach = True
    response = await upload(client)
    assert response.status_code == 503
    with resources.database.connect() as connection:
        connection.row_factory = dict_row
        intent = connection.execute("SELECT * FROM app.upload_intents").fetchone()
        assert intent["state"] == "stored"
        assert connection.execute("SELECT count(*) n FROM app.document_versions").fetchone()["n"] == 0
        assert connection.execute("SELECT count(*) n FROM app.ingestion_jobs").fetchone()["n"] == 0
    assert resources.head("originals", intent["object_key"])["ContentLength"] == len(pdf_bytes())
    service.fail_attach = False
    repeated = await upload(client)
    assert repeated.status_code == 202
    assert repeated.json()["version_id"] == str(intent["version_id"])
    assert repeated.json()["job_id"] == str(intent["job_id"])


@pytest.mark.asyncio
async def test_ambiguous_real_put_ack_is_reconciled_by_exact_head(api):
    client, service, resources = api
    class LostPutAck:
        calls = 0
        def put_object(self, **kwargs):
            self.calls += 1
            resources.s3_backend.put_object(**kwargs)
            raise EndpointConnectionError(endpoint_url="http://synthetic.invalid")
        def head_object(self, **kwargs):
            return resources.s3_backend.head_object(**kwargs)
    storage = LostPutAck()
    service.storage = storage
    response = await upload(client)
    assert response.status_code == 202, response.text
    assert storage.calls == 1
    with resources.database.connect() as connection:
        assert connection.execute("SELECT state FROM app.upload_intents").fetchone()[0] == "attached"


@pytest.mark.asyncio
async def test_missing_storage_keeps_reserved_intent_without_public_driver_error(api):
    client, service, resources = api
    class UnavailableStore:
        def put_object(self, **kwargs):
            raise EndpointConnectionError(endpoint_url="http://PRIVATE_SENTINEL.invalid")
        def head_object(self, **kwargs):
            raise EndpointConnectionError(endpoint_url="http://PRIVATE_SENTINEL.invalid")
    service.storage = UnavailableStore()
    response = await upload(client)
    assert response.status_code == 503
    assert "PRIVATE_SENTINEL" not in response.text
    with resources.database.connect() as connection:
        assert connection.execute("SELECT state FROM app.upload_intents").fetchone()[0] == "reserved"
        assert connection.execute("SELECT count(*) FROM app.ingestion_jobs").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_durable_queue_capacity_precedes_put_and_cancel_releases_slot(api):
    client, service, resources = api
    service.settings = service.settings.model_copy(update={"ingestion_max_queued": 1})
    first = await upload(client)
    assert first.status_code == 202, first.text
    assert (await upload(client)).json() == first.json()
    rejected = await upload(client, key="second-queued-document")
    assert rejected.status_code == 429, rejected.text
    assert rejected.json()["error"]["code"] == "CAPACITY_EXCEEDED"
    assert rejected.headers["Retry-After"] == "2"
    with resources.database.connect("backend") as connection:
        assert connection.execute("SELECT count(*) FROM app.upload_intents").fetchone()[0] == 1
        connection.execute("SELECT app.cancel_ingestion(%s,%s)", (UUID(first.json()["job_id"]), "local-operator"))
    accepted = await upload(client, key="second-queued-document")
    assert accepted.status_code == 202, accepted.text
