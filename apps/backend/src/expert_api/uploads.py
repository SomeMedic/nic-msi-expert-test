"""Short PostgreSQL commands surrounding an immutable S3 source upload."""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hashlib
import json
import re
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from expert_clients.settings import Settings
from expert_contracts.documents import UploadAccepted, UploadLinks
from expert_contracts.errors import ErrorCode
from expert_observability.tracing import correlation_fields
from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.upload_parser import ParsedUpload

_IDEMPOTENCY_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


def idempotency_key(value: str | None) -> str:
    if value is None or _IDEMPOTENCY_KEY.fullmatch(value) is None:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "Idempotency-Key"})
    return value


def fingerprint(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


async def bind_queued_trace(cursor, job_id: UUID, principal_id: str) -> None:
    """Bind only the server's current trace inside the command's own transaction."""
    trace_id = correlation_fields().get("trace_id")
    if trace_id is not None:
        await cursor.execute("SELECT trace_id,status,lease_owner,lease_epoch FROM app.ingestion_jobs WHERE id=%s",
                             (job_id,))
        job = await cursor.fetchone()
        if job is None:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        if job["trace_id"] is None and (job["status"] != "queued" or job["lease_owner"] is not None
                                      or job["lease_epoch"] != 0):
            # Legacy running/terminal replays have no queued binding authority.
            # A live worker can establish the first trace only through its E fence.
            return
        await cursor.execute("SELECT app.bind_queued_ingestion_trace(%s,%s,%s)",
                             (job_id, principal_id, trace_id))
        await cursor.fetchone()


async def finish_blocking(operation, *args):
    """Keep a spool/client alive until its bounded synchronous operation exits."""
    task = asyncio.create_task(asyncio.to_thread(operation, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        # Retrieve any exception, but preserve request cancellation semantics.
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


class UploadService:
    def __init__(self, settings: Settings, pool, storage):
        self.settings, self.pool, self.storage = settings, pool, storage
        self.active = 0

    @asynccontextmanager
    async def admission(self):
        # No await between checking and incrementing on this process's event loop.
        if self.active >= self.settings.upload_max_concurrency:
            raise ApiError(ErrorCode.CAPACITY_EXCEEDED, details={"retry_after_seconds": 2})
        self.active += 1
        try:
            async with asyncio.timeout(self.settings.upload_timeout_seconds):
                yield
        except TimeoutError:
            raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
        finally:
            self.active -= 1

    async def _command(self, statement: str, parameters: tuple, *, trace_principal: str | None = None) -> dict:
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        row = await (await cursor.execute(statement, parameters)).fetchone()
                        if row is not None and trace_principal is not None:
                            await bind_queued_trace(cursor, row["job_id"], trace_principal)
        if row is None:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return row

    async def accept(self, principal: Principal, key: str, document_id: UUID | None, upload: ParsedUpload) -> UploadAccepted:
        options = upload.options
        if document_id is None and options.expected_current_publication_id is not None:
            raise ApiError(ErrorCode.VERSION_CONFLICT)
        pipeline = self.settings.ingestion_pipeline_fingerprint
        identity = fingerprint({
            "command": "upload", "document_id": str(document_id) if document_id else None,
            "options": options.model_dump(mode="json"), "sha256": upload.sha256,
            "size_bytes": upload.size, "filename": upload.filename, "pipeline": pipeline,
        })
        scoped_key = f"upload:{document_id or 'new'}:{key}"
        intent = await self._command(
            "SELECT * FROM app.reserve_upload(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (principal.subject, scoped_key, identity, document_id, Jsonb(options.metadata.model_dump(mode="json")),
             upload.sha256, upload.size, upload.filename, options.expected_current_publication_id,
             options.auto_publish, pipeline, self.settings.s3_bucket_originals, 3600, self.settings.ingestion_max_queued),
        )
        if intent["state"] == "attached":
            return self._accepted(intent)
        if intent["state"] not in {"reserved", "stored"}:
            raise ApiError(ErrorCode.TERMINAL_CONFLICT)
        if intent["expires_at"] <= datetime.now(timezone.utc):
            raise ApiError(ErrorCode.TERMINAL_CONFLICT)
        object_version = await finish_blocking(self._store, intent, upload)
        await self._command("SELECT * FROM app.mark_upload_stored(%s,%s,%s,%s,%s)",
                            (intent["id"], principal.subject, object_version, upload.sha256, upload.size))
        attached = await self._command("SELECT * FROM app.attach_upload(%s,%s)", (intent["id"], principal.subject),
                                       trace_principal=principal.subject)
        return self._accepted(attached)

    def _store(self, intent: dict, upload: ParsedUpload) -> str | None:
        expected_key = f"originals/{intent['document_id']}/{intent['version_id']}/{upload.sha256}.pdf"
        if intent["object_key"] != expected_key or intent["bucket"] != self.settings.s3_bucket_originals:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        address = {"Bucket": intent["bucket"], "Key": expected_key}
        if intent["state"] == "reserved":
            try:
                self.storage.put_object(
                    **address, Body=upload.file.file, ContentLength=upload.size,
                    ContentType="application/pdf", Metadata={"sha256": upload.sha256},
                    ChecksumSHA256=base64.b64encode(bytes.fromhex(upload.sha256)).decode("ascii"),
                    IfNoneMatch="*",
                )
            except (ClientError, BotoCoreError, OSError):
                # Both conditional duplicates and an unknown PUT outcome are
                # resolved using this exact durable object's metadata below.
                pass
        version = intent["object_version_id"]
        if version is not None:
            address["VersionId"] = version
        try:
            head = self.storage.head_object(**address)
        except (ClientError, BotoCoreError, OSError):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None
        if (head.get("ContentLength") != upload.size
                or head.get("Metadata", {}).get("sha256") != upload.sha256
                or head.get("ContentType") != "application/pdf"
                or (version is not None and head.get("VersionId") != version)):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return head.get("VersionId")

    @staticmethod
    def _accepted(intent: dict) -> UploadAccepted:
        document_id, version_id, job_id = intent["document_id"], intent["version_id"], intent["job_id"]
        return UploadAccepted(document_id=document_id, version_id=version_id, job_id=job_id, links=UploadLinks(
            document=f"/api/v1/documents/{document_id}", version=f"/api/v1/versions/{version_id}",
            job=f"/api/v1/ingestion-jobs/{job_id}", events=f"/api/v1/ingestion-jobs/{job_id}/events",
        ))
