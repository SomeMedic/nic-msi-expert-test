"""Authenticated, SHA-verified originals and committed citation-only fragments."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import tempfile
import time
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar
from urllib.parse import quote
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError
from psycopg.rows import dict_row
from pydantic import ValidationError
from starlette.responses import StreamingResponse

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_clients.settings import Settings
from expert_contracts.errors import ErrorCode
from expert_contracts.sources import PublicEvidence

CHUNK_BYTES = 64 * 1024
MAX_FRAGMENT_BYTES = 1024 * 1024
T = TypeVar("T")


def _close_body_preserving_primary(stream: Any, *, primary_failure: bool) -> None:
    try:
        stream.close()
    except Exception:
        if not primary_failure:
            raise


class SourceStorage(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


class SourceFile(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...
    def write(self, data: bytes, /) -> int: ...
    def seek(self, offset: int, /) -> int: ...
    def flush(self) -> None: ...
    def close(self) -> None: ...


@dataclass(frozen=True)
class SourceReference:
    version_id: UUID
    document_id: UUID
    object_id: UUID
    bucket: str
    key: str
    object_version_id: str | None
    sha256: str
    size: int
    filename: str


class UnsatisfiableRange(Exception):
    def __init__(self, size: int):
        self.size = size


def byte_range(value: str | None, size: int) -> tuple[int, int, bool]:
    """One RFC byte range; reject malformed/multipart ranges without reading storage."""
    if value is None:
        return 0, size - 1, False
    match = re.fullmatch(r"bytes=([0-9]{0,20})-([0-9]{0,20})", value)
    if match is None or not any(match.groups()):
        raise UnsatisfiableRange(size)
    first, last = match.groups()
    if first:
        start, end = int(first), int(last) if last else size - 1
        if start >= size or start > end:
            raise UnsatisfiableRange(size)
        return start, min(end, size - 1), True
    length = int(last)
    if length <= 0:
        raise UnsatisfiableRange(size)
    return max(0, size - length), size - 1, True


def source_headers(reference: SourceReference) -> dict[str, str]:
    filename = reference.filename.replace("\\", "/").split("/")[-1]
    filename = "".join(char for char in filename if not unicodedata.category(char).startswith("C"))[:150]
    filename = filename.strip(" .") or "source.pdf"
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return {
        "Cache-Control": "no-store, private", "Pragma": "no-cache", "Accept-Ranges": "bytes",
        "ETag": f'"sha256-{reference.sha256}"', "X-Content-Type-Options": "nosniff",
        "Content-Disposition": f"inline; filename=\"source.pdf\"; filename*=UTF-8''{quote(filename, safe='')}",
        "Content-Security-Policy": "sandbox; default-src 'none'; base-uri 'none'; form-action 'none'",
        "Referrer-Policy": "no-referrer", "Cross-Origin-Resource-Policy": "same-origin",
    }


async def drain_blocking(operation: Callable[[], T]) -> T:
    """A cancelled request still owns its bounded thread until all I/O has stopped."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        if not task.cancelled():
            task.exception()
        raise asyncio.CancelledError
    return task.result()


def public_evidence(value: object, run_id: UUID, evidence_id: str) -> PublicEvidence:
    try:
        # PublicEvidence bounds excerpt/spans, while inherited path components
        # have no individual length cap. Reject oversized complete projections.
        if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode()) > MAX_FRAGMENT_BYTES:
            raise ValueError()
        result = PublicEvidence.model_validate(value)
        if (result.run_id != run_id or result.evidence_id != evidence_id
                or result.source_url != f"/api/v1/versions/{result.document_version_id}/source"):
            raise ValueError()
        return result
    except (ValueError, ValidationError, TypeError):
        raise ApiError(ErrorCode.GENERATION_INVALID) from None


class VerifiedSourceResponse(StreamingResponse):
    """Own the temporary file and admission slot, including disconnect/cancellation."""
    def __init__(self, service: SourceViewerService, reference: SourceReference, principal: Principal,
                 file: SourceFile, start: int, end: int, partial: bool, deadline: float):
        self.service, self.file = service, file
        self.reference, self.principal, self.deadline = reference, principal, deadline
        self._closed = False
        headers = source_headers(reference)
        headers["Content-Length"] = str(end - start + 1)
        if partial:
            headers["Content-Range"] = f"bytes {start}-{end}/{reference.size}"

        async def stream():
            file.seek(start)
            remaining = end - start + 1
            while remaining:
                # A revoke during a long transfer terminates it before another chunk.
                await service.check_reference(reference, principal)
                chunk = file.read(min(CHUNK_BYTES, remaining))
                if not chunk:
                    raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                remaining -= len(chunk)
                yield chunk

        super().__init__(stream(), status_code=206 if partial else 200, headers=headers, media_type="application/pdf")

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self.file.close()
            finally:
                self.service._active -= 1

    async def __call__(self, scope, receive, send) -> None:
        try:
            async with asyncio.timeout_at(self.deadline):
                await self.service.check_reference(self.reference, self.principal)
                await super().__call__(scope, receive, send)
        finally:
            self.close()


class SourceViewerService:
    def __init__(self, settings: Settings, pool, storage: SourceStorage, *, temp_root: Path | None = None,
                 concurrency: int = 1, operation_timeout_seconds: float = 60):
        if concurrency != 1 or not 0 < operation_timeout_seconds <= 300:
            raise ValueError("SOURCE_VIEWER_BOUNDS_INVALID")
        self.settings, self.pool, self.storage = settings, pool, storage
        self.temp_root = temp_root
        self.operation_timeout_seconds = operation_timeout_seconds
        self._active = 0

    async def reference(self, version_id: UUID, principal: Principal) -> SourceReference:
        # This deployment has an authenticated shared document library. Run
        # evidence additionally has a principal predicate inside its SQL routine.
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("""SELECT v.id AS version_id,v.logical_document_id AS document_id,
                        v.source_sha256,v.content_size,v.original_filename,d.security_revoked_at,d.purge_plan_id,
                        o.id AS object_id,o.bucket,o.object_key,o.object_version_id,o.sha256,o.size_bytes,
                        o.media_type,o.kind,o.state FROM app.document_versions v
                        JOIN app.logical_documents d ON d.id=v.logical_document_id
                        JOIN app.stored_objects o ON o.id=v.source_object_id
                        WHERE v.id=%s""", (version_id,))
                    row = await cursor.fetchone()
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        if row["security_revoked_at"] is not None:
            raise ApiError(ErrorCode.SOURCE_REVOKED)
        if row.get("purge_plan_id") is not None:
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        if (row["state"] != "attached" or row["kind"] != "original" or row["media_type"] != "application/pdf"
                or row["sha256"] != row["source_sha256"] or row["size_bytes"] != row["content_size"]
                or not 0 < row["size_bytes"] <= self.settings.upload_max_bytes
                or row["bucket"] != self.settings.s3_bucket_originals
                or row["object_key"] != f"originals/{row['document_id']}/{version_id}/{row['sha256']}.pdf"):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        return SourceReference(version_id=version_id, document_id=row["document_id"], object_id=row["object_id"],
            bucket=row["bucket"], key=row["object_key"], object_version_id=row["object_version_id"],
            sha256=row["sha256"], size=row["size_bytes"], filename=row["original_filename"])

    async def check_reference(self, expected: SourceReference, principal: Principal) -> None:
        if await self.reference(expected.version_id, principal) != expected:
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)

    def _download(self, reference: SourceReference, target: SourceFile, deadline: float) -> None:
        address: dict[str, Any] = {"Bucket": reference.bucket, "Key": reference.key}
        if reference.object_version_id is not None:
            address["VersionId"] = reference.object_version_id
        try:
            response = self.storage.get_object(**address)
            stream = response["Body"]
            primary_failure = False
            try:
                if (response.get("ContentLength") != reference.size or response.get("ContentType") != "application/pdf"
                        or response.get("Metadata", {}).get("sha256") != reference.sha256
                        or (reference.object_version_id is not None and response.get("VersionId") != reference.object_version_id)):
                    raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                digest, size, prefix = hashlib.sha256(), 0, b""
                while True:
                    if time.monotonic() >= deadline:
                        raise ApiError(ErrorCode.DEADLINE_EXCEEDED)
                    chunk = stream.read(min(CHUNK_BYTES, reference.size - size + 1))
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > reference.size:
                        raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                    digest.update(chunk)
                    if len(prefix) < 5:
                        prefix = (prefix + chunk)[:5]
                    target.write(chunk)
                if size != reference.size or digest.hexdigest() != reference.sha256 or prefix != b"%PDF-":
                    raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                target.flush()
            except BaseException:
                primary_failure = True
                raise
            finally:
                _close_body_preserving_primary(stream, primary_failure=primary_failure)
        except ApiError:
            raise
        except (ClientError, BotoCoreError, OSError, KeyError):
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None

    async def original(self, version_id: UUID, principal: Principal, *, range_header: str | None = None,
                       if_range: str | None = None) -> VerifiedSourceResponse:
        if self._active:
            raise ApiError(ErrorCode.CAPACITY_EXCEEDED, details={"retry_after_seconds": 1})
        self._active += 1
        file = None
        transferred = False
        deadline = asyncio.get_running_loop().time() + self.operation_timeout_seconds
        try:
            async with asyncio.timeout_at(deadline):
                reference = await self.reference(version_id, principal)
                if if_range is not None and if_range != source_headers(reference)["ETag"]:
                    range_header = None
                start, end, partial = byte_range(range_header, reference.size)
                file = tempfile.TemporaryFile(mode="w+b", prefix="expert-source-", dir=self.temp_root)
                await drain_blocking(lambda: self._download(reference, file, time.monotonic() + self.operation_timeout_seconds))
                await self.check_reference(reference, principal)
                response = VerifiedSourceResponse(self, reference, principal, file, start, end, partial, deadline)
                transferred = True
                return response
        except TimeoutError:
            raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
        except OSError:
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None
        finally:
            if not transferred:
                if file is not None:
                    file.close()
                self._active -= 1

    async def evidence(self, run_id: UUID, evidence_id: str, principal: Principal) -> PublicEvidence:
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}", evidence_id) is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("SELECT agent.get_public_evidence(%s,%s,%s) AS evidence",
                                         (run_id, principal.subject, evidence_id))
                    row = await cursor.fetchone()
        if row is None or row["evidence"] is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        result = public_evidence(row["evidence"], run_id, evidence_id)
        await self.reference(result.document_version_id, principal)
        return result
