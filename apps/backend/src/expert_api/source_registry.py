"""Private source-registry projection for retrieval runtime source checks."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar
from uuid import UUID, uuid4

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import APIRouter, Depends, FastAPI, Request
from fastapi.routing import APIRoute
from psycopg.rows import dict_row
from starlette.responses import Response

from expert_clients.settings import Settings
from expert_contracts.errors import ErrorCode
from expert_contracts.retrieval_sources import (
    RegistryBlock,
    RegistryOwnerHash,
    RegistryPage,
    SourceRegistryRequest,
    SourceRegistryResponse,
    canonical_owner_hash,
)
from expert_api.errors import ApiError, database_failures
from expert_observability.web import error_response

MAX_ARTIFACT_BYTES = 256 * 1024 * 1024
MAX_REQUEST_BYTES = 256 * 1024
MAX_BLOCK_TEXT_CHARS = 2_000_000
_PARSE_ARTIFACT_KEY = re.compile(
    r"^parses/(?P<version>[0-9a-f-]{36})/(?P<parse>[0-9a-f-]{36})/"
    r"(?P<intent>[0-9a-f-]{36})/(?P<sha>[0-9a-f]{64})\.json$"
)

T = TypeVar("T")


class SourceRegistryStorage(Protocol):
    def get_object(self, **kwargs: Any) -> dict[str, Any]: ...


@dataclass(frozen=True)
class _ArtifactRef:
    run_id: Any
    snapshot_id: Any
    parse_generation_id: Any
    document_version_id: Any
    source_sha256: str
    artifact_object_id: Any
    bucket: str
    object_key: str
    object_version_id: str | None
    media_type: str
    artifact_size_bytes: int
    artifact_sha256: str
    physical_page_count: int


def _service(request: Request) -> "SourceRegistryService":
    service: SourceRegistryService | None = getattr(request.app.state, "source_registry", None)
    if service is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    return service


def _internal_auth(request: Request) -> None:
    settings: Settings | None = getattr(request.app.state, "settings", None)
    if settings is None:
        service = getattr(request.app.state, "source_registry", None)
        settings = getattr(service, "settings", None)
    if settings is None:
        raise ApiError(ErrorCode.DEPENDENCY_UNAVAILABLE)
    token = settings.require_secret("agent_runtime_token").get_secret_value()
    expected = b"Bearer " + token.encode("utf-8")
    actual = request.headers.get("authorization", "").encode("utf-8", "surrogatepass")
    if not hmac.compare_digest(actual, expected):
        raise ApiError(ErrorCode.UNAUTHENTICATED)


async def _finish_blocking(operation: Callable[[], T]) -> T:
    """Drain a bounded synchronous storage call before propagating cancellation."""
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


def _close_body_preserving_primary(stream: Any, *, primary_failure: bool) -> None:
    close = getattr(stream, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:
        if not primary_failure:
            raise


def _operation_timeout(request: Request) -> float:
    service: SourceRegistryService | None = getattr(request.app.state, "source_registry", None)
    return service.operation_timeout_seconds if service is not None else 30.0


async def _bounded_request(request: Request, max_bytes: int) -> Request | Response:
    request_id = _request_id_from_scope(request.scope)
    size = 0
    chunks: list[bytes] = []
    async for chunk in request.stream():
        size += len(chunk)
        if size > max_bytes:
            return error_response(
                ErrorCode.SIZE_LIMIT_EXCEEDED.value,
                request_id,
                details={"max_bytes": max_bytes},
            )
        chunks.append(chunk)
    body = b"".join(chunks)
    delivered = False

    async def replay_receive():
        nonlocal delivered
        if delivered:
            return {"type": "http.request", "body": b"", "more_body": False}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(request.scope, replay_receive)


class SourceRegistryBoundedRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Any]:
        original = super().get_route_handler()

        async def bounded(request: Request) -> Response:
            try:
                _internal_auth(request)
                async with asyncio.timeout(_operation_timeout(request)):
                    checked = await _bounded_request(request, MAX_REQUEST_BYTES)
                    if isinstance(checked, Response):
                        return checked
                    return await original(checked)
            except TimeoutError:
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None

        return bounded


class SourceRegistryBodyLimitMiddleware:
    def __init__(self, app, *, max_bytes: int = MAX_REQUEST_BYTES):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if (
            scope["type"] != "http"
            or scope.get("method") != "POST"
            or scope.get("path") != "/internal/v1/retrieval/source-registry"
        ):
            await self.app(scope, receive, send)
            return
        request = Request(scope, receive)
        try:
            _internal_auth(request)
            async with asyncio.timeout(_operation_timeout(request)):
                checked = await _bounded_request(request, self.max_bytes)
        except ApiError as error:
            await error_response(error.code.value, _request_id_from_scope(scope), details=error.details)(scope, receive, send)
            return
        except TimeoutError:
            await error_response(ErrorCode.DEADLINE_EXCEEDED.value, _request_id_from_scope(scope))(scope, receive, send)
            return
        if isinstance(checked, Response):
            await checked(scope, receive, send)
            return
        await self.app(scope, checked.receive, send)


def install_source_registry_body_limit(app: FastAPI, *, max_bytes: int = MAX_REQUEST_BYTES) -> None:
    app.add_middleware(SourceRegistryBodyLimitMiddleware, max_bytes=max_bytes)


def _request_id_from_scope(scope) -> UUID:
    value = scope.get("state", {}).get("request_id")
    if isinstance(value, UUID):
        return value
    headers = dict(scope.get("headers", []))
    try:
        return UUID(headers.get(b"x-request-id", b"").decode("ascii"))
    except (ValueError, UnicodeError):
        return uuid4()


router = APIRouter(
    prefix="/internal/v1/retrieval",
    tags=["Internal Source Registry"],
    route_class=SourceRegistryBoundedRoute,
)


@router.post("/source-registry", response_model=SourceRegistryResponse, operation_id="get_source_registry_blocks")
async def get_source_registry_blocks(
    payload: SourceRegistryRequest,
    request: Request,
    _auth: None = Depends(_internal_auth),
) -> SourceRegistryResponse:
    return await _service(request).get(payload)


class SourceRegistryService:
    def __init__(
        self,
        settings: Settings,
        pool,
        storage: SourceRegistryStorage,
        *,
        concurrency: int = 1,
        operation_timeout_seconds: float = 30,
    ):
        if concurrency != 1:
            raise ValueError("source registry admission concurrency is fixed at 1")
        if operation_timeout_seconds <= 0 or operation_timeout_seconds > 300:
            raise ValueError("source registry operation timeout must be in (0, 300]")
        self.settings, self.pool, self.storage = settings, pool, storage
        self.operation_timeout_seconds = operation_timeout_seconds
        self._capacity = concurrency
        self._admission_lock = asyncio.Lock()
        self._active = 0

    @asynccontextmanager
    async def _admission(self):
        async with self._admission_lock:
            if self._active >= self._capacity:
                raise ApiError(ErrorCode.CAPACITY_EXCEEDED, details={"retry_after_seconds": 2})
            self._active += 1
        try:
            yield
        finally:
            async with self._admission_lock:
                self._active -= 1

    async def get(self, payload: SourceRegistryRequest) -> SourceRegistryResponse:
        async with self._admission():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    artifact = await self._lookup(payload)
                    response = await self._read_and_project(payload, artifact)
                    latest = await self._lookup(payload)
                    if latest != artifact:
                        raise ApiError(ErrorCode.VERSION_CONFLICT)
                    try:
                        response.validate_binding(payload)
                    except ValueError:
                        raise ApiError(ErrorCode.GENERATION_INVALID) from None
                    return response
            except TimeoutError:
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None

    async def _lookup(self, payload: SourceRegistryRequest) -> _ArtifactRef:
        statement = """
            SELECT
                r.id AS run_id,
                r.snapshot_id AS snapshot_id,
                r.status AS run_status,
                r.cancel_requested_at AS cancel_requested_at,
                si.document_version_id AS document_version_id,
                si.parse_generation_id AS parse_generation_id,
                v.source_sha256 AS source_sha256,
                d.security_revoked_at AS security_revoked_at,
                d.purge_plan_id AS purge_plan_id,
                pg.status AS parse_status,
                pg.quality_report AS quality_report,
                pg.artifact_object_id AS artifact_object_id,
                pg.physical_page_count AS physical_page_count,
                o.bucket AS bucket,
                o.object_key AS object_key,
                o.object_version_id AS object_version_id,
                o.media_type AS media_type,
                o.size_bytes AS artifact_size_bytes,
                o.sha256 AS artifact_sha256,
                o.kind AS artifact_kind,
                o.state AS artifact_state
            FROM agent.runs r
            JOIN agent.kb_snapshot_items si
              ON si.snapshot_id = r.snapshot_id
             AND si.document_version_id = %s
             AND si.parse_generation_id = %s
            JOIN app.document_versions v
              ON v.id = si.document_version_id
             AND v.source_sha256 = %s
            JOIN app.logical_documents d
              ON d.id = si.logical_document_id
             AND d.id = v.logical_document_id
            JOIN knowledge.parse_generations pg
              ON pg.id = si.parse_generation_id
             AND pg.document_version_id = si.document_version_id
             AND pg.source_sha256 = v.source_sha256
            JOIN app.stored_objects o
              ON o.id = pg.artifact_object_id
             AND o.id = %s
            WHERE r.id = %s
              AND r.snapshot_id = %s
        """
        with database_failures():
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        row = await (await cursor.execute(
                            statement,
                            (
                                payload.document_version_id,
                                payload.parse_generation_id,
                                payload.source_sha256,
                                payload.artifact_object_id,
                                payload.run_id,
                                payload.snapshot_id,
                            ),
                        )).fetchone()
        if row is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        if row["security_revoked_at"] is not None:
            raise ApiError(ErrorCode.SOURCE_REVOKED)
        if row.get("purge_plan_id") is not None:
            raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
        if row["cancel_requested_at"] is not None or row["run_status"] in {
            "cancelling",
            "cancelled",
            "completed",
            "refused",
            "failed",
        }:
            raise ApiError(ErrorCode.TERMINAL_CONFLICT)
        if (
            row["parse_status"] != "ready"
            or row["quality_report"].get("status") != "passed"
            or row["artifact_kind"] != "parse_artifact"
            or row["artifact_state"] != "attached"
            or row["media_type"] != "application/json"
        ):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        size = int(row["artifact_size_bytes"])
        if size < 1 or size > MAX_ARTIFACT_BYTES:
            raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": MAX_ARTIFACT_BYTES})
        page_count = row["physical_page_count"]
        if not isinstance(page_count, int) or page_count < 1 or page_count > 500:
            raise ApiError(ErrorCode.GENERATION_INVALID)
        artifact = _ArtifactRef(
            run_id=row["run_id"],
            snapshot_id=row["snapshot_id"],
            parse_generation_id=row["parse_generation_id"],
            document_version_id=row["document_version_id"],
            source_sha256=row["source_sha256"],
            artifact_object_id=row["artifact_object_id"],
            bucket=row["bucket"],
            object_key=row["object_key"],
            object_version_id=row["object_version_id"],
            media_type=row["media_type"],
            artifact_size_bytes=size,
            artifact_sha256=row["artifact_sha256"],
            physical_page_count=page_count,
        )
        self._validate_object_identity(artifact)
        return artifact

    def _validate_object_identity(self, artifact: _ArtifactRef) -> None:
        if artifact.bucket != self.settings.s3_bucket_artifacts:
            raise ApiError(ErrorCode.GENERATION_INVALID)
        match = _PARSE_ARTIFACT_KEY.fullmatch(artifact.object_key)
        if (
            match is None
            or match["version"] != str(artifact.document_version_id)
            or match["parse"] != str(artifact.parse_generation_id)
            or match["sha"] != artifact.artifact_sha256
        ):
            raise ApiError(ErrorCode.GENERATION_INVALID)

    async def _read_and_project(self, payload: SourceRegistryRequest, artifact: _ArtifactRef) -> SourceRegistryResponse:
        def read_and_project() -> SourceRegistryResponse:
            address: dict[str, Any] = {"Bucket": artifact.bucket, "Key": artifact.object_key}
            if artifact.object_version_id is not None:
                address["VersionId"] = artifact.object_version_id
            try:
                response = self.storage.get_object(**address)
                stream = response["Body"]
                primary_failure = False
                try:
                    if response.get("ContentLength") != artifact.artifact_size_bytes:
                        raise ApiError(ErrorCode.GENERATION_INVALID)
                    if response.get("ContentType") != "application/json":
                        raise ApiError(ErrorCode.GENERATION_INVALID)
                    if response.get("Metadata", {}).get("sha256") != artifact.artifact_sha256:
                        raise ApiError(ErrorCode.GENERATION_INVALID)
                    if artifact.object_version_id is not None and response.get("VersionId") != artifact.object_version_id:
                        raise ApiError(ErrorCode.GENERATION_INVALID)
                    body = stream.read(artifact.artifact_size_bytes + 1)
                except BaseException:
                    primary_failure = True
                    raise
                finally:
                    _close_body_preserving_primary(stream, primary_failure=primary_failure)
            except ApiError:
                raise
            except (ClientError, BotoCoreError, OSError, KeyError):
                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE) from None
            if len(body) != artifact.artifact_size_bytes or len(body) > MAX_ARTIFACT_BYTES:
                raise ApiError(ErrorCode.GENERATION_INVALID)
            if hashlib.sha256(body).hexdigest() != artifact.artifact_sha256:
                raise ApiError(ErrorCode.GENERATION_INVALID)
            return self._project(payload, artifact, body)

        return await _finish_blocking(read_and_project)

    def _project(self, payload: SourceRegistryRequest, artifact: _ArtifactRef, body: bytes) -> SourceRegistryResponse:
        try:
            root = json.loads(body)
            document = root["document"]
        except (KeyError, TypeError, json.JSONDecodeError, UnicodeDecodeError):
            raise ApiError(ErrorCode.GENERATION_INVALID) from None
        if (
            root.get("schema_version") != "p04.artifact.v1"
            or document.get("version_id") != str(payload.document_version_id)
            or document.get("parse_generation_id") != str(payload.parse_generation_id)
            or document.get("source_sha256") != payload.source_sha256
        ):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        pages = self._pages(document, artifact.physical_page_count)
        blocks = self._blocks(document, payload)
        owner_hashes = self._owner_hashes(document, payload)
        return SourceRegistryResponse(
            run_id=payload.run_id,
            snapshot_id=payload.snapshot_id,
            parse_generation_id=payload.parse_generation_id,
            document_version_id=payload.document_version_id,
            source_sha256=payload.source_sha256,
            artifact_object_id=payload.artifact_object_id,
            artifact_sha256=artifact.artifact_sha256,
            artifact_size_bytes=artifact.artifact_size_bytes,
            page_count=artifact.physical_page_count,
            pages=pages,
            blocks=blocks,
            owner_hashes=owner_hashes,
        )

    def _pages(self, document: dict[str, Any], page_count: int) -> tuple[RegistryPage, ...]:
        raw_pages = document.get("pages")
        if not isinstance(raw_pages, list):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        page_by_number: dict[int, dict[str, Any]] = {}
        for page in raw_pages:
            if not isinstance(page, dict):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            number = page.get("pdf_page")
            if not isinstance(number, int) or isinstance(number, bool):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            page_by_number[number] = page
        if set(page_by_number) != set(range(1, page_count + 1)):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        return tuple(
            RegistryPage(pdf_page=number, width=page_by_number[number]["width"], height=page_by_number[number]["height"])
            for number in range(1, page_count + 1)
        )

    def _blocks(self, document: dict[str, Any], payload: SourceRegistryRequest) -> tuple[RegistryBlock, ...]:
        raw_blocks = document.get("blocks")
        if not isinstance(raw_blocks, list):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        by_id: dict[str, dict[str, Any]] = {}
        for block in raw_blocks:
            if not isinstance(block, dict):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            block_id = block.get("block_id")
            if not isinstance(block_id, str) or block_id in by_id:
                raise ApiError(ErrorCode.GENERATION_INVALID)
            by_id[block_id] = block
        projected: list[RegistryBlock] = []
        total_chars = 0
        for block_id in payload.block_ids:
            block = by_id.get(block_id)
            if block is None:
                raise ApiError(ErrorCode.NOT_FOUND)
            text = block.get("text")
            pdf_page = block.get("pdf_page")
            if not isinstance(text, str) or not isinstance(pdf_page, int) or isinstance(pdf_page, bool):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            total_chars += len(text)
            if total_chars > MAX_BLOCK_TEXT_CHARS:
                raise ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_chars": MAX_BLOCK_TEXT_CHARS})
            projected.append(RegistryBlock(block_id=block_id, pdf_page=pdf_page, text=text))
        return tuple(projected)

    def _owner_hashes(self, document: dict[str, Any], payload: SourceRegistryRequest) -> tuple[RegistryOwnerHash, ...]:
        if not payload.owners:
            return ()
        raw_nodes = document.get("nodes")
        if not isinstance(raw_nodes, list):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        nodes: dict[str, dict[str, Any]] = {}
        for node in raw_nodes:
            if not isinstance(node, dict):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            node_id = node.get("id")
            if not isinstance(node_id, str) or node_id in nodes:
                raise ApiError(ErrorCode.GENERATION_INVALID)
            nodes[node_id] = node
        projected: list[RegistryOwnerHash] = []
        for owner in payload.owners:
            node = nodes.get(str(owner.node_id))
            if node is None:
                raise ApiError(ErrorCode.NOT_FOUND)
            if owner.owner_kind == "node_body":
                text = node.get("own_body")
                char_map = node.get("char_map")
            else:
                text, char_map = self._table_cell_owner(node, owner.text_owner_id)
            if not isinstance(text, str) or not isinstance(char_map, list) or not all(
                isinstance(segment, dict) for segment in char_map
            ):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            try:
                digest = canonical_owner_hash(owner, text, char_map)
            except (TypeError, ValueError):
                raise ApiError(ErrorCode.GENERATION_INVALID) from None
            projected.append(
                RegistryOwnerHash(
                    node_id=owner.node_id,
                    owner_kind=owner.owner_kind,
                    text_owner_id=owner.text_owner_id,
                    sha256=digest,
                )
            )
        return tuple(projected)

    @staticmethod
    def _table_cell_owner(node: dict[str, Any], cell_id: str) -> tuple[Any, Any]:
        table = node.get("table")
        if not isinstance(table, dict):
            raise ApiError(ErrorCode.NOT_FOUND)
        cells = table.get("cells")
        if not isinstance(cells, list):
            raise ApiError(ErrorCode.GENERATION_INVALID)
        matched: dict[str, Any] | None = None
        for cell in cells:
            if not isinstance(cell, dict):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            current = cell.get("id")
            if not isinstance(current, str):
                raise ApiError(ErrorCode.GENERATION_INVALID)
            if current == cell_id:
                if matched is not None:
                    raise ApiError(ErrorCode.GENERATION_INVALID)
                matched = cell
        if matched is None:
            raise ApiError(ErrorCode.NOT_FOUND)
        return matched.get("text"), matched.get("char_map")
