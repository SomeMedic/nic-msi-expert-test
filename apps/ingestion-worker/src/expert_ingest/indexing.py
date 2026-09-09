"""Persist complete, real embedding batches; publication belongs to the job finalizer."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Literal, Protocol
from uuid import UUID, NAMESPACE_URL, uuid5

from psycopg import Error as PostgresError, OperationalError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from expert_clients.http import DependencyError, ServiceClient
from expert_contracts.inference import DocumentEmbeddingItem, DocumentEmbeddingRequest, DocumentEmbeddingResponse
from expert_contracts.ml_profile import RuntimeProfile
from expert_observability.tracing import safe_span
from expert_ingest.parsing.artifact_store import finish_storage_call
from expert_ingest.parsing.chunking import ChunkingConfig, ChunkProjection, project_chunks
from expert_ingest.parsing.service import ParsedGeneration
from expert_ingest.parsing.tokenizer import TokenizerBudget
from expert_ingest.transport import JobRejected, LEASE_ERRORS
from expert_ingest.worker import Execution, PipelineFailure


class Embeddings(Protocol):
    async def profile(self, request_id: UUID) -> RuntimeProfile: ...
    async def documents(self, request: DocumentEmbeddingRequest, *, profile: RuntimeProfile) -> DocumentEmbeddingResponse: ...


class RemoteEmbeddings:
    def __init__(self, client: ServiceClient, *, timeout_seconds: float = 120):
        if not 0 < timeout_seconds <= 300:
            raise ValueError("Invalid embedding request timeout")
        self.client, self.timeout_seconds = client, timeout_seconds

    async def profile(self, request_id: UUID) -> RuntimeProfile:
        return await self.client.get("/v1/profile", RuntimeProfile, request_id=request_id,
                                     timeout_seconds=min(10, self.timeout_seconds))

    async def documents(self, request: DocumentEmbeddingRequest, *, profile: RuntimeProfile) -> DocumentEmbeddingResponse:
        return await self.client.post("/v1/embeddings/documents", request, DocumentEmbeddingResponse,
            request_id=request.request_id, timeout_seconds=self.timeout_seconds,
            expected_response_headers={"X-Model-Fingerprint": profile.embedding_recipe.model_fingerprint,
                                       "X-Runtime-Fingerprint": profile.embedding_recipe.runtime_fingerprint})


@dataclass(frozen=True)
class IndexingOptions:
    batch_items: int = 16
    batch_tokens: int = 8192
    batch_bytes: int = 7 * 1024**2

    def __post_init__(self) -> None:
        if (not 1 <= self.batch_items <= 16 or not 512 <= self.batch_tokens <= 8192
                or not 65536 <= self.batch_bytes <= 7 * 1024**2):
            raise ValueError("Invalid index batch limits")


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False, separators=(",", ":")).encode()).hexdigest()


def _batches(projection: ChunkProjection, index_id: UUID, kind: Literal["chunks", "descriptors"],
             options: IndexingOptions):
    batch: list[dict[str, Any]] = []
    tokens, size = 0, 2
    drafts = projection.chunks if kind == "chunks" else projection.descriptors
    for draft in drafts:
        item_id = (uuid5(index_id, f"chunk:{draft.node_id}:{getattr(draft, 'chunk_index', 0)}")
                   if kind == "chunks" else draft.node_id)
        item = {"id": str(item_id), "draft": draft.model_dump(mode="json"), "input_tokens": draft.input_tokens}
        # Leave bounded space for 1536 full JSON floats and metadata per item.
        item_size = len(json.dumps(item, ensure_ascii=False, allow_nan=False).encode()) + 65536
        if item_size > options.batch_bytes or draft.input_tokens > options.batch_tokens:
            raise PipelineFailure("GENERATION_INVALID")
        if batch and (len(batch) == options.batch_items or tokens + draft.input_tokens > options.batch_tokens
                      or size + item_size > options.batch_bytes):
            yield batch
            batch, tokens, size = [], 0, 2
        batch.append(item)
        tokens += draft.input_tokens
        size += item_size
    if batch:
        yield batch


class IndexingService:
    def __init__(self, embeddings: Embeddings, tokenizer: TokenizerBudget, *,
                 chunking: ChunkingConfig | None = None, options: IndexingOptions | None = None):
        self.embeddings, self.tokenizer = embeddings, tokenizer
        self.chunking, self.options = chunking or ChunkingConfig(), options or IndexingOptions()

    async def _query(self, execution: Execution, query: str, parameters: tuple[Any, ...]) -> dict[str, Any] | None:
        async with execution.transaction() as connection:
            try:
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(query, parameters)
                    return await cursor.fetchone()
            except PostgresError as error:
                code = error.diag.message_primary
                if code in LEASE_ERRORS or code in {"SOURCE_REVOKED", "VERSION_CONFLICT"}:
                    raise JobRejected(code) from None
                temporary = (isinstance(error, OperationalError) and error.sqlstate is None
                             or error.sqlstate == "55P03"
                             or error.sqlstate and error.sqlstate[:2] in {"08", "53", "57", "40"})
                raise PipelineFailure("DEPENDENCY_UNAVAILABLE" if temporary else "GENERATION_INVALID",
                                      retryable=bool(temporary)) from None

    async def _call(self, execution: Execution, function: str, *args: Any) -> dict[str, Any]:
        params = (execution.job_id, execution.owner, execution.epoch, *args)
        row = await self._query(execution, f"SELECT * FROM {function}({','.join('%s' for _ in params)})", params)
        if row is None:
            raise PipelineFailure("GENERATION_INVALID")
        return row

    async def _committed(self, execution: Execution, index_id: UUID, kind: str, batch_id: UUID,
                         items: list[dict[str, Any]], metadata: dict[str, Any]) -> bool:
        # PostgreSQL's jsonb serialization defines this digest. Comparing it in
        # PostgreSQL avoids pretending Python JSON whitespace matches jsonb::text.
        row = await self._query(execution,
            "SELECT request_hash=encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') AS matches, "
            "inserted_count FROM knowledge.index_write_batches "
            "WHERE index_generation_id=%s AND batch_kind=%s AND batch_id=%s",
            (Jsonb({"items": items, "metadata": metadata}), index_id, kind, batch_id))
        if row is None:
            return False
        if not row["matches"] or row["inserted_count"] != len(items):
            raise PipelineFailure("GENERATION_INVALID")
        return True

    async def prepare(self, execution: Execution, parsed: ParsedGeneration) -> UUID:
        """Return a ready index UUID only after both projections have real vectors."""
        index_id: UUID | None = None
        try:
            await execution.guard()
            if (parsed.quality_report.status != "passed"
                    or parsed.canonical_document.parse_generation_id != parsed.parse_generation_id):
                raise PipelineFailure("EXTRACTION_QUALITY_FAILED")
            profile = await self.embeddings.profile(execution.job_id)
            recipe = profile.embedding_recipe
            if (recipe.model != self.tokenizer.model_id or recipe.revision != self.tokenizer.revision
                    or recipe.tokenizer_fingerprint != self.tokenizer.fingerprint
                    or profile.capabilities.embedding.model != recipe.model
                    or profile.capabilities.embedding.revision != recipe.revision
                    or profile.capabilities.embedding.dimension != recipe.dimension
                    or profile.capabilities.embedding.device != recipe.device
                    or profile.capabilities.embedding.max_input_tokens != 512
                    or profile.capabilities.embedding.max_batch_tokens < 512):
                raise PipelineFailure("MODEL_UNAVAILABLE")
            limits = IndexingOptions(
                batch_items=min(self.options.batch_items, profile.capabilities.embedding.max_batch_items),
                batch_tokens=min(self.options.batch_tokens, profile.capabilities.embedding.max_batch_tokens),
                batch_bytes=self.options.batch_bytes,
            )
            await execution.advance("chunking", "stage.started")
            projection = await finish_storage_call(project_chunks, parsed.canonical_document,
                                                    self.tokenizer, self.chunking)
            if projection.status != "passed" or not projection.chunks or not projection.descriptors:
                raise PipelineFailure(projection.error_code or "GENERATION_INVALID")
            await execution.guard()
            identity = _digest([projection.manifest.model_dump(mode="json"), recipe.model_dump(mode="json")])
            operation = uuid5(NAMESPACE_URL, f"expert:index:{execution.job_id}:{execution.epoch}:{identity}")
            row = await self._call(execution, "knowledge.begin_index", operation, parsed.parse_generation_id,
                Jsonb(projection.manifest.model_dump(mode="json")), Jsonb(recipe.model_dump(mode="json")),
                len(projection.chunks), len(projection.descriptors))
            index_id = row["id"]
            if (row["parse_generation_id"] != parsed.parse_generation_id
                    or row["document_version_id"] != parsed.canonical_document.version_id):
                raise PipelineFailure("GENERATION_INVALID")
            if row["status"] == "ready":
                return index_id
            if row["status"] != "staging":
                raise PipelineFailure("GENERATION_INVALID")
            metadata = {"model": recipe.model, "revision": recipe.revision, "dimension": 1536, "normalized": True}
            await execution.advance("embedding", "stage.started", total=len(projection.chunks) + len(projection.descriptors))
            processed = 0
            for kind in ("chunks", "descriptors"):
                for items in _batches(projection, index_id, kind, limits):
                    batch_id = uuid5(index_id, f"{kind}:{_digest(items)}")
                    if not await self._committed(execution, index_id, kind, batch_id, items, metadata):
                        request = DocumentEmbeddingRequest(request_id=batch_id, items=[
                            DocumentEmbeddingItem(id=item["id"], text=item["draft"]["embedding_text" if kind == "chunks" else "text"])
                            for item in items
                        ])
                        with safe_span("ingestion.embed", stage="embedding", model=recipe.model,
                                       model_revision=recipe.revision, count=len(request.items),
                                       input_tokens=sum(item["input_tokens"] for item in items)):
                            response = await self.embeddings.documents(request, profile=profile)
                        try:
                            response.validate_binding(request)
                        except ValueError:
                            raise PipelineFailure("GENERATION_INVALID") from None
                        if {key: getattr(response, key) for key in metadata} != metadata:
                            raise PipelineFailure("GENERATION_INVALID")
                        by_id = {item.id: item for item in response.items}
                        if any(by_id[item["id"]].input_tokens != item["input_tokens"] for item in items):
                            raise PipelineFailure("GENERATION_INVALID")
                        complete = [{**item, "vector": by_id[item["id"]].vector} for item in items]
                        if len(json.dumps(complete, allow_nan=False, ensure_ascii=False).encode()) > limits.batch_bytes:
                            raise PipelineFailure("GENERATION_INVALID")
                        # The response arrives before opening this transaction;
                        # cancellation/expiry is checked again by its write fence.
                        await self._call(execution, f"knowledge.write_index_{kind}", index_id, batch_id,
                                         Jsonb(complete), Jsonb(metadata))
                    processed += len(items)
                    await execution.advance("embedding", "stage.progress", processed=processed,
                                            total=len(projection.chunks) + len(projection.descriptors))
            await execution.advance("indexing", "stage.started")
            ready = await self._call(execution, "knowledge.finalize_index", index_id)
            if ready["status"] != "ready":
                raise PipelineFailure("GENERATION_INVALID")
            await execution.advance("ready_to_publish", "stage.started", processed=processed, total=processed)
            await execution.advance("ready_to_publish", "stage.completed", processed=processed, total=processed)
            return index_id
        except (DependencyError, PipelineFailure) as error:
            failure = (PipelineFailure(
                ("MODEL_TIMEOUT" if error.code == "DEPENDENCY_TIMEOUT" else "MODEL_UNAVAILABLE")
                if error.retryable else "GENERATION_INVALID", retryable=error.retryable)
                if isinstance(error, DependencyError) else error)
            if index_id is not None and not failure.retryable:
                await self._call(execution, "knowledge.fail_index", index_id, failure.code)
            raise failure from None
