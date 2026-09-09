"""Compose the bounded parser and real embedding worker; own their shutdown order."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, cast
from uuid import UUID, uuid4

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.http import ServiceClient
from expert_clients.settings import ConfigurationError, Settings
from expert_observability.tracing import correlation_context, inject_trace_headers, safe_span

from expert_ingest.indexing import IndexingOptions, IndexingService, RemoteEmbeddings
from expert_ingest.parsing.artifact_cleanup import ParseArtifactCleanupService
from expert_ingest.parsing.artifact_store import ParseArtifactStore, finish_storage_call
from expert_ingest.parsing.runner import ParserFailure, ParserRunner, RunnerOptions
from expert_ingest.parsing.service import ParsingService
from expert_ingest.parsing.tokenizer import FridaTokenizerBudget
from expert_ingest.source import LocalSource, OriginalSource
from expert_ingest.transport import PostgresIngestionJobs, RedisIngestionStream
from expert_ingest.worker import Execution, IngestionWorker, PipelineFailure, WorkerOptions

logger = logging.getLogger(__name__)


class DocumentPipeline:
    def __init__(self, parsing: ParsingService, indexing: IndexingService):
        self.parsing, self.indexing = parsing, indexing

    async def __call__(self, execution: Execution, source: LocalSource) -> UUID:
        with correlation_context(job_id=getattr(execution, "job_id", None),
                                 execution_epoch=getattr(execution, "epoch", None),
                                 attempt=getattr(execution, "attempt", None),
                                 version_id=getattr(getattr(source, "reference", None), "version_id", None)):
            try:
                with safe_span("ingestion.parse", stage="parsing"):
                    parsed = await self.parsing.prepare(execution, source)
            except ParserFailure as error:
                raise PipelineFailure(error.code, retryable=error.code in {
                    "DEPENDENCY_UNAVAILABLE", "DEADLINE_EXCEEDED",
                }) from None
            await execution.guard()
            with safe_span("ingestion.index", stage="indexing"):
                return await self.indexing.prepare(execution, parsed)


class IngestionRuntime:
    def __init__(self, settings: Settings, dependencies: Dependencies):
        self.settings, self.dependencies = settings, dependencies
        self.client: ServiceClient | None = None
        self.embeddings: RemoteEmbeddings | None = None
        self.tokenizer: FridaTokenizerBudget | None = None
        self.worker: IngestionWorker | None = None
        self.cleanup: ParseArtifactCleanupService | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self.client is not None or self._task is not None:
            raise RuntimeError("Ingestion runtime already started")
        settings, dependencies = self.settings, self.dependencies
        if dependencies.pool is None or dependencies.redis is None or dependencies.storage is None:
            raise ConfigurationError("Ingestion transports are not initialized")
        if settings.embedding_model_path is None or settings.docling_artifacts_path is None:
            raise ConfigurationError("Ingestion requires embedding_model_path and docling_artifacts_path")
        if settings.ingestion_concurrency != 1:
            # The measured 6 GiB profile admits exactly one parser child.
            raise ConfigurationError("The parser resource profile requires ingestion_concurrency=1")
        try:
            runner = ParserRunner(RunnerOptions(
                # A venv interpreter is commonly a symlink. Resolving it launches
                # the base Python and loses pyvenv.cfg/package isolation.
                python=settings.ingestion_parser_python_path.absolute(),
                root=settings.ingestion_temp_root / "parser",
                assets_path=settings.docling_artifacts_path.resolve(strict=True),
                asset_lock_path=settings.ingestion_parser_assets_lock_path.resolve(strict=True),
                runtime_profile_path=settings.ingestion_parser_profile_path.resolve(strict=True),
                review_registry_path=settings.ingestion_region_reviews_path.resolve(strict=True),
                max_source_bytes=settings.upload_max_bytes, max_pages=settings.pdf_max_pages,
                enforce_sandbox=True,
            ))
            self.tokenizer = await finish_storage_call(FridaTokenizerBudget.from_local,
                settings.embedding_model_path, settings.models_lock_path)
        except (OSError, ValueError):
            raise ConfigurationError("Local parser or tokenizer preparation is invalid") from None
        if settings.embedding_revision != self.tokenizer.revision:
            raise ConfigurationError("Ingestion embedding_revision differs from local tokenizer")
        self.client = ServiceClient(str(settings.retrieval_ml_url),
            settings.require_secret("retrieval_ml_token"),
            connect_timeout=settings.http_connect_timeout_seconds,
            pool_timeout=settings.http_pool_timeout_seconds, trace_headers=inject_trace_headers)
        self.embeddings = RemoteEmbeddings(self.client, timeout_seconds=settings.ml_request_timeout_seconds)
        if not await self._model_ready():
            raise ConfigurationError("Ingestion embedding service is unavailable or incompatible")
        storage = cast(Any, dependencies.storage)
        parsing = ParsingService(runner, ParseArtifactStore(storage),
            parser_fingerprint=settings.ingestion_pipeline_fingerprint)
        indexing = IndexingService(self.embeddings, self.tokenizer, options=IndexingOptions(
            batch_items=settings.embedding_max_batch_items, batch_tokens=settings.embedding_max_batch_tokens))
        sources = OriginalSource(storage, settings.ingestion_temp_root / "originals",
            bucket=settings.s3_bucket_originals, max_bytes=settings.upload_max_bytes,
            timeout_seconds=settings.ingestion_download_timeout_seconds)
        stream = RedisIngestionStream(dependencies.redis, stream=settings.redis_stream_ingestion)
        await stream.ensure_group()
        self.worker = IngestionWorker(stream, PostgresIngestionJobs(dependencies.pool), sources,
            DocumentPipeline(parsing, indexing), options=WorkerOptions(
                concurrency=settings.ingestion_concurrency, lease_seconds=settings.ingestion_lease_seconds,
                heartbeat_seconds=settings.ingestion_heartbeat_seconds,
                job_timeout_seconds=settings.ingestion_job_timeout_seconds,
                reclaim_idle_seconds=settings.ingestion_reclaim_idle_seconds,
                reconcile_seconds=settings.ingestion_reconcile_seconds))
        self.cleanup = ParseArtifactCleanupService(settings, dependencies.pool, storage)
        self._stop = asyncio.Event()
        self.cleanup.start()
        self._task = asyncio.create_task(self.worker.run(self._stop), name="ingestion-worker")
        self._task.add_done_callback(self._completed)

    @staticmethod
    def _completed(task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            logger.error("dependency.failed", extra={"safe_fields": {"error_code": "DEPENDENCY_UNAVAILABLE"}})

    async def _model_ready(self) -> bool:
        if self.embeddings is None or self.tokenizer is None:
            return False
        try:
            profile = await self.embeddings.profile(uuid4())
            recipe = profile.embedding_recipe
            capability = profile.capabilities.embedding
            return (recipe.tokenizer_fingerprint == self.tokenizer.fingerprint
                    and recipe.revision == self.tokenizer.revision
                    and capability.model == recipe.model and capability.revision == recipe.revision
                    and capability.dimension == recipe.dimension and capability.device == recipe.device
                    and capability.max_input_tokens == recipe.max_input_tokens
                    and capability.max_batch_tokens >= 512 and capability.max_batch_items >= 1)
        except Exception:
            return False

    async def health(self) -> list[DependencyHealth]:
        running = self._task is not None and not self._task.done() and not self._stop.is_set()
        return [DependencyHealth("ingestion_pipeline", running),
                DependencyHealth("embedding_model", running and await self._model_ready()),
                self.cleanup.health() if self.cleanup else DependencyHealth("parse_artifact_cleanup", False)]

    async def close(self) -> None:
        self._stop.set()
        drain = asyncio.create_task(self._close())
        try:
            await asyncio.shield(drain)
        except asyncio.CancelledError:
            # Repeated cancellation cannot release clients beneath a parser/SDK task.
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    continue
            if not drain.cancelled():
                drain.exception()
            raise

    async def _close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        if self.cleanup is not None:
            await self.cleanup.close()
            self.cleanup = None
        if self.client is not None:
            await self.client.aclose()
            self.client = None
