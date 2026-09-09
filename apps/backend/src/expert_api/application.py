"""Public API composition; startup validates source identity without loading models."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
from typing import cast

from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.health import PeerHealth
from expert_clients.http import ServiceClient
from expert_clients.runtime_identity import load_runtime_identity
from expert_clients.settings import ConfigurationError, Settings
from expert_observability.health import install_health
from expert_observability.tracing import configure_tracing, inject_trace_headers
from expert_observability.web import install_http_boundary

from .auth import AuthService
from .api.debug_capture import internal_router as debug_capture_internal_router
from .debug_capture import CaptureStorage, DebugCaptureService
from .debug_cleanup import DebugCleanupService, DebugCleanupStorage
from .document_tree import DocumentTreeService
from .errors import install_api_errors
from .job_events import JobEventService
from .jobs import JobService
from .library import LibraryService
from .openapi import public_openapi
from .purge import PurgeService
from .purge_cleanup import PurgeCleanupService, PurgeCleanupStorage
from .routes import install_public_routes
from .run_events import RunEventService
from .runs import RunService
from .source_registry import SourceRegistryService, SourceRegistryStorage, router as source_registry_router
from .source_viewer import SourceStorage, SourceViewerService
from .upload_cleanup import CleanupStorage, UploadCleanupService
from .uploads import UploadService
from .version_commands import VersionCommandService


def create_app(settings: Settings, *, dependencies: Dependencies | None = None) -> FastAPI:
    settings.validate_service()
    if settings.service_name != "backend":
        raise ConfigurationError("backend service required")
    dependencies = dependencies or Dependencies(settings)
    outbox_health = PeerHealth("outbox-publisher", str(settings.outbox_publisher_url), component="outbox_delivery")
    runtime_health = PeerHealth("agent-runtime", str(settings.agent_runtime_url), component="answer_runtime")
    runtime_client = ServiceClient(str(settings.agent_runtime_url), settings.require_secret("agent_runtime_token"),
                                   max_connections=4, pool_timeout=1, max_response_bytes=65536,
                                   trace_headers=inject_trace_headers)

    async def readiness():
        checks, outbox, runtime = await asyncio.gather(dependencies.check(), outbox_health.check(), runtime_health.check())
        cleanup = getattr(app.state, "upload_cleanup", None)
        return [*checks, outbox, runtime,
                cleanup.health() if cleanup else DependencyHealth("upload_cleanup", False)]

    async def system_checks():
        checks = await readiness()
        debug_cleanup = getattr(app.state, "debug_cleanup", None)
        purge_cleanup = getattr(app.state, "purge_cleanup", None)
        # Retention failure is visible to operations, but optional debug storage
        # is not an admission/readiness dependency for answering questions.
        return [*checks, debug_cleanup.health() if debug_cleanup else DependencyHealth("debug_cleanup", False),
                purge_cleanup.health() if purge_cleanup else DependencyHealth("purge_cleanup", False)]

    async def shutdown(cleanup, debug_cleanup, purge_cleanup, capture, tracing):
        try:
            try:
                cleanup_outcomes = await asyncio.gather(*(service.close() for service in (cleanup, debug_cleanup, purge_cleanup)
                                                          if service is not None), return_exceptions=True)
                for outcome in cleanup_outcomes:
                    if isinstance(outcome, BaseException):
                        raise outcome
            finally:
                if capture is not None:
                    await capture.close()
        finally:
            try:
                # Every independent HTTP transport is closed even if one fails.
                outcomes = await asyncio.gather(runtime_client.aclose(), outbox_health.aclose(),
                                                runtime_health.aclose(), return_exceptions=True)
                for outcome in outcomes:
                    if isinstance(outcome, BaseException):
                        raise outcome
            finally:
                try:
                    await dependencies.close()
                finally:
                    if tracing is not None:
                        await asyncio.to_thread(tracing.shutdown)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.settings, application.state.dependencies = settings, dependencies
        application.state.system_checks = system_checks
        cleanup = None
        debug_cleanup = None
        purge_cleanup = None
        capture = None
        tracing = None
        try:
            tracing = configure_tracing(settings)
            identity = load_runtime_identity(settings.runtime_manifest_path, root=Path.cwd())
            if any(getattr(settings, name, None) != value for name, value in identity.semantic_limits.items()):
                raise ConfigurationError("Runtime semantic limits differ from the prepared identity")
            await dependencies.open()
            pool = dependencies.pool
            if pool is None or dependencies.storage is None or dependencies.redis is None:
                raise ConfigurationError("Backend transports are not initialized")
            application.state.auth = AuthService(settings, pool)
            application.state.uploads = UploadService(settings, pool, dependencies.storage)
            application.state.jobs = JobService(settings, pool)
            application.state.job_events = JobEventService(settings, pool, dependencies.redis)
            application.state.library = LibraryService(pool)
            application.state.document_tree = DocumentTreeService(pool)
            application.state.version_commands = VersionCommandService(pool)
            application.state.purge = PurgeService(settings, pool)
            application.state.source_registry = SourceRegistryService(settings, pool,
                cast(SourceRegistryStorage, dependencies.storage))
            application.state.source_viewer = SourceViewerService(settings, pool,
                cast(SourceStorage, dependencies.storage))
            application.state.runs = RunService(settings, pool, runtime_client=runtime_client,
                configuration_fingerprint=identity.configuration_fingerprint)
            application.state.run_events = RunEventService(settings, pool, dependencies.redis)
            capture = DebugCaptureService(settings, pool, cast(CaptureStorage, dependencies.storage))
            application.state.debug_capture = capture
            debug_cleanup = DebugCleanupService(settings, pool, cast(DebugCleanupStorage, dependencies.storage))
            application.state.debug_cleanup = debug_cleanup
            purge_cleanup = PurgeCleanupService(settings, pool, cast(PurgeCleanupStorage, dependencies.storage))
            application.state.purge_cleanup = purge_cleanup
            cleanup = UploadCleanupService(settings, cast(AsyncConnectionPool, pool),
                                            cast(CleanupStorage, dependencies.storage))
            application.state.upload_cleanup = cleanup
            cleanup.start()
            debug_cleanup.start()
            purge_cleanup.start()
            yield
        finally:
            task = asyncio.create_task(shutdown(cleanup, debug_cleanup, purge_cleanup, capture, tracing))
            interrupted = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    interrupted = True
            if interrupted:
                if not task.cancelled():
                    task.exception()
                raise asyncio.CancelledError
            await task

    app = FastAPI(title="Digital Expert backend", version="1.0.0", lifespan=lifespan,
                  docs_url="/docs" if settings.app_env == "local" else None, redoc_url=None)
    install_http_boundary(app, internal_token=None)
    install_api_errors(app)
    install_health(app, settings.service_name, readiness)
    install_public_routes(app)
    app.include_router(source_registry_router, include_in_schema=False)
    app.include_router(debug_capture_internal_router, include_in_schema=False)
    # FastAPI invalidates an assigned schema when its route-version cache has
    # not been populated. Own the public generator so live docs cannot regain
    # raw validation schemas or lose authentication/error declarations.
    app.openapi = lambda: public_openapi(app)  # type: ignore[method-assign]
    return app
