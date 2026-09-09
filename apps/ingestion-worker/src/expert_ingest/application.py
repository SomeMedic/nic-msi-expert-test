"""Health server with a real supervised ingestion pipeline in the same process."""
from contextlib import asynccontextmanager
import asyncio
from typing import Callable, Protocol

from fastapi import FastAPI

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.settings import Settings
from expert_observability.health import install_health
from expert_observability.tracing import TracingHandle, configure_tracing
from expert_observability.web import install_http_boundary

from expert_ingest.runtime import IngestionRuntime


class Runtime(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def health(self) -> list[DependencyHealth]: ...


def create_app(settings: Settings, *, dependencies: Dependencies | None = None,
               runtime_factory: Callable[[Settings, Dependencies], Runtime] = IngestionRuntime) -> FastAPI:
    settings.validate_service()
    if settings.service_name != "ingestion-worker":
        raise ValueError("ingestion-worker role required")
    dependencies = dependencies or Dependencies(settings)
    runtime = runtime_factory(settings, dependencies)

    async def shutdown(tracing: TracingHandle):
        try:
            await runtime.close()
        finally:
            try:
                await dependencies.close()
            finally:
                await asyncio.to_thread(tracing.shutdown)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.settings = settings
        application.state.dependencies = dependencies
        application.state.ingestion_runtime = runtime
        tracing = configure_tracing(settings)
        application.state.tracing = tracing
        try:
            await dependencies.open()
            await runtime.start()
            yield
        finally:
            drain = asyncio.create_task(shutdown(tracing))
            try:
                await asyncio.shield(drain)
            except asyncio.CancelledError:
                while not drain.done():
                    try:
                        await asyncio.shield(drain)
                    except asyncio.CancelledError:
                        continue
                if not drain.cancelled():
                    drain.exception()
                raise

    async def readiness():
        return [*(await dependencies.check()), *(await runtime.health())]

    application = FastAPI(title="Digital Expert ingestion-worker", version="1.0.0", lifespan=lifespan,
                          docs_url=None, redoc_url=None, openapi_url=None)
    install_http_boundary(application, internal_token=None)
    install_health(application, settings.service_name, readiness)
    return application
