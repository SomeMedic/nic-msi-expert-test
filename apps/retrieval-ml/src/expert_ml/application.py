"""Composition factory; production always defaults to verified local model inference."""
from contextlib import asynccontextmanager
import asyncio
from typing import Any, Callable

from fastapi import FastAPI

from expert_clients.dependencies import Dependencies
from expert_clients.settings import Settings
from expert_observability.health import install_health
from expert_observability.tracing import configure_tracing
from expert_observability.web import install_http_boundary

from .cuda_models import configured_models
from .api import BodyLimit, install_model_errors, router
from .runtime import ModelRuntime


def create_app(settings: Settings, *, dependencies: Dependencies | None = None,
               model_factory: Callable[[Settings], Any] = configured_models) -> FastAPI:
    settings.validate_service()
    if settings.service_name != "retrieval-ml":
        raise ValueError("retrieval-ml role required")
    dependencies = dependencies or Dependencies(settings)
    model_runtime = ModelRuntime(settings, factory=model_factory)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.settings = settings
        application.state.dependencies = dependencies
        application.state.model_runtime = model_runtime
        tracing = configure_tracing(settings)
        application.state.tracing = tracing

        async def shutdown():
            try:
                await model_runtime.close()
            finally:
                try:
                    await dependencies.close()
                finally:
                    await asyncio.to_thread(tracing.shutdown)
        try:
            await dependencies.open()
            await model_runtime.start()
            yield
        finally:
            drain = asyncio.create_task(shutdown())
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
        dependencies.models_ready = model_runtime.ready
        return await dependencies.check()

    application = FastAPI(title="Digital Expert retrieval-ml", version="1.0.0", lifespan=lifespan,
                          docs_url=None, redoc_url=None, openapi_url=None)
    application.add_middleware(BodyLimit)
    install_http_boundary(application, internal_token=settings.require_secret("retrieval_ml_token").get_secret_value().encode())
    install_model_errors(application)
    install_health(application, settings.service_name, readiness)
    application.include_router(router)
    return application
