"""Outbox process: health server plus supervised publisher/reconciler tasks."""
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI

from expert_clients.dependencies import Dependencies
from expert_clients.settings import load_settings
from expert_observability.health import install_health
from expert_observability.logging import configure_logging
from expert_observability.tracing import configure_tracing
from expert_observability.web import install_http_boundary

from .publisher import OutboxPublisher, RedisOutboxSink
from .store import PostgresOutboxStore

settings = load_settings("outbox-publisher")
configure_logging(settings.service_name, settings.log_level)
dependencies = Dependencies(settings)
publisher = OutboxPublisher(settings, PostgresOutboxStore(dependencies), RedisOutboxSink(dependencies))


@asynccontextmanager
async def lifespan(application: FastAPI):
    application.state.settings = settings
    application.state.dependencies = dependencies
    application.state.publisher = publisher
    tracing = configure_tracing(settings)
    application.state.tracing = tracing

    async def shutdown():
        try:
            await publisher.close()
        finally:
            try:
                await dependencies.close()
            finally:
                await asyncio.to_thread(tracing.shutdown)
    try:
        await dependencies.open()
        publisher.start()
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


app = FastAPI(
    title="Digital Expert outbox-publisher", version="1.0.0", lifespan=lifespan,
    docs_url="/docs" if settings.app_env == "local" else None, redoc_url=None,
)
install_http_boundary(app, internal_token=None)

async def check_readiness():
    return [*(await dependencies.check()), publisher.health()]


install_health(app, settings.service_name, check_readiness)
