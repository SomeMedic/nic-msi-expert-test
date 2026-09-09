"""Private HTTP API and supervised durable graph process share a lifecycle."""
import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.runtime_identity import RuntimeIdentityError
from expert_clients.settings import ConfigurationError, Settings
from expert_observability.health import install_health
from expert_observability.tracing import TracingHandle, configure_tracing
from expert_observability.web import install_http_boundary

from .api.runs import router
from .runtime import AgentRuntime


def create_app(settings: Settings, *, dependencies: Dependencies | None = None,
               runtime_factory: Callable[[Settings, Dependencies], AgentRuntime] = AgentRuntime) -> FastAPI:
    settings.validate_service()
    if settings.service_name != "agent-runtime":
        raise ConfigurationError("agent-runtime service required")
    dependencies = dependencies or Dependencies(settings)
    runtime = runtime_factory(settings, dependencies)
    active = False
    tracing: TracingHandle | None = None

    async def shutdown():
        failed, interrupted = False, False
        operations = [runtime.close, dependencies.close]
        if tracing is not None:
            handle = tracing
            operations.append(lambda: asyncio.to_thread(handle.shutdown))
        for operation in operations:
            try:
                await operation()
            except asyncio.CancelledError:
                interrupted = True
            except Exception:
                failed = True
        if interrupted:
            raise asyncio.CancelledError
        if failed:
            raise ConfigurationError("Agent application shutdown failed") from None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal active, tracing
        app.state.settings, app.state.dependencies = settings, dependencies
        app.state.agent_runtime = runtime
        app.state.run_manager = None
        try:
            tracing = configure_tracing(settings)
            await dependencies.open()
            try:
                async with asyncio.timeout(5):
                    checks = await dependencies.check()
                if (not {"database", "event_transport"} <= {item.name for item in checks}
                        or not all(item.ready for item in checks)):
                    raise ConfigurationError("Agent transports are unavailable")
            except Exception:
                raise ConfigurationError("Agent transports are unavailable") from None
            await runtime.start()
            app.state.run_manager = runtime.manager
            active = True
            yield
        except (ConfigurationError, RuntimeIdentityError):
            raise
        except Exception:
            raise ConfigurationError("Agent application startup or lifecycle failed") from None
        finally:
            active = False
            app.state.run_manager = None
            task = asyncio.create_task(shutdown())
            interrupted = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    interrupted = True
                except Exception:
                    break
            if interrupted:
                if not task.cancelled():
                    task.exception()
                raise asyncio.CancelledError
            await task

    async def readiness():
        if not active:
            return [DependencyHealth("application", False)]
        try:
            async with asyncio.timeout(5):
                async with asyncio.TaskGroup() as probes:
                    transport_task = probes.create_task(dependencies.check())
                    models_task = probes.create_task(runtime.health())
                transport, models = transport_task.result(), models_task.result()
            if (active and {"database", "event_transport", "generative_model", "retrieval_models", "run_executor"}
                    <= {item.name for item in (*transport, *models)}):
                return [*transport, *models]
        except Exception:
            pass
        return [DependencyHealth("application", False)]

    app = FastAPI(title="Digital Expert agent-runtime", version="1.0.0", lifespan=lifespan,
                  docs_url=None, redoc_url=None, openapi_url=None)
    install_http_boundary(app, internal_token=settings.require_secret("agent_runtime_token").get_secret_value().encode())
    install_health(app, settings.service_name, readiness)
    app.include_router(router, include_in_schema=False)
    return app
