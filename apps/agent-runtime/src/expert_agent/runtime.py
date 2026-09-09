"""Lightweight application composition; models remain in their local services."""
from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import httpx

from expert_clients.dependencies import Dependencies, DependencyHealth
from expert_clients.http import ServiceClient
from expert_clients.runtime_identity import load_runtime_identity
from expert_clients.settings import ConfigurationError, Settings
from expert_observability.tracing import inject_trace_headers

from .graph import AnswerGraph
from .debug_capture import HttpDebugCaptureSink
from .llm import LlmGateway, ServingProfile
from .retrieval.ml import RemoteModels
from .retrieval.repository import RetrievalRepository
from .retrieval.service import RetrievalService
from .retrieval.sources import SourceRegistryLoader
from .retrieval.types import RetrievalConfig
from .run_manager import RunManager


class AgentRuntime:
    def __init__(self, settings: Settings, dependencies: Dependencies):
        self.settings, self.dependencies = settings, dependencies
        self.gateway: LlmGateway | None = None
        self.models: RemoteModels | None = None
        self.manager: RunManager | None = None
        self.clients: list[ServiceClient] = []
        self.llm_health: httpx.AsyncClient | None = None
        self._started = False
        self._initialized = False
        self._closing = False
        self._close_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._initialized or self._closing:
            raise ConfigurationError("Agent runtime already initialized")
        self._initialized = True
        settings, pool = self.settings, self.dependencies.pool
        if pool is None:
            raise ConfigurationError("Agent database transport is not initialized")
        identity = load_runtime_identity(settings.runtime_manifest_path, root=Path.cwd())
        if any(getattr(settings, name, None) != value for name, value in identity.semantic_limits.items()):
            raise ConfigurationError("Runtime semantic limits differ from the prepared identity")
        profile = ServingProfile.model_validate(identity.serving_profile)
        if (settings.llm_model, settings.llm_revision) != (profile.model, profile.revision):
            raise ConfigurationError("LLM configuration differs from the prepared identity")
        config = RetrievalConfig(**identity.retrieval_config)
        model_client = ServiceClient(str(settings.retrieval_ml_url), settings.require_secret("retrieval_ml_token"),
            max_connections=2, pool_timeout=1, trace_headers=inject_trace_headers)
        self.clients.append(model_client)
        source_client = ServiceClient(str(settings.backend_internal_url), settings.require_secret("agent_runtime_token"),
            max_connections=1, pool_timeout=1, trace_headers=inject_trace_headers)
        self.clients.append(source_client)
        self.models = RemoteModels(model_client, timeout_seconds=settings.ml_request_timeout_seconds)
        self.gateway = LlmGateway(str(settings.llm_base_url), settings.require_secret("llm_api_key"),
            profile=profile, models_lock=settings.models_lock_path,
            prompts_directory=Path("config/prompts/p07.v1"),
            capture_sink=HttpDebugCaptureSink(source_client,
                sensitive_values=(settings.require_secret("agent_runtime_token"),)))
        await self.gateway.open(deadline=asyncio.get_running_loop().time() + 15)
        await self.models.profile(uuid4(), cancel=asyncio.Event(), deadline=asyncio.get_running_loop().time() + 10)
        self.llm_health = httpx.AsyncClient(base_url=str(settings.llm_base_url).rstrip("/").removesuffix("/v1"),
            headers={"Authorization": "Bearer " + settings.require_secret("llm_api_key").get_secret_value()},
            trust_env=False, follow_redirects=False, timeout=httpx.Timeout(2, connect=1, pool=1),
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1))
        retrieval = RetrievalService(RetrievalRepository(pool, config), self.models, SourceRegistryLoader(source_client), config)
        graph = AnswerGraph(pool, retrieval, self.gateway, identity.configuration_fingerprint,
                            context_token_budget=identity.context_token_budget)
        self.manager = RunManager(settings, pool, execute=graph.execute,
                                  configuration_fingerprint=identity.configuration_fingerprint)
        await self.manager.open()
        self._started = True

    async def close(self) -> None:
        """Drain the executor before transports, even if the caller is cancelled."""
        self._started = False
        self._closing = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_resources())
        interrupted = False
        while not self._close_task.done():
            try:
                await asyncio.shield(self._close_task)
            except asyncio.CancelledError:
                interrupted = True
            except Exception:
                break
        if interrupted:
            if not self._close_task.cancelled():
                self._close_task.exception()
            raise asyncio.CancelledError
        await self._close_task

    async def _close_resources(self) -> None:
        failed, interrupted = False, False
        # The manager retains its global permit until active HTTP calls drain.
        # A failed close must not skip a later independent transport.
        for resource in (self.manager, self.gateway, *self.clients, self.llm_health):
            if resource is None:
                continue
            try:
                await resource.aclose()
            except asyncio.CancelledError:
                interrupted = True
            except Exception:
                failed = True
        self.models = None
        self.clients.clear()
        self.llm_health = None
        if interrupted:
            raise asyncio.CancelledError
        if failed:
            raise ConfigurationError("Agent runtime shutdown failed") from None

    async def health(self) -> list[DependencyHealth]:
        unavailable = [DependencyHealth(name, False) for name in
                       ("generative_model", "retrieval_models", "run_executor")]
        if not self._started or self._closing:
            return unavailable

        async def generation():
            if self.llm_health is None:
                return False
            try:
                async with asyncio.timeout(2):
                    async with self.llm_health.stream("GET", "/health") as response:
                        return response.status_code == 200
            except Exception:
                return False

        async def retrieval():
            if self.models is None:
                return False
            try:
                await self.models.profile(uuid4(), cancel=asyncio.Event(), deadline=asyncio.get_running_loop().time() + 2)
                return True
            except Exception:
                return False

        llm, models = await asyncio.gather(generation(), retrieval())
        if not self._started or self._closing:
            return unavailable
        return [DependencyHealth("generative_model", llm), DependencyHealth("retrieval_models", models),
                DependencyHealth("run_executor", bool(self._started and self.manager and self.manager.ready))]
