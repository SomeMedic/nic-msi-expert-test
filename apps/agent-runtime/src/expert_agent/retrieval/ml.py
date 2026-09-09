"""Real authenticated ML calls with request IDs, recipe binding and cancellation."""
from __future__ import annotations

import asyncio
from collections.abc import MutableMapping
from dataclasses import replace
import json
import re
from typing import Awaitable, TypeVar
from uuid import UUID

import httpx
from pydantic import ValidationError

from expert_clients.http import ServiceClient
from expert_clients.http import DependencyError
from expert_contracts.errors import ErrorCode, ErrorEnvelope
from expert_contracts.inference import QueryEmbeddingRequest, QueryEmbeddingResponse, RerankCandidate, RerankRequest, RerankResponse
from expert_contracts.ml_profile import RuntimeProfile

from .types import FusedHit, PreparedQuery, RetrievalError

T = TypeVar("T")


class _RerankTokenLimitExceeded(RuntimeError):
    pass


def rerank_candidate_text(hit: FusedHit) -> str:
    if not hit.chunk.embedding_text.strip():
        raise RetrievalError("RETRIEVAL_CONTRACT_INVALID")
    return hit.chunk.embedding_text


async def bounded(operation: Awaitable[T], cancel: asyncio.Event, deadline: float) -> T:
    """Deadline uses asyncio loop monotonic time, shared with the run caller."""
    call = asyncio.ensure_future(operation)
    stopping = asyncio.create_task(cancel.wait())
    try:
        remaining = deadline - asyncio.get_running_loop().time()
        if cancel.is_set():
            raise RetrievalError("RUN_CANCELLED")
        if remaining <= 0:
            raise RetrievalError("RUN_DEADLINE_EXCEEDED")
        done, _ = await asyncio.wait((call, stopping), timeout=remaining, return_when=asyncio.FIRST_COMPLETED)
        if stopping in done:
            raise RetrievalError("RUN_CANCELLED")
        if call not in done:
            raise RetrievalError("RUN_DEADLINE_EXCEEDED")
        return await call
    finally:
        for task in (call, stopping):
            if not task.done():
                task.cancel()
        await asyncio.gather(call, stopping, return_exceptions=True)


class RemoteModels:
    def __init__(self, client: ServiceClient, *, timeout_seconds: float = 120):
        if not 0 < timeout_seconds <= 300:
            raise ValueError("Invalid ML timeout")
        self.client, self.timeout_seconds = client, timeout_seconds

    async def profile(self, request_id: UUID, *, cancel: asyncio.Event, deadline: float) -> RuntimeProfile:
        profile = await bounded(self.client.get("/v1/profile", RuntimeProfile, request_id=request_id,
                                                timeout_seconds=min(10, self.timeout_seconds)), cancel, deadline)
        embedding, reranker = profile.embedding_recipe, profile.reranker_recipe
        if (profile.capabilities.embedding.model != embedding.model
                or profile.capabilities.embedding.revision != embedding.revision
                or profile.capabilities.embedding.dimension != embedding.dimension
                or profile.capabilities.embedding.max_input_tokens != embedding.max_input_tokens
                or profile.capabilities.reranker.model != reranker.model
                or profile.capabilities.reranker.revision != reranker.revision
                or profile.capabilities.reranker.score_type != reranker.score_type):
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        return profile

    async def query(self, request_id: UUID, question: PreparedQuery, profile: RuntimeProfile,
                    *, cancel: asyncio.Event, deadline: float) -> QueryEmbeddingResponse:
        recipe = profile.embedding_recipe
        response = await bounded(self.client.post("/v1/embeddings/query",
            QueryEmbeddingRequest(request_id=request_id, text=question.search_question), QueryEmbeddingResponse,
            request_id=request_id, timeout_seconds=self.timeout_seconds,
            expected_response_headers={"X-Model-Fingerprint": recipe.model_fingerprint,
                                       "X-Runtime-Fingerprint": recipe.runtime_fingerprint}), cancel, deadline)
        if (response.model, response.revision, response.dimension, response.normalized) != (
                recipe.model, recipe.revision, recipe.dimension, True):
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        return response

    async def rerank(self, request_id: UUID, question: PreparedQuery, hits: tuple[FusedHit, ...],
                     profile: RuntimeProfile, top_k: int, *, cancel: asyncio.Event, deadline: float) -> tuple[FusedHit, ...]:
        if not hits:
            return ()
        # UUID aliases remain stable independently of completion order and batching.
        candidates = [RerankCandidate(id=str(hit.chunk.id), text=rerank_candidate_text(hit)) for hit in hits]
        recipe = profile.reranker_recipe
        capability = profile.capabilities.reranker
        # Try the runtime's item capacity first and split only when the model
        # explicitly reports a token-limit envelope. Every candidate is still
        # scored before one global top-k, preserving RRF20 quality gates.
        batch_size = min(capability.max_batch_items, len(candidates))
        if batch_size < 1 or top_k < 1 or capability.max_batch_tokens < 1 or recipe.max_input_tokens < 1:
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        scores = []
        for start in range(0, len(candidates), batch_size):
            batch = candidates[start:start + batch_size]
            scores.extend(await self._rerank_batch(request_id, question, batch, profile, cancel=cancel, deadline=deadline))
        by_id = {str(hit.chunk.id): hit for hit in hits}
        return tuple(replace(by_id[score.id], rerank_score=score.score)
                     for score in sorted(scores, key=lambda score: (-score.score, score.id))[:top_k])

    async def _rerank_batch(self, request_id: UUID, question: PreparedQuery, batch: list[RerankCandidate],
                            profile: RuntimeProfile, *, cancel: asyncio.Event, deadline: float):
        try:
            return await self._post_rerank_batch(request_id, question, batch, profile, cancel=cancel, deadline=deadline)
        except _RerankTokenLimitExceeded:
            if len(batch) == 1:
                raise RetrievalError("TOKEN_LIMIT_EXCEEDED") from None
            midpoint = len(batch) // 2
            left = await self._rerank_batch(request_id, question, batch[:midpoint], profile, cancel=cancel, deadline=deadline)
            right = await self._rerank_batch(request_id, question, batch[midpoint:], profile, cancel=cancel, deadline=deadline)
            return [*left, *right]

    async def _post_rerank_batch(self, request_id: UUID, question: PreparedQuery, batch: list[RerankCandidate],
                                 profile: RuntimeProfile, *, cancel: asyncio.Event, deadline: float) -> list:
        recipe = profile.reranker_recipe
        request = RerankRequest(request_id=request_id, query=question.search_question, candidates=batch, top_k=len(batch))
        response = await bounded(self._post_rerank_raw(request, request_id, recipe.model_fingerprint, recipe.runtime_fingerprint),
                                 cancel, deadline)
        try:
            response.validate_binding(request)
        except ValueError:
            raise RetrievalError("MODEL_RESPONSE_BINDING_INVALID") from None
        if (response.model, response.revision, response.score_type) != (recipe.model, recipe.revision, recipe.score_type):
            raise RetrievalError("MODEL_PROFILE_MISMATCH")
        return list(response.scores)

    async def _post_rerank_raw(self, request: RerankRequest, request_id: UUID,
                               model_fingerprint: str, runtime_fingerprint: str) -> RerankResponse:
        headers = {"X-Request-ID": str(request_id)}
        calls = getattr(self.client, "calls", None)
        if isinstance(calls, MutableMapping):
            calls["rerank_batch_post"] = int(calls.get("rerank_batch_post", 0)) + 1
        trace_headers = getattr(self.client, "_trace_headers", None)
        if trace_headers is not None:
            try:
                parent = trace_headers().get("traceparent", "")
            except Exception:
                parent = ""
            if (isinstance(parent, str) and re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]", parent)
                    and int(parent[3:35], 16) and int(parent[36:52], 16)):
                headers["traceparent"] = parent
        try:
            async with asyncio.timeout(self.timeout_seconds):
                async with self.client._client.stream(
                    "POST", "v1/rerank", json=request.model_dump(mode="json"), headers=headers,
                    timeout=httpx.Timeout(self.timeout_seconds,
                                          connect=self.client._connect_timeout,
                                          pool=self.client._pool_timeout),
                ) as response:
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(body) + len(chunk) > self.client._max_response_bytes:
                            raise DependencyError("DEPENDENCY_RESPONSE_TOO_LARGE", retryable=False)
                        body.extend(chunk)
                    if response.status_code < 200 or response.status_code >= 300:
                        if response.status_code == 422 and _is_token_limit_envelope(bytes(body)):
                            raise _RerankTokenLimitExceeded
                        raise DependencyError(
                            "DEPENDENCY_REJECTED", retryable=response.status_code in {429, 502, 503, 504},
                            status_code=response.status_code,
                        )
                    if (response.headers.get("X-Model-Fingerprint") != model_fingerprint
                            or response.headers.get("X-Runtime-Fingerprint") != runtime_fingerprint):
                        raise DependencyError("DEPENDENCY_CONTRACT_VIOLATION", retryable=False)
        except (httpx.TimeoutException, TimeoutError):
            raise DependencyError("DEPENDENCY_TIMEOUT", retryable=True) from None
        except httpx.HTTPError:
            raise DependencyError("DEPENDENCY_UNAVAILABLE", retryable=True) from None
        try:
            return RerankResponse.model_validate_json(body)
        except (ValidationError, ValueError):
            raise DependencyError("DEPENDENCY_CONTRACT_VIOLATION", retryable=False) from None


def _is_token_limit_envelope(body: bytes) -> bool:
    try:
        return ErrorEnvelope.model_validate_json(body).error.code == ErrorCode.TOKEN_LIMIT_EXCEEDED
    except (ValidationError, ValueError, json.JSONDecodeError):
        return False
