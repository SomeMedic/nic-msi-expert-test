"""Private P06 composition; graph/fencing/final publication remain caller-owned."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import Literal
from uuid import UUID

from expert_contracts.inference import QueryEmbeddingResponse
from expert_contracts.internal import EvidencePack
from expert_contracts.ml_profile import RuntimeProfile
from expert_contracts.model import RouteDecision
from expert_observability.tracing import safe_span

from .expansion import build_evidence_pack_async, expand_candidates
from .fusion import fuse_rrf
from .ml import RemoteModels, bounded
from .repository import RetrievalRepository
from .sources import SourceRegistryLoader, requested_blocks, requested_owners
from .types import DescriptorHit, FusedHit, PackedEvidence, PreparedQuery, RetrievalConfig, RetrievalError, Snapshot, prepare_query


@dataclass(frozen=True)
class PreparedRetrieval:
    snapshot: Snapshot
    question: PreparedQuery
    descriptors: tuple[DescriptorHit, ...]
    # Reconstruct fresh validated wire values on use; retained state is immutable.
    profile_json: str | None = field(repr=False)
    query_embedding_json: str | None = field(repr=False)


@dataclass(frozen=True)
class RetrievalResult:
    status: Literal["context", "no_context"]
    reason: Literal["retrieved", "empty_snapshot", "empty_hits", "atomic_context_too_large"]
    evidence: PackedEvidence | None
    ranked_hits: tuple[FusedHit, ...]
    search_path: Literal["current", "historical"] | None


def routing_input(prepared: PreparedRetrieval) -> dict:
    """Private Router input: descriptors are a non-exhaustive routing projection."""
    return {"original_question": prepared.question.original_question,
            "catalog_is_exhaustive": False,
            "descriptors": [{"id": hit.alias, "text": hit.text} for hit in prepared.descriptors]}


class RetrievalService:
    def __init__(self, repository: RetrievalRepository, models: RemoteModels, sources: SourceRegistryLoader,
                 config: RetrievalConfig | None = None):
        self.repository, self.models, self.sources = repository, models, sources
        self.config = config or repository.config
        if self.config != repository.config:
            raise ValueError("Repository and service retrieval configuration must match")

    async def prepare(self, run_id: UUID, principal_id: str, *, cancel: asyncio.Event,
                      deadline: float) -> PreparedRetrieval:
        snapshot = await bounded(self.repository.snapshot(run_id, principal_id), cancel, deadline)
        question = prepare_query(snapshot.original_question)
        if not snapshot.items:
            return PreparedRetrieval(snapshot, question, (), None, None)
        profile = await self.models.profile(run_id, cancel=cancel, deadline=deadline)
        with safe_span("model.embed", run_id=run_id, snapshot_id=snapshot.snapshot_id):
            embedding = await self.models.query(run_id, question, profile, cancel=cancel, deadline=deadline)
        with safe_span("router.catalog_search", run_id=run_id, snapshot_id=snapshot.snapshot_id) as span:
            descriptors = await bounded(self.repository.descriptors(snapshot, embedding, profile.embedding_recipe), cancel, deadline)
            span.set_attributes(count=len(descriptors))
        return PreparedRetrieval(snapshot, question, descriptors, profile.model_dump_json(), embedding.model_dump_json())

    async def retrieve(self, prepared: PreparedRetrieval, *, token_budget: int,
                       count_evidence: Callable[[EvidencePack, str], Awaitable[int]],
                       cancel: asyncio.Event, deadline: float, route: RouteDecision | None = None) -> RetrievalResult:
        if prepared.question != prepare_query(prepared.snapshot.original_question):
            raise RetrievalError("RETRIEVAL_STATE_INVALID")
        if route is not None:
            route.validate_binding({hit.alias for hit in prepared.descriptors})
        # Caller owns conservative out-of-scope policy. Router IDs never narrow SQL
        # eligibility, and an uncertain classification always permits this stage.
        if not prepared.snapshot.items:
            await bounded(self.repository.check_access(prepared.snapshot), cancel, deadline)
            return RetrievalResult("no_context", "empty_snapshot", None, (), None)
        if prepared.profile_json is None or prepared.query_embedding_json is None:
            raise RetrievalError("RETRIEVAL_STATE_INVALID")
        profile = RuntimeProfile.model_validate_json(prepared.profile_json)
        embedding = QueryEmbeddingResponse.model_validate_json(prepared.query_embedding_json)
        batch = await bounded(self.repository.channels(prepared.snapshot, embedding,
            prepared.question.search_question, profile.embedding_recipe), cancel, deadline)
        with safe_span("retrieval.fusion") as span:
            fused = fuse_rrf(batch, self.config)
            span.set_attributes(fused_count=len(fused))
        with safe_span("rerank") as span:
            ranked = await self.models.rerank(prepared.snapshot.run_id, prepared.question, fused, profile,
                self.config.rerank_top_k, cancel=cancel, deadline=deadline)
            span.set_attributes(reranked_count=len(ranked))
        if not ranked:
            await bounded(self.repository.check_access(prepared.snapshot), cancel, deadline)
            return RetrievalResult("no_context", "empty_hits", None, (), batch.path)
        ranked = ranked[:self.config.max_context_candidates]
        nodes, identities = await bounded(self.repository.expansion_inputs(prepared.snapshot,
            tuple(hit.chunk for hit in ranked)), cancel, deadline)
        registries = {}
        verified = []
        for identity in identities:
            registry = await self.sources.load(prepared.snapshot, identity,
                requested_blocks(nodes, identity.parse_generation_id), prepared.snapshot.run_id,
                cancel=cancel, deadline=deadline, owners=requested_owners(nodes, identity.parse_generation_id))
            registries[identity.parse_generation_id] = registry
            verified.append(replace(identity, artifact_sha256=registry.artifact_sha256))
        await bounded(self.repository.check_access(prepared.snapshot), cancel, deadline)
        try:
            expanded = expand_candidates(ranked, nodes, tuple(verified), registries, self.config)
            packed = await bounded(build_evidence_pack_async(prepared.snapshot, expanded, nodes, tuple(verified),
                token_budget=token_budget, count_evidence=count_evidence, config=self.config), cancel, deadline)
        except RetrievalError as error:
            if error.code != "ATOMIC_CONTEXT_TOO_LARGE":
                raise
            await bounded(self.repository.check_access(prepared.snapshot), cancel, deadline)
            return RetrievalResult("no_context", "atomic_context_too_large", None, ranked, batch.path)
        await bounded(self.repository.check_access(prepared.snapshot), cancel, deadline)
        return RetrievalResult("context", "retrieved", packed, ranked, batch.path)
