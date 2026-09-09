"""Rank fusion; scores are ordering utilities, never answerability confidence."""
from __future__ import annotations

from uuid import UUID

from .types import CandidateBatch, Chunk, FusedHit, RetrievalConfig, RetrievalError


def fuse_rrf(batch: CandidateBatch, config: RetrievalConfig) -> tuple[FusedHit, ...]:
    chunks: dict[tuple[UUID, UUID], Chunk] = {}
    ranks: list[dict[tuple[UUID, UUID], int]] = []
    for channel in (batch.dense, batch.lexical):
        channel_ranks: dict[tuple[UUID, UUID], int] = {}
        for rank, hit in enumerate(channel, 1):
            key = hit.chunk.key
            if key in chunks and chunks[key] != hit.chunk:
                raise RetrievalError("RETRIEVAL_CONTRACT_INVALID")
            chunks[key] = hit.chunk
            channel_ranks.setdefault(key, rank)
        ranks.append(channel_ranks)
    result = [FusedHit(chunk, sum(1 / (config.rrf_constant + rank[key]) for rank in ranks if key in rank),
                       ranks[0].get(key), ranks[1].get(key)) for key, chunk in chunks.items()]
    return tuple(sorted(result, key=lambda hit: (-hit.rrf_score, str(hit.chunk.id),
                                                 str(hit.chunk.index_generation_id)))[:config.fusion_top_k])
