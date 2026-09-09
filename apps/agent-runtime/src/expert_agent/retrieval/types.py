"""Immutable private retrieval values; never public OpenAPI roots."""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import math
from typing import Any, Literal
from uuid import UUID

from expert_contracts.internal import EvidencePack, SnapshotItem
from expert_contracts.sources import SourceSpan


class RetrievalError(RuntimeError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RetrievalConfig:
    # Source05/22 engineering starting values, not calibrated quality thresholds.
    dense_top_k: int = 20
    lexical_top_k: int = 20
    fusion_top_k: int = 20
    rerank_top_k: int = 6
    descriptor_top_k: int = 12
    max_context_candidates: int = 8
    rrf_constant: int = 60
    max_lexical_terms: int = 128
    max_expansion_nodes: int = 128
    max_ancestor_depth: int = 32
    max_units: int = 72
    max_excerpt_characters: int = 30000
    statement_timeout_ms: int = 10000

    def __post_init__(self) -> None:
        for name in ("dense_top_k", "lexical_top_k", "fusion_top_k", "rerank_top_k",
                     "descriptor_top_k", "max_context_candidates", "max_lexical_terms", "max_expansion_nodes"):
            if not 1 <= getattr(self, name) <= 256:
                raise ValueError("Invalid bounded retrieval limit")
        if not (1 <= self.rrf_constant <= 1000 and 1 <= self.max_ancestor_depth <= 64
                and 1 <= self.max_units <= 72 and 1 <= self.max_excerpt_characters <= 30000
                and 1 <= self.statement_timeout_ms <= 30000):
            raise ValueError("Invalid retrieval admission bound")


@dataclass(frozen=True)
class Snapshot:
    run_id: UUID
    principal_id: str = field(repr=False)
    snapshot_id: UUID
    catalog_epoch: int
    selection_hash: str
    items: tuple[SnapshotItem, ...]
    original_question: str = field(default="", repr=False)


@dataclass(frozen=True)
class Chunk:
    id: UUID
    index_generation_id: UUID
    parse_generation_id: UUID
    document_version_id: UUID
    node_id: UUID
    source_text: str = field(repr=False)
    header_text: str = field(repr=False)
    source_spans: tuple[SourceSpan, ...]
    content_hash: str
    # JSON strings keep nested persisted metadata immutable without importing the
    # ingestion-worker package and its process/dependency surface into runtime.
    projection_json: str = field(repr=False)
    embedding_text: str = field(default="", repr=False)

    @property
    def key(self) -> tuple[UUID, UUID]:
        return self.index_generation_id, self.id


@dataclass(frozen=True)
class ChannelHit:
    chunk: Chunk
    score: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.score):
            raise RetrievalError("RETRIEVAL_CONTRACT_INVALID")


@dataclass(frozen=True)
class CandidateBatch:
    dense: tuple[ChannelHit, ...]
    lexical: tuple[ChannelHit, ...]
    path: Literal["current", "historical"]
    catalog_epoch: int


@dataclass(frozen=True)
class FusedHit:
    chunk: Chunk
    rrf_score: float
    dense_rank: int | None
    lexical_rank: int | None
    rerank_score: float | None = None


@dataclass(frozen=True)
class DescriptorHit:
    alias: str
    node_id: UUID
    index_generation_id: UUID
    parse_generation_id: UUID
    text: str = field(repr=False)
    distance: float
    source_spans: tuple[SourceSpan, ...]
    content_hash: str


@dataclass(frozen=True)
class Node:
    id: UUID
    parse_generation_id: UUID
    parent_id: UUID | None
    node_type: str
    canonical_text: str = field(repr=False)
    structural_path: tuple[str, ...]
    source_spans: tuple[SourceSpan, ...]
    metadata_json: str = field(repr=False)
    table_json: str | None = field(repr=False)


@dataclass(frozen=True)
class SourceBlock:
    id: str
    pdf_page: int
    text: str = field(repr=False)


@dataclass(frozen=True)
class SourceRegistry:
    """Loader must authenticate artifact bytes before constructing this registry."""
    parse_generation_id: UUID
    document_version_id: UUID
    source_sha256: str
    artifact_object_id: UUID
    artifact_sha256: str
    blocks: tuple[SourceBlock, ...]
    page_sizes: tuple[tuple[float, float], ...]
    owner_hashes: tuple[tuple[UUID, str, str, str], ...] = ()


@dataclass(frozen=True)
class SourceIdentity:
    parse_generation_id: UUID
    document_version_id: UUID
    artifact_object_id: UUID
    source_sha256: str
    document_title: str
    artifact_sha256: str | None = None


@dataclass(frozen=True)
class Expansion:
    chunk: Chunk
    node_id: UUID
    text: str = field(repr=False)
    spans: tuple[SourceSpan, ...]
    relation: Literal["hit", "scope", "header", "unit", "note", "caption", "table", "template"]
    required: bool
    owner_ranges_json: str


@dataclass(frozen=True)
class ExpandedCandidate:
    hit: FusedHit
    fragments: tuple[Expansion, ...]


@dataclass(frozen=True)
class PackedEvidence:
    pack: EvidencePack
    # Includes relation and canonical bindings absent from public/shared DTOs.
    bindings_json: str
    dropped_candidate_ids: tuple[UUID, ...]


@dataclass(frozen=True)
class PreparedQuery:
    original_question: str = field(repr=False)
    search_question: str = field(repr=False)


def prepare_query(original_question: str) -> PreparedQuery:
    import unicodedata

    if not original_question.strip() or len(original_question) > 4000:
        raise RetrievalError("QUERY_INVALID")
    return PreparedQuery(original_question, " ".join(unicodedata.normalize("NFC", original_question).split()))
