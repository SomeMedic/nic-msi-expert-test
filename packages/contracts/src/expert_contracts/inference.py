"""Internal inference JSON contracts; model IDs/revisions travel with scores."""
import math
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, model_validator

from .common import NonBlank, PositiveInt, ShortID, StrictDTO, unique

Vector = Annotated[list[Annotated[float, Field(strict=True)]], Field(min_length=1536, max_length=1536)]


def validate_vector(vector: list[float], normalized: bool) -> None:
    norm = math.sqrt(math.fsum(v * v for v in vector))
    if not math.isfinite(norm) or norm == 0:
        raise ValueError("embedding must be finite and have a nonzero norm")
    if normalized and not math.isclose(norm, 1.0, rel_tol=1e-3, abs_tol=1e-3):
        raise ValueError("embedding is not L2-normalized")


class QueryEmbeddingRequest(StrictDTO):
    request_id: UUID
    text: NonBlank = Field(max_length=4000)


class EmbeddingMetadata(StrictDTO):
    model: NonBlank = Field(max_length=200)
    revision: NonBlank = Field(max_length=100)
    dimension: Literal[1536] = 1536
    normalized: Literal[True] = True
    processing_ms: float = Field(ge=0)


class QueryEmbeddingResponse(EmbeddingMetadata):
    vector: Vector
    input_tokens: int = Field(ge=1, le=512, strict=True)

    @model_validator(mode="after")
    def valid_vector(self):
        validate_vector(self.vector, self.normalized)
        return self


class DocumentEmbeddingItem(StrictDTO):
    id: ShortID
    text: NonBlank = Field(max_length=30000)


class DocumentEmbeddingRequest(StrictDTO):
    request_id: UUID
    items: list[DocumentEmbeddingItem] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def unique_items(self):
        unique([item.id for item in self.items], "embedding input IDs")
        return self


class DocumentEmbeddingResult(StrictDTO):
    id: ShortID
    vector: Vector
    input_tokens: int = Field(ge=1, le=512, strict=True)


class DocumentEmbeddingResponse(EmbeddingMetadata):
    items: list[DocumentEmbeddingResult] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def valid_items(self):
        unique([item.id for item in self.items], "embedding output IDs")
        for item in self.items:
            validate_vector(item.vector, self.normalized)
        return self

    def validate_binding(self, request: DocumentEmbeddingRequest) -> None:
        if {item.id for item in self.items} != {item.id for item in request.items}:
            raise ValueError("embedding response IDs must exactly match request")


class RerankCandidate(StrictDTO):
    id: ShortID
    text: NonBlank = Field(max_length=30000)


class RerankRequest(StrictDTO):
    request_id: UUID
    query: NonBlank = Field(max_length=4000)
    candidates: list[RerankCandidate] = Field(min_length=1, max_length=256)
    top_k: int = Field(ge=1, le=256, strict=True)

    @model_validator(mode="after")
    def valid_candidates(self):
        unique([c.id for c in self.candidates], "rerank candidate IDs")
        if self.top_k > len(self.candidates):
            raise ValueError("top_k exceeds candidate count")
        return self


class RerankScore(StrictDTO):
    id: ShortID
    score: float
    rank: PositiveInt


class RerankResponse(StrictDTO):
    scores: list[RerankScore] = Field(max_length=256)
    score_type: Literal["raw_logit_difference", "sigmoid"]
    model: NonBlank = Field(max_length=200)
    revision: NonBlank = Field(max_length=100)
    processing_ms: float = Field(ge=0)

    @model_validator(mode="after")
    def score_convention(self):
        unique([s.id for s in self.scores], "rerank score IDs")
        if [s.rank for s in self.scores] != list(range(1, len(self.scores) + 1)):
            raise ValueError("ranks must be ordered and contiguous from one")
        if self.score_type == "sigmoid" and any(not 0 <= s.score <= 1 for s in self.scores):
            raise ValueError("sigmoid score outside [0,1]")
        if self.scores != sorted(self.scores, key=lambda score: (-score.score, score.id)):
            raise ValueError("scores must descend with stable ID tie-break")
        return self

    def validate_binding(self, request: RerankRequest) -> None:
        if len(self.scores) != request.top_k or not {s.id for s in self.scores} <= {c.id for c in request.candidates}:
            raise ValueError("rerank scores do not match requested candidates/top_k")


class ModelCapability(StrictDTO):
    model: NonBlank
    revision: NonBlank
    device: Literal["cpu", "cuda"]
    max_input_tokens: PositiveInt
    max_batch_items: PositiveInt
    max_batch_tokens: PositiveInt
    dimension: PositiveInt | None = None
    score_type: Literal["raw_logit_difference", "sigmoid"] | None = None


class CapabilitiesResponse(StrictDTO):
    embedding: ModelCapability
    reranker: ModelCapability
