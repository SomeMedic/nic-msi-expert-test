"""Private application transport. None of these are public OpenAPI roots."""
from collections.abc import Mapping, Sequence
from typing import Literal
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from .common import Count, LegalStatus, NonBlank, RunStage, RunStatus, SHA256, ShortID, StrictDTO, UTCDateTime, unique
from .sources import SourceSpan


class SnapshotItem(StrictDTO):
    model_config = ConfigDict(frozen=True)
    snapshot_id: UUID
    logical_document_id: UUID
    document_version_id: UUID
    parse_generation_id: UUID
    index_generation_id: UUID
    publication_id: UUID
    legal_status_at_capture: Literal[LegalStatus.ACTIVE] = LegalStatus.ACTIVE


class EvidenceUnit(StrictDTO):
    model_config = ConfigDict(frozen=True)
    evidence_id: ShortID
    document_version_id: UUID
    index_generation_id: UUID
    canonical_node_id: UUID
    source_chunk_ids: tuple[UUID, ...] = Field(min_length=1, max_length=100)
    document_title: NonBlank = Field(max_length=500)
    structural_path: tuple[NonBlank, ...] = Field(max_length=32)
    excerpt: NonBlank = Field(max_length=30000)
    source_spans: tuple[SourceSpan, ...] = Field(min_length=1, max_length=500)
    content_hash: SHA256

    @model_validator(mode="after")
    def exact_fragments(self):
        unique(self.source_chunk_ids, "source chunks")
        anchors = [(s.pdf_page, s.block_id, s.start_offset, s.end_offset) for s in self.source_spans]
        unique(anchors, "source spans")
        if any(s.start_offset == s.end_offset for s in self.source_spans):
            raise ValueError("evidence source spans must contain text")
        return self


class EvidencePack(StrictDTO):
    model_config = ConfigDict(frozen=True)
    pack_id: UUID
    run_id: UUID
    snapshot_id: UUID
    units: tuple[EvidenceUnit, ...] = Field(max_length=72)
    llm_token_count: Count
    manifest_hash: SHA256

    @model_validator(mode="after")
    def unique_evidence(self):
        unique([u.evidence_id for u in self.units], "evidence IDs")
        return self

    def validate_binding(self, items: Sequence[SnapshotItem], *, node_generations: Mapping[UUID, UUID], chunk_generations: Mapping[UUID, UUID]) -> None:
        if any(item.snapshot_id != self.snapshot_id for item in items):
            raise ValueError("snapshot items belong to another snapshot")
        unique([item.logical_document_id for item in items], "snapshot logical documents")
        unique([item.index_generation_id for item in items], "snapshot index generations")
        by_index = {item.index_generation_id: item for item in items}
        for unit in self.units:
            item = by_index.get(unit.index_generation_id)
            if item is None or item.document_version_id != unit.document_version_id:
                raise ValueError("evidence version/index is outside the pinned snapshot")
            if node_generations.get(unit.canonical_node_id) != item.parse_generation_id:
                raise ValueError("evidence node is outside the pinned parse generation")
            if any(chunk_generations.get(cid) != item.index_generation_id for cid in unit.source_chunk_ids):
                raise ValueError("evidence chunk is outside the pinned index generation")


class StartRunRequest(StrictDTO):
    schema_version: Literal[1] = 1
    request_id: UUID
    execution_request_id: UUID


class StartRunResponse(StrictDTO):
    run_id: UUID
    status: Literal["accepted", "already_running", "already_terminal"]


class ExecutionStatus(StrictDTO):
    run_id: UUID
    status: RunStatus
    current_stage: RunStage | None = None
    execution_epoch: Count
    lease_until: UTCDateTime | None = None
    heartbeat_at: UTCDateTime | None = None


class IngestionCommand(StrictDTO):
    schema_version: Literal[1] = 1
    event_id: UUID
    job_id: UUID
