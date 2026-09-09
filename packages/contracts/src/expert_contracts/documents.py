"""Document lifecycle commands and safe read models; no ORM/vendor models."""
from datetime import date
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from .common import ApiPath, Count, CursorPage, GenerationStatus, IngestionStage, JobStatus, LegalStatus, NonBlank, PositiveInt, PublicationStatus, StrictDTO, UTCDateTime, unique, validate_times
from .errors import ErrorInfo
from .sources import SourceDescriptor


class DocumentMetadata(StrictDTO):
    schema_version: Literal[1] = 1
    title: NonBlank = Field(max_length=500)
    legal_status: LegalStatus
    approved_at: date
    document_type: NonBlank | None = Field(default=None, max_length=100)
    document_number: NonBlank | None = Field(default=None, max_length=100)
    authority: NonBlank | None = Field(default=None, max_length=300)
    version_label: NonBlank | None = Field(default=None, max_length=200)
    edition_at: date | None = None
    effective_from: date | None = None
    effective_to: date | None = None

    @model_validator(mode="after")
    def inclusive_dates(self):
        if self.effective_from is not None and self.effective_to is not None and self.effective_to < self.effective_from:
            raise ValueError("inclusive effective_to cannot precede effective_from")
        return self


class VersionUploadOptions(StrictDTO):
    metadata: DocumentMetadata
    expected_current_publication_id: UUID | None
    auto_publish: bool = True


class UploadLinks(StrictDTO):
    document: ApiPath
    version: ApiPath
    job: ApiPath
    events: ApiPath


class UploadAccepted(StrictDTO):
    document_id: UUID
    version_id: UUID
    job_id: UUID
    status: Literal["queued"] = "queued"
    links: UploadLinks


class ReindexRequest(StrictDTO):
    pipeline_config_alias: NonBlank = Field(max_length=200, pattern=r"^[A-Za-z0-9:._-]{1,200}$")
    expected_current_publication_id: UUID | None
    auto_publish: bool = True


class PublishRequest(StrictDTO):
    index_generation_id: UUID
    expected_current_publication_id: UUID | None
    operation_id: UUID


class DeactivateRequest(StrictDTO):
    reason: NonBlank = Field(max_length=1000)


class ArchiveDocumentRequest(StrictDTO):
    expected_current_publication_id: UUID | None
    operation_id: UUID
    reason: NonBlank = Field(max_length=2000)


class LibraryProfile(StrictDTO):
    logical_document_count: Count
    version_count: Count
    eligible_document_count: Count
    last_publication_at: UTCDateTime | None

    @model_validator(mode="after")
    def eligible_count(self):
        if self.eligible_document_count > self.logical_document_count:
            raise ValueError("eligible document count exceeds logical document count")
        return self


class PublicationInfo(StrictDTO):
    publication_id: UUID
    document_version_id: UUID
    index_generation_id: UUID
    published_at: UTCDateTime
    retired_at: UTCDateTime | None = None

    @model_validator(mode="after")
    def chronological(self):
        if self.retired_at is not None and self.retired_at < self.published_at:
            raise ValueError("retirement precedes publication")
        return self


class QualitySummary(StrictDTO):
    status: Literal["pending", "passed", "warning", "failed"]
    reason_codes: list[NonBlank] = Field(default_factory=list, max_length=100)
    warning_count: Count = 0


class GenerationSummary(StrictDTO):
    generation_id: UUID
    kind: Literal["parse", "index"]
    status: GenerationStatus
    created_at: UTCDateTime
    completed_at: UTCDateTime | None = None
    node_count: Count | None = None
    chunk_count: Count | None = None
    routing_count: Count | None = None
    quality: QualitySummary | None = None

    @model_validator(mode="after")
    def generation_state(self):
        if self.status == GenerationStatus.READY and self.completed_at is None:
            raise ValueError("ready generation requires completed_at")
        validate_times(self.created_at, None, self.completed_at)
        return self


class VersionSummary(StrictDTO):
    version_id: UUID
    document_id: UUID
    metadata: DocumentMetadata
    publication_status: PublicationStatus
    created_at: UTCDateTime
    published_at: UTCDateTime | None = None
    deactivated_at: UTCDateTime | None = None


class VersionDetail(VersionSummary):
    source: SourceDescriptor
    generations: list[GenerationSummary] = Field(default_factory=list, max_length=100)
    current_publication: PublicationInfo | None = None

    @model_validator(mode="after")
    def version_bindings(self):
        if self.source.version_id != self.version_id:
            raise ValueError("source belongs to another version")
        if self.current_publication is not None and self.current_publication.document_version_id != self.version_id:
            raise ValueError("publication belongs to another version")
        unique([g.generation_id for g in self.generations], "generation IDs")
        return self


class DocumentSummary(StrictDTO):
    document_id: UUID
    canonical_title: NonBlank = Field(max_length=500)
    document_type: NonBlank | None = None
    document_number: NonBlank | None = None
    authority: NonBlank | None = None
    current_publication: PublicationInfo | None = None
    version_count: Count
    created_at: UTCDateTime
    archived_at: UTCDateTime | None = None
    security_revoked: bool = False
    purge_status: Literal["purge_pending", "failed", "completed"] | None = None
    purge_plan_id: UUID | None = None
    purged_at: UTCDateTime | None = None


class DocumentDetail(DocumentSummary):
    versions: list[VersionSummary] = Field(max_length=100)
    next_versions_cursor: str | None = None

    @model_validator(mode="after")
    def document_bindings(self):
        unique([v.version_id for v in self.versions], "version IDs")
        if any(v.document_id != self.document_id for v in self.versions):
            raise ValueError("version belongs to another document")
        if len(self.versions) > self.version_count:
            raise ValueError("version_count is smaller than returned versions")
        return self


class DocumentList(CursorPage[DocumentSummary]):
    pass


class JobProgress(StrictDTO):
    processed_units: Count
    total_units: Count | None
    unit: Literal["pages", "chunks"]

    @model_validator(mode="after")
    def bounded_progress(self):
        if self.total_units is not None and self.processed_units > self.total_units:
            raise ValueError("processed_units exceeds total_units")
        return self


class IngestionJob(StrictDTO):
    job_id: UUID
    version_id: UUID
    status: JobStatus
    stage: IngestionStage | None = None
    attempt: Count
    max_attempts: PositiveInt
    progress: JobProgress | None = None
    cancel_requested: bool = False
    created_at: UTCDateTime
    started_at: UTCDateTime | None = None
    finished_at: UTCDateTime | None = None
    available_at: UTCDateTime | None = None
    last_sequence: PositiveInt
    error: ErrorInfo | None = None
    trace_id: str | None = Field(default=None, min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")

    @model_validator(mode="after")
    def job_invariants(self):
        if self.trace_id == "0" * 32:
            raise ValueError("trace_id must be nonzero")
        validate_times(self.created_at, self.started_at, self.finished_at)
        if self.attempt > self.max_attempts:
            raise ValueError("attempt exceeds max_attempts")
        terminal = self.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
        if terminal != (self.finished_at is not None):
            raise ValueError("finished_at must correspond to terminal job state")
        if self.status == JobStatus.CANCELLED and not self.cancel_requested:
            raise ValueError("cancelled job requires durable cancel request")
        if self.status == JobStatus.COMPLETED and self.error is not None:
            raise ValueError("completed job cannot expose an error")
        return self


class JobCommandAccepted(StrictDTO):
    job_id: UUID
    status: JobStatus
    last_sequence: PositiveInt


class PurgeReferenceCounts(StrictDTO):
    active_runs: Count
    snapshots: Count
    results: Count
    checkpoints: Count
    objects: Count
    active_ingestion_jobs: Count = 0
    pending_reservations: Count = 0


class PurgePlan(StrictDTO):
    plan_id: UUID
    document_id: UUID
    plan_version: PositiveInt
    created_at: UTCDateTime
    expires_at: UTCDateTime
    allowed: bool
    blockers: list[NonBlank] = Field(max_length=100)
    references: PurgeReferenceCounts
    retention_policy: Literal["p14.purge.v1"] = "p14.purge.v1"
    eligible_after: UTCDateTime | None = None

    @model_validator(mode="after")
    def valid_plan(self):
        if self.expires_at <= self.created_at:
            raise ValueError("purge plan expiry must follow creation")
        refs = self.references
        if self.allowed and (self.blockers or refs.active_runs or refs.snapshots or refs.results or refs.checkpoints
                             or refs.active_ingestion_jobs or refs.pending_reservations):
            raise ValueError("purge cannot be allowed with protected references/blockers")
        return self


class PurgeRequest(StrictDTO):
    plan_id: UUID
    plan_version: PositiveInt


class PurgeAccepted(StrictDTO):
    plan_id: UUID
    document_id: UUID
    status: Literal["purge_pending", "completed"] = "purge_pending"
