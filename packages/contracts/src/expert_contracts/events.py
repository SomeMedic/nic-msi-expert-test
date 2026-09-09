"""Append-only public event envelopes with closed, type-specific payloads."""
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, TypeAdapter

from .common import Count, IngestionStage, PositiveInt, RunStage, StrictDTO, UTCDateTime
from .documents import JobProgress
from .errors import ErrorCode, ErrorInfo, RefusalCode


class EmptyEventData(StrictDTO):
    pass


class StageStartedData(StrictDTO):
    message_code: Literal["RUN_SNAPSHOTTING", "RUN_ROUTING", "RUN_RETRIEVING", "RUN_RERANKING", "RUN_BUILDING_CONTEXT", "RUN_DRAFTING", "RUN_CHECKING_CITATIONS", "RUN_VALIDATING", "RUN_REPAIRING", "RUN_REVALIDATING", "RUN_RENDERING", "RUN_FINALIZING"] | None = None


class StageCompletedData(StrictDTO):
    duration_ms: Count
    candidate_count: Count | None = None
    evidence_count: Count | None = None
    claim_count: Count | None = None


class StageRetryData(StrictDTO):
    error_code: ErrorCode
    retry_after_seconds: float = Field(ge=0)


class CompletedData(StrictDTO):
    result_id: UUID
    claim_count: PositiveInt
    citation_count: PositiveInt


class RefusedData(StrictDTO):
    result_id: UUID
    refusal_code: RefusalCode


class FailedData(StrictDTO):
    error: ErrorInfo


class RunEventBase(StrictDTO):
    schema_version: Literal[1] = 1
    event_id: UUID
    run_id: UUID
    sequence: PositiveInt
    stage: RunStage | None = None
    attempt: PositiveInt = 1
    execution_epoch: Count
    occurred_at: UTCDateTime


class RunCreatedEvent(RunEventBase):
    type: Literal["run.created"]
    data: EmptyEventData


class RunStartedEvent(RunEventBase):
    type: Literal["run.started"]
    data: EmptyEventData


class RunResumingEvent(RunEventBase):
    type: Literal["run.resuming"]
    data: EmptyEventData


class StageStartedEvent(RunEventBase):
    type: Literal["stage.started"]
    stage: RunStage
    data: StageStartedData


class StageCompletedEvent(RunEventBase):
    type: Literal["stage.completed"]
    stage: RunStage
    data: StageCompletedData


class StageRetryScheduledEvent(RunEventBase):
    type: Literal["stage.retry_scheduled"]
    stage: RunStage
    data: StageRetryData


class RunCancelRequestedEvent(RunEventBase):
    type: Literal["run.cancel_requested"]
    data: EmptyEventData


class RunCompletedEvent(RunEventBase):
    type: Literal["run.completed"]
    data: CompletedData


class RunRefusedEvent(RunEventBase):
    type: Literal["run.refused"]
    data: RefusedData


class RunFailedEvent(RunEventBase):
    type: Literal["run.failed"]
    data: FailedData


class RunCancelledEvent(RunEventBase):
    type: Literal["run.cancelled"]
    data: EmptyEventData


PublicRunEvent = Annotated[
    RunCreatedEvent | RunStartedEvent | RunResumingEvent | StageStartedEvent |
    StageCompletedEvent | StageRetryScheduledEvent | RunCancelRequestedEvent |
    RunCompletedEvent | RunRefusedEvent | RunFailedEvent | RunCancelledEvent,
    Field(discriminator="type"),
]


class IngestionReadyData(StrictDTO):
    version_id: UUID
    index_generation_id: UUID


class IngestionCompletedData(StrictDTO):
    version_id: UUID
    index_generation_id: UUID
    publication_id: UUID | None


class IngestionEventBase(StrictDTO):
    schema_version: Literal[1] = 1
    event_id: UUID
    job_id: UUID
    sequence: PositiveInt
    stage: IngestionStage | None = None
    attempt: PositiveInt = 1
    execution_epoch: Count = 0
    occurred_at: UTCDateTime


class IngestionCreatedEvent(IngestionEventBase):
    type: Literal["ingestion.created"]
    data: EmptyEventData


class IngestionStageStartedEvent(IngestionEventBase):
    type: Literal["stage.started"]
    stage: IngestionStage
    data: EmptyEventData


class IngestionProgressEvent(IngestionEventBase):
    type: Literal["stage.progress"]
    stage: IngestionStage
    data: JobProgress


class IngestionStageCompletedEvent(IngestionEventBase):
    type: Literal["stage.completed"]
    stage: IngestionStage
    data: StageCompletedData


class IngestionReadyEvent(IngestionEventBase):
    type: Literal["ingestion.ready_to_publish"]
    data: IngestionReadyData


class IngestionCompletedEvent(IngestionEventBase):
    type: Literal["ingestion.completed"]
    data: IngestionCompletedData


class IngestionFailedEvent(IngestionEventBase):
    type: Literal["ingestion.failed"]
    data: FailedData


class IngestionCancelledEvent(IngestionEventBase):
    type: Literal["ingestion.cancelled"]
    data: EmptyEventData


PublicIngestionEvent = Annotated[
    IngestionCreatedEvent | IngestionStageStartedEvent | IngestionProgressEvent |
    IngestionStageCompletedEvent | IngestionReadyEvent | IngestionCompletedEvent |
    IngestionFailedEvent | IngestionCancelledEvent,
    Field(discriminator="type"),
]

RUN_EVENT_ADAPTER: TypeAdapter[PublicRunEvent] = TypeAdapter(PublicRunEvent)
INGESTION_EVENT_ADAPTER: TypeAdapter[PublicIngestionEvent] = TypeAdapter(PublicIngestionEvent)
EVENT_ADAPTERS = {"run": RUN_EVENT_ADAPTER, "ingestion": INGESTION_EVENT_ADAPTER}
PUBLIC_EVENT_MODELS = (
    RunCreatedEvent, RunStartedEvent, RunResumingEvent, StageStartedEvent,
    StageCompletedEvent, StageRetryScheduledEvent, RunCancelRequestedEvent,
    RunCompletedEvent, RunRefusedEvent, RunFailedEvent, RunCancelledEvent,
    IngestionCreatedEvent, IngestionStageStartedEvent, IngestionProgressEvent,
    IngestionStageCompletedEvent, IngestionReadyEvent, IngestionCompletedEvent,
    IngestionFailedEvent, IngestionCancelledEvent,
)
