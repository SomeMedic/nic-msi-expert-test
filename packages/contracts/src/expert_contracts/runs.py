"""Public run read models: only committed, verified answers or typed refusals."""
from typing import Annotated, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, model_validator

from .common import Count, CursorPage, NonBlank, PositiveInt, RunLinks, RunStage, RunStatus, ShortID, StrictDTO, UTCDateTime, unique, validate_times
from .errors import ErrorInfo, REFUSAL_MESSAGES, RefusalCode
from .debug import DebugCaptureSummary, DebugModelCall
from .sources import CitationDTO


MAX_PUBLIC_CITATIONS_PER_CLAIM = 10


class SnapshotInfo(StrictDTO):
    id: UUID
    captured_at: UTCDateTime
    version_count: Count


class CreateRunRequest(StrictDTO):
    question: NonBlank = Field(max_length=4000)
    debug_capture: bool = False


class RunAccepted(StrictDTO):
    run_id: UUID
    status: Literal["created"] = "created"
    last_sequence: PositiveInt = 1
    links: RunLinks


class PublicClaim(StrictDTO):
    claim_id: ShortID
    text: NonBlank = Field(max_length=1800)
    citation_ids: list[ShortID] = Field(min_length=1, max_length=MAX_PUBLIC_CITATIONS_PER_CLAIM)

    @model_validator(mode="after")
    def unique_citations(self):
        unique(self.citation_ids, "claim citation IDs")
        return self


class ValidationSummary(StrictDTO):
    status: Literal["confirmed"] = "confirmed"
    claim_count: PositiveInt
    supported_count: PositiveInt
    repair_used: bool

    @model_validator(mode="after")
    def confirmed_counts(self):
        if self.claim_count != self.supported_count:
            raise ValueError("published answer requires all claims supported")
        return self


class CriticPublicResult(StrictDTO):
    status: Literal["confirmed", "partially_confirmed", "hallucinated"]
    reason: NonBlank = Field(max_length=500)


class FinalAnswer(StrictDTO):
    kind: Literal["completed"] = "completed"
    result_id: UUID
    text: NonBlank = Field(max_length=30000)
    claims: list[PublicClaim] = Field(min_length=1, max_length=12)
    citations: list[CitationDTO] = Field(min_length=1, max_length=72)
    validation: ValidationSummary
    snapshot: SnapshotInfo

    @model_validator(mode="after")
    def citation_binding(self):
        unique([c.claim_id for c in self.claims], "public claim IDs")
        unique([c.citation_id for c in self.citations], "citation IDs")
        unique([c.evidence_id for c in self.citations], "cited evidence IDs")
        referenced = {cid for claim in self.claims for cid in claim.citation_ids}
        if referenced != {c.citation_id for c in self.citations}:
            raise ValueError("public citations must exactly match cited claim references")
        if self.validation.claim_count != len(self.claims):
            raise ValueError("validation counts do not match claims")
        if self.snapshot.version_count == 0:
            raise ValueError("empty snapshot cannot produce a verified answer")
        return self


class RefusalResult(StrictDTO):
    kind: Literal["refused"] = "refused"
    result_id: UUID
    code: RefusalCode
    text: NonBlank = Field(max_length=1000)
    snapshot: SnapshotInfo

    @model_validator(mode="after")
    def exact_message(self):
        if self.text != REFUSAL_MESSAGES[self.code]:
            raise ValueError("refusal must use the canonical message")
        return self


RunResult = Annotated[FinalAnswer | RefusalResult, Field(discriminator="kind")]


class PublicRun(StrictDTO):
    run_id: UUID
    status: RunStatus
    current_stage: RunStage | None = None
    stage_attempt: PositiveInt = 1
    created_at: UTCDateTime
    started_at: UTCDateTime | None = None
    finished_at: UTCDateTime | None = None
    cancel_requested: bool = False
    snapshot: SnapshotInfo | None = None
    last_sequence: PositiveInt
    result: RunResult | None = None
    error: ErrorInfo | None = None

    @model_validator(mode="after")
    def public_state(self):
        validate_times(self.created_at, self.started_at, self.finished_at)
        terminal = self.status in {RunStatus.COMPLETED, RunStatus.REFUSED, RunStatus.FAILED, RunStatus.CANCELLED}
        if terminal != (self.finished_at is not None):
            raise ValueError("finished_at must correspond to terminal state")
        if self.status in {RunStatus.COMPLETED, RunStatus.REFUSED}:
            if self.result is None or self.result.kind != self.status.value:
                raise ValueError("terminal answer/refusal requires matching result")
            if self.snapshot != self.result.snapshot:
                raise ValueError("run and result snapshots differ")
        elif self.result is not None:
            raise ValueError("unverified/non-answer run cannot expose a result")
        if (self.status == RunStatus.FAILED) != (self.error is not None):
            raise ValueError("technical error is present exactly for failed runs")
        if self.status in {RunStatus.CANCELLING, RunStatus.CANCELLED} and not self.cancel_requested:
            raise ValueError("cancelling/cancelled requires durable cancel request")
        return self


class RunSummary(StrictDTO):
    run_id: UUID
    question_excerpt: NonBlank = Field(max_length=500)
    status: RunStatus
    created_at: UTCDateTime
    duration_ms: Count | None = None


class RunList(CursorPage[RunSummary]):
    pass


class RunCancelAccepted(StrictDTO):
    run_id: UUID
    status: Literal["cancelling"] = "cancelling"
    cancel_requested: Literal[True] = True
    last_sequence: PositiveInt


class DebugStep(StrictDTO):
    stage: RunStage
    attempt: PositiveInt
    status: Literal["started", "completed", "failed", "retry_scheduled"]
    occurred_at: UTCDateTime
    duration_ms: Count | None = None
    input_tokens: Count | None = None
    output_tokens: Count | None = None
    candidate_count: Count | None = None


class RunDebug(StrictDTO):
    run_id: UUID
    trace_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    steps: list[DebugStep] = Field(max_length=500)
    status: RunStatus | None = None
    duration_ms: Count | None = None
    snapshot: SnapshotInfo | None = None
    configuration_fingerprint: NonBlank | None = Field(default=None, max_length=200)
    trace_url: str | None = Field(default=None, max_length=2000)
    model_calls: list[DebugModelCall] = Field(default_factory=list, max_length=100)
    capture: DebugCaptureSummary | None = None
    private_payload_expires_at: UTCDateTime | None = None
    payloads_purged_at: UTCDateTime | None = None

    @model_validator(mode="after")
    def safe_diagnostics(self):
        unique([item.artifact_id for item in self.model_calls], "model artifact IDs")
        if self.trace_id == "0" * 32:
            raise ValueError("zero trace identity is invalid")
        if self.trace_url is not None:
            url = urlsplit(self.trace_url)
            if (self.trace_id is None or url.scheme not in {"http", "https"} or not url.hostname
                    or url.username is not None or url.password is not None or url.query or url.fragment
                    or not url.path.endswith("/trace/" + self.trace_id) or "\\" in self.trace_url
                    or any(ord(character) <= 32 for character in self.trace_url)):
                raise ValueError("trace URL must be a configured trace link without credentials")
        if self.payloads_purged_at is not None and (
            self.private_payload_expires_at is None or self.payloads_purged_at < self.private_payload_expires_at
        ):
            raise ValueError("private payload purge cannot precede retention expiry")
        return self
