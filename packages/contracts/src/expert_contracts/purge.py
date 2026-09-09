"""Public purge progress and private SQL-selected object deletion authority."""
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from .common import Count, PositiveInt, SHA256, StrictDTO, UTCDateTime


class PurgeFailureCode(StrEnum):
    STORAGE_UNAVAILABLE = "PURGE_STORAGE_UNAVAILABLE"
    OBJECT_MISMATCH = "PURGE_OBJECT_MISMATCH"
    DELETE_UNVERIFIED = "PURGE_DELETE_UNVERIFIED"
    ATTEMPTS_EXHAUSTED = "PURGE_ATTEMPTS_EXHAUSTED"


class PurgeStatus(StrictDTO):
    plan_id: UUID
    document_id: UUID
    plan_version: PositiveInt
    status: Literal["planned", "purge_pending", "failed", "completed"]
    created_at: UTCDateTime
    expires_at: UTCDateTime
    accepted_at: UTCDateTime | None
    completed_at: UTCDateTime | None
    total_object_count: Count
    deleted_object_count: Count
    error_code: PurgeFailureCode | None

    @model_validator(mode="after")
    def progress(self):
        if self.expires_at <= self.created_at:
            raise ValueError("purge plan expiry must follow creation")
        if self.deleted_object_count > self.total_object_count:
            raise ValueError("deleted count exceeds purge manifest")
        if self.status == "completed" and (self.completed_at is None or self.accepted_at is None
                                           or self.deleted_object_count != self.total_object_count):
            raise ValueError("completed purge requires complete durable deletion")
        if self.status == "planned" and (self.accepted_at is not None or self.completed_at is not None):
            raise ValueError("planned purge cannot be accepted/completed")
        if self.status != "planned" and self.accepted_at is None:
            raise ValueError("accepted purge requires its durable acceptance time")
        if self.status != "completed" and self.completed_at is not None:
            raise ValueError("only completed purge can expose completion time")
        return self


class PurgeObjectClaim(StrictDTO):
    """Private transport: deliberately excluded from public schema roots."""

    plan_id: UUID
    document_id: UUID
    object_id: UUID
    document_version_id: UUID
    parse_generation_id: UUID | None
    intent_id: UUID | None
    kind: Literal["original", "parse_artifact"]
    bucket: Literal["originals", "artifacts"]
    object_key: str = Field(min_length=1, max_length=4096)
    object_version_id: str | None = Field(max_length=1024)
    sha256: SHA256
    size_bytes: PositiveInt = Field(le=268435456)
    media_type: Literal["application/pdf", "application/json", "image/png"]
    claim_token: UUID
    claim_until: UTCDateTime
    attempt: PositiveInt = Field(le=8)
    audit: bool

    @model_validator(mode="after")
    def exact_object_identity(self):
        if self.kind == "original":
            expected = f"originals/{self.document_id}/{self.document_version_id}/{self.sha256}.pdf"
            if self.bucket != "originals" or self.media_type != "application/pdf" or self.parse_generation_id is not None:
                raise ValueError("invalid original identity")
        else:
            if (self.bucket != "artifacts" or self.parse_generation_id is None or self.intent_id is None
                    or self.media_type not in {"application/json", "image/png"}):
                raise ValueError("invalid parse artifact identity")
            suffix = ".json" if self.media_type == "application/json" else ".png"
            expected = f"parses/{self.document_version_id}/{self.parse_generation_id}/{self.intent_id}/{self.sha256}{suffix}"
        if self.object_key != expected:
            raise ValueError("object key does not match immutable purge identity")
        return self
