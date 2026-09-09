"""Safe diagnostics and capture availability; never model messages or storage keys."""
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictBool, model_validator

from .common import ApiPath, Count, NonBlank, PositiveInt, SHA256, StrictDTO, UTCDateTime, unique


class DebugCapturePolicy(StrictDTO):
    debug_capture_allowed: StrictBool
    debug_capture_ttl_hours: int = Field(ge=1, le=168, strict=True)


class DebugCaptureSummary(StrictDTO):
    enabled: StrictBool
    policy_version: Literal["p11.capture.v1"]
    expires_at: UTCDateTime | None
    status: Literal["disabled", "pending", "available", "partial", "expired", "unavailable"]
    part_count: Count = Field(le=60)
    attached_count: Count = Field(le=60)
    unavailable_count: Count = Field(le=60)

    @model_validator(mode="after")
    def availability(self):
        if self.attached_count > self.part_count:
            raise ValueError("attached count exceeds registered parts")
        if not self.enabled and (self.status != "disabled" or self.expires_at is not None):
            raise ValueError("disabled capture cannot claim availability or expiry")
        if self.enabled and (self.status == "disabled" or self.expires_at is None):
            raise ValueError("capture consent requires expiry and an enabled state")
        if self.status in {"available", "partial"} and self.attached_count == 0:
            raise ValueError("available capture requires an attached part")
        return self


class DebugModelCall(StrictDTO):
    artifact_id: UUID
    call_id: UUID | None
    role: Literal["router", "drafter", "critic", "repair"]
    status: Literal["completed", "failed"]
    execution_epoch: PositiveInt
    schema_attempt: int | None = Field(ge=0, le=1, strict=True)
    model_revision: NonBlank | None = Field(max_length=200)
    prompt_version: NonBlank | None = Field(max_length=200)
    profile_sha256: SHA256 | None
    prompt_sha256: SHA256 | None
    schema_sha256: SHA256 | None
    input_tokens: Count | None
    output_tokens: Count | None
    elapsed_ms: Count | None
    created_at: UTCDateTime
    payload_purged: StrictBool


class DebugCaptureItem(StrictDTO):
    part_id: UUID
    call_id: UUID
    execution_epoch: PositiveInt
    part: Literal["request", "response"]
    role: Literal["router", "drafter", "critic", "repair"]
    schema_attempt: int = Field(ge=0, le=1, strict=True)
    created_at: UTCDateTime
    expires_at: UTCDateTime
    state: Literal["reserved", "attached", "cleanup_pending", "cleaned"]
    size_bytes: PositiveInt = Field(le=2 * 1024**2)
    payload_size_bytes: Count = Field(le=1024**2)
    payload_sha256: SHA256
    download_url: ApiPath | None

    @model_validator(mode="after")
    def attachment_state(self):
        if self.download_url is not None and self.state != "attached":
            raise ValueError("download requires an attached part")
        if self.expires_at <= self.created_at:
            raise ValueError("capture expiry precedes creation")
        return self


class DebugCaptureList(DebugCaptureSummary):
    run_id: UUID
    items: list[DebugCaptureItem] = Field(max_length=60)

    @model_validator(mode="after")
    def part_bindings(self):
        unique([item.part_id for item in self.items], "capture part IDs")
        if len(self.items) != self.part_count or sum(item.state == "attached" for item in self.items) != self.attached_count:
            raise ValueError("capture counts must match the complete bounded registry")
        for item in self.items:
            if item.download_url is not None and (
                item.download_url != f"/api/v1/runs/{self.run_id}/debug/captures/{item.part_id}"
                or self.status in {"disabled", "expired", "unavailable"}
            ):
                raise ValueError("capture download must match an available run/part")
            if item.expires_at != self.expires_at:
                raise ValueError("part expiry must match immutable run capture policy")
        return self
