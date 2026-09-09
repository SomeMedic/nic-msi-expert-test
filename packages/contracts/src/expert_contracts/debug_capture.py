"""Private opt-in exchange transport. Never export these roots to browser OpenAPI.

Raw bytes are not recovery artifacts or an inference cache. HTTP credentials,
headers, URLs and exceptions have no fields in this wire format.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import ConfigDict, Field, StrictBool, model_validator

from .common import NonBlank, PositiveInt, SHA256, StrictDTO, UTCDateTime

RAW_PART_MAX_BYTES = 1024**2
CAPTURE_ENVELOPE_MAX_BYTES = 2 * 1024**2
BASE64_MAX_CHARS = 4 * ((RAW_PART_MAX_BYTES + 2) // 3)
CaptureRole = Literal["router", "drafter", "critic", "repair"]


class DebugExecutionPolicy(StrictDTO):
    model_config = ConfigDict(frozen=True)
    enabled: StrictBool
    policy_version: Literal["p11.capture.v1"]
    expires_at: UTCDateTime | None

    @model_validator(mode="after")
    def enabled_expiry(self):
        if self.enabled and self.expires_at is None:
            raise ValueError("enabled capture requires authoritative expiry")
        return self


class DebugCaptureContext(StrictDTO):
    model_config = ConfigDict(frozen=True)
    run_id: UUID
    owner: UUID
    execution_epoch: PositiveInt
    call_id: UUID
    schema_attempt: int = Field(ge=0, le=1, strict=True)
    policy_version: Literal["p11.capture.v1"]
    expires_at: UTCDateTime
    configuration_fingerprint: NonBlank = Field(max_length=200)


class _DebugPart(StrictDTO):
    model_config = ConfigDict(frozen=True)
    role: CaptureRole
    request_sha256: SHA256
    payload_sha256: SHA256
    payload_encoding: Literal["base64"]
    payload_base64: str = Field(max_length=BASE64_MAX_CHARS, repr=False)
    model_revision: NonBlank = Field(max_length=200)
    profile_sha256: SHA256
    prompt_version: NonBlank = Field(max_length=200)
    prompt_sha256: SHA256
    schema_sha256: SHA256
    input_sha256: SHA256
    messages_sha256: SHA256

    def payload_bytes(self) -> bytes:
        try:
            raw = base64.b64decode(self.payload_base64, validate=True)
        except (ValueError, binascii.Error):
            raise ValueError("capture payload must be canonical base64") from None
        if len(raw) > RAW_PART_MAX_BYTES or base64.b64encode(raw).decode("ascii") != self.payload_base64:
            raise ValueError("capture payload size or encoding is invalid")
        if hashlib.sha256(raw).hexdigest() != self.payload_sha256:
            raise ValueError("capture payload hash mismatch")
        return raw

    @model_validator(mode="after")
    def exact_payload(self):
        self.payload_bytes()
        return self


class DebugRequestPart(_DebugPart):
    part: Literal["request"]

    @model_validator(mode="after")
    def request_identity(self):
        if self.request_sha256 != self.payload_sha256:
            raise ValueError("request part must contain the exact hashed request bytes")
        return self


class DebugResponsePart(_DebugPart):
    part: Literal["response"]
    http_status: int = Field(ge=100, le=599, strict=True)


DebugCapturePart = Annotated[DebugRequestPart | DebugResponsePart, Field(discriminator="part")]


class DebugCaptureSubmission(StrictDTO):
    model_config = ConfigDict(frozen=True)
    context: DebugCaptureContext
    part: DebugCapturePart = Field(repr=False)


class DebugCaptureReceipt(StrictDTO):
    model_config = ConfigDict(frozen=True)
    state: Literal["attached", "unavailable", "oversized"]
    part_id: UUID | None
    expires_at: UTCDateTime | None

    @model_validator(mode="after")
    def attachment_identity(self):
        if self.state == "attached" and (self.part_id is None or self.expires_at is None):
            raise ValueError("attached receipt requires persisted identity and expiry")
        return self


class DebugObjectRef(StrictDTO):
    """Backend-only exact object identity returned by controlled SQL routines."""
    part_id: UUID
    object_id: UUID
    bucket: Literal["debug"]
    object_key: NonBlank = Field(max_length=500)
    object_version_id: NonBlank | None = Field(max_length=1000)
    sha256: SHA256
    size_bytes: PositiveInt = Field(le=CAPTURE_ENVELOPE_MAX_BYTES)
    expires_at: UTCDateTime
    state: Literal["reserved", "attached", "cleanup_pending", "cleaned"]

    @model_validator(mode="after")
    def exact_debug_key(self):
        pieces = self.object_key.split("/")
        if (len(pieces) != 8 or pieces[0] != "runs" or pieces[2] != "debug"
                or pieces[5] not in {"request", "response"} or not pieces[3].isdigit()
                or str(int(pieces[3])) != pieces[3] or int(pieces[3]) <= 0
                or pieces[6] != str(self.object_id) or pieces[7] != self.sha256 + ".json"):
            raise ValueError("invalid exact debug object key")
        for position in (1, 4):
            if str(UUID(pieces[position])) != pieces[position]:
                raise ValueError("invalid canonical debug identity")
        return self


class DebugCleanupClaim(DebugObjectRef):
    state: Literal["cleanup_pending"]
    claim_token: UUID
    claim_until: UTCDateTime


def capture_object_bytes(submission: DebugCaptureSubmission) -> bytes:
    """Canonical immutable JSON object; its SHA/size differ from the raw part."""
    context, part = submission.context, submission.part
    envelope = {
        "schema_version": "p11.capture-part.v1", "run_id": str(context.run_id), "call_id": str(context.call_id),
        "execution_epoch": context.execution_epoch, "part": part.part, "role": part.role,
        "schema_attempt": context.schema_attempt, "request_sha256": part.request_sha256,
        "payload_sha256": part.payload_sha256, "payload_encoding": "base64", "payload_base64": part.payload_base64,
    }
    if isinstance(part, DebugResponsePart):
        envelope["http_status"] = part.http_status
    value = json.dumps(envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(value) > CAPTURE_ENVELOPE_MAX_BYTES:
        raise ValueError("capture object exceeds the envelope limit")
    return value


def capture_registry_metadata(submission: DebugCaptureSubmission) -> dict[str, str | int]:
    """Closed SQL metadata; no payload bytes or caller-supplied object locator."""
    context, part = submission.context, submission.part
    return {
        "policy_version": context.policy_version, "configuration_fingerprint": context.configuration_fingerprint,
        "model_revision": part.model_revision, "profile_sha256": part.profile_sha256,
        "prompt_version": part.prompt_version, "prompt_sha256": part.prompt_sha256,
        "schema_sha256": part.schema_sha256, "input_sha256": part.input_sha256,
        "messages_sha256": part.messages_sha256, "payload_size_bytes": len(part.payload_bytes()),
    }
