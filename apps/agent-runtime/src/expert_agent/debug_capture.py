"""Opt-in private capture transport; no storage credentials, queue or model retry."""
from __future__ import annotations

import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal, Protocol
from uuid import UUID, uuid5

from pydantic import SecretStr

from expert_clients.http import ServiceClient
from expert_contracts.debug_capture import (
    DebugCaptureContext, DebugCaptureReceipt, DebugCaptureSubmission, DebugRequestPart, DebugResponsePart,
)

CAPTURE_TIMEOUT_SECONDS = 2.0
MAX_CAPTURE_BYTES = 1024 * 1024
type CapturePart = DebugRequestPart | DebugResponsePart


@dataclass(frozen=True)
class CapturePolicy:
    enabled: bool
    policy_version: Literal["p11.capture.v1"] = "p11.capture.v1"
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        if (type(self.enabled) is not bool or self.policy_version != "p11.capture.v1"
                or (self.enabled and self.expires_at is None)
                or (self.expires_at is not None and (
                    self.expires_at.tzinfo is None or self.expires_at.utcoffset() is None))):
            raise ValueError("Invalid private capture policy")

    @property
    def active(self) -> bool:
        return (self.enabled and self.expires_at is not None
                and self.expires_at > datetime.now(timezone.utc))


@dataclass(frozen=True)
class CaptureContext:
    run_id: UUID
    owner: UUID
    execution_epoch: int
    call_id: UUID
    schema_attempt: int
    policy: CapturePolicy
    configuration_fingerprint: str

    def wire(self) -> DebugCaptureContext:
        if self.policy.expires_at is None:
            raise ValueError("Invalid private capture policy")
        return DebugCaptureContext(run_id=self.run_id, owner=self.owner,
            execution_epoch=self.execution_epoch, call_id=self.call_id, schema_attempt=self.schema_attempt,
            policy_version=self.policy.policy_version, expires_at=self.policy.expires_at,
            configuration_fingerprint=self.configuration_fingerprint)


class DebugCaptureSink(Protocol):
    async def submit(self, context: CaptureContext, part: CapturePart) -> DebugCaptureReceipt: ...


class HttpDebugCaptureSink:
    """The composition owns the client's lifetime; payloads never enter telemetry."""

    def __init__(self, client: ServiceClient, *, sensitive_values: tuple[SecretStr, ...] = ()):
        self._client = client
        self._sensitive = tuple(value.get_secret_value().encode("utf-8")
                                for value in sensitive_values if value.get_secret_value())

    async def submit(self, context: CaptureContext, part: CapturePart) -> DebugCaptureReceipt:
        if not context.policy.active:
            return DebugCaptureReceipt(state="unavailable", part_id=None, expires_at=None)
        raw = base64.b64decode(part.payload_base64, validate=True)
        if any(value in raw for value in self._sensitive):
            return DebugCaptureReceipt(state="unavailable", part_id=None, expires_at=None)
        submission = DebugCaptureSubmission(context=context.wire(), part=part)
        if any(value in submission.model_dump_json().encode("utf-8") for value in self._sensitive):
            return DebugCaptureReceipt(state="unavailable", part_id=None, expires_at=None)
        return await self._client.post(f"/internal/v1/runs/{context.run_id}/debug-parts", submission,
            DebugCaptureReceipt, request_id=uuid5(context.call_id,
                f"capture:{context.execution_epoch}:{part.part}"), timeout_seconds=CAPTURE_TIMEOUT_SECONDS)


async def submit_bounded(sink: DebugCaptureSink, context: CaptureContext,
                         part: CapturePart, *, deadline: float) -> None:
    """Optional capture failure cannot turn a model result into a business failure.

    A full allowance must remain. The absolute run deadline is never reset or
    extended. Caller cancellation propagates through the inline transport and
    closes its response; there is no detached delivery to outlive the run.
    """
    if not context.policy.active or deadline - asyncio.get_running_loop().time() < CAPTURE_TIMEOUT_SECONDS:
        return
    delivery = asyncio.ensure_future(sink.submit(context, part))
    try:
        async with asyncio.timeout_at(min(deadline,
                asyncio.get_running_loop().time() + CAPTURE_TIMEOUT_SECONDS)):
            await asyncio.shield(delivery)
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise
    except Exception:
        # Never log the exception: validation and dependency errors may retain
        # their private input internally.
        pass
    finally:
        if not delivery.done():
            delivery.cancel()
        interrupted = False
        while not delivery.done():
            try:
                await asyncio.shield(delivery)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    interrupted = True
            except Exception:
                break
        # Retrieve any private exception without logging or retaining it.
        if not delivery.cancelled():
            delivery.exception()
        if interrupted:
            raise asyncio.CancelledError
