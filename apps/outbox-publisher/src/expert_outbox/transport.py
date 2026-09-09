"""Allowlisted Redis transport envelopes; PostgreSQL remains authoritative.

Source 08, physical pages 1 and 3: duplicate event IDs are expected, and
notification streams are broadcast hints, never consumer-group authority.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass(frozen=True)
class OutboxEvent:
    event_id: UUID
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    schema_version: int
    topic: str
    payload: Any
    publish_attempts: int
    claim_token: UUID
    # Read from the claimed PG aggregate, never accepted from payload/Redis.
    trace_id: str | None = None


@dataclass(frozen=True)
class RedisMessage:
    stream: str
    fields: dict[str, str]
    notification: bool = False


class InvalidOutboxMessage(ValueError):
    def __init__(self, code: str = "OUTBOX_INVALID_PAYLOAD"):
        # No untrusted topic, payload or exception text enters the error.
        super().__init__(code)
        self.code = code


def _uuid(value: Any) -> str:
    if isinstance(value, UUID):
        return str(value)
    if not isinstance(value, str) or len(value) != 36:
        raise InvalidOutboxMessage()
    try:
        return str(UUID(value))
    except ValueError:
        raise InvalidOutboxMessage() from None


def prepare_message(
    event: OutboxEvent, ingestion_stream: str, notification_prefix: str = "expert:",
) -> RedisMessage:
    """Copy explicitly permitted IDs only; never serialize arbitrary row JSON."""
    if event.topic not in {"ingestion.jobs.v1", "ingestion.events.v1", "run.events", "knowledge.events"}:
        raise InvalidOutboxMessage("OUTBOX_INVALID_TOPIC")
    if type(event.schema_version) is not int or event.schema_version != 1 or not isinstance(event.payload, dict):
        raise InvalidOutboxMessage()
    payload = event.payload
    fields = {"schema_version": "1", "event_id": str(event.event_id)}

    if event.topic == "knowledge.events":
        if event.aggregate_type != "document" or event.event_type not in {
                "document.published", "document.security_revoked", "document.deactivated", "document.archived"}:
            raise InvalidOutboxMessage()
        if event.event_type == "document.deactivated":
            if set(payload) != {"document_id", "version_id"} or _uuid(payload["document_id"]) != str(event.aggregate_id):
                raise InvalidOutboxMessage()
            fields.update(document_id=str(event.aggregate_id), version_id=_uuid(payload["version_id"]))
            return RedisMessage(notification_prefix + "knowledge.events", fields, True)
        if event.event_type in {"document.security_revoked", "document.archived"}:
            if set(payload) != {"document_id"} or _uuid(payload["document_id"]) != str(event.aggregate_id):
                raise InvalidOutboxMessage()
            fields["document_id"] = str(event.aggregate_id)
            return RedisMessage(notification_prefix + "knowledge.events", fields, True)
        keys = {"publication_id", "version_id", "index_generation_id"}
        if set(payload) != keys:
            raise InvalidOutboxMessage()
        fields.update({key: _uuid(payload[key]) for key in keys})
        return RedisMessage(notification_prefix + "knowledge.events", fields, True)

    is_run = event.topic == "run.events"
    identity = "run_id" if is_run else "job_id"
    notification = event.topic != "ingestion.jobs.v1"
    keys = {"schema_version", "event_id", identity} | ({"sequence"} if notification else set())
    if set(payload) != keys or type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise InvalidOutboxMessage()
    if _uuid(payload["event_id"]) != str(event.event_id):
        raise InvalidOutboxMessage()
    identifier = _uuid(payload[identity])
    if identifier != str(event.aggregate_id) or event.aggregate_type != ("run" if is_run else "ingestion_job"):
        raise InvalidOutboxMessage()
    fields[identity] = identifier
    if notification:
        sequence = payload["sequence"]
        if type(sequence) is not int or not 1 <= sequence <= 2**63 - 1:
            raise InvalidOutboxMessage()
        fields["sequence"] = str(sequence)
        stem = "run.events:" if is_run else "ingestion.events:"
        return RedisMessage(notification_prefix + stem + identifier, fields, True)
    if event.event_type != "ingestion.dispatch":
        raise InvalidOutboxMessage()
    return RedisMessage(ingestion_stream, fields)
