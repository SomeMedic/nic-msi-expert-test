"""Durable model stage execution: save typed artifacts before graph checkpoint.

Inference is outside database transactions. Stable step/call identities survive
lease takeover; only an explicit durable schema budget permits another call.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
import json
from typing import Any, Literal, Protocol, cast
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from expert_contracts.common import SHA256, StrictDTO
from expert_contracts.events import StageCompletedData, StageStartedData
from expert_observability.tracing import safe_span

from .checkpoint import ExecutionError, ExecutionToken, fenced_transaction, retry_database
from .llm.types import CallContext, LlmError, PreparedCall, RoleResult, ServingProfile, canonical_json, sha256

StageMessageCode = Literal[
    "RUN_SNAPSHOTTING", "RUN_ROUTING", "RUN_RETRIEVING", "RUN_RERANKING", "RUN_BUILDING_CONTEXT",
    "RUN_DRAFTING", "RUN_CHECKING_CITATIONS", "RUN_VALIDATING", "RUN_REPAIRING", "RUN_REVALIDATING",
    "RUN_RENDERING", "RUN_FINALIZING",
]
_STAGE_MESSAGE_CODES: dict[str, StageMessageCode] = {
    "snapshotting": "RUN_SNAPSHOTTING",
    "routing": "RUN_ROUTING",
    "retrieving": "RUN_RETRIEVING",
    "reranking": "RUN_RERANKING",
    "building_context": "RUN_BUILDING_CONTEXT",
    "drafting": "RUN_DRAFTING",
    "checking_citations": "RUN_CHECKING_CITATIONS",
    "validating": "RUN_VALIDATING",
    "repairing": "RUN_REPAIRING",
    "revalidating": "RUN_REVALIDATING",
    "rendering": "RUN_RENDERING",
    "finalizing": "RUN_FINALIZING",
}
_MODEL_OUTPUT_REFUSAL_FAILURE_KINDS = frozenset({"contract", "truncated", "provider_refusal"})


def _stage_message_code(stage: str) -> StageMessageCode:
    return _STAGE_MESSAGE_CODES[stage]


class StepIdentity(StrictDTO):
    model_config = ConfigDict(frozen=True)
    input_sha256: SHA256
    snapshot_id: UUID
    configuration_fingerprint: str = Field(min_length=1, max_length=1000)
    model_revision: str | None = Field(max_length=200)
    profile_sha256: SHA256 | None
    prompt_version: str | None = Field(max_length=200)
    prompt_sha256: SHA256 | None
    schema_sha256: SHA256 | None
    evidence_pack_id: UUID | None
    parent_artifact_ids: tuple[UUID, ...] = Field(max_length=8)

    @property
    def digest(self) -> str:
        return sha256(canonical_json(self.model_dump(mode="json")))


@dataclass(frozen=True)
class StepArtifact:
    id: UUID
    logical_step_key: str
    identity: StepIdentity
    kind: str
    status: Literal["completed", "failed"]
    private_json: str = field(repr=False)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> StepArtifact:
        return cls(row["id"], row["logical_step_key"], StepIdentity.model_validate(row["identity"]),
                   row["kind"], row["status"], canonical_json(row["private_json"]))

    def value[T: BaseModel](self, schema: type[T]) -> T:
        if self.status != "completed":
            raise ExecutionError("ARTIFACT_NOT_COMPLETED")
        return schema.model_validate_json(canonical_json(json.loads(self.private_json)["value"]))


class ModelOutputRejected(LlmError):
    """Terminal model-output failure backed by a durable failed artifact."""

    def __init__(self, artifact_id: UUID, *, call_id: UUID, schema_attempt: int, failure_kind: str):
        super().__init__("OUTPUT_SCHEMA_INVALID", call_id=call_id,
                         schema_attempt=schema_attempt, failure_kind=failure_kind)
        self.artifact_id = artifact_id


class StageStore(Protocol):
    token: ExecutionToken
    async def find(self, key: str, identity: StepIdentity) -> StepArtifact | None: ...
    async def save(self, artifact: StepArtifact) -> StepArtifact: ...
    async def event(self, operation: UUID, stage: str, event_type: str, attempt: int, data: dict) -> None: ...
    async def schema_retry(self, operation: UUID, role: str, failed_artifact: UUID) -> int: ...
    async def guard(self) -> None: ...


class PostgresStageStore:
    def __init__(self, pool: AsyncConnectionPool, token: ExecutionToken):
        self.pool, self.token = pool, token

    @retry_database
    async def _row(self, statement: str, parameters: tuple):
        async with fenced_transaction(self.pool, self.token) as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute(statement, parameters)
                return await cursor.fetchone()

    async def find(self, key: str, identity: StepIdentity) -> StepArtifact | None:
        row = await self._row("SELECT * FROM agent.find_step_artifact(%s,%s,%s,%s,%s)",
                             (*self.token.parameters, key, Jsonb(identity.model_dump(mode="json"))))
        return StepArtifact.from_row(row) if row is not None else None

    async def save(self, artifact: StepArtifact) -> StepArtifact:
        row = await self._row("SELECT * FROM agent.save_step_artifact(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (*self.token.parameters, artifact.id, artifact.logical_step_key,
             Jsonb(artifact.identity.model_dump(mode="json")), artifact.kind, artifact.status,
             Jsonb(json.loads(artifact.private_json))))
        if row is None:
            raise ExecutionError("DATABASE_UNAVAILABLE")
        return StepArtifact.from_row(row)

    async def event(self, operation: UUID, stage: str, event_type: str, attempt: int, data: dict) -> None:
        await self._row("SELECT * FROM agent.record_stage_event(%s,%s,%s,%s,%s,%s,%s,%s)",
                        (*self.token.parameters, operation, stage, event_type, attempt, Jsonb(data)))

    async def schema_retry(self, operation: UUID, role: str, failed_artifact: UUID) -> int:
        try:
            row = await self._row("SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s) AS attempt",
                                 (*self.token.parameters, operation, role, failed_artifact))
        except ExecutionError as error:
            if error.code in {"SCHEMA_RETRY_EXHAUSTED", "SCHEMA_RETRY_FORBIDDEN"}:
                raise ExecutionError("OUTPUT_SCHEMA_INVALID") from None
            raise
        if row is None or row["attempt"] != 1:
            raise ExecutionError("OUTPUT_SCHEMA_INVALID")
        return row["attempt"]

    @retry_database
    async def guard(self) -> None:
        async with fenced_transaction(self.pool, self.token):
            pass


class StageExecutor:
    def __init__(self, store: StageStore, *, snapshot_id: UUID, configuration_fingerprint: str,
                 profile: ServingProfile, deadline: float, cancel: asyncio.Event):
        self.store, self.snapshot_id, self.configuration = store, snapshot_id, configuration_fingerprint
        self.profile, self.deadline, self.cancel = profile, deadline, cancel

    def identity(self, prepared: PreparedCall, evidence_pack_id: UUID | None,
                 parents: tuple[UUID, ...]) -> StepIdentity:
        return StepIdentity(input_sha256=prepared.input_sha256, snapshot_id=self.snapshot_id,
            configuration_fingerprint=self.configuration, model_revision=self.profile.revision,
            profile_sha256=self.profile.fingerprint, prompt_version=prepared.prompt_version,
            prompt_sha256=prepared.prompt_sha256, schema_sha256=sha256(prepared.schema_json),
            evidence_pack_id=evidence_pack_id, parent_artifact_ids=parents)

    async def _guard(self) -> None:
        if self.cancel.is_set():
            raise asyncio.CancelledError
        if asyncio.get_running_loop().time() >= self.deadline:
            raise ExecutionError("DEADLINE_EXCEEDED")
        await self.store.guard()

    async def _event(self, artifact: StepArtifact, stage: str, event_type: str, attempt: int, data: dict) -> None:
        operation = uuid5(artifact.id, f"event:{self.store.token.epoch}:{event_type}")
        await self.store.event(operation, stage, event_type, attempt, data)

    async def _invoke[T](self, operation: Awaitable[T]) -> T:
        call = asyncio.ensure_future(operation)
        stopping = asyncio.create_task(self.cancel.wait())
        try:
            await asyncio.wait((call, stopping), return_when=asyncio.FIRST_COMPLETED)
            if self.cancel.is_set():
                raise asyncio.CancelledError
            return await call
        finally:
            for task in (call, stopping):
                if not task.done():
                    task.cancel()
            drain = asyncio.gather(call, stopping, return_exceptions=True)
            interrupted = False
            while not drain.done():
                try:
                    await asyncio.shield(drain)
                except asyncio.CancelledError:
                    interrupted = True
            if interrupted:
                raise asyncio.CancelledError

    @staticmethod
    def _model_output_rejected(artifact: StepArtifact, failed: dict) -> ModelOutputRejected | LlmError:
        if (failed["error_code"] != "OUTPUT_SCHEMA_INVALID"
                or failed["failure_kind"] not in _MODEL_OUTPUT_REFUSAL_FAILURE_KINDS):
            return LlmError(failed["error_code"], call_id=UUID(failed["call_id"]),
                            schema_attempt=failed["schema_attempt"], failure_kind=failed["failure_kind"])
        return ModelOutputRejected(artifact.id, call_id=UUID(failed["call_id"]),
                                   schema_attempt=failed["schema_attempt"], failure_kind=failed["failure_kind"])

    @asynccontextmanager
    async def _deadline(self):
        try:
            async with asyncio.timeout_at(self.deadline):
                yield
        except TimeoutError:
            raise ExecutionError("DEADLINE_EXCEEDED") from None

    async def model[T: BaseModel](self, prepared: PreparedCall, invoke: Callable[[CallContext], Awaitable[RoleResult[T]]],
            *, phase: Literal["initial", "repaired"] = "initial", evidence_pack_id: UUID | None = None,
            parents: tuple[UUID, ...] = ()) -> StepArtifact:
        identity = self.identity(prepared, evidence_pack_id, parents)
        stage = {"router": "routing", "drafter": "drafting", "critic": "validating", "repair": "repairing"}[prepared.role]
        if phase == "repaired" and prepared.role == "critic":
            stage = "revalidating"
        async with self._deadline():
            for schema_attempt in (0, 1):
                await self._guard()
                key = f"{prepared.role}.{phase}.schema{schema_attempt}"
                artifact_id = uuid5(self.store.token.run_id, f"artifact:{key}:{identity.digest}")
                call_id = uuid5(artifact_id, "model-call")
                context = CallContext(call_id, self.deadline, schema_attempt)
                attempt = 2 * (self.store.token.epoch - 1) + schema_attempt + 1
                artifact = await self.store.find(key, identity)
                if artifact is not None:
                    if (artifact.id != artifact_id or artifact.identity != identity or artifact.kind != prepared.role):
                        raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
                    if artifact.status == "completed":
                        await self._event(artifact, stage, "stage.started", attempt,
                            StageStartedData(message_code=_stage_message_code(stage)).model_dump(mode="json"))
                        await self._event(artifact, stage, "stage.completed", attempt,
                            StageCompletedData(duration_ms=0).model_dump(mode="json"))
                        return artifact
                else:
                    pending = StepArtifact(artifact_id, key, identity, prepared.role, "failed", "{}")
                    await self._event(pending, stage, "stage.started", attempt,
                        StageStartedData(message_code=_stage_message_code(stage)).model_dump(mode="json"))
                    result = None
                    failure = None
                    try:
                        span_name = {"router": "router.classify", "drafter": "drafter.generate",
                                     "critic": "critic.validate", "repair": "repair.generate"}[prepared.role]
                        with safe_span(span_name, run_id=self.store.token.run_id, snapshot_id=self.snapshot_id,
                                       stage=stage, model_revision=self.profile.revision,
                                       prompt_revision=prepared.prompt_sha256, schema_attempt=schema_attempt,
                                       repair_attempt=int(phase == "repaired")) as span:
                            result = await self._invoke(invoke(context))
                            span.set_attributes(input_tokens=result.provenance.input_tokens,
                                                output_tokens=result.provenance.output_tokens)
                    except LlmError as error:
                        # A schema-valid fabricated citation is audited as a draft;
                        # the mandatory precheck node then produces a bound refusal.
                        if error.failure_kind == "citation_binding" and error.private_result is not None:
                            result = cast(RoleResult[T], error.private_result)
                        else:
                            failure = {"error_code": error.code, "call_id": str(call_id),
                                       "schema_attempt": schema_attempt, "failure_kind": error.failure_kind}
                    await self._guard()
                    if result is not None:
                        provenance = result.provenance
                        if (provenance.call_id != call_id or provenance.schema_attempt != schema_attempt
                                or provenance.role != prepared.role or provenance.input_sha256 != identity.input_sha256
                                or provenance.model != self.profile.model
                                or provenance.messages_sha256 != sha256(prepared.messages_json)
                                or provenance.revision != identity.model_revision
                                or provenance.profile_sha256 != identity.profile_sha256
                                or provenance.prompt_sha256 != identity.prompt_sha256
                                or provenance.prompt_version != identity.prompt_version
                                or provenance.schema_sha256 != identity.schema_sha256
                                or provenance.evidence_manifest_sha256 != prepared.evidence_manifest_sha256):
                            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
                        payload = asdict(provenance) | {"call_id": str(provenance.call_id)}
                        artifact = await self.store.save(StepArtifact(artifact_id, key, identity, prepared.role,
                            "completed", canonical_json({"value": result.value.model_dump(mode="json"), "provenance": payload})))
                        await self._event(artifact, stage, "stage.completed", attempt,
                            StageCompletedData(duration_ms=provenance.elapsed_ms).model_dump(mode="json"))
                        return artifact
                    if failure is None:
                        raise ExecutionError("INTERNAL_ERROR")
                    artifact = await self.store.save(StepArtifact(artifact_id, key, identity, prepared.role,
                        "failed", canonical_json(failure)))
                failed = json.loads(artifact.private_json)
                if (schema_attempt == 0 and failed["error_code"] == "OUTPUT_SCHEMA_INVALID"
                        and failed["failure_kind"] in {"contract", "truncated"}):
                    try:
                        await self.store.schema_retry(uuid5(artifact.id, "schema-retry"), prepared.role, artifact.id)
                    except ExecutionError as error:
                        if error.code == "OUTPUT_SCHEMA_INVALID":
                            raise self._model_output_rejected(artifact, failed) from None
                        raise
                    await self._event(artifact, stage, "stage.started", attempt,
                        StageStartedData(message_code=_stage_message_code(stage)).model_dump(mode="json"))
                    await self._event(artifact, stage, "stage.retry_scheduled", attempt,
                        {"error_code": "OUTPUT_SCHEMA_INVALID", "retry_after_seconds": 0})
                    continue
                raise self._model_output_rejected(artifact, failed)
        raise ExecutionError("OUTPUT_SCHEMA_INVALID")
