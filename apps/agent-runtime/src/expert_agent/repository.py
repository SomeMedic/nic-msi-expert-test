"""Short fenced graph operations; no inference, HTTP or public draft access."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timezone
import json
from uuid import UUID, uuid5

from psycopg import Error as PostgresError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from expert_contracts.internal import EvidencePack
from expert_contracts.runs import FinalAnswer, RefusalResult, SnapshotInfo

from .checkpoint import ExecutionError, ExecutionToken, database_error, fenced_transaction, retry_database
from .debug_capture import CapturePolicy
from .execution import PostgresStageStore, StepArtifact, StepIdentity
from .llm.prompts import render_evidence
from .llm.types import canonical_json, sha256
from .retrieval.types import PackedEvidence
from .validation import PolicyDecision


@dataclass(frozen=True)
class RunContext:
    run_id: UUID
    principal_id: str = field(repr=False)
    question: str = field(repr=False)
    configuration_fingerprint: str
    snapshot: SnapshotInfo
    version_labels: tuple[tuple[UUID, str | None], ...]
    capture_policy: CapturePolicy = CapturePolicy(False)


def pack_manifest(packed: PackedEvidence) -> str:
    pack = packed.pack
    render_evidence(pack, packed.bindings_json)
    return canonical_json({"run_id": str(pack.run_id), "snapshot_id": str(pack.snapshot_id),
        "units": [unit.model_dump(mode="json") for unit in pack.units], "bindings": json.loads(packed.bindings_json)})


def empty_pack(run_id: UUID, snapshot_id: UUID) -> PackedEvidence:
    manifest = canonical_json({"run_id": str(run_id), "snapshot_id": str(snapshot_id), "units": [], "bindings": []})
    digest = sha256(manifest)
    return PackedEvidence(EvidencePack(pack_id=uuid5(run_id, digest), run_id=run_id, snapshot_id=snapshot_id,
        units=(), llm_token_count=0, manifest_hash=digest), "[]", ())


class GraphRepository:
    def __init__(self, pool: AsyncConnectionPool, token: ExecutionToken, configuration_fingerprint: str):
        self.pool, self.token, self.configuration = pool, token, configuration_fingerprint
        self.store = PostgresStageStore(pool, token)

    async def trace_id(self) -> str | None:
        row = await self.store._row("SELECT trace_id FROM agent.runs WHERE id=%s", (self.token.run_id,))
        if row is None:
            raise ExecutionError("RUN_NOT_FOUND")
        return row["trace_id"]

    async def bind_trace(self, trace_id: str) -> None:
        row = await self.store._row("SELECT agent.bind_run_trace(%s,%s,%s,%s) AS trace_id",
                                    (*self.token.parameters, trace_id))
        if row is None or row["trace_id"] != trace_id:
            raise ExecutionError("STEP_IDENTITY_MISMATCH")

    @retry_database
    async def context(self) -> RunContext:
        async with fenced_transaction(self.pool, self.token, repeatable_read=True) as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("SELECT agent.capture_snapshot(%s,%s,%s)", self.token.parameters)
                await cursor.execute("SELECT r.id,r.principal_id,r.question,r.configuration_fingerprint,s.id AS snapshot_id,"
                    "s.captured_at,r.debug_capture FROM agent.runs r JOIN agent.kb_snapshots s ON s.id=r.snapshot_id WHERE r.id=%s",
                    (self.token.run_id,))
                row = await cursor.fetchone()
                if row is None or row["configuration_fingerprint"] != self.configuration:
                    raise ExecutionError("STEP_IDENTITY_MISMATCH")
                await cursor.execute("SELECT i.document_version_id,v.version_label FROM agent.kb_snapshot_items i "
                    "JOIN app.document_versions v ON v.id=i.document_version_id WHERE i.snapshot_id=%s "
                    "ORDER BY i.document_version_id", (row["snapshot_id"],))
                versions = await cursor.fetchall()
                capture_policy = CapturePolicy(False)
                if row["debug_capture"]:
                    await cursor.execute("SELECT * FROM agent.get_run_capture_policy(%s,%s,%s)",
                                         self.token.parameters)
                    policy = await cursor.fetchone()
                    if policy is None:
                        raise ExecutionError("STEP_IDENTITY_MISMATCH")
                    capture_policy = CapturePolicy(**policy)
                return RunContext(row["id"], row["principal_id"], row["question"], row["configuration_fingerprint"],
                    SnapshotInfo(id=row["snapshot_id"], captured_at=row["captured_at"].astimezone(timezone.utc),
                        version_count=len(versions)), tuple((v["document_version_id"], v["version_label"]) for v in versions),
                    capture_policy)

    async def load_pack(self, pack_id: UUID | None = None) -> PackedEvidence | None:
        row = await self.store._row("SELECT ep.pack,ep.canonical_manifest FROM agent.evidence_packs ep "
            "JOIN agent.runs r ON r.id=ep.run_id AND r.snapshot_id=ep.snapshot_id WHERE ep.run_id=%s",
            (self.token.run_id,))
        if row is None:
            if pack_id is not None:
                raise ExecutionError("EVIDENCE_PACK_MISMATCH")
            return None
        pack = EvidencePack.model_validate(row["pack"])
        if pack.run_id != self.token.run_id or (pack_id is not None and pack.pack_id != pack_id):
            raise ExecutionError("EVIDENCE_PACK_MISMATCH")
        manifest = json.loads(row["canonical_manifest"])
        packed = PackedEvidence(pack, canonical_json(manifest["bindings"]), ())
        if pack_manifest(packed) != row["canonical_manifest"]:
            raise ExecutionError("EVIDENCE_PACK_MISMATCH")
        return packed

    async def save_pack(self, packed: PackedEvidence) -> PackedEvidence:
        await self.store._row("SELECT * FROM agent.save_evidence_pack(%s,%s,%s,%s,%s)",
            (*self.token.parameters, Jsonb(packed.pack.model_dump(mode="json")), pack_manifest(packed)))
        saved = await self.load_pack(packed.pack.pack_id)
        if saved is None:
            raise ExecutionError("EVIDENCE_PACK_MISMATCH")
        return saved

    async def artifact(self, artifact_id: UUID, *, kind: tuple[str, ...], pack_id: UUID | None = None) -> StepArtifact:
        row = await self.store._row("SELECT a.* FROM agent.run_step_artifacts a JOIN agent.runs r ON r.id=a.run_id "
            "AND r.snapshot_id=a.snapshot_id WHERE a.run_id=%s AND a.id=%s", (self.token.run_id, artifact_id))
        if row is None:
            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
        artifact = StepArtifact.from_row(row)
        if (artifact.kind not in kind or artifact.identity.configuration_fingerprint != self.configuration
                or (pack_id is not None and artifact.identity.evidence_pack_id != pack_id)):
            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
        return artifact

    async def local_artifact(self, context: RunContext, key: str, kind: str, packed: PackedEvidence,
                             private: dict, *, parents: tuple[UUID, ...] = (), failed: bool = False) -> StepArtifact:
        identity = StepIdentity(input_sha256=sha256(canonical_json({"key": key, "pack": packed.pack.manifest_hash,
                "parents": [str(p) for p in parents]})), snapshot_id=context.snapshot.id,
            configuration_fingerprint=self.configuration, model_revision=None, profile_sha256=None,
            prompt_version=None, prompt_sha256=None, schema_sha256=None,
            evidence_pack_id=packed.pack.pack_id, parent_artifact_ids=parents)
        artifact_id = uuid5(self.token.run_id, f"artifact:{key}:{identity.digest}")
        if failed:
            private = {"error_code": "VERIFICATION_FAILED", "call_id": str(uuid5(artifact_id, "precheck")),
                       "schema_attempt": 0, "failure_kind": "citation_binding"}
        return await self.store.save(StepArtifact(artifact_id, key, identity, kind,
            "failed" if failed else "completed", canonical_json(private)))

    async def policy_decision(self, context: RunContext, packed: PackedEvidence, *, draft: UUID, critic: UUID,
                              decision: PolicyDecision, raw_critic_policy: str) -> StepArtifact:
        private = {"draft_artifact_id": str(draft), "critic_artifact_id": str(critic),
            "action": decision.action, "outcome": decision.status, "reason": decision.reason,
            "refusal_code": "VERIFICATION_FAILED" if decision.action == "refuse" else None,
            "raw_critic_policy": raw_critic_policy, "evidence_manifest_sha256": packed.pack.manifest_hash,
            "policy_version": "p08.deterministic_policy.v1"}
        identity = StepIdentity(input_sha256=sha256(canonical_json({"key": "policy.decision",
                "pack": packed.pack.manifest_hash, "parents": [str(draft), str(critic)], "private": private})),
            snapshot_id=context.snapshot.id, configuration_fingerprint=self.configuration, model_revision=None,
            profile_sha256=None, prompt_version=None, prompt_sha256=None, schema_sha256=None,
            evidence_pack_id=packed.pack.pack_id, parent_artifact_ids=(draft, critic))
        artifact_id = uuid5(self.token.run_id, f"artifact:policy.decision:{identity.digest}")
        return await self.store.save(StepArtifact(artifact_id, "policy.decision", identity, "policy",
            "completed", canonical_json(private)))

    async def repair(self, decision_artifact_id: UUID) -> int:
        row = await self.store._row("SELECT agent.consume_repair(%s,%s,%s,%s,%s) AS attempt",
            (*self.token.parameters, uuid5(decision_artifact_id, "content-repair"), decision_artifact_id))
        if row is None or row["attempt"] != 1:
            raise ExecutionError("REPAIR_EXHAUSTED")
        return row["attempt"]

    @retry_database
    async def finalize(self, result: FinalAnswer | RefusalResult, *, draft: UUID | None,
                       critic: UUID | None, decision: UUID | None) -> str:
        # Finalizer owns cancellation/deadline precedence. A generic pre-guard
        # would prevent it from committing the winning terminal transition.
        operation = uuid5(self.token.run_id, "finalize:" + sha256(canonical_json({
            "result": result.model_dump(mode="json"), "draft": str(draft), "critic": str(critic), "decision": str(decision)})))
        try:
            async with self.pool.connection() as connection:
                async with connection.transaction():
                    await connection.execute("SET LOCAL statement_timeout='10s'")
                    await connection.execute("SET LOCAL lock_timeout='5s'")
                    async with connection.cursor(row_factory=dict_row) as cursor:
                        await cursor.execute("SELECT * FROM agent.finalize_run(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                            (*self.token.parameters, operation, result.kind, Jsonb(result.model_dump(mode="json")),
                             draft, critic, decision, None))
                        row = await cursor.fetchone()
                        if row is None:
                            raise ExecutionError("DATABASE_UNAVAILABLE")
                        return row["status"]
        except PostgresError as error:
            code = error.diag.message_primary
            if code == "TERMINAL_CONFLICT":
                raise ExecutionError(code) from None
            raise database_error(error) from None
