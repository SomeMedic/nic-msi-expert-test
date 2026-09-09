"""Real named LangGraph + SQL022 + restricted role, with explicit mock ML/source input.

These are orchestration/recovery/policy gates. They do not prove model entailment
or source extraction; those independent real-model and PDF gates are recorded.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
import pytest

from expert_agent.checkpoint import ExecutionToken
from expert_agent.execution import PostgresStageStore
from expert_agent.graph import AnswerGraph
from expert_agent.retrieval.repository import RetrievalRepository
from expert_agent.retrieval.service import PreparedRetrieval, RetrievalResult
from expert_agent.retrieval.types import PackedEvidence, prepare_query
from expert_contracts.internal import EvidencePack
from expert_contracts.runs import FinalAnswer, RefusalResult
from scripts.migrate import run_migrations
from tests.contract.test_llm_gateway import Server, gateway
from tests.integration.test_agent_execution import critic_value, draft_value, make_pack, query, setup_run


@pytest.fixture
def db(isolated_database):
    run_migrations(isolated_database.migrate_dsn.get_secret_value(), target_revision="p08_023_repeated_draft_guard")
    return isolated_database


class RoleServer(Server):
    def __init__(self, scenario="confirmed"):
        super().__init__()
        self.scenario, self.roles, self.critic_calls = scenario, [], 0
        self.fault_role = scenario.removesuffix("_provider_refusal").removesuffix("_schema_failed")
        self.fault_kind = ("provider_refusal" if scenario.endswith("_provider_refusal")
                           else "schema_failed" if scenario.endswith("_schema_failed") else None)

    async def __call__(self, request):
        if request.url.path == "/v1/chat/completions":
            self.mutate = lambda value: None
            body = json.loads(request.content)
            payload = json.loads(body["messages"][1]["content"])
            role = ("router" if "DESCRIPTORS" in payload else "repair" if "VERDICTS" in payload
                    else "critic" if "DRAFT" in payload else "drafter")
            self.roles.append(role)
            if role == "router":
                self.output = {"classification": "out_of_scope" if self.scenario == "out_of_scope" else "uncertain",
                               "matched_descriptor_ids": [], "reason": "Synthetic routing"}
            elif role in ("drafter", "repair"):
                self.output = draft_value("FAKE_ID" if self.scenario == "fabricated" else "E001")
                if self.scenario == "repeated_claim":
                    self.output["claims"].append(self.output["claims"][0] | {"claim_id": "c2"})
                if (self.scenario == "insufficient" or (role == "repair" and self.scenario == "empty_repair")):
                    self.output = {"disposition": "insufficient_evidence", "claims": [], "limitation": None}
            else:
                self.critic_calls += 1
                partial = (self.scenario in {"repair", "empty_repair", "repeat_partial", "repair_schema_failed",
                                             "critic_repaired_schema_failed"}
                           and (self.critic_calls == 1 or self.scenario == "repeat_partial"))
                self.output = critic_value("unsupported" if self.scenario == "unsupported" else
                                          "partially_supported" if partial else "supported")
            target_role = self.fault_role
            if self.fault_role == "critic_initial" and self.critic_calls <= 2:
                target_role = "critic"
            if self.fault_role == "critic_repaired" and self.critic_calls >= 2:
                target_role = "critic"
            if role == target_role:
                if self.fault_kind == "schema_failed":
                    self.output = "{"
                elif self.fault_kind == "provider_refusal":
                    self.mutate = lambda value: value["choices"][0]["message"].update(refusal="PRIVATE_REFUSAL")
        return await super().__call__(request)


class FakeRetrieval:
    """Explicit synthetic retrieval result, backed by real pinned SQL identities."""
    def __init__(self, pool, packed):
        self.repository, self.packed, self.calls = RetrievalRepository(pool), packed, 0
        self.prepare_calls = 0

    async def prepare(self, run_id, principal_id, *, cancel, deadline):
        self.prepare_calls += 1
        snapshot = await self.repository.snapshot(run_id, principal_id)
        return PreparedRetrieval(snapshot, prepare_query(snapshot.original_question), (), None, None)

    async def retrieve(self, prepared, *, token_budget, count_evidence, cancel, deadline, route):
        self.calls += 1
        if not self.packed.pack.units:
            return RetrievalResult("no_context", "empty_snapshot", None, (), None)
        count = await count_evidence(self.packed.pack, self.packed.bindings_json)
        assert count <= token_budget
        packed = replace(self.packed, pack=self.packed.pack.model_copy(update={"llm_token_count": count}))
        return RetrievalResult("context", "retrieved", packed, (), "current")


def pool(db):
    return AsyncConnectionPool(db.role_dsns["runtime"].get_secret_value(), min_size=0, max_size=4,
        open=False, kwargs={"autocommit": True, "prepare_threshold": 0, "row_factory": dict_row})


def packed(db, execution, snapshot, generation):
    value, manifest = make_pack(db, execution, snapshot, generation)
    return PackedEvidence(EvidencePack.model_validate(value), json.dumps(json.loads(manifest)["bindings"]), ())


@pytest.mark.parametrize("scenario,empty,status,refusal,roles", [
    ("confirmed", False, "completed", None, ["router", "drafter", "critic"]),
    ("out_of_scope", False, "refused", "OUT_OF_SCOPE", ["router"]),
    ("empty", True, "refused", "NO_RELEVANT_CONTEXT", []),
    ("out_of_scope", True, "refused", "NO_RELEVANT_CONTEXT", []),
    ("insufficient", False, "refused", "NO_RELEVANT_CONTEXT", ["router", "drafter"]),
    ("fabricated", False, "refused", "VERIFICATION_FAILED", ["router", "drafter"]),
    ("repeated_claim", False, "refused", "VERIFICATION_FAILED", ["router", "drafter"]),
    ("unsupported", False, "refused", "VERIFICATION_FAILED", ["router", "drafter", "critic"]),
    ("repair", False, "completed", None, ["router", "drafter", "critic", "repair", "critic"]),
    ("empty_repair", False, "refused", "VERIFICATION_FAILED", ["router", "drafter", "critic", "repair"]),
    ("repeat_partial", False, "refused", "VERIFICATION_FAILED", ["router", "drafter", "critic", "repair", "critic"]),
])
async def test_actual_graph_all_publication_branches(db, scenario, empty, status, refusal, roles):
    execution, snapshot, generation = setup_run(db, empty=empty)
    server, token = RoleServer(scenario), ExecutionToken(*execution)
    client = await gateway(server)
    try:
        async with pool(db) as connections:
            retrieval = FakeRetrieval(connections, packed(db, execution, snapshot, generation))
            graph = AnswerGraph(connections, retrieval, client, "test-config")
            await graph.execute(token, asyncio.Event(), asyncio.get_running_loop().time() + 30)
        run = query(db, "SELECT * FROM agent.runs WHERE id=%s", (token.run_id,))[0]
        result = query(db, "SELECT public_result FROM agent.run_results WHERE run_id=%s", (token.run_id,), "backend")[0]["public_result"]
        assert run["status"] == status and run["refusal_code"] == refusal and server.roles == roles
        if empty:
            # Source05 §05.6: an empty KB cannot establish domain mismatch.
            # It must not call even the embedding/descriptor/model boundary.
            assert retrieval.prepare_calls == retrieval.calls == 0
        if status == "completed":
            public = FinalAnswer.model_validate(result)
            assert public.validation.repair_used == (scenario == "repair") and public.claims[0].text == draft_value()["claims"][0]["text"]
        else:
            assert RefusalResult.model_validate(result).code == refusal
        assert run["repair_attempts"] == int("repair" in roles)
        events = query(db, "SELECT event_type,sequence,public_payload FROM agent.run_events WHERE run_id=%s ORDER BY sequence",
                       (token.run_id,), "backend")
        assert [row["sequence"] for row in events] == list(range(1, len(events) + 1))
        assert events[-1]["event_type"] == "run." + status
        assert "Synthetic routing" not in json.dumps(events) and "Synthetic contract test" not in json.dumps(events)
        assert len(query(db, "SELECT id FROM agent.evidence_packs WHERE run_id=%s", (token.run_id,))) == int(empty or scenario != "out_of_scope")
        checkpoints = query(db, "SELECT checkpoint FROM agent.checkpoints WHERE thread_id=%s", (str(token.run_id),))
        assert len(checkpoints) > 3
        assert "Требование установлено" not in json.dumps(checkpoints, ensure_ascii=False)
    finally:
        await client.aclose()


async def test_actual_graph_recovery_reuses_pack_draft_and_checkpoint_after_crash(db, monkeypatch):
    await exercise_recovery(db, monkeypatch)


async def exercise_recovery(db, monkeypatch):
    execution, snapshot, generation = setup_run(db)
    server, client = RoleServer(), None
    client = await gateway(server)
    original = PostgresStageStore.event
    crash = True

    async def event(store, operation, stage, event_type, attempt, data):
        nonlocal crash
        if crash and stage == "drafting" and event_type == "stage.completed":
            crash = False
            raise RuntimeError("injected crash after private artifact before checkpoint")
        return await original(store, operation, stage, event_type, attempt, data)

    monkeypatch.setattr(PostgresStageStore, "event", event)
    try:
        async with pool(db) as connections:
            retrieval = FakeRetrieval(connections, packed(db, execution, snapshot, generation))
            graph = AnswerGraph(connections, retrieval, client, "test-config")
            with pytest.raises(RuntimeError, match="injected crash"):
                await graph.execute(ExecutionToken(*execution), asyncio.Event(), asyncio.get_running_loop().time() + 30)
            assert server.roles == ["router", "drafter"]
            query(db, "UPDATE agent.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=%s RETURNING id",
                  (execution[0],), "migrate")
            owner = uuid4()
            acquired = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (execution[0], owner))[0]
            await graph.execute(ExecutionToken(execution[0], owner, acquired["execution_epoch"]),
                                asyncio.Event(), asyncio.get_running_loop().time() + 30)
            assert server.roles == ["router", "drafter", "critic"] and retrieval.calls == 1
            assert query(db, "SELECT status FROM agent.runs WHERE id=%s", (execution[0],))[0]["status"] == "completed"
    finally:
        await client.aclose()
    return execution[0]


@pytest.mark.parametrize("scenario,roles,budget_role", [
    ("router_schema_failed", ["router", "router"], "router"),
    ("drafter_schema_failed", ["router", "drafter", "drafter"], "drafter"),
    ("critic_initial_schema_failed", ["router", "drafter", "critic", "critic"], "critic"),
    ("repair_schema_failed", ["router", "drafter", "critic", "repair", "repair"], "repair"),
    ("critic_repaired_schema_failed", ["router", "drafter", "critic", "repair", "critic", "critic"], "critic"),
])
async def test_actual_graph_model_output_rejection_refuses_with_durable_failed_artifact(db, scenario, roles,
                                                                                       budget_role):
    execution, snapshot, generation = setup_run(db)
    server = RoleServer(scenario)
    client = await gateway(server)
    try:
        async with pool(db) as connections:
            retrieval = FakeRetrieval(connections, packed(db, execution, snapshot, generation))
            graph = AnswerGraph(connections, retrieval, client, "test-config")
            await graph.execute(ExecutionToken(*execution), asyncio.Event(), asyncio.get_running_loop().time() + 30)
        assert server.roles == roles
        run = query(db, "SELECT status,refusal_code FROM agent.runs WHERE id=%s", (execution[0],))[0]
        result = query(db, "SELECT public_result FROM agent.run_results WHERE run_id=%s", (execution[0],), "backend")[0]["public_result"]
        assert run == {"status": "refused", "refusal_code": "VERIFICATION_FAILED"}
        assert RefusalResult.model_validate(result).code == "VERIFICATION_FAILED"
        failures = query(db, "SELECT id,kind,status,private_json FROM agent.run_step_artifacts "
                         "WHERE run_id=%s AND status='failed' ORDER BY created_at", (execution[0],))
        assert len(failures) == 2
        assert failures[-1]["kind"] == budget_role
        assert failures[-1]["private_json"]["schema_attempt"] == 1
        assert query(db, "SELECT kind,role FROM agent.run_attempt_budgets WHERE run_id=%s AND kind=%s AND role=%s",
                     (execution[0], "schema", budget_role)) == [{"kind": "schema", "role": budget_role}]
        terminal = query(db, "SELECT event_type FROM agent.run_events WHERE run_id=%s ORDER BY sequence DESC LIMIT 1",
                         (execution[0],), "backend")[0]
        assert terminal == {"event_type": "run.refused"}
    finally:
        await client.aclose()


async def test_actual_graph_provider_refusal_refuses_without_schema_retry(db):
    execution, snapshot, generation = setup_run(db)
    server = RoleServer("drafter_provider_refusal")
    client = await gateway(server)
    try:
        async with pool(db) as connections:
            retrieval = FakeRetrieval(connections, packed(db, execution, snapshot, generation))
            graph = AnswerGraph(connections, retrieval, client, "test-config")
            await graph.execute(ExecutionToken(*execution), asyncio.Event(), asyncio.get_running_loop().time() + 30)
        assert server.roles == ["router", "drafter"]
        run = query(db, "SELECT status,refusal_code FROM agent.runs WHERE id=%s", (execution[0],))[0]
        assert run == {"status": "refused", "refusal_code": "VERIFICATION_FAILED"}
        failures = query(db, "SELECT kind,status,private_json FROM agent.run_step_artifacts "
                         "WHERE run_id=%s AND status='failed'", (execution[0],))
        assert len(failures) == 1
        assert failures[0]["kind"] == "drafter"
        assert failures[0]["private_json"]["failure_kind"] == "provider_refusal"
        assert not query(db, "SELECT kind,role FROM agent.run_attempt_budgets WHERE run_id=%s", (execution[0],))
    finally:
        await client.aclose()

