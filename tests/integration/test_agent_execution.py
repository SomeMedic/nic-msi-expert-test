"""Actual disposable PostgreSQL P08 contracts; model/source values are synthetic.

These tests prove SQL policy and race boundaries, not model semantic accuracy.
"""
from __future__ import annotations

from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
import hashlib
import json
from queue import Queue
from time import monotonic, sleep
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import ValidationError
import pytest

from expert_contracts.events import RUN_EVENT_ADAPTER
from expert_contracts.internal import EvidencePack
from expert_contracts.model import CriticInternalResult, DraftAnswer
from expert_contracts.runs import SnapshotInfo
from expert_agent.rendering import render_answer, render_refusal
from expert_contracts.errors import RefusalCode
from scripts.migrate import run_migrations
from tests.integration.test_migrations import publish, seed_ready_generation


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


@pytest.fixture
def db(isolated_database):
    run_migrations(isolated_database.migrate_dsn.get_secret_value())
    return isolated_database


def query(db, sql, args=(), role="runtime"):
    with db.connect(role) as connection:
        connection.row_factory = dict_row
        return connection.execute(sql, args).fetchall()


def state(db, run):
    return query(db, "SELECT * FROM agent.runs WHERE id=%s", (run,), "migrate")[0]


def setup_run(db, *, empty=False, recovery=2, structural_path=(), question="Какие требования?"):
    generation = None
    if not empty:
        with db.connect() as connection:
            generation = seed_ready_generation(connection, structural_path=structural_path)
            publish(connection, generation)
    run = uuid4()
    query(db, "SELECT * FROM agent.create_run(%s,%s,%s,%s,%s,clock_timestamp()+interval '5 minutes',%s,%s,%s)",
          (run, "operator", question, str(run), sha(str(run)), "test-config", False, recovery), "backend")
    owner = uuid4()
    acquired = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (run, owner))[0]
    execution = (run, owner, acquired["execution_epoch"])
    with db.connect("runtime") as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        snapshot = connection.execute("SELECT agent.capture_snapshot(%s,%s,%s)", execution).fetchone()[0]
    captured = query(db, "SELECT * FROM agent.kb_snapshots WHERE id=%s", (snapshot,))[0]
    info = SnapshotInfo(id=snapshot, captured_at=captured["captured_at"].astimezone(timezone.utc), version_count=0 if empty else 1)
    return execution, info, generation


def make_pack(db, execution, info, generation):
    units, bindings = [], []
    if generation:
        row = query(db, "SELECT c.id AS chunk,g.artifact_object_id,s.sha256,n.structural_path FROM knowledge.chunks c "
                    "JOIN knowledge.document_nodes n ON n.id=c.node_id "
                    "JOIN knowledge.parse_generations g ON g.id=c.parse_generation_id "
                    "JOIN app.stored_objects s ON s.id=g.artifact_object_id WHERE c.index_generation_id=%s",
                    (generation["index"],), "migrate")[0]
        units = [{"evidence_id": "E001", "document_version_id": str(generation["version"]),
                  "index_generation_id": str(generation["index"]), "canonical_node_id": str(generation["node"]),
                  "source_chunk_ids": [str(row["chunk"])], "document_title": "Synthetic regulation",
                  "structural_path": row["structural_path"], "excerpt": "Текст", "source_spans": [{"pdf_page": 1,
                  "printed_page_label": None, "block_id": "b1", "start_offset": 0, "end_offset": 5, "bbox": None}],
                  "content_hash": sha("Текст")}]
        bindings = [{"evidence_id": "E001", "parse_generation_id": str(generation["parse"]),
                     "artifact_object_id": str(row["artifact_object_id"]), "artifact_sha256": row["sha256"],
                     "relations": ["hit"], "owner_ranges": [],
                     "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges"}]
    manifest = canonical({"run_id": str(execution[0]), "snapshot_id": str(info.id), "units": units, "bindings": bindings})
    pack = {"pack_id": str(uuid4()), "run_id": str(execution[0]), "snapshot_id": str(info.id),
            "units": units, "llm_token_count": 5 if units else 0, "manifest_hash": sha(manifest)}
    return pack, manifest



def table_variant_pack(db, execution, info, generation):
    row = query(db, "SELECT c.id AS chunk,g.artifact_object_id,s.sha256,n.structural_path FROM knowledge.chunks c "
                "JOIN knowledge.document_nodes n ON n.id=c.node_id "
                "JOIN knowledge.parse_generations g ON g.id=c.parse_generation_id "
                "JOIN app.stored_objects s ON s.id=g.artifact_object_id WHERE c.index_generation_id=%s",
                (generation["index"],), "migrate")[0]
    rows = [
        ("E001", "Подземные воды", 1, 0),
        ("E002", "90", 1, 1),
        ("E003", "Подземные воды питьевого назначения", 2, 0),
        ("E004", "100", 2, 1),
    ]
    units, bindings = [], []
    for evidence_id, excerpt, row_number, column in rows:
        units.append({"evidence_id": evidence_id, "document_version_id": str(generation["version"]),
                      "index_generation_id": str(generation["index"]), "canonical_node_id": str(generation["node"]),
                      "source_chunk_ids": [str(row["chunk"])], "document_title": "Synthetic regulation",
                      "structural_path": row["structural_path"], "excerpt": excerpt, "source_spans": [{"pdf_page": 1,
                      "printed_page_label": None, "block_id": evidence_id, "start_offset": 0,
                      "end_offset": len(excerpt), "bbox": None}], "content_hash": sha(excerpt)})
        bindings.append({"evidence_id": evidence_id, "parse_generation_id": str(generation["parse"]),
                         "artifact_object_id": str(row["artifact_object_id"]), "artifact_sha256": row["sha256"],
                         "relations": ["hit"], "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges",
                         "owner_ranges": [{"table_id": "T1", "table_kind": "source_table", "cell": {
                             "role": "data", "row": row_number, "column": column,
                             "row_span": 1, "column_span": 1, "empty": False}}]})
    manifest = canonical({"run_id": str(execution[0]), "snapshot_id": str(info.id), "units": units, "bindings": bindings})
    pack = {"pack_id": str(uuid4()), "run_id": str(execution[0]), "snapshot_id": str(info.id),
            "units": units, "llm_token_count": 20, "manifest_hash": sha(manifest)}
    return pack, manifest


def persist_pack(db, execution, pack, manifest):
    return query(db, "SELECT * FROM agent.save_evidence_pack(%s,%s,%s,%s,%s)", (*execution, Jsonb(pack), manifest))[0]


def identity(info, pack, key, *, parents=(), model=True):
    return {"input_sha256": sha(key), "snapshot_id": str(info.id), "configuration_fingerprint": "test-config",
            "model_revision": "pinned-test-revision" if model else None,
            "profile_sha256": "a" * 64 if model else None, "prompt_version": "p08.test.v1" if model else None,
            "prompt_sha256": "b" * 64 if model else None, "schema_sha256": "c" * 64 if model else None,
            "evidence_pack_id": pack["pack_id"] if pack else None, "parent_artifact_ids": [str(x) for x in parents]}


def model_payload(kind, value, ident, pack, *, schema_attempt=0):
    return {"value": value, "provenance": {"call_id": str(uuid4()), "role": kind, "schema_attempt": schema_attempt,
            "model": "Qwen/Qwen3-14B-AWQ", "revision": ident["model_revision"], "profile_sha256": ident["profile_sha256"],
            "prompt_version": ident["prompt_version"], "prompt_sha256": ident["prompt_sha256"],
            "schema_sha256": ident["schema_sha256"], "input_sha256": ident["input_sha256"],
            "messages_sha256": "d" * 64, "request_sha256": "e" * 64, "token_ids_sha256": "f" * 64,
            "input_tokens": 100, "output_tokens": 30, "elapsed_ms": 10,
            "evidence_manifest_sha256": pack["manifest_hash"] if pack else None}}


def save(db, execution, key, ident, kind, payload, *, status="completed", artifact_id=None):
    return query(db, "SELECT * FROM agent.save_step_artifact(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                 (*execution, artifact_id or uuid4(), key, Jsonb(ident), kind, status, Jsonb(payload)))[0]


def save_policy(db, execution, info, pack, draft, critic, *, action="repair",
                outcome="partially_confirmed", reason="INCOMPLETE_TABLE_VARIANT",
                raw_critic_policy="render"):
    payload = {"draft_artifact_id": str(draft["id"]), "critic_artifact_id": str(critic["id"]),
               "action": action, "outcome": outcome, "reason": reason,
               "refusal_code": "VERIFICATION_FAILED" if action == "refuse" else None,
               "raw_critic_policy": raw_critic_policy, "evidence_manifest_sha256": pack["manifest_hash"],
               "policy_version": "p08.deterministic_policy.v1"}
    ident = identity(info, pack, "policy.decision", parents=[draft["id"], critic["id"]], model=False)
    ident["input_sha256"] = sha(canonical({"key": "policy.decision", "pack": pack["manifest_hash"],
                                           "parents": [str(draft["id"]), str(critic["id"])],
                                           "private": payload}))
    return save(db, execution, "policy.decision", ident, "policy", payload)


def draft_value(evidence_id="E001"):
    return {"disposition": "answer", "claims": [{"claim_id": "claim1", "text": "Требование установлено.",
            "evidence_ids": [evidence_id]}], "limitation": None}


def critic_value(verdict="supported", *, question="yes", issues=None, missing=None, claim_id="claim1"):
    reason, issue = {"supported": ("SUPPORTED", "none"), "partially_supported": ("MISSING_CONDITION", "missing_condition"),
                     "unsupported": ("UNSUPPORTED", "other"), "contradicted": ("CONTRADICTED", "other")}[verdict]
    return {"claim_verdicts": [{"claim_id": claim_id, "verdict": verdict, "reason_code": reason,
            "explanation": "Synthetic contract test", "evidence_ids": ["E001"], "issue_type": issue}],
            "question_match": question, "missing_answer_parts": missing or [], "global_issues": issues or []}


def evidence_ids(count):
    return [f"E{i:03d}" for i in range(1, count + 1)]


def pack_with_evidence_ids(pack, manifest, ids):
    source_unit = deepcopy(pack["units"][0])
    source_binding = json.loads(manifest)["bindings"][0]
    units, bindings = [], []
    for evidence_id in ids:
        unit = deepcopy(source_unit)
        unit["evidence_id"] = evidence_id
        units.append(unit)
        binding = deepcopy(source_binding)
        binding["evidence_id"] = evidence_id
        bindings.append(binding)
    expanded_manifest = canonical({
        "run_id": pack["run_id"], "snapshot_id": pack["snapshot_id"],
        "units": units, "bindings": bindings,
    })
    return {
        **pack, "pack_id": str(uuid4()), "units": units,
        "llm_token_count": len(units) * 5, "manifest_hash": sha(expanded_manifest),
    }, expanded_manifest


def draft_with_evidence_ids(ids):
    value = draft_value()
    value["claims"][0]["evidence_ids"] = list(ids)
    return value


def critic_with_evidence_ids(ids):
    value = critic_value()
    value["claim_verdicts"][0]["evidence_ids"] = list(ids)
    return value


def artifacts(db, execution, info, pack, *, critic=None):
    draft = draft_value()
    ident = identity(info, pack, "drafter.0")
    d = save(db, execution, "drafter.0", ident, "drafter", model_payload("drafter", draft, ident, pack))
    critic = critic or critic_value()
    ident = identity(info, pack, "critic.initial.0", parents=[d["id"]])
    c = save(db, execution, "critic.initial.0", ident, "critic", model_payload("critic", critic, ident, pack))
    return d, c


def answer(info, pack, d, c, *, repair=0):
    value = render_answer(DraftAnswer.model_validate(d["private_json"]["value"]),
                          CriticInternalResult.model_validate(c["private_json"]["value"]), EvidencePack.model_validate(pack),
                          info, version_labels={unit.document_version_id: None for unit in EvidencePack.model_validate(pack).units},
                          repair_attempts=repair)
    return value.model_dump(mode="json")


def finalize(db, execution, result=None, d=None, c=None, decision=None, *, outcome="completed", operation=None, error=None):
    return query(db, "SELECT * FROM agent.finalize_run(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                 (*execution, operation or uuid4(), outcome, Jsonb(result) if result is not None else None,
                  d["id"] if d else None, c["id"] if c else None, decision["id"] if decision else None, error))[0]


def expire(db, execution):
    query(db, "UPDATE agent.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=%s RETURNING id", (execution[0],), "migrate")


def takeover(db, execution):
    expire(db, execution)
    owner = uuid4()
    row = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (execution[0], owner))[0]
    assert row["outcome"] == "acquired"
    return execution[0], owner, row["execution_epoch"]


def test_upgrade_repeat_and_narrow_grants(db):
    repeated = run_migrations(db.migrate_dsn.get_secret_value())
    assert not repeated["changed"] and repeated["after_revision"] == "p08_023_repeated_draft_guard"
    for role in ("runtime", "backend", "ingest", "outbox"):
        with db.connect(role, autocommit=True) as connection:
            for statement in ("INSERT INTO agent.evidence_packs DEFAULT VALUES", "UPDATE agent.run_step_artifacts SET status='failed' WHERE false",
                              "DELETE FROM agent.run_attempt_budgets WHERE false", "SELECT agent._end_without_result(NULL,'failed','INTERNAL_ERROR')",
                              "SELECT agent._policy_decision('{}'::jsonb)"):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    connection.execute(statement)


def test_completed_result_exact_replay_and_public_event_payload(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    original = persist_pack(db, execution, pack, manifest)
    assert persist_pack(db, execution, pack, manifest) == original
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    op = uuid4()
    completed = finalize(db, execution, result, d, c, operation=op)
    assert completed["status"] == "completed"
    assert finalize(db, execution, result, d, c, operation=op) == completed
    rows = query(db, "SELECT public_result FROM agent.run_results WHERE run_id=%s", (execution[0],), "backend")
    assert rows == [{"public_result": result}]
    events = query(db, "SELECT * FROM agent.run_events WHERE run_id=%s ORDER BY sequence", (execution[0],), "backend")
    assert [e["sequence"] for e in events] == [1, 2, 3]
    for event in events:
        RUN_EVENT_ADAPTER.validate_python({"schema_version": 1, "event_id": event["event_id"], "run_id": event["run_id"],
            "sequence": event["sequence"], "type": event["event_type"], "stage": event["stage"], "attempt": event["attempt"],
            "execution_epoch": event["execution_epoch"], "occurred_at": event["created_at"], "data": event["public_payload"]})
    assert query(db, "SELECT count(*) AS count FROM app.outbox_events WHERE aggregate_id=%s", (execution[0],), "migrate")[0]["count"] == 3


@pytest.mark.parametrize("change", ["text", "claim", "citation", "validation", "snapshot", "unknown"])
def test_finalizer_rejects_unverified_or_changed_public_fields(db, change):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    if change == "text":
        result["text"] += " Непроверенное новое утверждение."
    elif change == "claim":
        result["claims"][0]["text"] += " Изменено."
    elif change == "citation":
        result["citations"][0]["source_url"] = "/api/v1/versions/foreign/source"
    elif change == "validation":
        result["validation"]["supported_count"] = 2
    elif change == "snapshot":
        result["snapshot"]["id"] = str(uuid4())
    else:
        result["unverified"] = True
    with pytest.raises(psycopg.Error):
        finalize(db, execution, result, d, c)
    assert state(db, execution[0])["status"] == "running"
    assert query(db, "SELECT count(*) AS count FROM agent.run_results WHERE run_id=%s", (execution[0],), "backend")[0]["count"] == 0


@pytest.mark.parametrize("change", ["excerpt", "duplicate", "source", "artifact", "span", "snapshot"])
def test_pack_digest_source_identity_and_boundaries(db, change):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    m = json.loads(manifest)
    if change == "excerpt":
        pack["units"][0]["excerpt"] += " changed"
    elif change == "duplicate":
        pack["units"].append(deepcopy(pack["units"][0]))
        m["bindings"].append(deepcopy(m["bindings"][0]))
    elif change == "source":
        pack["units"][0]["source_chunk_ids"] = [str(uuid4())]
    elif change == "artifact":
        m["bindings"][0]["artifact_sha256"] = "b" * 64
    elif change == "span":
        pack["units"][0]["source_spans"][0]["start_offset"] = None
    else:
        pack["snapshot_id"] = str(uuid4())
    m.update(units=pack["units"], snapshot_id=pack["snapshot_id"])
    manifest = canonical(m)
    pack["manifest_hash"] = sha(manifest)
    with pytest.raises(psycopg.Error):
        persist_pack(db, execution, pack, manifest)


def test_artifact_reuse_exact_identity_across_epoch_and_old_owner_denied(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, _ = artifacts(db, execution, info, pack)
    next_execution = takeover(db, execution)
    found = query(db, "SELECT * FROM agent.find_step_artifact(%s,%s,%s,%s,%s)",
                  (*next_execution, d["logical_step_key"], Jsonb(d["identity"])))
    assert found == [d] and found[0]["execution_epoch"] == 1
    assert save(db, next_execution, d["logical_step_key"], d["identity"], "drafter", d["private_json"], artifact_id=d["id"]) == d
    changed = {**d["identity"], "schema_sha256": "9" * 64}
    assert query(db, "SELECT * FROM agent.find_step_artifact(%s,%s,%s,%s,%s)",
                 (*next_execution, d["logical_step_key"], Jsonb(changed))) == []
    with pytest.raises(psycopg.errors.RaiseException, match="STALE_EXECUTION"):
        save(db, execution, d["logical_step_key"], d["identity"], "drafter", d["private_json"], artifact_id=d["id"])
    with pytest.raises(psycopg.errors.RaiseException, match="IDEMPOTENCY_CONFLICT"):
        save(db, next_execution, d["logical_step_key"], changed, "drafter", d["private_json"], artifact_id=d["id"])


def test_schema_budget_replay_and_provider_refusal_are_distinct(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "drafter.0")
    failure = {"error_code": "OUTPUT_SCHEMA_INVALID", "call_id": str(uuid4()), "schema_attempt": 0, "failure_kind": "contract"}
    failed = save(db, execution, "drafter.0", ident, "drafter", failure, status="failed")
    op = uuid4()
    args = (*execution, op, "drafter", failed["id"])
    assert query(db, "SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s) AS attempt", args) == [{"attempt": 1}]
    next_execution = takeover(db, execution)
    assert query(db, "SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s) AS attempt",
                 (*next_execution, op, "drafter", failed["id"])) == [{"attempt": 1}]
    with pytest.raises(psycopg.errors.RaiseException, match="SCHEMA_RETRY_EXHAUSTED"):
        query(db, "SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s)", (*next_execution, uuid4(), "drafter", failed["id"]))
    refused = save(db, next_execution, "router.0", identity(info, None, "router.0"), "router",
                  {**failure, "failure_kind": "provider_refusal"}, status="failed")
    with pytest.raises(psycopg.errors.RaiseException, match="SCHEMA_RETRY_FORBIDDEN"):
        query(db, "SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s)", (*next_execution, uuid4(), "router", refused["id"]))


def test_repair_budget_requires_partial_and_second_critic_to_complete(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack, critic=critic_value("partially_supported"))
    op = uuid4()
    sql = "SELECT agent.consume_repair(%s,%s,%s,%s,%s) AS attempt"
    assert query(db, sql, (*execution, op, c["id"])) == [{"attempt": 1}]
    execution = takeover(db, execution)
    assert query(db, sql, (*execution, op, c["id"])) == [{"attempt": 1}]
    with pytest.raises(psycopg.errors.RaiseException, match="REPAIR_EXHAUSTED"):
        query(db, sql, (*execution, uuid4(), c["id"]))
    ident = identity(info, pack, "repair.0", parents=[c["id"]])
    repaired = save(db, execution, "repair.0", ident, "repair", model_payload("repair", draft_value(), ident, pack))
    ident = identity(info, pack, "critic.repaired.0", parents=[repaired["id"]])
    critic = save(db, execution, "critic.repaired.0", ident, "critic", model_payload("critic", critic_value(), ident, pack))
    result = answer(info, pack, repaired, critic, repair=1)
    assert finalize(db, execution, result, repaired, critic)["status"] == "completed"


def test_table_variant_render_critic_requires_bound_policy_artifact_for_repair(db):
    execution, info, generation = setup_run(db, question="Какие требования для подземных вод?")
    pack, manifest = table_variant_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    draft = draft_with_evidence_ids(["E001", "E002"])
    ident = identity(info, pack, "draft.table_variant")
    d = save(db, execution, "draft.table_variant", ident, "drafter", model_payload("drafter", draft, ident, pack))
    critic = critic_with_evidence_ids(["E001", "E002"])
    ident = identity(info, pack, "critic.table_variant", parents=[d["id"]])
    c = save(db, execution, "critic.table_variant", ident, "critic", model_payload("critic", critic, ident, pack))
    assert query(db, "SELECT agent._policy(%s) AS policy", (Jsonb(critic),), "migrate") == [{"policy": "render"}]
    policy = save_policy(db, execution, info, pack, d, c)
    sql = "SELECT agent.consume_repair(%s,%s,%s,%s,%s) AS attempt"
    with pytest.raises(psycopg.errors.RaiseException, match="REPAIR_FORBIDDEN"):
        query(db, sql, (*execution, uuid4(), c["id"]))
    op = uuid4()
    assert query(db, sql, (*execution, op, policy["id"])) == [{"attempt": 1}]
    execution = takeover(db, execution)
    assert query(db, sql, (*execution, op, policy["id"])) == [{"attempt": 1}]
    with pytest.raises(psycopg.errors.RaiseException, match="REPAIR_EXHAUSTED"):
        query(db, sql, (*execution, uuid4(), policy["id"]))


def test_policy_artifact_rejects_cross_pack_or_forged_raw_policy(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack, critic=critic_value())
    ident = identity(info, pack, "policy.bad_manifest", parents=[d["id"], c["id"]], model=False)
    bad_manifest = {**{
        "draft_artifact_id": str(d["id"]), "critic_artifact_id": str(c["id"]),
        "action": "repair", "outcome": "partially_confirmed", "reason": "INCOMPLETE_TABLE_VARIANT",
        "refusal_code": None, "raw_critic_policy": "render", "evidence_manifest_sha256": "0" * 64,
        "policy_version": "p08.deterministic_policy.v1"}}
    with pytest.raises(psycopg.errors.RaiseException, match="POLICY_DECISION_MISMATCH"):
        save(db, execution, "policy.bad_manifest", ident, "policy", bad_manifest)
    forged_raw = {**bad_manifest, "evidence_manifest_sha256": pack["manifest_hash"], "raw_critic_policy": "repair"}
    with pytest.raises(psycopg.errors.RaiseException, match="POLICY_DECISION_MISMATCH"):
        save(db, execution, "policy.forged_raw", ident, "policy", forged_raw)


def test_policy_artifact_rejects_cross_run_parent(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    _, c = artifacts(db, execution, info, pack, critic=critic_value())
    other_execution, other_info, other_generation = setup_run(db)
    other_pack, other_manifest = make_pack(db, other_execution, other_info, other_generation)
    persist_pack(db, other_execution, other_pack, other_manifest)
    other_d, _ = artifacts(db, other_execution, other_info, other_pack, critic=critic_value())
    ident = identity(info, pack, "policy.cross_run", parents=[other_d["id"], c["id"]], model=False)
    with pytest.raises(psycopg.errors.RaiseException, match="ARTIFACT_PARENT_MISMATCH"):
        save(db, execution, "policy.cross_run", ident, "policy", {
            "draft_artifact_id": str(other_d["id"]), "critic_artifact_id": str(c["id"]),
            "action": "repair", "outcome": "partially_confirmed", "reason": "INCOMPLETE_TABLE_VARIANT",
            "refusal_code": None, "raw_critic_policy": "render", "evidence_manifest_sha256": pack["manifest_hash"],
            "policy_version": "p08.deterministic_policy.v1"})


def test_policy_refusal_for_table_variant_requires_consumed_repair_budget(db):
    execution, info, generation = setup_run(db, question="Какие требования для подземных вод?")
    pack, manifest = table_variant_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    draft = draft_with_evidence_ids(["E001", "E002"])
    ident = identity(info, pack, "draft.table_variant")
    d = save(db, execution, "draft.table_variant", ident, "drafter", model_payload("drafter", draft, ident, pack))
    ident = identity(info, pack, "critic.table_variant", parents=[d["id"]])
    c = save(db, execution, "critic.table_variant", ident, "critic", model_payload("critic", critic_with_evidence_ids(["E001", "E002"]), ident, pack))
    with pytest.raises(psycopg.errors.RaiseException, match="POLICY_DECISION_MISMATCH"):
        save_policy(db, execution, info, pack, d, c, action="refuse")


def test_policy_refusal_after_repair_exhaustion_is_verified_without_raw_critic_mutation(db):
    execution, info, generation = setup_run(db, question="Какие требования для подземных вод?")
    pack, manifest = table_variant_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    draft = draft_with_evidence_ids(["E001", "E002"])
    ident = identity(info, pack, "draft.table_variant")
    d = save(db, execution, "draft.table_variant", ident, "drafter", model_payload("drafter", draft, ident, pack))
    ident = identity(info, pack, "critic.table_variant", parents=[d["id"]])
    c = save(db, execution, "critic.table_variant", ident, "critic", model_payload("critic", critic_with_evidence_ids(["E001", "E002"]), ident, pack))
    repair_policy = save_policy(db, execution, info, pack, d, c)
    query(db, "SELECT agent.consume_repair(%s,%s,%s,%s,%s)", (*execution, uuid4(), repair_policy["id"]))
    ident = identity(info, pack, "repair.table_variant", parents=[c["id"]])
    repaired = save(db, execution, "repair.table_variant", ident, "repair", model_payload("repair", draft, ident, pack))
    ident = identity(info, pack, "critic.repaired.table_variant", parents=[repaired["id"]])
    raw_after_repair = save(db, execution, "critic.repaired.table_variant", ident, "critic",
        model_payload("critic", critic_with_evidence_ids(["E001", "E002"]), ident, pack))
    refusal_policy = save_policy(db, execution, info, pack, repaired, raw_after_repair, action="refuse")
    result = render_refusal(execution[0], info, RefusalCode.VERIFICATION_FAILED).model_dump(mode="json")
    assert finalize(db, execution, result, repaired, raw_after_repair, decision=refusal_policy, outcome="refused")["status"] == "refused"
    assert raw_after_repair["private_json"]["value"]["missing_answer_parts"] == []


def test_policy_artifact_cannot_authorize_completed_publication(db):
    execution, info, generation = setup_run(db, question="Какие требования для подземных вод?")
    pack, manifest = table_variant_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    draft = draft_with_evidence_ids(["E001", "E002"])
    ident = identity(info, pack, "draft.table_variant")
    d = save(db, execution, "draft.table_variant", ident, "drafter", model_payload("drafter", draft, ident, pack))
    ident = identity(info, pack, "critic.table_variant", parents=[d["id"]])
    c = save(db, execution, "critic.table_variant", ident, "critic", model_payload("critic", critic_with_evidence_ids(["E001", "E002"]), ident, pack))
    policy = save_policy(db, execution, info, pack, d, c)
    result = answer(info, pack, d, c)
    with pytest.raises(psycopg.errors.RaiseException, match="UNVERIFIED_RESULT"):
        finalize(db, execution, result, d, c, decision=policy)


def test_render_critic_without_table_variant_still_cannot_consume_repair(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    _, c = artifacts(db, execution, info, pack, critic=critic_value())
    with pytest.raises(psycopg.errors.RaiseException, match="REPAIR_FORBIDDEN"):
        query(db, "SELECT agent.consume_repair(%s,%s,%s,%s,%s)", (*execution, uuid4(), c["id"]))


@pytest.mark.parametrize("critic", [critic_value("unsupported"), critic_value("contradicted"), critic_value(issues=["Unclassified issue"]), critic_value(question="no")])
def test_unsafe_critic_never_allows_repair_or_verified_completion(db, critic):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack, critic=critic)
    with pytest.raises(psycopg.errors.RaiseException, match="REPAIR_FORBIDDEN"):
        query(db, "SELECT agent.consume_repair(%s,%s,%s,%s,%s)", (*execution, uuid4(), c["id"]))
    result = render_refusal(execution[0], info, RefusalCode.VERIFICATION_FAILED).model_dump(mode="json")
    assert finalize(db, execution, result, decision=c, outcome="refused")["status"] == "refused"


def test_fabricated_evidence_is_durable_precheck_refusal(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "drafter.0")
    d = save(db, execution, "drafter.0", ident, "drafter", model_payload("drafter", draft_value("invented"), ident, pack))
    ident = identity(info, pack, "precheck.0", parents=[d["id"]], model=False)
    failed = save(db, execution, "precheck.0", ident, "precheck", {"error_code": "VERIFICATION_FAILED", "call_id": str(uuid4()),
                  "schema_attempt": 0, "failure_kind": "citation_binding"}, status="failed")
    result = render_refusal(execution[0], info, RefusalCode.VERIFICATION_FAILED).model_dump(mode="json")
    assert finalize(db, execution, result, decision=failed, outcome="refused")["status"] == "refused"


def test_empty_pack_refusal_requires_durable_retrieval(db):
    execution, info, generation = setup_run(db, empty=True)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "retrieval", model=False)
    a = save(db, execution, "retrieval", ident, "retrieval", {"evidence_pack_id": pack["pack_id"], "disposition": "empty"})
    result = render_refusal(execution[0], info, RefusalCode.NO_RELEVANT_CONTEXT).model_dump(mode="json")
    assert finalize(db, execution, result, decision=a, outcome="refused")["status"] == "refused"


def test_recovery_is_bounded_and_cancel_never_resumes(db):
    execution, _, _ = setup_run(db, empty=True, recovery=0)
    expire(db, execution)
    acquired = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (execution[0], uuid4()))[0]
    assert acquired["outcome"] == "recovery_exhausted" and acquired["status"] == "failed"
    assert state(db, execution[0])["recovery_attempts"] == 0
    execution, _, _ = setup_run(db, empty=True)
    query(db, "SELECT * FROM agent.request_cancel(%s,'operator')", (execution[0],), "backend")
    before = state(db, execution[0])
    assert query(db, "SELECT * FROM agent.recover_runs(100)") == []
    rejected = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (execution[0], uuid4()))[0]
    assert rejected["outcome"] == "cancel_requested" and rejected["status"] == "cancelling"
    assert state(db, execution[0]) == before  # Live owner retains its drain/finalization boundary.
    expire(db, execution)
    recovered = query(db, "SELECT * FROM agent.recover_runs(100)")
    assert any(r["id"] == execution[0] and r["status"] == "cancelled" for r in recovered)
    assert query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (execution[0], uuid4()))[0]["outcome"] == "already_terminal"


def test_cancel_before_finalize_wins_and_revoke_blocks_pinned_run(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    query(db, "SELECT * FROM agent.request_cancel(%s,'operator')", (execution[0],), "backend")
    assert finalize(db, execution, result, d, c)["status"] == "cancelled"
    other, _, _ = setup_run(db, empty=True)
    # setup_run captured all already-published documents despite no new fixture.
    assert state(db, other[0])["snapshot_id"] is not None
    revoked = query(db, "SELECT app.revoke_document(%s,'admin','SECURITY_REVOKED') AS count", (generation["document"],), "backend")[0]
    assert revoked["count"] == 1 and state(db, other[0])["error_code"] == "SOURCE_REVOKED"
    with pytest.raises(psycopg.Error):
        query(db, "SELECT agent.guard_run_write(%s,%s,%s)", other)


def test_stage_events_are_closed_idempotent_and_ordered(db):
    execution, _, _ = setup_run(db, empty=True)
    op = uuid4()
    sql = "SELECT * FROM agent.record_stage_event(%s,%s,%s,%s,%s,%s,%s,%s)"
    args = (*execution, op, "drafting", "stage.started", 22, Jsonb({"message_code": "RUN_DRAFTING"}))
    original = query(db, sql, args)[0]
    assert query(db, sql, args) == [original]
    complete = {"duration_ms": 0, "candidate_count": None, "evidence_count": 1, "claim_count": 1}
    args = (*execution, uuid4(), "drafting", "stage.completed", 22, Jsonb(complete))
    completed = query(db, sql, args)[0]
    assert completed["sequence"] == original["sequence"] + 1
    assert query(db, sql, (*args[:-1], Jsonb({**complete, "duration_ms": 100}))) == [completed]
    with pytest.raises(psycopg.Error):
        query(db, sql, (*execution, uuid4(), "drafting", "stage.started", 23, Jsonb({"message_code": "RUN_DRAFTING", "draft": "leak"})))
    with pytest.raises(psycopg.errors.RaiseException, match="STAGE_NOT_STARTED"):
        query(db, sql, (*execution, uuid4(), "validating", "stage.completed", 23, Jsonb(complete)))


@pytest.mark.parametrize("change", ["model", "prompt", "profile", "input", "schema", "pack", "token_sum", "null"])
def test_model_provenance_must_equal_full_identity(db, change):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "draft.0")
    payload = model_payload("drafter", draft_value(), ident, pack)
    field = {"model": "revision", "prompt": "prompt_sha256", "profile": "profile_sha256", "input": "input_sha256",
             "schema": "schema_sha256", "pack": "evidence_manifest_sha256", "null": "input_sha256"}.get(change)
    if change == "token_sum":
        payload["provenance"].update(input_tokens=8192, output_tokens=1)
    else:
        payload["provenance"][field] = None if change == "null" else "9" * 64
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_MODEL_PROVENANCE"):
        save(db, execution, "draft.0", ident, "drafter", payload)


def test_critic_foreign_claim_and_evidence_are_rejected(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "draft")
    d = save(db, execution, "draft", ident, "drafter", model_payload("drafter", draft_value(), ident, pack))
    for i, value in enumerate([critic_value(claim_id="foreign"), critic_value()]):
        if i:
            value["claim_verdicts"][0]["evidence_ids"] = ["foreign"]
        ident = identity(info, pack, f"critic.{i}", parents=[d["id"]])
        with pytest.raises(psycopg.errors.InvalidParameterValue, match="CRITIC_BINDING_INVALID"):
            save(db, execution, f"critic.{i}", ident, "critic", model_payload("critic", value, ident, pack))


def test_draft_and_critic_accept_ten_canonical_evidence_ids(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    ids = evidence_ids(10)
    pack, manifest = pack_with_evidence_ids(pack, manifest, ids)
    persist_pack(db, execution, pack, manifest)
    draft = DraftAnswer.model_validate(draft_with_evidence_ids(ids)).model_dump(mode="json")
    ident = identity(info, pack, "draft.ten")
    d = save(db, execution, "draft.ten", ident, "drafter", model_payload("drafter", draft, ident, pack))
    critic = CriticInternalResult.model_validate(critic_with_evidence_ids(ids)).model_dump(mode="json")
    ident = identity(info, pack, "critic.ten", parents=[d["id"]])
    c = save(db, execution, "critic.ten", ident, "critic", model_payload("critic", critic, ident, pack))
    assert d["private_json"]["value"]["claims"][0]["evidence_ids"] == ids
    assert c["private_json"]["value"]["claim_verdicts"][0]["evidence_ids"] == ids


def test_draft_and_critic_reject_eleven_canonical_evidence_ids(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    ids = evidence_ids(11)
    pack, manifest = pack_with_evidence_ids(pack, manifest, ids)
    persist_pack(db, execution, pack, manifest)
    draft = draft_with_evidence_ids(ids)
    with pytest.raises(ValidationError):
        DraftAnswer.model_validate(draft)
    ident = identity(info, pack, "draft.eleven")
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_DRAFT_ARTIFACT"):
        save(db, execution, "draft.eleven", ident, "drafter", model_payload("drafter", draft, ident, pack))
    valid_draft = DraftAnswer.model_validate(draft_with_evidence_ids(ids[:10])).model_dump(mode="json")
    ident = identity(info, pack, "draft.ten")
    d = save(db, execution, "draft.ten", ident, "drafter", model_payload("drafter", valid_draft, ident, pack))
    critic = critic_with_evidence_ids(ids)
    with pytest.raises(ValidationError):
        CriticInternalResult.model_validate(critic)
    ident = identity(info, pack, "critic.eleven", parents=[d["id"]])
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_CRITIC_ARTIFACT"):
        save(db, execution, "critic.eleven", ident, "critic", model_payload("critic", critic, ident, pack))


def test_draft_and_critic_duplicate_evidence_ids_stay_rejected(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    duplicate = ["E001", "E001"]
    draft = draft_with_evidence_ids(duplicate)
    with pytest.raises(ValidationError):
        DraftAnswer.model_validate(draft)
    ident = identity(info, pack, "draft.duplicate")
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_DRAFT_ARTIFACT"):
        save(db, execution, "draft.duplicate", ident, "drafter", model_payload("drafter", draft, ident, pack))
    ident = identity(info, pack, "draft.valid")
    d = save(db, execution, "draft.valid", ident, "drafter", model_payload("drafter", draft_value(), ident, pack))
    critic = critic_with_evidence_ids(duplicate)
    with pytest.raises(ValidationError):
        CriticInternalResult.model_validate(critic)
    ident = identity(info, pack, "critic.duplicate", parents=[d["id"]])
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_CRITIC_ARTIFACT"):
        save(db, execution, "critic.duplicate", ident, "critic", model_payload("critic", critic, ident, pack))


def test_draft_foreign_binding_stays_a_precheck_rejection(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    foreign = draft_value("foreign")
    assert query(db, "SELECT agent._draft(%s) AS shape, agent._draft_bound(%s,%s) AS bound",
                 (Jsonb(foreign), Jsonb(foreign), Jsonb(pack)), "migrate") == [{"shape": True, "bound": False}]
    ident = identity(info, pack, "draft.foreign")
    d = save(db, execution, "draft.foreign", ident, "drafter", model_payload("drafter", foreign, ident, pack))
    ident = identity(info, pack, "precheck.foreign", parents=[d["id"]], model=False)
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="DRAFT_BINDING_INVALID"):
        save(db, execution, "precheck.foreign", ident, "precheck", {"draft_artifact_id": str(d["id"])})


def test_out_of_scope_refusal_has_router_proof_and_immutable_rows(db):
    execution, info, _ = setup_run(db, empty=True)
    ident = identity(info, None, "router.0")
    route = {"classification": "out_of_scope", "matched_descriptor_ids": [], "reason": "Synthetic route"}
    a = save(db, execution, "router.0", ident, "router", model_payload("router", route, ident, None))
    result = render_refusal(execution[0], info, RefusalCode.OUT_OF_SCOPE).model_dump(mode="json")
    assert finalize(db, execution, result, decision=a, outcome="refused")["status"] == "refused"
    with db.connect("migrate", autocommit=True) as connection:
        for statement in ("UPDATE agent.run_step_artifacts SET private_json='{}' WHERE run_id=%s",
                          "UPDATE agent.run_results SET public_result='{}' WHERE run_id=%s",
                          "UPDATE agent.runs SET max_recovery_attempts=10 WHERE id=%s"):
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(statement, (execution[0],))


def test_failure_terminal_has_safe_public_error_and_no_result(db):
    execution, _, _ = setup_run(db, empty=True)
    failed = finalize(db, execution, outcome="failed", error="OUTPUT_SCHEMA_INVALID")
    assert failed["status"] == "failed" and failed["result_id"] is None
    assert failed["terminal_error"]["code"] == "OUTPUT_SCHEMA_INVALID"
    event = query(db, "SELECT * FROM agent.run_events WHERE run_id=%s ORDER BY sequence DESC LIMIT 1", (execution[0],), "backend")[0]
    assert event["public_payload"] == {"error": failed["terminal_error"]}
    assert event["public_payload"]["error"]["details"] == {}


def test_finalizer_tail_lease_check_rolls_back_result_event_and_outbox(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    with db.connect("migrate") as connection:
        # Fixture-only latency fault waits until the actual DB lease timestamp.
        connection.execute("CREATE FUNCTION agent.test_wait_for_lease() RETURNS trigger LANGUAGE plpgsql AS $$ "
                           "DECLARE expiry timestamptz; BEGIN SELECT lease_until INTO expiry FROM agent.runs WHERE id=NEW.run_id; "
                           "PERFORM pg_sleep(greatest(0,extract(epoch from expiry-clock_timestamp()))+0.03); RETURN NEW; END $$")
        connection.execute("CREATE TRIGGER test_wait_for_lease AFTER INSERT ON agent.run_results FOR EACH ROW EXECUTE FUNCTION agent.test_wait_for_lease()")
        connection.execute("UPDATE agent.runs SET lease_until=clock_timestamp()+interval '300 milliseconds' WHERE id=%s", (execution[0],))
    with pytest.raises(psycopg.errors.RaiseException, match="LEASE_EXPIRED"):
        finalize(db, execution, result, d, c)
    with db.connect("migrate") as connection:
        assert connection.execute("SELECT count(*) FROM agent.run_results WHERE run_id=%s", (execution[0],)).fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM agent.run_events WHERE run_id=%s", (execution[0],)).fetchone() == (2,)
        assert connection.execute("SELECT count(*) FROM app.outbox_events WHERE aggregate_id=%s", (execution[0],)).fetchone() == (2,)
    assert state(db, execution[0])["status"] == "running"


def observed_block(db, blocker, waiter):
    with db.connect("admin", autocommit=True) as connection:
        deadline = monotonic() + 5
        while monotonic() < deadline:
            if connection.execute("SELECT %s=ANY(pg_blocking_pids(%s))", (blocker, waiter)).fetchone()[0]:
                return
            sleep(0.01)
    pytest.fail("Expected blocking relationship was not observed in PostgreSQL")


@pytest.mark.parametrize("command", ["cancel", "revoke"])
def test_completion_commits_before_waiting_cancel_or_security_revoke(db, command):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    pids: Queue[int] = Queue()

    def competing_command():
        with db.connect("backend") as connection:
            pids.put(connection.info.backend_pid)
            if command == "cancel":
                return connection.execute("SELECT status FROM agent.request_cancel(%s,'operator')", (execution[0],)).fetchone()[0]
            return connection.execute("SELECT app.revoke_document(%s,'admin','ACCESS_WITHDRAWN')", (generation["document"],)).fetchone()[0]

    with ThreadPoolExecutor(max_workers=1) as executor:
        with db.connect("runtime") as connection:
            row = connection.execute("SELECT status FROM agent.finalize_run(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                     (*execution, uuid4(), "completed", Jsonb(result), d["id"], c["id"], None, None)).fetchone()
            assert row == ("completed",)
            future = executor.submit(competing_command)
            observed_block(db, connection.info.backend_pid, pids.get(timeout=5))
        assert future.result(timeout=5) == ("completed" if command == "cancel" else 0)
    assert state(db, execution[0])["status"] == "completed"
    if command == "revoke":
        assert query(db, "SELECT security_revoked_at IS NOT NULL AS revoked FROM app.logical_documents WHERE id=%s",
                     (generation["document"],), "backend")[0]["revoked"]


def test_expired_deadline_is_failed_even_if_rendered_answer_supplied(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    # Privileged fixture-only creation of an elapsed deadline; production cannot
    # alter the immutable deadline. Wait against PG clock, never a guessed sleep.
    with db.connect("migrate") as connection:
        connection.execute("ALTER TABLE agent.runs DISABLE TRIGGER run_identity_guard")
        connection.execute("UPDATE agent.runs SET deadline_at=clock_timestamp()+interval '10 milliseconds' WHERE id=%s", (execution[0],))
        connection.execute("ALTER TABLE agent.runs ENABLE TRIGGER run_identity_guard")
        connection.execute("SELECT pg_sleep(0.02)")
        assert connection.execute("SELECT deadline_at<clock_timestamp() FROM agent.runs WHERE id=%s", (execution[0],)).fetchone() == (True,)
    row = finalize(db, execution, result, d, c)
    assert row["status"] == "failed" and row["error_code"] == "DEADLINE_EXCEEDED" and row["result_id"] is None


def test_fractional_retry_delay_and_null_failure_code(db):
    execution, info, _ = setup_run(db, empty=True)
    sql = "SELECT * FROM agent.record_stage_event(%s,%s,%s,%s,%s,%s,%s,%s)"
    query(db, sql, (*execution, uuid4(), "routing", "stage.started", 1, Jsonb({"message_code": "RUN_ROUTING"})))
    retried = query(db, sql, (*execution, uuid4(), "routing", "stage.retry_scheduled", 1,
                           Jsonb({"error_code": "OUTPUT_SCHEMA_INVALID", "retry_after_seconds": 0.25})))[0]
    assert retried["public_payload"]["retry_after_seconds"] == 0.25
    failure = {"error_code": None, "call_id": str(uuid4()), "schema_attempt": 0, "failure_kind": "contract"}
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="INVALID_STEP_FAILURE"):
        save(db, execution, "router.0", identity(info, None, "router.0"), "router", failure, status="failed")


def test_recovery_cleans_only_departed_backend_checkpoint_bindings(db):
    execution, _, _ = setup_run(db, empty=True)
    with db.connect("runtime") as departed:
        departed_pid = departed.info.backend_pid
        departed.execute("SELECT agent.guard_run_write(%s,%s,%s)", execution)
    with db.connect("runtime") as live:
        live_pid = live.info.backend_pid
        live.execute("SELECT agent.guard_run_write(%s,%s,%s)", execution)
        live.commit()
        assert query(db, "SELECT count(*) AS count FROM agent.checkpoint_write_bindings WHERE backend_pid IN (%s,%s)",
                     (departed_pid, live_pid), "migrate")[0]["count"] == 2
        query(db, "SELECT * FROM agent.recover_runs(100)")
        rows = query(db, "SELECT backend_pid FROM agent.checkpoint_write_bindings ORDER BY backend_pid", (), "migrate")
        assert rows == [{"backend_pid": live_pid}]
        live.execute("SELECT agent.guard_run_write(%s,%s,%s)", execution)


def test_false_precheck_failure_cannot_create_refusal(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    d, _ = artifacts(db, execution, info, pack)
    ident = identity(info, pack, "precheck.false", parents=[d["id"]], model=False)
    false_proof = save(db, execution, "precheck.false", ident, "precheck", {"error_code": "VERIFICATION_FAILED", "call_id": str(uuid4()),
                       "schema_attempt": 0, "failure_kind": "citation_binding"}, status="failed")
    result = render_refusal(execution[0], info, RefusalCode.VERIFICATION_FAILED).model_dump(mode="json")
    with pytest.raises(psycopg.errors.RaiseException, match="UNVERIFIED_REFUSAL"):
        finalize(db, execution, result, decision=false_proof, outcome="refused")


def test_valid_long_canonical_path_survives_evidence_and_public_citation(db):
    path = ("Раздел " + "а" * 1900,)
    execution, info, generation = setup_run(db, structural_path=path)
    pack, manifest = make_pack(db, execution, info, generation)
    EvidencePack.model_validate(pack)
    persist_pack(db, execution, pack, manifest)
    d, c = artifacts(db, execution, info, pack)
    result = answer(info, pack, d, c)
    assert result["citations"][0]["structural_path"] == list(path)
    assert finalize(db, execution, result, d, c)["status"] == "completed"


def test_same_public_create_retry_keeps_original_server_config_and_cap(db):
    execution, _, _ = setup_run(db, empty=True, recovery=0)
    original = state(db, execution[0])
    replayed = query(db, "SELECT * FROM agent.create_run(%s,%s,%s,%s,%s,clock_timestamp()+interval '10 minutes',%s,%s,%s)",
                     (uuid4(), "operator", original["question"], original["idempotency_key"], original["request_hash"],
                      "new-server-config", False, 10), "backend")[0]
    assert replayed == original and replayed["configuration_fingerprint"] == "test-config" and replayed["max_recovery_attempts"] == 0


def test_schema_retry_budget_binds_exact_identity_and_one_artifact(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    ident = identity(info, pack, "same-effective-input")
    failure = {"error_code": "OUTPUT_SCHEMA_INVALID", "call_id": str(uuid4()), "schema_attempt": 0, "failure_kind": "contract"}
    failed = save(db, execution, "drafter.schema0", ident, "drafter", failure, status="failed")
    query(db, "SELECT agent.consume_schema_retry(%s,%s,%s,%s,%s,%s)", (*execution, uuid4(), "drafter", failed["id"]))
    changed = {**ident, "input_sha256": sha("other-input")}
    with pytest.raises(psycopg.errors.RaiseException, match="SCHEMA_RETRY_IDENTITY_MISMATCH"):
        save(db, execution, "drafter.schema1", changed, "drafter", model_payload("drafter", draft_value(), changed, pack, schema_attempt=1))
    payload = model_payload("drafter", draft_value(), ident, pack, schema_attempt=1)
    valid = save(db, execution, "drafter.schema1", ident, "drafter", payload)
    assert save(db, execution, "drafter.schema1", ident, "drafter", payload, artifact_id=valid["id"]) == valid
    with pytest.raises(psycopg.errors.RaiseException, match="SCHEMA_RETRY_EXHAUSTED"):
        save(db, execution, "drafter.another-schema1", ident, "drafter", payload)


def test_capacity_error_remains_typed_durable_technical_failure(db):
    execution, info, _ = setup_run(db, empty=True)
    failure = {"error_code": "CAPACITY_EXCEEDED", "call_id": str(uuid4()), "schema_attempt": 0, "failure_kind": "contract"}
    save(db, execution, "router.0", identity(info, None, "router.0"), "router", failure, status="failed")
    final = finalize(db, execution, outcome="failed", error="CAPACITY_EXCEEDED")
    assert final["terminal_error"]["code"] == "CAPACITY_EXCEEDED" and final["result_id"] is None


def test_empty_repair_has_bound_verification_refusal_path(db):
    execution, info, generation = setup_run(db)
    pack, manifest = make_pack(db, execution, info, generation)
    persist_pack(db, execution, pack, manifest)
    _, critic = artifacts(db, execution, info, pack, critic=critic_value("partially_supported"))
    query(db, "SELECT agent.consume_repair(%s,%s,%s,%s,%s)", (*execution, uuid4(), critic["id"]))
    ident = identity(info, pack, "repair.empty", parents=[critic["id"]])
    value = {"disposition": "insufficient_evidence", "claims": [], "limitation": "Insufficient evidence"}
    repaired = save(db, execution, "repair.empty", ident, "repair", model_payload("repair", value, ident, pack))
    ident = identity(info, pack, "precheck.empty", parents=[repaired["id"]], model=False)
    with pytest.raises(psycopg.errors.InvalidParameterValue, match="DRAFT_BINDING_INVALID"):
        save(db, execution, "precheck.empty.success", ident, "precheck", {"draft_artifact_id": str(repaired["id"])})
    failed = save(db, execution, "precheck.empty", ident, "precheck", {"error_code": "VERIFICATION_FAILED", "call_id": str(uuid4()),
                  "schema_attempt": 0, "failure_kind": "citation_binding"}, status="failed")
    result = render_refusal(execution[0], info, RefusalCode.VERIFICATION_FAILED).model_dump(mode="json")
    assert finalize(db, execution, result, decision=failed, outcome="refused")["status"] == "refused"


@pytest.mark.parametrize("cause", ["cancel", "deadline"])
def test_acquire_terminalizes_never_started_cancelled_or_expired_run(db, cause):
    run = uuid4()
    query(db, "SELECT * FROM agent.create_run(%s,%s,%s,%s,%s,clock_timestamp()+interval '100 milliseconds',%s,%s,%s)",
          (run, "operator", "Требование?", str(run), sha(str(run)), "test-config", False, 2), "backend")
    if cause == "cancel":
        query(db, "SELECT * FROM agent.request_cancel(%s,'operator')", (run,), "backend")
    else:
        with db.connect("migrate") as connection:
            connection.execute("SELECT pg_sleep(greatest(0,extract(epoch FROM deadline_at-clock_timestamp()))+0.02) FROM agent.runs WHERE id=%s", (run,))
    result = query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (run, uuid4()))[0]
    assert result["outcome"] == ("cancel_requested" if cause == "cancel" else "deadline_exceeded")
    assert result["status"] == ("cancelled" if cause == "cancel" else "failed")
    ended = state(db, run)
    assert ended["execution_owner"] is None and ended["execution_epoch"] == 0 and ended["finished_at"] is not None
    assert query(db, "SELECT * FROM agent.acquire_run(%s,%s,60)", (run, uuid4()))[0]["outcome"] == "already_terminal"
