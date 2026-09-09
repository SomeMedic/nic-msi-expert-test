"""Explicit fake store/HTTP fault injection; actual PostgreSQL has its own gate."""
from __future__ import annotations

import asyncio
from dataclasses import replace
import json
from uuid import UUID, uuid4

import pytest

from expert_agent.checkpoint import ExecutionError, ExecutionToken
from expert_agent.execution import ModelOutputRejected, StageExecutor
from expert_agent.llm import LlmError
from expert_contracts.model import DraftAnswer
from tests.contract.test_llm_gateway import QUESTION, Server, draft, evidence, gateway, verdict


class Store:
    def __init__(self, run):
        self.token = ExecutionToken(run, uuid4(), 1)
        self.artifacts, self.events, self.budgets = {}, {}, {}
        self.fail_completion_once = False
        self.stale = False

    async def guard(self):
        if self.stale:
            raise ExecutionError("STALE_EXECUTION")

    async def find(self, key, identity):
        await self.guard()
        return self.artifacts.get((key, identity.digest))

    async def save(self, artifact):
        await self.guard()
        self.artifacts[(artifact.logical_step_key, artifact.identity.digest)] = artifact
        return artifact

    async def event(self, operation, stage, event_type, attempt, data):
        await self.guard()
        if self.fail_completion_once and event_type == "stage.completed":
            self.fail_completion_once = False
            raise RuntimeError("crash after artifact before checkpoint")
        payload = data.copy()
        payload.pop("duration_ms", None)
        value = (stage, event_type, attempt, payload)
        assert operation not in self.events or self.events[operation] == value
        self.events[operation] = value

    async def schema_retry(self, operation, role, failed_artifact):
        await self.guard()
        value = (operation, failed_artifact)
        if role in self.budgets and self.budgets[role] != value:
            raise ExecutionError("OUTPUT_SCHEMA_INVALID")
        self.budgets[role] = value
        return 1


def executor(store, pack, client, *, cancel=None, seconds=10):
    return StageExecutor(store, snapshot_id=pack.snapshot_id, configuration_fingerprint="fixed-test-config",
        profile=client.profile, deadline=asyncio.get_running_loop().time() + seconds,
        cancel=cancel or asyncio.Event())


def calls(server):
    return [request for request in server.requests if request.url.path == "/v1/chat/completions"]


async def test_crash_after_artifact_reuses_result_and_finishes_stage_after_takeover():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    store.fail_completion_once = True
    prepared = client.prepare_draft(QUESTION, pack, bindings)
    def invoke(context):
        return client.draft(QUESTION, pack, bindings, context=context)
    try:
        with pytest.raises(RuntimeError, match="crash after artifact"):
            await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack.pack_id)
        store.token = ExecutionToken(pack.run_id, uuid4(), 2)
        artifact = await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack.pack_id)
        repeated = await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack.pack_id)
        assert artifact == repeated and len(calls(server)) == 1 and len(store.artifacts) == 1
        assert artifact.value(DraftAnswer).claims[0].text
        assert len([value for value in store.events.values() if value[1] == "stage.completed"]) == 1
        assert next(value for value in store.events.values() if value[1] == "stage.completed")[2] == 3
    finally:
        await client.aclose()


async def test_exact_identity_changes_do_not_reuse_a_different_model_input():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        for question in (QUESTION, QUESTION + " Уточните условие."):
            await executor(store, pack, client).model(client.prepare_draft(question, pack, bindings),
                lambda context: client.draft(question, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
        assert len(calls(server)) == 2 and len(store.artifacts) == 2
    finally:
        await client.aclose()


async def test_schema_retry_is_durable_separate_and_not_replenished_by_recovery():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    original_output = server.output
    invokes = 0

    async def invoke(context):
        nonlocal invokes
        invokes += 1
        server.output = "{" if context.schema_attempt == 0 else original_output
        return await client.draft(QUESTION, pack, bindings, context=context)

    prepared = client.prepare_draft(QUESTION, pack, bindings)
    try:
        result = await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack.pack_id)
        assert invokes == 2 and len(store.budgets) == 1 and result.status == "completed"
        assert json.loads(result.private_json)["provenance"]["schema_attempt"] == 1
        assert len([row for row in store.events.values() if row[1] == "stage.retry_scheduled"]) == 1
        store.token = ExecutionToken(pack.run_id, uuid4(), 2)
        repeated = await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack.pack_id)
        assert repeated == result and invokes == 2 and len(store.budgets) == 1
    finally:
        await client.aclose()


async def test_second_schema_failure_is_terminal_rejection_and_cannot_be_retried_on_replay():
    server, (pack, bindings) = Server("{"), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    prepared = client.prepare_draft(QUESTION, pack, bindings)
    try:
        rejected = None
        for _ in range(2):
            with pytest.raises(ModelOutputRejected, match="OUTPUT_SCHEMA_INVALID") as caught:
                await executor(store, pack, client).model(prepared,
                    lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
            rejected = caught.value
        assert len(calls(server)) == 2 and len(store.budgets) == 1
        assert rejected is not None and rejected.schema_attempt == 1
        assert rejected.artifact_id in {artifact.id for artifact in store.artifacts.values()}
    finally:
        await client.aclose()


async def test_valid_fabricated_draft_is_saved_for_mandatory_precheck_without_schema_retry():
    server, (pack, bindings) = Server(), evidence()
    server.output["claims"][0]["evidence_ids"] = ["fabricated"]
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        artifact = await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings),
            lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
        assert artifact.value(DraftAnswer).claims[0].evidence_ids == ["fabricated"]
        assert len(calls(server)) == 1 and not store.budgets
        assert "fabricated" not in repr(artifact)
    finally:
        await client.aclose()


async def test_provider_refusal_is_terminal_model_rejection_without_retry():
    server, (pack, bindings) = Server(), evidence()
    server.mutate = lambda value: value["choices"][0]["message"].update(refusal="PRIVATE_REFUSAL")
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        with pytest.raises(ModelOutputRejected) as caught:
            await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings),
                lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
        assert caught.value.failure_kind == "provider_refusal"
        assert caught.value.artifact_id in {artifact.id for artifact in store.artifacts.values()}
        assert len(calls(server)) == 1 and not store.budgets
    finally:
        await client.aclose()


async def test_ambiguous_transport_failure_remains_error_without_retry():
    server, (pack, bindings) = Server(), evidence()
    server.status = 503
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        with pytest.raises(LlmError, match="MODEL_UNAVAILABLE"):
            await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings),
                lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
        assert len(calls(server)) == 1 and not store.budgets
    finally:
        await client.aclose()


async def test_non_allowlisted_schema_failure_kind_remains_llm_error():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)

    async def invoke(context):
        raise LlmError("OUTPUT_SCHEMA_INVALID", call_id=context.call_id, schema_attempt=context.schema_attempt,
                       failure_kind="citation_binding")

    try:
        with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID") as caught:
            await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings), invoke,
                                                     evidence_pack_id=pack.pack_id)
        assert type(caught.value) is LlmError
        assert caught.value.failure_kind == "citation_binding"
        assert len(calls(server)) == 0 and not store.budgets
    finally:
        await client.aclose()


@pytest.mark.parametrize("role", ["router", "drafter", "critic", "repair"])
async def test_all_roles_refuse_after_two_invalid_model_outputs_without_third_inference(role):
    server, (pack, bindings) = Server("{"), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    source_draft = DraftAnswer.model_validate(draft().model_dump(mode="json"))
    source_verdict = verdict(partial=True)
    pack_id: UUID | None
    parents: tuple[UUID, ...]
    if role == "router":
        prepared = client.prompts.router(QUESTION, ())
        pack_id, parents = None, ()
    elif role == "critic":
        prepared = client.prompts.answer("critic", QUESTION, pack, bindings, source_draft)
        pack_id, parents = pack.pack_id, (uuid4(),)
    elif role == "repair":
        prepared = client.prompts.answer("repair", QUESTION, pack, bindings, source_draft, source_verdict)
        pack_id, parents = pack.pack_id, (uuid4(),)
    else:
        prepared = client.prepare_draft(QUESTION, pack, bindings)
        pack_id, parents = pack.pack_id, ()

    async def invoke(context):
        if role == "router":
            return await client.router(QUESTION, (), context=context)
        if role == "critic":
            return await client.critic(QUESTION, pack, bindings, source_draft, context=context)
        if role == "repair":
            return await client.repair(QUESTION, pack, bindings, source_draft, source_verdict, context=context)
        return await client.draft(QUESTION, pack, bindings, context=context)

    try:
        rejected = None
        for _ in range(2):
            with pytest.raises(ModelOutputRejected) as caught:
                await executor(store, pack, client).model(prepared, invoke, evidence_pack_id=pack_id,
                                                          parents=parents)
            rejected = caught.value
        assert rejected is not None and rejected.failure_kind == "contract"
        assert rejected.schema_attempt == 1
        assert len(calls(server)) == 2
        assert len([artifact for artifact in store.artifacts.values() if artifact.kind == role]) == 2
        assert len(store.budgets) == 1
    finally:
        await client.aclose()


async def test_initial_and_repaired_critic_share_schema_retry_budget_and_terminalize_second_initial_failure():
    server, (pack, bindings) = Server("{"), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)
    source_draft = DraftAnswer.model_validate(draft().model_dump(mode="json"))
    first = client.prompts.answer("critic", QUESTION, pack, bindings, source_draft)
    changed_draft = DraftAnswer(disposition="answer", limitation=None,
        claims=[{"claim_id": "c1", "text": QUESTION, "evidence_ids": ["E001"]}])
    second = client.prompts.answer("critic", QUESTION, pack, bindings, changed_draft)
    try:
        with pytest.raises(ModelOutputRejected) as first_error:
            await executor(store, pack, client).model(first,
                lambda context: client.critic(QUESTION, pack, bindings, source_draft, context=context),
                phase="initial", evidence_pack_id=pack.pack_id, parents=(uuid4(),))
        assert first_error.value.schema_attempt == 1 and len(calls(server)) == 2

        with pytest.raises(ModelOutputRejected) as second_error:
            await executor(store, pack, client).model(second,
                lambda context: client.critic(QUESTION, pack, bindings, changed_draft, context=context),
                phase="repaired", evidence_pack_id=pack.pack_id, parents=(uuid4(),))
        assert second_error.value.schema_attempt == 0
        assert second_error.value.artifact_id in {artifact.id for artifact in store.artifacts.values()}
        assert len(calls(server)) == 3 and len(store.budgets) == 1
    finally:
        await client.aclose()


async def test_lost_lease_after_model_cannot_save_result():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)

    async def invoke(context):
        result = await client.draft(QUESTION, pack, bindings, context=context)
        store.stale = True
        return result

    try:
        with pytest.raises(ExecutionError, match="STALE_EXECUTION"):
            await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings), invoke,
                                                     evidence_pack_id=pack.pack_id)
        assert len(calls(server)) == 1 and not store.artifacts
    finally:
        await client.aclose()


async def test_actual_http_task_cancel_releases_transport_without_artifact_or_retry():
    server, (pack, bindings) = Server(), evidence()
    server.hold = asyncio.Event()
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        task = asyncio.create_task(executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings),
            lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id))
        await asyncio.wait_for(server.started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert server.closed and not store.artifacts and not store.budgets
    finally:
        await client.aclose()


async def test_response_provenance_must_match_requested_immutable_identity():
    server, (pack, bindings) = Server(), evidence()
    client = await gateway(server)
    store = Store(pack.run_id)

    async def invoke(context):
        result = await client.draft(QUESTION, pack, bindings, context=context)
        return replace(result, provenance=replace(result.provenance, input_sha256="b" * 64))

    try:
        with pytest.raises(ExecutionError, match="ARTIFACT_IDENTITY_MISMATCH"):
            await executor(store, pack, client).model(client.prepare_draft(QUESTION, pack, bindings), invoke,
                                                     evidence_pack_id=pack.pack_id)
        assert not store.artifacts
    finally:
        await client.aclose()


async def test_cancel_event_aborts_active_http_without_waiting_for_role_timeout():
    server, (pack, bindings) = Server(), evidence()
    server.hold = asyncio.Event()
    client = await gateway(server)
    store, cancellation = Store(pack.run_id), asyncio.Event()
    try:
        task = asyncio.create_task(executor(store, pack, client, cancel=cancellation).model(
            client.prepare_draft(QUESTION, pack, bindings),
            lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id))
        await asyncio.wait_for(server.started.wait(), 2)
        cancellation.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert server.closed and not store.artifacts and not store.budgets
    finally:
        await client.aclose()


async def test_deadline_during_active_http_maps_to_same_safe_code_and_drains():
    server, (pack, bindings) = Server(), evidence()
    server.hold = asyncio.Event()
    client = await gateway(server)
    store = Store(pack.run_id)
    try:
        with pytest.raises(ExecutionError, match="DEADLINE_EXCEEDED"):
            await executor(store, pack, client, seconds=0.05).model(client.prepare_draft(QUESTION, pack, bindings),
                lambda context: client.draft(QUESTION, pack, bindings, context=context), evidence_pack_id=pack.pack_id)
        assert server.closed and not store.artifacts and not store.budgets
    finally:
        await client.aclose()


