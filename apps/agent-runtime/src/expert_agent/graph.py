"""Named LangGraph answer flow over immutable evidence and durable typed artifacts.

Checkpoints contain identifiers and decisions, never model messages or drafts.
Finalization happens after the graph's final durable checkpoint: a terminal run
cannot authorize another checkpoint write. PostgreSQL remains the final arbiter.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import nullcontext
from typing import Literal, TypedDict
from uuid import UUID, uuid5

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from psycopg_pool import AsyncConnectionPool

from expert_contracts.errors import RefusalCode
from expert_contracts.events import StageCompletedData, StageStartedData
from expert_contracts.model import CriticInternalResult, DraftAnswer, RouteDecision
from expert_observability.tracing import correlation_context, restored_run_trace, safe_span

from .checkpoint import ExecutionError, ExecutionToken, FencedCheckpointAdapter
from .debug_capture import CaptureContext
from .execution import ModelOutputRejected, StageExecutor
from .llm.client import LlmGateway
from .llm.types import CallContext, Descriptor
from .rendering import render_answer, render_refusal
from .repository import GraphRepository, RunContext, empty_pack
from .retrieval.service import RetrievalService
from .retrieval.types import PackedEvidence
from .validation import ValidationFailure, aggregate, critic_storage_policy, enrich_table_variant_missing_parts, precheck_draft

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


def _stage_message_code(stage: str) -> StageMessageCode:
    return _STAGE_MESSAGE_CODES[stage]


class GraphState(TypedDict, total=False):
    run_id: str
    snapshot_id: str
    configuration_fingerprint: str
    route_id: str
    route: str
    pack_id: str
    retrieval_id: str
    draft_id: str | None
    critic_id: str | None
    precheck_id: str | None
    decision_id: str
    repair_attempts: int
    next_action: str
    outcome: str
    refusal_code: str


def _required_state_uuid(value: str | None) -> UUID:
    if value is None:
        raise ExecutionError("GENERATION_INVALID")
    return UUID(value)


class AnswerGraph:
    def __init__(self, pool: AsyncConnectionPool, retrieval: RetrievalService, gateway: LlmGateway,
                 configuration_fingerprint: str, *, context_token_budget: int = 5000):
        # Reserve room beyond the complete Drafter prompt for structured draft
        # and Critic/Repair inputs. Every role also checks its exact full count.
        if type(context_token_budget) is not int or not 512 <= context_token_budget <= 5000:
            raise ValueError("Invalid bounded full-message context budget")
        self.pool, self.retrieval, self.gateway = pool, retrieval, gateway
        self.configuration, self.context_token_budget = configuration_fingerprint, context_token_budget

    async def execute(self, token: ExecutionToken, cancel: asyncio.Event, deadline: float) -> None:
        repository = GraphRepository(self.pool, token, self.configuration)
        trace_id = await repository.trace_id()
        with (restored_run_trace(trace_id) if trace_id else nullcontext(),
              correlation_context(run_id=token.run_id, execution_epoch=token.epoch,
                                  configuration_fingerprint=self.configuration),
              safe_span("run.execute") as span):
            if span.trace_id is not None:
                await repository.bind_trace(span.trace_id)
            await self._execute(repository, cancel, deadline)

    async def _execute(self, repository: GraphRepository, cancel: asyncio.Event, deadline: float) -> None:
        token = repository.token
        session = _Session(self, repository, cancel, deadline)
        saver = FencedCheckpointAdapter(self.pool, token)
        graph = session.compile(saver)
        config: RunnableConfig = {"configurable": {"thread_id": str(token.run_id), "checkpoint_ns": ""}, "recursion_limit": 32}
        checkpoint = await saver.aget_tuple(config)
        initial: GraphState | None = None if checkpoint is not None else {
            "run_id": str(token.run_id), "configuration_fingerprint": self.configuration, "repair_attempts": 0}
        try:
            async with asyncio.timeout_at(deadline):
                result = await graph.ainvoke(initial, config, durability="sync")
                with safe_span("finalizer.commit", stage="finalizing", snapshot_id=result.get("snapshot_id")):
                    await session.finalize(result)
        except TimeoutError:
            raise ExecutionError("DEADLINE_EXCEEDED") from None


class _Session:
    def __init__(self, application: AnswerGraph, repository: GraphRepository, cancel: asyncio.Event, deadline: float):
        self.app, self.repository, self.cancel, self.deadline = application, repository, cancel, deadline
        self.token, self.store = repository.token, repository.store

    async def guard(self) -> None:
        if self.cancel.is_set():
            raise asyncio.CancelledError
        if asyncio.get_running_loop().time() >= self.deadline:
            raise ExecutionError("DEADLINE_EXCEEDED")
        await self.store.guard()

    async def context(self, state: GraphState) -> RunContext:
        await self.guard()
        if (state.get("run_id") != str(self.token.run_id)
                or state.get("configuration_fingerprint") != self.app.configuration
                or state.get("repair_attempts", 0) not in (0, 1)):
            raise ExecutionError("STEP_IDENTITY_MISMATCH")
        context = await self.repository.context()
        if state.get("snapshot_id", str(context.snapshot.id)) != str(context.snapshot.id):
            raise ExecutionError("STEP_IDENTITY_MISMATCH")
        return context

    def executor(self, context: RunContext) -> StageExecutor:
        return StageExecutor(self.store, snapshot_id=context.snapshot.id,
            configuration_fingerprint=self.app.configuration, profile=self.app.gateway.profile,
            deadline=self.deadline, cancel=self.cancel)

    def capture(self, context: RunContext, call: CallContext) -> CaptureContext | None:
        # Construct only inside the actual inference callback. Reused artifacts
        # do not produce capture traffic or a fictitious physical exchange.
        if not context.capture_policy.active:
            return None
        return CaptureContext(self.token.run_id, self.token.owner, self.token.epoch,
            call.call_id, call.schema_attempt, context.capture_policy, self.app.configuration)

    async def packed(self, state: GraphState) -> PackedEvidence:
        packed = await self.repository.load_pack(UUID(state["pack_id"]))
        if packed is None:
            raise ExecutionError("EVIDENCE_PACK_MISMATCH")
        return packed

    async def event(self, stage: str, event_type: str, state: GraphState) -> None:
        # Two citation checks in one execution epoch have different attempts.
        ordinal = state.get("repair_attempts", 0) if stage == "checking_citations" else 0
        attempt = 2 * (self.token.epoch - 1) + ordinal + 1
        operation = uuid5(self.token.run_id, f"local:{self.token.epoch}:{stage}:{attempt}:{event_type}")
        data = (StageStartedData(message_code=_stage_message_code(stage)) if event_type == "stage.started"
                else StageCompletedData(duration_ms=0))
        await self.store.event(operation, stage, event_type, attempt, data.model_dump(mode="json"))

    def local(self, stage: str, operation: Callable[[GraphState], Awaitable[GraphState]]):
        async def node(state: GraphState) -> GraphState:
            span_name = {"snapshotting": "snapshot.capture", "retrieving": "retrieval.search",
                         "building_context": "context.build", "checking_citations": "citations.check",
                         "rendering": None}[stage]
            with safe_span(span_name, stage=stage) if span_name else nullcontext():
                await self.guard()
                await self.event(stage, "stage.started", state)
                result = await operation(state)
                await self.guard()
                await self.event(stage, "stage.completed", state)
                return result
        return node

    def compile(self, saver):
        graph = StateGraph(GraphState)
        graph.add_node("load_run_and_snapshot", self.local("snapshotting", self.load))
        graph.add_node("Router", self.router)
        graph.add_node("Retriever", self.local("retrieving", self.retrieve))
        graph.add_node("build_context", self.local("building_context", self.build_context))
        graph.add_node("Drafter", self.draft)
        graph.add_node("citation_prechecks", self.local("checking_citations", self.precheck))
        graph.add_node("Critic", self.critic)
        graph.add_node("policy", self.policy)
        graph.add_node("Repair", self.repair)
        graph.add_node("Renderer", self.local("rendering", self.render))
        graph.add_node("DefaultIntent", self.default_intent)
        graph.add_node("NoContext", self.no_context)
        graph.add_node("VerificationFailure", self.verification_failure)
        graph.add_edge(START, "load_run_and_snapshot")
        graph.add_conditional_edges("load_run_and_snapshot", lambda state: state["next_action"])
        graph.add_conditional_edges("Router", lambda state: state["next_action"] if state.get("next_action") == "VerificationFailure"
                                    else "DefaultIntent" if state["route"] == "out_of_scope" else "Retriever")
        graph.add_conditional_edges("Retriever", lambda state: "NoContext" if state["next_action"] == "empty" else "build_context")
        graph.add_edge("build_context", "Drafter")
        graph.add_conditional_edges("Drafter", lambda state: state["next_action"])
        graph.add_conditional_edges("citation_prechecks", lambda state: state["next_action"])
        graph.add_conditional_edges("Critic", lambda state: state["next_action"] if state.get("next_action") == "VerificationFailure"
                                    else "policy")
        graph.add_conditional_edges("policy", lambda state: state["next_action"])
        graph.add_conditional_edges("Repair", lambda state: state["next_action"] if state.get("next_action") == "VerificationFailure"
                                    else "citation_prechecks")
        for name in ("Renderer", "DefaultIntent", "NoContext", "VerificationFailure"):
            graph.add_edge(name, END)
        return graph.compile(checkpointer=saver)

    async def load(self, state: GraphState) -> GraphState:
        context = await self.context(state)
        # Source05 §05.6: without an eligible source there is no domain signal.
        # Persist the usual empty retrieval proof, without asking a model to
        # classify missing knowledge as an out-of-scope question.
        return {"snapshot_id": str(context.snapshot.id),
                "next_action": "Retriever" if context.snapshot.version_count == 0 else "Router"}

    async def router(self, state: GraphState) -> GraphState:
        context = await self.context(state)
        prepared = await self.app.retrieval.prepare(self.token.run_id, context.principal_id,
            cancel=self.cancel, deadline=self.deadline)
        descriptors = tuple(Descriptor(descriptor_id=hit.alias, text=hit.text) for hit in prepared.descriptors)
        prompt = self.app.gateway.prompts.router(context.question, descriptors)
        try:
            artifact = await self.executor(context).model(prompt,
                lambda call: self.app.gateway.router(context.question, descriptors, context=call,
                                                    capture=self.capture(context, call)))
        except ModelOutputRejected as error:
            return self.model_output_refusal(error)
        route = artifact.value(RouteDecision)
        return {"route_id": str(artifact.id), "route": route.classification, "decision_id": str(artifact.id)}

    async def retrieve(self, state: GraphState) -> GraphState:
        context = await self.context(state)
        # A crash after pack persistence must never retrieve a replacement pack.
        packed = await self.repository.load_pack()
        if packed is None and context.snapshot.version_count == 0:
            packed = await self.repository.save_pack(empty_pack(self.token.run_id, context.snapshot.id))
        if packed is None:
            prepared = await self.app.retrieval.prepare(self.token.run_id, context.principal_id,
                cancel=self.cancel, deadline=self.deadline)
            route = (await self.repository.artifact(UUID(state["route_id"]), kind=("router",))).value(RouteDecision)

            async def count(pack, bindings):
                prompt = self.app.gateway.prepare_draft(context.question, pack, bindings)
                call = CallContext(uuid5(self.token.run_id, "count:" + prompt.input_sha256), self.deadline)
                return await self.app.gateway.count_prepared(prompt, context=call)

            found = await self.app.retrieval.retrieve(prepared, token_budget=self.app.context_token_budget,
                count_evidence=count, cancel=self.cancel, deadline=self.deadline, route=route)
            packed = await self.repository.save_pack(found.evidence or empty_pack(self.token.run_id, context.snapshot.id))
        artifact = await self.repository.local_artifact(context, "retrieval.frozen", "retrieval", packed,
            {"evidence_pack_id": str(packed.pack.pack_id), "disposition": "ready" if packed.pack.units else "empty"})
        return {"pack_id": str(packed.pack.pack_id), "retrieval_id": str(artifact.id), "decision_id": str(artifact.id),
                "next_action": "ready" if packed.pack.units else "empty"}

    async def build_context(self, state: GraphState) -> GraphState:
        await self.context(state)
        packed = await self.packed(state)
        if not packed.pack.units or packed.pack.llm_token_count > self.app.context_token_budget:
            raise ExecutionError("EVIDENCE_PACK_MISMATCH")
        return {"pack_id": str(packed.pack.pack_id)}

    async def draft(self, state: GraphState) -> GraphState:
        context, packed = await self.context(state), await self.packed(state)
        prompt = self.app.gateway.prepare_draft(context.question, packed.pack, packed.bindings_json)
        try:
            artifact = await self.executor(context).model(prompt,
                lambda call: self.app.gateway.draft(context.question, packed.pack, packed.bindings_json, context=call,
                                                   capture=self.capture(context, call)),
                evidence_pack_id=packed.pack.pack_id)
        except ModelOutputRejected as error:
            return self.model_output_refusal(error)
        draft = artifact.value(DraftAnswer)
        return {"draft_id": str(artifact.id), "decision_id": str(artifact.id),
                "next_action": "NoContext" if draft.disposition == "insufficient_evidence" else "citation_prechecks"}

    async def precheck(self, state: GraphState) -> GraphState:
        context, packed = await self.context(state), await self.packed(state)
        artifact = await self.repository.artifact(_required_state_uuid(state.get("draft_id")), kind=("drafter", "repair"), pack_id=packed.pack.pack_id)
        draft, failed = artifact.value(DraftAnswer), False
        try:
            precheck_draft(draft, packed.pack, bindings_json=packed.bindings_json)
            if draft.disposition != "answer" or not draft.claims:
                raise ValidationFailure("VERIFICATION_FAILED")
        except ValidationFailure as error:
            if error.code != "VERIFICATION_FAILED":
                raise
            failed = True
        checked = await self.repository.local_artifact(context, f"precheck.{artifact.id}", "precheck", packed,
            {"draft_artifact_id": str(artifact.id)}, parents=(artifact.id,), failed=failed)
        return {"precheck_id": str(checked.id), "decision_id": str(checked.id),
                "next_action": "VerificationFailure" if failed else "Critic"}

    async def critic(self, state: GraphState) -> GraphState:
        context, packed = await self.context(state), await self.packed(state)
        draft_artifact = await self.repository.artifact(_required_state_uuid(state.get("draft_id")), kind=("drafter", "repair"), pack_id=packed.pack.pack_id)
        checked = await self.repository.artifact(UUID(state["precheck_id"]), kind=("precheck",), pack_id=packed.pack.pack_id)
        if checked.status != "completed" or checked.identity.parent_artifact_ids != (draft_artifact.id,):
            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
        draft = draft_artifact.value(DraftAnswer)
        prompt = self.app.gateway.prompts.answer("critic", context.question, packed.pack, packed.bindings_json, draft)
        try:
            artifact = await self.executor(context).model(prompt,
                lambda call: self.app.gateway.critic(context.question, packed.pack, packed.bindings_json, draft, context=call,
                                                    capture=self.capture(context, call)),
                phase="repaired" if state.get("repair_attempts") else "initial",
                evidence_pack_id=packed.pack.pack_id, parents=(draft_artifact.id,))
        except ModelOutputRejected as error:
            return self.model_output_refusal(error)
        return {"critic_id": str(artifact.id), "decision_id": str(artifact.id)}

    async def policy(self, state: GraphState) -> GraphState:
        context = await self.context(state)
        packed = await self.packed(state)
        draft_artifact = await self.repository.artifact(_required_state_uuid(state.get("draft_id")), kind=("drafter", "repair"),
                    pack_id=packed.pack.pack_id)
        draft = draft_artifact.value(DraftAnswer)
        critic = await self.repository.artifact(_required_state_uuid(state.get("critic_id")), kind=("critic",), pack_id=packed.pack.pack_id)
        if critic.identity.parent_artifact_ids != (_required_state_uuid(state.get("draft_id")),):
            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
        raw_critic = critic.value(CriticInternalResult)
        decision = aggregate(draft, raw_critic, packed.pack, repair_attempts=state.get("repair_attempts", 0),
                             bindings_json=packed.bindings_json, question=context.question)
        raw_policy = critic_storage_policy(raw_critic)
        decision_id = critic.id
        if decision.action != raw_policy:
            policy = await self.repository.policy_decision(context, packed, draft=draft_artifact.id, critic=critic.id,
                decision=decision, raw_critic_policy=raw_policy)
            decision_id = policy.id
        return {"next_action": {"render": "Renderer", "repair": "Repair", "refuse": "VerificationFailure"}[decision.action],
                "decision_id": str(decision_id)}

    async def repair(self, state: GraphState) -> GraphState:
        context, packed = await self.context(state), await self.packed(state)
        draft = (await self.repository.artifact(_required_state_uuid(state.get("draft_id")), kind=("drafter",),
                    pack_id=packed.pack.pack_id)).value(DraftAnswer)
        critic = await self.repository.artifact(_required_state_uuid(state.get("critic_id")), kind=("critic",), pack_id=packed.pack.pack_id)
        raw_verdicts = critic.value(CriticInternalResult)
        verdicts = enrich_table_variant_missing_parts(context.question, draft, raw_verdicts, packed.pack,
            packed.bindings_json)
        attempt = await self.repository.repair(_required_state_uuid(state.get("decision_id")))
        prompt = self.app.gateway.prompts.answer("repair", context.question, packed.pack, packed.bindings_json, draft, verdicts)
        try:
            artifact = await self.executor(context).model(prompt,
                lambda call: self.app.gateway.repair(context.question, packed.pack, packed.bindings_json, draft, verdicts, context=call,
                                                    capture=self.capture(context, call)),
                evidence_pack_id=packed.pack.pack_id, parents=(critic.id,))
        except ModelOutputRejected as error:
            return self.model_output_refusal(error)
        return {"draft_id": str(artifact.id), "critic_id": None, "precheck_id": None, "repair_attempts": attempt,
                "decision_id": str(artifact.id)}

    async def render(self, state: GraphState) -> GraphState:
        # Validate deterministic public shape here, but persist only references.
        await self.answer(state)
        return {"outcome": "completed"}

    async def answer(self, state: GraphState):
        context, packed = await self.context(state), await self.packed(state)
        draft = (await self.repository.artifact(_required_state_uuid(state.get("draft_id")), kind=("drafter", "repair"),
                    pack_id=packed.pack.pack_id)).value(DraftAnswer)
        critic = await self.repository.artifact(_required_state_uuid(state.get("critic_id")), kind=("critic",), pack_id=packed.pack.pack_id)
        if critic.identity.parent_artifact_ids != (_required_state_uuid(state.get("draft_id")),):
            raise ExecutionError("ARTIFACT_IDENTITY_MISMATCH")
        return render_answer(draft, critic.value(CriticInternalResult), packed.pack, context.snapshot,
            version_labels=dict(context.version_labels), repair_attempts=state.get("repair_attempts", 0),
            bindings_json=packed.bindings_json, question=context.question)

    async def default_intent(self, state: GraphState) -> GraphState:
        await self.context(state)
        return {"outcome": "refused", "refusal_code": RefusalCode.OUT_OF_SCOPE.value}

    async def no_context(self, state: GraphState) -> GraphState:
        await self.context(state)
        return {"outcome": "refused", "refusal_code": RefusalCode.NO_RELEVANT_CONTEXT.value}

    async def verification_failure(self, state: GraphState) -> GraphState:
        await self.context(state)
        return {"outcome": "refused", "refusal_code": RefusalCode.VERIFICATION_FAILED.value}

    def model_output_refusal(self, error: ModelOutputRejected) -> GraphState:
        return {"next_action": "VerificationFailure", "draft_id": None, "critic_id": None,
                "decision_id": str(error.artifact_id)}

    async def finalize(self, state: GraphState) -> None:
        context = await self.context(state)
        if state.get("outcome") == "completed":
            result = await self.answer(state)
        elif state.get("outcome") == "refused":
            result = render_refusal(self.token.run_id, context.snapshot, RefusalCode(state["refusal_code"]))
        else:
            raise ExecutionError("GENERATION_INVALID")
        await self.event("finalizing", "stage.started", state)
        await self.repository.finalize(result, draft=_required_state_uuid(state.get("draft_id")) if state.get("draft_id") else None,
            critic=_required_state_uuid(state.get("critic_id")) if state.get("critic_id") else None, decision=_required_state_uuid(state.get("decision_id")))
