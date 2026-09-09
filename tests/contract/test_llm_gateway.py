"""Transport/policy tests use explicit mock HTTP. They are not model quality evidence."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr, ValidationError

from expert_agent.llm import CallContext, Descriptor, LlmError, LlmGateway, ServingProfile
from expert_agent.llm import prompts as prompt_module
from expert_agent.llm.prompts import PromptCatalog
from expert_agent.llm.types import MODEL, canonical_json, ordered_json, sha256
from expert_contracts.internal import EvidencePack, EvidenceUnit
from expert_contracts.model import CriticInternalResult, DraftAnswer
from expert_contracts.sources import SourceSpan

ROOT = Path(__file__).resolve().parents[2]
QUESTION = "В какой срок регистрируется заявка и с какого момента он начинается?"
TEXT = "После получения полного комплекта документов сотрудник регистрирует заявку в течение 3 рабочих дней."
PRIVATE = "PRIVATE_PROVIDER_ERROR_OR_TEXT"


def evidence_record(projected: dict, name: str, index: int) -> dict:
    return dict(zip(projected[f"{name}_fields"], projected[name][index], strict=True))


def profile():
    row = json.loads((ROOT / "models.lock.json").read_bytes())["models"]["llm"]
    files = sorted([{k: f[k] for k in ("path", "size_bytes", "sha256")} for f in row["files"]], key=lambda f: f["path"])
    token = [f for f in files if f["path"] in {"tokenizer.json", "tokenizer_config.json", "merges.txt", "vocab.json",
                                             "added_tokens.json", "special_tokens_map.json", "chat_template.jinja"}]
    return ServingProfile(model_fingerprint=sha256(canonical_json(files)),
        tokenizer_fingerprint=sha256(canonical_json(token)), chat_template_sha256=sha256("explicit test template"),
        observed_config_sha256=sha256("explicit test attestation"))


def evidence(*, empty=False, template=False):
    run, snapshot = uuid4(), uuid4()
    units = () if empty else (EvidenceUnit(evidence_id="E001", document_version_id=uuid4(),
        index_generation_id=uuid4(), canonical_node_id=uuid4(), source_chunk_ids=(uuid4(),),
        document_title="Синтетическое правило", structural_path=("1. Регистрация",), excerpt=TEXT,
        source_spans=(SourceSpan(pdf_page=1, block_id="block1", start_offset=0, end_offset=len(TEXT)),),
        content_hash=sha256(TEXT)),)
    bindings = [] if empty else [{"evidence_id": "E001", "relations": ["template" if template else "hit"],
        "owner_ranges": [{"table_kind": "blank_template" if template else None,
                          "template_grid": [{"row": 1, "column": 1, "text": ""}] if template else []}]}]
    manifest = {"run_id": str(run), "snapshot_id": str(snapshot),
                "units": [u.model_dump(mode="json") for u in units], "bindings": bindings}
    return EvidencePack(pack_id=uuid4(), run_id=run, snapshot_id=snapshot, units=units,
                        llm_token_count=0, manifest_hash=sha256(canonical_json(manifest))), canonical_json(bindings)


def table_evidence():
    run, snapshot, table_node, scope_node = uuid4(), uuid4(), UUID(int=701), UUID(int=702)
    table_id = str(UUID(int=703))
    rows = [
        ("E001", "Поиск и оценка", table_node, ("Статья 43", "clause 1", "table"), "data", 2, 0, 1, ["hit"]),
        ("E002", "120", table_node, ("Статья 43", "clause 1", "table"), "data", 2, 1, 1, ["hit"]),
        ("E003", "540", table_node, ("Статья 43", "clause 1", "table"), "data", 2, 2, 1, ["hit"]),
        ("E004", "рублей за один квадратный километр участка недр в год", scope_node,
         ("Статья 43", "clause 1"), None, None, None, 1, ["scope"]),
        ("E005", "Ставка", table_node, ("Статья 43", "clause 1", "table"), "header", 0, 1, 2, ["header"]),
        ("E006", "минимальная", table_node, ("Статья 43", "clause 1", "table"), "header", 1, 1, 1, ["header"]),
        ("E007", "максимальная", table_node, ("Статья 43", "clause 1", "table"), "header", 1, 2, 1, ["header"]),
    ]
    units, bindings = [], []
    for evidence_id, text, node, path, role, row, column, span, relations in rows:
        units.append(EvidenceUnit(evidence_id=evidence_id, document_version_id=UUID(int=1),
            index_generation_id=UUID(int=2), canonical_node_id=node, source_chunk_ids=(UUID(int=3),),
            document_title="Закон", structural_path=path, excerpt=text,
            source_spans=(SourceSpan(pdf_page=73, block_id=evidence_id, start_offset=0, end_offset=len(text)),),
            content_hash=sha256(text)))
        owner: dict[str, object] = {"owner_kind": "node_body" if role is None else "table_cell",
                 "text_owner_id": evidence_id, "start": 0, "end": len(text),
                 "table_id": None if role is None else table_id,
                 "table_kind": None if role is None else "data",
                 "cell": None if role is None else {"row": row, "column": column, "row_span": 1,
                                                     "column_span": span, "role": role},
                 "template_grid": []}
        bindings.append({"evidence_id": evidence_id, "parse_generation_id": str(UUID(int=4)),
            "artifact_object_id": str(UUID(int=5)), "artifact_sha256": "1" * 64,
            "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges",
            "relations": relations, "owner_ranges": [owner]})
    manifest = {"run_id": str(run), "snapshot_id": str(snapshot),
                "units": [unit.model_dump(mode="json") for unit in units], "bindings": bindings}
    return EvidencePack(pack_id=uuid4(), run_id=run, snapshot_id=snapshot, units=tuple(units),
                        llm_token_count=0, manifest_hash=sha256(canonical_json(manifest))), canonical_json(bindings)


def draft():
    return DraftAnswer(disposition="answer", claims=[{"claim_id": "c1", "text": TEXT, "evidence_ids": ["E001"]}], limitation=None)


def verdict(*, partial=False, wrong=False):
    return CriticInternalResult(claim_verdicts=[{"claim_id": "c1",
        "verdict": "contradicted" if wrong else "partially_supported" if partial else "supported",
        "reason_code": "WRONG_VALUE" if wrong else "MISSING_CONDITION" if partial else "SUPPORTED",
        "explanation": "Проверка синтетического условия", "evidence_ids": ["E001"],
        "issue_type": "wrong_value" if wrong else "missing_condition" if partial else "none"}],
        question_match="yes", missing_answer_parts=[], global_issues=[])


def context(*, seconds=20, attempt=0, call_id=None):
    return CallContext(call_id or uuid4(), asyncio.get_running_loop().time() + seconds, attempt)


class Server:
    def __init__(self, output=None):
        self.output = output or draft().model_dump(mode="json")
        self.requests = []
        self.count = 200
        self.mutate = lambda value: None
        self.hold = None
        self.started = asyncio.Event()
        self.closed = False
        self.status = 200

    async def __call__(self, request):
        self.requests.append(request)
        assert request.headers["authorization"] == "Bearer explicit-test-key"
        if request.url.path == "/version":
            return httpx.Response(200, json={"version": "0.12.0"})
        if request.url.path == "/v1/models":
            return httpx.Response(200, json={"data": [{"id": MODEL, "max_model_len": 8192}]})
        if request.url.path == "/tokenize":
            return httpx.Response(200, json={"count": self.count, "tokens": list(range(self.count)), "max_model_len": 8192})
        assert request.url.path == "/v1/chat/completions"
        if self.hold:
            self.started.set()
            try:
                await self.hold.wait()
            except asyncio.CancelledError:
                self.closed = True
                raise
        if self.status != 200:
            return httpx.Response(self.status, json={"error": PRIVATE})
        raw = self.output if isinstance(self.output, str) else canonical_json(self.output)
        value = {"model": MODEL, "choices": [{"index": 0, "finish_reason": "stop",
            "message": {"role": "assistant", "content": raw}}],
            "usage": {"prompt_tokens": self.count, "completion_tokens": 80, "total_tokens": self.count + 80}}
        self.mutate(value)
        return httpx.Response(200, json=value)


async def gateway(server):
    client = LlmGateway("http://llm:8000/v1", SecretStr("explicit-test-key"), profile=profile(),
        models_lock=ROOT / "models.lock.json", prompts_directory=ROOT / "config/prompts/p07.v1",
        transport=httpx.MockTransport(server))
    await client.open(deadline=asyncio.get_running_loop().time() + 10)
    return client


async def test_real_wire_shape_is_stable_closed_no_thinking_and_token_parity():
    server = Server()
    client = await gateway(server)
    pack, bindings = evidence()
    ctx = context()
    result = await client.draft(QUESTION, pack, bindings, context=ctx)
    first = server.requests[-1].content
    again = await client.draft(QUESTION, pack, bindings, context=ctx)
    assert first == server.requests[-1].content
    assert result.value == again.value
    generation = json.loads(first)
    tokenize = json.loads(server.requests[-2].content)
    for key in tokenize:
        assert generation[key] == tokenize[key]
    assert len(generation["messages"]) == 2
    assert generation["chat_template_kwargs"] == {"enable_thinking": False}
    assert generation["max_tokens"] == 1600 and generation["temperature"] == 0
    assert generation["response_format"]["json_schema"]["strict"] is True
    assert generation["response_format"]["json_schema"]["schema"]["additionalProperties"] is False
    schema = generation["response_format"]["json_schema"]["schema"]
    assert list(schema["anyOf"][0]["properties"]) == ["disposition", "claims", "limitation"]
    assert list(schema["$defs"]["DraftClaim"]["properties"]) == ["claim_id", "text", "evidence_ids"]
    assert first == ordered_json(generation).encode("utf-8")
    assert "tools" not in generation and "truncate_prompt_tokens" not in generation
    assert result.provenance.messages_sha256 == sha256(canonical_json(generation["messages"]))
    assert result.provenance.prompt_sha256 == sha256(generation["messages"][0]["content"])
    assert result.provenance.request_sha256 == sha256(first)
    assert result.provenance.input_tokens == 200 and result.provenance.output_tokens == 80
    assert result.provenance.evidence_manifest_sha256 == pack.manifest_hash
    await client.aclose()


def test_prepared_schema_preserves_dto_property_order_with_canonical_input_identity():
    pack, bindings = evidence()
    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("drafter", QUESTION, pack, bindings)
    schema = json.loads(prepared.schema_json)
    assert list(schema["anyOf"][0]["properties"]) == ["disposition", "claims", "limitation"]
    assert list(schema["$defs"]["DraftClaim"]["properties"]) == ["claim_id", "text", "evidence_ids"]
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    assert prepared.input_sha256 == sha256(canonical_json(payload))


def test_prepared_draft_schema_binds_numeric_table_citations_to_source_closures():
    pack, bindings = table_evidence()
    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("drafter", QUESTION, pack, bindings)
    schema = json.loads(prepared.schema_json)
    evidence_ids = schema["$defs"]["DraftClaim"]["properties"]["evidence_ids"]
    base_enum = evidence_ids["anyOf"][0]["items"]["enum"]
    prefixes = [[item["const"] for item in branch["prefixItems"]] for branch in evidence_ids["anyOf"][1:]]
    assert "E002" not in base_enum and "E003" not in base_enum
    assert ["E001", "E002", "E004", "E005", "E006"] in prefixes
    assert ["E001", "E003", "E004", "E005", "E007"] in prefixes
    assert ["E001", "E002", "E003", "E004", "E005", "E006", "E007"] in prefixes
    assert prepared.input_sha256 == sha256(canonical_json(json.loads(json.loads(prepared.messages_json)[1]["content"])))


def test_source_bound_draft_schema_is_wire_only_and_system_shows_structural_schema():
    pack, bindings = table_evidence()
    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("drafter", QUESTION, pack, bindings)
    system = json.loads(prepared.messages_json)[0]["content"]
    system_schema = json.loads(system.rsplit("Схема ответа JSON:\n", 1)[1])
    wire_schema = json.loads(prepared.schema_json)

    assert "Наборы ссылок дополнительно ограничены структурой EVIDENCE" in system
    assert "prefixItems" not in system.rsplit("Схема ответа JSON:\n", 1)[1]
    assert "prefixItems" in prepared.schema_json
    assert system_schema == prompt_module.role_schema("drafter", {})
    assert wire_schema == prompt_module.role_schema("drafter", json.loads(json.loads(prepared.messages_json)[1]["content"]))
    assert sha256(ordered_json(system_schema)) != sha256(prepared.schema_json)
    assert prepared.prompt_sha256 == sha256(system)


async def test_generation_request_uses_full_source_bound_schema_while_tokenized_messages_use_structural_schema():
    server = Server()
    client = await gateway(server)
    pack, bindings = table_evidence()
    await client.draft(QUESTION, pack, bindings, context=context())

    tokenize = json.loads(server.requests[-2].content)
    generation = json.loads(server.requests[-1].content)
    message_schema = tokenize["messages"][0]["content"].rsplit("Схема ответа JSON:\n", 1)[1]
    wire_schema = generation["response_format"]["json_schema"]["schema"]

    assert "prefixItems" not in message_schema
    assert "prefixItems" in ordered_json(wire_schema)
    assert wire_schema == json.loads(PromptCatalog(ROOT / "config/prompts/p07.v1").answer(
        "drafter", QUESTION, pack, bindings).schema_json)
    assert generation["messages"] == tokenize["messages"]
    await client.aclose()


@pytest.mark.parametrize("role", ["router", "drafter", "critic", "repair"])
def test_effective_prompt_digest_covers_file_wrapper_and_schema(role):
    directory = ROOT / "config/prompts/p07.v1"
    prepared = PromptCatalog(directory).prepare(role, {"QUESTION": "Synthetic question"})
    system = json.loads(prepared.messages_json)[0]["content"]
    raw = (directory / f"{role}.txt").read_bytes()
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["prompts"][role]["sha256"] == sha256(raw)
    assert system.startswith(raw.decode("utf-8"))
    assert "Все поля обязательны." in system
    expected_system_schema = ordered_json(prompt_module.role_schema(role, {})) if role in {"drafter", "critic", "repair"} else prepared.schema_json
    assert system.endswith(expected_system_schema)
    assert prepared.prompt_sha256 == sha256(system) != sha256(raw)


def test_changed_system_wrapper_changes_durable_prompt_identity(monkeypatch):
    prompts = PromptCatalog(ROOT / "config/prompts/p07.v1")
    pack, bindings = evidence()
    first = prompts.answer("drafter", QUESTION, pack, bindings)
    monkeypatch.setattr(prompt_module, "TEMPLATE_RULE", prompt_module.TEMPLATE_RULE + "\nSynthetic changed wrapper.")
    changed = prompts.answer("drafter", QUESTION, pack, bindings)
    assert first.input_sha256 == changed.input_sha256 and first.schema_json == changed.schema_json
    assert first.prompt_version == changed.prompt_version
    assert first.messages_json != changed.messages_json
    assert first.prompt_sha256 != changed.prompt_sha256


async def test_exact_packing_count_excludes_bookkeeping_and_keeps_template_metadata():
    server, (pack, bindings) = Server(), evidence(template=True)
    client = await gateway(server)
    prepared = client.prepare_draft(QUESTION, pack, bindings)
    count = await client.count_prepared(prepared, context=context())
    updated = pack.model_copy(update={"llm_token_count": count})
    assert client.prepare_draft(QUESTION, updated, bindings).messages_json == prepared.messages_json
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    binding = evidence_record(payload["EVIDENCE"], "bindings", 0)
    owner = evidence_record(payload["EVIDENCE"], "owners", binding["owner_indices"][0])
    assert owner["table_kind"] == "blank_template"
    assert "llm_token_count" not in prepared.messages_json and "pack_id" not in prepared.messages_json
    assert client.input_token_limit("drafter") == 6592
    await client.aclose()


@pytest.mark.parametrize("attempt", [0, 1])
@pytest.mark.parametrize("count,allowed", [(6591, True), (6592, True), (6593, False)])
async def test_complete_window_admission(count, allowed, attempt):
    server = Server()
    server.count = count
    client = await gateway(server)
    pack, bindings = evidence()
    if allowed:
        assert (await client.draft(QUESTION, pack, bindings, context=context(attempt=attempt))).provenance.input_tokens == count
    else:
        with pytest.raises(LlmError, match="TOKEN_LIMIT_EXCEEDED"):
            await client.draft(QUESTION, pack, bindings, context=context(attempt=attempt))
        assert not any(r.url.path == "/v1/chat/completions" for r in server.requests)
    await client.aclose()


@pytest.mark.parametrize("output", ["not JSON", '```json\n{}\n```', '{"disposition":"answer","disposition":"insufficient_evidence","claims":[],"limitation":"x"}',
    '{"disposition":"insufficient_evidence","claims":[],"limitation":NaN}',
    {"disposition": "answer", "claims": [], "limitation": None},
    {"disposition": "insufficient_evidence", "claims": [], "limitation": None, "unexpected": True}])
async def test_invalid_schema_is_technical_and_never_retried(output):
    server = Server(output)
    client = await gateway(server)
    pack, bindings = evidence()
    ctx = context(attempt=1)
    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID") as caught:
        await client.draft(QUESTION, pack, bindings, context=ctx)
    assert caught.value.call_id == ctx.call_id and caught.value.schema_attempt == 1
    assert caught.value.private_result is None
    assert len([r for r in server.requests if r.url.path == "/v1/chat/completions"]) == 1
    await client.aclose()


@pytest.mark.parametrize("kind", ["model", "tokens", "length", "refusal", "reasoning", "tools", "role", "choices"])
async def test_untrusted_provider_envelope(kind):
    server = Server()
    def mutate(value):
        if kind == "model":
            value["model"] = "other-model"
        elif kind == "tokens":
            value["usage"]["prompt_tokens"] += 1
        elif kind == "length":
            value["choices"][0]["finish_reason"] = "length"
        elif kind == "refusal":
            value["choices"][0]["message"]["refusal"] = PRIVATE
        elif kind == "reasoning":
            value["choices"][0]["message"]["reasoning_content"] = PRIVATE
        elif kind == "tools":
            value["choices"][0]["message"]["tool_calls"] = [{"name": "external_search"}]
        elif kind == "role":
            value["choices"][0]["message"]["role"] = "user"
        else:
            value["choices"] *= 2
    server.mutate = mutate
    client = await gateway(server)
    pack, bindings = evidence()
    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID") as caught:
        await client.draft(QUESTION, pack, bindings, context=context())
    assert PRIVATE not in str(caught.value)
    if kind == "refusal":
        assert caught.value.failure_kind == "provider_refusal"
    await client.aclose()


async def test_binding_router_draft_critic_and_partial_repair():
    server = Server({"classification": "in_scope", "matched_descriptor_ids": ["D1"], "reason": "Тема совпадает"})
    client = await gateway(server)
    assert (await client.router(QUESTION, (Descriptor(descriptor_id="D1", text="Регистрация заявки"),), context=context())).value.classification == "in_scope"
    pack, bindings = evidence()
    server.output = verdict().model_dump(mode="json")
    result = await client.critic(QUESTION, pack, bindings, draft(), context=context())
    assert result.value.claim_verdicts[0].verdict == "supported"
    server.output = draft().model_dump(mode="json")
    repaired = await client.repair(QUESTION, pack, bindings, draft(), verdict(partial=True), context=context())
    assert repaired.value.disposition == "answer"  # Still an unverified draft.
    await client.aclose()


@pytest.mark.parametrize("role", ["drafter", "repair"])
async def test_draft_and_repair_return_exact_model_produced_citations(role):
    output = DraftAnswer(disposition="answer", limitation=None,
        claims=[{"claim_id": "c1", "text": "Минимум 120, максимум 540.", "evidence_ids": ["E002", "E003"]}]
    ).model_dump(mode="json")
    server = Server(output)
    client = await gateway(server)
    pack, bindings = table_evidence()
    if role == "drafter":
        result = await client.draft(QUESTION, pack, bindings, context=context())
    else:
        result = await client.repair(QUESTION, pack, bindings, draft(), verdict(partial=True), context=context())
    assert result.value.model_dump(mode="json") == output
    await client.aclose()


@pytest.mark.parametrize("role", ["router", "drafter", "critic"])
async def test_foreign_alias_or_missing_claim_rejected(role):
    server = Server()
    client = await gateway(server)
    pack, bindings = evidence()
    with pytest.raises(LlmError, match="VERIFICATION_FAILED" if role == "drafter" else "OUTPUT_SCHEMA_INVALID"):
        if role == "router":
            server.output = {"classification": "in_scope", "matched_descriptor_ids": ["foreign"], "reason": "x"}
            await client.router(QUESTION, (), context=context())
        elif role == "drafter":
            server.output = draft().model_dump(mode="json")
            server.output["claims"][0]["evidence_ids"] = ["foreign"]
            await client.draft(QUESTION, pack, bindings, context=context())
        else:
            server.output = verdict().model_dump(mode="json")
            server.output["claim_verdicts"] = []
            await client.critic(QUESTION, pack, bindings, draft(), context=context())
    await client.aclose()


@pytest.mark.parametrize("v", [verdict(), verdict(wrong=True)])
async def test_repair_rejects_confirmed_and_hallucinated_inputs(v):
    server = Server()
    client = await gateway(server)
    pack, bindings = evidence()
    with pytest.raises(LlmError, match="INVALID_REQUEST"):
        await client.repair(QUESTION, pack, bindings, draft(), v, context=context())
    assert len(server.requests) == 2  # Startup only, no inference/tokenize.
    await client.aclose()


async def test_empty_evidence_is_normal_model_disposition_not_technical_failure():
    client = await gateway(Server({"disposition": "insufficient_evidence", "claims": [], "limitation": "Нет источника"}))
    pack, bindings = evidence(empty=True)
    assert (await client.draft(QUESTION, pack, bindings, context=context())).value.disposition == "insufficient_evidence"
    await client.aclose()


async def test_busy_is_bounded_and_cancellation_interrupts_http():
    server = Server()
    server.hold = asyncio.Event()
    client = await gateway(server)
    pack, bindings = evidence()
    task = asyncio.create_task(client.draft(QUESTION, pack, bindings, context=context()))
    await server.started.wait()
    with pytest.raises(LlmError, match="CAPACITY_EXCEEDED"):
        await client.draft(QUESTION, pack, bindings, context=context())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert server.closed
    server.hold = None
    assert (await client.draft(QUESTION, pack, bindings, context=context())).value.disposition == "answer"
    await client.aclose()


async def test_deadline_during_http_and_pre_admission(monkeypatch):
    server = Server()
    server.hold = asyncio.Event()
    client = await gateway(server)
    pack, bindings = evidence()
    loop = asyncio.get_running_loop()
    real_time = loop.time
    offset = 0.0
    task = None
    try:
        with monkeypatch.context() as clock_patch:
            clock_patch.setattr(loop, "time", lambda: real_time() + offset)
            ctx = context()
            task = asyncio.create_task(client.draft(QUESTION, pack, bindings, context=ctx))
            # Enter the held HTTP operation before expiring its real asyncio
            # deadline. Scheduler load cannot turn this into pre-admission timeout.
            await asyncio.wait_for(server.started.wait(), timeout=2)
            offset = ctx.deadline - real_time() + 1
            with pytest.raises(LlmError, match="DEADLINE_EXCEEDED") as caught:
                await task
            assert caught.value.call_id == ctx.call_id
            assert server.closed
            before = len(server.requests)
            with pytest.raises(LlmError, match="DEADLINE_EXCEEDED"):
                await client.draft(QUESTION, pack, bindings, context=context(seconds=-1))
            assert len(server.requests) == before
    finally:
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.aclose()


async def test_failed_dependency_no_raw_message_or_automatic_retry():
    server = Server()
    server.status = 503
    client = await gateway(server)
    pack, bindings = evidence()
    with pytest.raises(LlmError, match="MODEL_UNAVAILABLE") as caught:
        await client.draft(QUESTION, pack, bindings, context=context())
    assert PRIVATE not in str(caught.value) and len(server.requests) == 4
    await client.aclose()


def test_artifact_profile_schema_attempt_and_prompt_fail_closed(tmp_path):
    data = profile().model_dump()
    for key, value in [("structured_backend", "auto"), ("disable_fallback", False), ("revision", "other"), ("enable_thinking", True)]:
        with pytest.raises(ValidationError):
            ServingProfile.model_validate(data | {key: value})
    with pytest.raises(ValueError):
        CallContext(uuid4(), 10, 2)
    with pytest.raises(LlmError, match="MODEL_UNAVAILABLE"):
        profile().model_copy(update={"model_fingerprint": "0" * 64}).validate_lock(ROOT / "models.lock.json")
    with pytest.raises(LlmError, match="MODEL_UNAVAILABLE"):
        PromptCatalog(tmp_path)


def test_manifest_change_and_external_origin_rejected():
    pack, bindings = evidence()
    prompts = PromptCatalog(ROOT / "config/prompts/p07.v1")
    modified = json.loads(bindings)
    modified[0]["relations"] = ["template"]
    with pytest.raises(LlmError, match="INVALID_REQUEST"):
        prompts.answer("drafter", QUESTION, pack, canonical_json(modified))
    with pytest.raises(ValueError):
        LlmGateway("https://api.external.example/v1", SecretStr("x"), profile=profile(),
            models_lock=ROOT / "models.lock.json", prompts_directory=ROOT / "config/prompts/p07.v1")


async def test_explicit_schema_retry_retains_same_input_and_records_attempt():
    server = Server()
    client = await gateway(server)
    pack, bindings = evidence()
    call_id = uuid4()
    first = await client.draft(QUESTION, pack, bindings, context=context(call_id=call_id))
    second = await client.draft(QUESTION, pack, bindings, context=context(call_id=call_id, attempt=1))
    assert first.provenance.input_sha256 == second.provenance.input_sha256
    assert first.provenance.messages_sha256 == second.provenance.messages_sha256
    assert first.provenance.request_sha256 != second.provenance.request_sha256
    assert second.provenance.schema_attempt == 1
    await client.aclose()


async def test_explicit_schema_retry_can_use_larger_output_allowance_without_changing_identity():
    server = Server()
    server.count = 3510

    def mutate(value):
        generation = json.loads(server.requests[-1].content)
        if generation["max_tokens"] == 1600:
            value["choices"][0]["finish_reason"] = "length"
        else:
            value["usage"]["completion_tokens"] = 1601
            value["usage"]["total_tokens"] = server.count + 1601

    server.mutate = mutate
    client = await gateway(server)
    pack, bindings = evidence()
    call_id = uuid4()
    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID") as first:
        await client.draft(QUESTION, pack, bindings, context=context(call_id=call_id))
    second = await client.draft(QUESTION, pack, bindings, context=context(call_id=call_id, attempt=1))

    completion_requests = [r.content for r in server.requests if r.url.path == "/v1/chat/completions"]
    generations = [json.loads(content) for content in completion_requests]
    assert [item["max_tokens"] for item in generations] == [1600, 3200]
    assert [item["request_id"] for item in generations] == [f"{call_id}:0", f"{call_id}:1"]
    for key in ("model", "messages", "chat_template_kwargs", "add_generation_prompt",
                "add_special_tokens", "continue_final_message", "temperature", "seed", "n", "stream",
                "response_format"):
        assert generations[0][key] == generations[1][key]
    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("drafter", QUESTION, pack, bindings)
    assert first.value.failure_kind == "truncated"
    assert second.provenance.input_tokens == 3510 and second.provenance.output_tokens == 1601
    assert second.provenance.input_sha256 == prepared.input_sha256
    assert second.provenance.messages_sha256 == sha256(prepared.messages_json)
    assert second.provenance.schema_sha256 == sha256(prepared.schema_json)
    assert second.provenance.request_sha256 == sha256(completion_requests[1])
    assert second.provenance.request_sha256 != sha256(completion_requests[0])
    await client.aclose()


@pytest.mark.parametrize("count,limit", [(4992, 3200), (4993, 3199), (5000, 3192), (6591, 1601), (6592, 1600)])
async def test_explicit_schema_retry_output_allowance_is_clamped_to_max_model_len(count, limit):
    server = Server()
    server.count = count
    client = await gateway(server)
    pack, bindings = evidence()
    await client.draft(QUESTION, pack, bindings, context=context(attempt=1))

    generation = json.loads(server.requests[-1].content)
    assert generation["max_tokens"] == limit
    assert server.count + generation["max_tokens"] == 8192
    await client.aclose()


async def test_retry_rejects_provider_usage_above_actual_clamped_allowance():
    server = Server()
    server.count = 6591

    def mutate(value):
        value["usage"]["completion_tokens"] = 1602
        value["usage"]["total_tokens"] = server.count + 1602

    server.mutate = mutate
    client = await gateway(server)
    pack, bindings = evidence()
    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID"):
        await client.draft(QUESTION, pack, bindings, context=context(attempt=1))
    assert json.loads(server.requests[-1].content)["max_tokens"] == 1601
    await client.aclose()


async def test_explicit_schema_retry_still_reports_truncated_failure():
    server = Server()
    server.mutate = lambda value: value["choices"][0].update(finish_reason="length")
    client = await gateway(server)
    pack, bindings = evidence()
    ctx = context(attempt=1)

    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID") as caught:
        await client.draft(QUESTION, pack, bindings, context=ctx)

    generation = json.loads(server.requests[-1].content)
    assert generation["max_tokens"] == 3200
    assert caught.value.call_id == ctx.call_id and caught.value.schema_attempt == 1
    assert caught.value.failure_kind == "truncated"
    await client.aclose()


@pytest.mark.parametrize("partial,wrong", [(False, False), (True, False), (False, True)])
async def test_critic_cannot_use_other_uncited_pack_evidence_for_any_verdict(partial, wrong):
    pack, bindings_json = evidence()
    other = pack.units[0].model_copy(update={"evidence_id": "E002"})
    units = pack.units + (other,)
    bindings = json.loads(bindings_json)
    bindings.append(bindings[0] | {"evidence_id": "E002"})
    digest = sha256(canonical_json({"run_id": str(pack.run_id), "snapshot_id": str(pack.snapshot_id),
        "units": [u.model_dump(mode="json") for u in units], "bindings": bindings}))
    pack = pack.model_copy(update={"units": units, "manifest_hash": digest})
    server = Server(verdict(partial=partial, wrong=wrong).model_dump(mode="json"))
    server.output["claim_verdicts"][0]["evidence_ids"] = ["E002"]
    client = await gateway(server)
    with pytest.raises(LlmError, match="OUTPUT_SCHEMA_INVALID"):
        await client.critic(QUESTION, pack, canonical_json(bindings), draft(), context=context())
    await client.aclose()


async def test_critic_decoder_binds_each_claim_without_rewriting_model_output():
    pack, bindings = table_evidence()
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "minimum", "text": "Минимум 120.", "evidence_ids": ["E002", "E006"]},
        {"claim_id": "maximum", "text": "Максимум 540.", "evidence_ids": ["E003", "E007"]},
    ], limitation=None)
    output = verdict().model_dump(mode="json")
    first = output["claim_verdicts"][0]
    output["claim_verdicts"] = [first | {"claim_id": claim.claim_id, "evidence_ids": claim.evidence_ids}
                                 for claim in source.claims]
    server = Server(output)
    client = await gateway(server)
    result = await client.critic(QUESTION, pack, bindings, source, context=context())
    assert result.value.model_dump(mode="json") == output
    wire = json.loads(server.requests[-1].content)
    schema = wire["response_format"]["json_schema"]["schema"]
    assert list(schema["properties"]) == ["claim_verdicts", "question_match", "missing_answer_parts", "global_issues"]
    branches = schema["$defs"]["ClaimVerdict"]["anyOf"]
    assert list(branches[0]["properties"]) == [
        "claim_id", "verdict", "reason_code", "explanation", "evidence_ids", "issue_type"]
    assert [(b["properties"]["claim_id"]["const"], b["properties"]["evidence_ids"]["items"]["enum"])
            for b in branches] == [("minimum", ["E002", "E006"]), ("maximum", ["E003", "E007"])]
    assert schema["properties"]["claim_verdicts"]["minItems"] == 2
    assert schema["properties"]["claim_verdicts"]["maxItems"] == 2
    for branch in branches:
        assert branch["additionalProperties"] is False
        assert set(branch["required"]) == set(branch["properties"])
        assert branch["properties"]["verdict"]["enum"] == [
            "supported", "partially_supported", "unsupported", "contradicted"]
    # Effective identity changes with claim ownership; the reusable base schema is untouched.
    catalog = PromptCatalog(ROOT / "config/prompts/p07.v1")
    prepared = catalog.answer("critic", QUESTION, pack, bindings, source)
    second = source.model_copy(update={"claims": [source.claims[0]]})
    changed = catalog.answer("critic", QUESTION, pack, bindings, second)
    assert prepared.schema_json != changed.schema_json
    assert prepared.prompt_sha256 == changed.prompt_sha256
    assert prepared == catalog.answer("critic", QUESTION, pack, bindings, source)
    assert "allOf" not in prompt_module.role_schema("critic", {})["$defs"]["ClaimVerdict"]
    await client.aclose()


def test_critic_payload_adds_claim_scoped_table_row_view_without_uncited_excerpts():
    pack, bindings = table_evidence()
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "minimum", "text": "Минимум 120.", "evidence_ids": ["E001", "E002", "E004", "E005", "E006"]},
        {"claim_id": "maximum", "text": "Максимум 540.", "evidence_ids": ["E001", "E003", "E004", "E005", "E007"]},
    ], limitation=None)

    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("critic", QUESTION, pack, bindings, source)
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    view_payload = payload["CLAIM_SOURCE_VIEW"]
    excerpts = dict(view_payload["cited_excerpts"])
    view = {item["claim_id"]: item for item in view_payload["claims"]}
    minimum_rows = view["minimum"]["table_rows"]
    maximum_rows = view["maximum"]["table_rows"]
    assert len(minimum_rows) == 1 and len(maximum_rows) == 1
    assert minimum_rows[0]["cited_row_evidence_ids"] == ["E001", "E002", "E004", "E005", "E006"]
    assert maximum_rows[0]["cited_row_evidence_ids"] == ["E001", "E003", "E004", "E005", "E007"]
    assert minimum_rows[0]["row_citation_complete"] is False
    assert maximum_rows[0]["row_citation_complete"] is False
    assert excerpts["E001"] == "Поиск и оценка"
    assert excerpts["E002"] == "120"
    assert excerpts["E003"] == "540"
    assert minimum_rows[0]["subject"] == ["E001"]
    assert minimum_rows[0]["values"] == ["E002"]
    assert maximum_rows[0]["values"] == ["E003"]
    minimum_context = minimum_rows[0]["context"]
    assert "E006" in minimum_context
    assert "E007" not in minimum_context
    assert "E003" not in canonical_json(minimum_rows[0])
    assert "E002" not in canonical_json(maximum_rows[0])
    assert payload["DRAFT"] == source.model_dump(mode="json")


def test_claim_source_view_marks_incomplete_rows_without_claiming_complete_citations():
    pack, bindings = table_evidence()
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "minimum", "text": "Минимум 120.", "evidence_ids": ["E002", "E004", "E005", "E006"]},
    ], limitation=None)

    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("critic", QUESTION, pack, bindings, source)
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    row = payload["CLAIM_SOURCE_VIEW"]["claims"][0]["table_rows"][0]

    assert row["cited_row_evidence_ids"] == ["E002", "E004", "E005", "E006"]
    assert row["row_citation_complete"] is False
    assert "complete_row_citation_ids" not in row
    assert row["subject"] == []
    assert row["values"] == ["E002"]
    assert dict(payload["CLAIM_SOURCE_VIEW"]["cited_excerpts"])["E002"] == "120"


def test_claim_source_view_does_not_admit_sibling_rows_from_shared_header_or_scope_only():
    run, snapshot, table_node, scope_node = uuid4(), uuid4(), UUID(int=801), UUID(int=802)
    table_id = str(UUID(int=803))
    rows = [
        ("E001", "Категория А", table_node, ("Статья 1", "table"), "data", 1, 0, 1, ["hit"]),
        ("E002", "120", table_node, ("Статья 1", "table"), "data", 1, 1, 1, ["hit"]),
        ("E003", "Категория Б", table_node, ("Статья 1", "table"), "data", 2, 0, 1, ["hit"]),
        ("E004", "220", table_node, ("Статья 1", "table"), "data", 2, 1, 1, ["hit"]),
        ("E005", "рублей в год", scope_node, ("Статья 1",), None, None, None, 1, ["scope"]),
        ("E006", "Ставка", table_node, ("Статья 1", "table"), "header", 0, 1, 1, ["header"]),
    ]
    units, evidence_bindings = [], []
    for evidence_id, text, node, path, role, row, column, span, relations in rows:
        units.append(EvidenceUnit(evidence_id=evidence_id, document_version_id=UUID(int=1),
            index_generation_id=UUID(int=2), canonical_node_id=node, source_chunk_ids=(UUID(int=3),),
            document_title="Закон", structural_path=path, excerpt=text,
            source_spans=(SourceSpan(pdf_page=1, block_id=evidence_id, start_offset=0, end_offset=len(text)),),
            content_hash=sha256(text)))
        owner: dict[str, object] = {"owner_kind": "node_body" if role is None else "table_cell",
            "text_owner_id": evidence_id, "start": 0, "end": len(text),
            "table_id": None if role is None else table_id, "table_kind": None if role is None else "data",
            "cell": None if role is None else {"row": row, "column": column, "row_span": 1,
                                                "column_span": span, "role": role},
            "template_grid": []}
        evidence_bindings.append({"evidence_id": evidence_id, "parse_generation_id": str(UUID(int=4)),
            "artifact_object_id": str(UUID(int=5)), "artifact_sha256": "2" * 64,
            "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges",
            "relations": relations, "owner_ranges": [owner]})
    manifest = {"run_id": str(run), "snapshot_id": str(snapshot),
                "units": [unit.model_dump(mode="json") for unit in units], "bindings": evidence_bindings}
    pack = EvidencePack(pack_id=uuid4(), run_id=run, snapshot_id=snapshot, units=tuple(units),
                        llm_token_count=0, manifest_hash=sha256(canonical_json(manifest)))
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "minimum", "text": "Минимум 120.", "evidence_ids": ["E002", "E005", "E006"]},
    ], limitation=None)

    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer(
        "critic", QUESTION, pack, canonical_json(evidence_bindings), source)
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    rows_view = payload["CLAIM_SOURCE_VIEW"]["claims"][0]["table_rows"]

    assert [row["row"] for row in rows_view] == [1]
    assert rows_view[0]["values"] == ["E002"]
    view_json = canonical_json(payload["CLAIM_SOURCE_VIEW"])
    assert "E003" not in view_json
    assert "E004" not in view_json
    assert "Категория Б" not in view_json
    assert "220" not in view_json


def test_claim_source_view_omitted_for_prose_only_claims():
    pack, bindings = evidence()
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "prose", "text": TEXT, "evidence_ids": ["E001"]},
    ], limitation=None)

    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer("critic", QUESTION, pack, bindings, source)
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])

    assert "CLAIM_SOURCE_VIEW" not in payload
    assert payload["DRAFT"] == source.model_dump(mode="json")


def test_claim_source_view_mixed_claims_repeats_only_table_related_excerpts():
    pack, bindings = table_evidence()
    prose_text = "Общее описание процедуры без табличного значения."
    prose_unit = EvidenceUnit(evidence_id="E900", document_version_id=UUID(int=11),
        index_generation_id=UUID(int=12), canonical_node_id=UUID(int=13), source_chunk_ids=(UUID(int=14),),
        document_title="Закон", structural_path=("Статья 1",), excerpt=prose_text,
        source_spans=(SourceSpan(pdf_page=1, block_id="E900", start_offset=0, end_offset=len(prose_text)),),
        content_hash=sha256(prose_text))
    evidence_bindings = json.loads(bindings)
    evidence_bindings.append({"evidence_id": "E900", "parse_generation_id": str(UUID(int=15)),
        "artifact_object_id": str(UUID(int=16)), "artifact_sha256": "9" * 64,
        "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges",
        "relations": ["hit"], "owner_ranges": [{"owner_kind": "node_body", "text_owner_id": "E900",
            "start": 0, "end": len(prose_text), "table_id": None, "table_kind": None, "cell": None,
            "template_grid": []}]})
    units = (*pack.units, prose_unit)
    manifest = {"run_id": str(pack.run_id), "snapshot_id": str(pack.snapshot_id),
                "units": [unit.model_dump(mode="json") for unit in units], "bindings": evidence_bindings}
    mixed_pack = pack.model_copy(update={"units": units, "manifest_hash": sha256(canonical_json(manifest))})
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "prose", "text": prose_text, "evidence_ids": ["E900"]},
        {"claim_id": "table", "text": "Минимум 120.", "evidence_ids": ["E002", "E004", "E005", "E006"]},
    ], limitation=None)

    prepared = PromptCatalog(ROOT / "config/prompts/p07.v1").answer(
        "critic", QUESTION, mixed_pack, canonical_json(evidence_bindings), source)
    payload = json.loads(json.loads(prepared.messages_json)[1]["content"])
    view = payload["CLAIM_SOURCE_VIEW"]

    assert [claim["claim_id"] for claim in view["claims"]] == ["table"]
    excerpts = dict(view["cited_excerpts"])
    assert "E900" not in excerpts
    assert "E002" in excerpts
    assert prose_text not in canonical_json(view)


def test_claim_source_view_is_only_added_to_critic_payload():
    pack, bindings = table_evidence()
    source = DraftAnswer(disposition="answer", claims=[
        {"claim_id": "minimum", "text": "Минимум 120.", "evidence_ids": ["E001", "E002", "E004", "E005", "E006"]},
    ], limitation=None)
    verdicts = CriticInternalResult(claim_verdicts=[{"claim_id": "minimum", "verdict": "partially_supported",
        "reason_code": "MISSING_CONDITION", "explanation": "Needs row condition.", "evidence_ids": source.claims[0].evidence_ids,
        "issue_type": "missing_condition"}], question_match="partial",
        missing_answer_parts=["Неполная строка."], global_issues=[])
    prompts = PromptCatalog(ROOT / "config/prompts/p07.v1")

    critic_payload = json.loads(json.loads(prompts.answer("critic", QUESTION, pack, bindings, source).messages_json)[1]["content"])
    repair_payload = json.loads(json.loads(prompts.answer("repair", QUESTION, pack, bindings, source, verdicts).messages_json)[1]["content"])
    draft_payload = json.loads(json.loads(prompts.answer("drafter", QUESTION, pack, bindings).messages_json)[1]["content"])

    assert "CLAIM_SOURCE_VIEW" in critic_payload
    assert "CLAIM_SOURCE_VIEW" not in repair_payload
    assert "CLAIM_SOURCE_VIEW" not in draft_payload


@pytest.mark.parametrize("value", [{}, {"disposition": "insufficient_evidence", "claims": [], "limitation": None}])
def test_critic_dynamic_schema_rejects_missing_or_empty_claims(value):
    with pytest.raises(LlmError, match="INVALID_REQUEST"):
        prompt_module.role_schema("critic", {"DRAFT": value})


def test_invalid_unicode_question_has_safe_error():
    prompts = PromptCatalog(ROOT / "config/prompts/p07.v1")
    with pytest.raises(LlmError, match="INVALID_REQUEST"):
        prompts.router("question \ud800", ())


@pytest.mark.parametrize("role", ["drafter", "repair"])
async def test_invented_citation_retains_private_completed_artifact_and_provenance(role):
    output = draft().model_dump(mode="json")
    output["claims"][0].update({"text": PRIVATE, "evidence_ids": ["invented"]})
    server = Server(output)
    client = await gateway(server)
    pack, bindings = evidence()
    ctx = context()
    with pytest.raises(LlmError, match="VERIFICATION_FAILED") as caught:
        if role == "drafter":
            await client.draft(QUESTION, pack, bindings, context=ctx)
        else:
            await client.repair(QUESTION, pack, bindings, draft(), verdict(partial=True), context=ctx)
    error = caught.value
    assert error.failure_kind == "citation_binding" and error.private_result is not None
    completed = error.private_result
    assert completed.value.model_dump(mode="json") == output
    assert completed.provenance.call_id == ctx.call_id
    assert completed.provenance.role == role and completed.provenance.schema_attempt == 0
    assert completed.provenance.request_sha256 == sha256(server.requests[-1].content)
    assert completed.provenance.evidence_manifest_sha256 == pack.manifest_hash
    assert completed.provenance.input_tokens == 200 and completed.provenance.output_tokens == 80
    assert PRIVATE not in str(error) and PRIVATE not in repr(error) and PRIVATE not in repr(completed)
    assert len([r for r in server.requests if r.url.path == "/v1/chat/completions"]) == 1
    await client.aclose()


@pytest.mark.parametrize("issue,reason,globals_", [
    ("missing_condition", "MISSING_CONDITION", ["Unclassified global issue"]),
    ("wrong_scope", "WRONG_SCOPE", []),
    ("missing_condition", "WRONG_VALUE", []),
    ("citation_mismatch", "MISSING_CONDITION", []),
])
async def test_direct_repair_rejects_global_or_unsafe_partial_before_http(issue, reason, globals_):
    data = verdict(partial=True).model_dump(mode="json")
    data["claim_verdicts"][0].update(issue_type=issue, reason_code=reason)
    data["global_issues"] = globals_
    unsafe = CriticInternalResult.model_validate(data)
    server = Server()
    client = await gateway(server)
    pack, bindings = evidence()
    before = len(server.requests)
    with pytest.raises(LlmError, match="INVALID_REQUEST"):
        await client.repair(QUESTION, pack, bindings, draft(), unsafe, context=context())
    assert len(server.requests) == before  # Neither tokenization nor completion.
    await client.aclose()
