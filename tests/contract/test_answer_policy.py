"""Source06 severity table and deterministic renderer; no semantic model claim."""
from datetime import datetime, timezone
import hashlib
from uuid import uuid4

import pytest

from expert_agent.rendering import render_answer, render_refusal
from expert_agent.validation import ValidationFailure, aggregate, critic_storage_policy, precheck_draft
from expert_contracts.errors import REFUSAL_MESSAGES, RefusalCode
from expert_contracts.internal import EvidencePack, EvidenceUnit
from expert_contracts.model import CriticInternalResult, DraftAnswer, ReasonCode
from expert_contracts.runs import SnapshotInfo
from expert_contracts.sources import SourceSpan


def evidence_unit(evidence_id: str, *, document_version_id=None):
    text = f"{evidence_id}: The sample must be measured within 3 days after complete registration."
    return EvidenceUnit(evidence_id=evidence_id, document_version_id=document_version_id or uuid4(), index_generation_id=uuid4(),
        canonical_node_id=uuid4(), source_chunk_ids=(uuid4(),), document_title="Actual snapshot title",
        structural_path=("Section IV", "3.2.15"), excerpt=text,
        source_spans=(SourceSpan(pdf_page=12, printed_page_label="10", block_id=f"b-{evidence_id}",
                                 start_offset=0, end_offset=len(text)),),
        content_hash=hashlib.sha256(text.encode()).hexdigest())


@pytest.fixture
def evidence():
    unit = evidence_unit("E001")
    return EvidencePack(pack_id=uuid4(), run_id=uuid4(), snapshot_id=uuid4(), units=(unit,),
                        llm_token_count=100, manifest_hash="a" * 64)


def draft(*, second=False, empty=False):
    claims = [{"claim_id": "first", "text": "Measure within 3 days after complete registration.",
               "evidence_ids": ["E001"]}]
    if second:
        claims.append({"claim_id": "second", "text": "Registration must be complete.", "evidence_ids": ["E001"]})
    return DraftAnswer(disposition="insufficient_evidence" if empty else "answer",
                       claims=[] if empty else claims, limitation=None)


def critic(*, verdict="supported", issue="none", reason="SUPPORTED", match="yes", missing=(), globals=(), second=False):
    verdicts = [{"claim_id": "first", "verdict": verdict, "reason_code": reason, "explanation": "Fixture verdict.",
                 "evidence_ids": ["E001"], "issue_type": issue}]
    if second:
        verdicts.append({**verdicts[0], "claim_id": "second", "verdict": "unsupported", "reason_code": "UNSUPPORTED",
                        "issue_type": "irrelevant_evidence"})
    return CriticInternalResult(claim_verdicts=verdicts, question_match=match,
                                missing_answer_parts=list(missing), global_issues=list(globals))


@pytest.mark.parametrize("kwargs,attempt,status,action", [
    ({}, 0, "confirmed", "render"), ({}, 1, "confirmed", "render"),
    ({"verdict": "partially_supported", "issue": "missing_condition", "reason": "MISSING_CONDITION"}, 0,
     "partially_confirmed", "repair"),
    ({"verdict": "partially_supported", "issue": "incomplete_support", "reason": "INCOMPLETE_SUPPORT"}, 0,
     "partially_confirmed", "repair"),
    ({"verdict": "partially_supported", "issue": "missing_condition", "reason": "MISSING_CONDITION"}, 1,
     "partially_confirmed", "refuse"),
    ({"verdict": "unsupported", "issue": "irrelevant_evidence", "reason": "UNSUPPORTED"}, 0, "hallucinated", "refuse"),
    ({"verdict": "contradicted", "issue": "wrong_value", "reason": "WRONG_VALUE"}, 0, "hallucinated", "refuse"),
    ({"verdict": "partially_supported", "issue": "wrong_scope", "reason": "WRONG_SCOPE"}, 0, "hallucinated", "refuse"),
    ({"match": "no"}, 0, "hallucinated", "refuse"),
    ({"globals": ("Unresolved source conflict",)}, 0, "hallucinated", "refuse"),
    ({"match": "partial"}, 0, "partially_confirmed", "repair"),
    ({"missing": ("A mandatory condition",)}, 1, "partially_confirmed", "refuse"),
])
def test_severity_table(evidence, kwargs, attempt, status, action):
    result = aggregate(draft(), critic(**kwargs), evidence, repair_attempts=attempt)
    assert (result.status, result.action) == (status, action)


def test_one_unsupported_claim_blocks_an_otherwise_supported_answer(evidence):
    assert aggregate(draft(second=True), critic(second=True), evidence, repair_attempts=0).action == "refuse"


def test_fabricated_source_is_verification_refusal_not_schema_retry(evidence):
    value = draft()
    value.claims[0].evidence_ids = ["fabricated"]
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        precheck_draft(value, evidence)


@pytest.mark.parametrize("mode", ["missing_verdict", "foreign_claim", "foreign_evidence", "inconsistent_reason"])
def test_critic_binding_failure_is_technical_schema_error(evidence, mode):
    value = critic()
    if mode == "missing_verdict":
        value.claim_verdicts = []
    elif mode == "foreign_claim":
        value.claim_verdicts[0].claim_id = "alien"
    elif mode == "foreign_evidence":
        value.claim_verdicts[0].evidence_ids = ["unknown"]
    else:
        value.claim_verdicts[0].reason_code = ReasonCode.WRONG_VALUE
    with pytest.raises(ValidationFailure, match="OUTPUT_SCHEMA_INVALID"):
        aggregate(draft(), value, evidence, repair_attempts=0)


def test_empty_draft_cannot_be_confirmed(evidence):
    value = critic()
    value.claim_verdicts = []
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        aggregate(draft(empty=True), value, evidence, repair_attempts=1)


def test_renderer_preserves_seven_model_citations_for_one_supported_claim():
    document_version_id = uuid4()
    units = tuple(evidence_unit(f"E{index:03}", document_version_id=document_version_id) for index in range(1, 8))
    evidence = EvidencePack(pack_id=uuid4(), run_id=uuid4(), snapshot_id=uuid4(), units=units,
                            llm_token_count=100, manifest_hash="a" * 64)
    value = DraftAnswer(disposition="answer", claims=[{
        "claim_id": "first",
        "text": "One claim needs the full row context and seven cited source units.",
        "evidence_ids": [unit.evidence_id for unit in units],
    }], limitation=None)
    review = CriticInternalResult(claim_verdicts=[{
        "claim_id": "first",
        "verdict": "supported",
        "reason_code": "SUPPORTED",
        "explanation": "Fixture verdict.",
        "evidence_ids": [unit.evidence_id for unit in units],
        "issue_type": "none",
    }], question_match="yes", missing_answer_parts=[], global_issues=[])
    snapshot = SnapshotInfo(id=evidence.snapshot_id, captured_at=datetime.now(timezone.utc), version_count=1)

    result = render_answer(value, review, evidence, snapshot, version_labels={document_version_id: "2026 edition"}, repair_attempts=0)

    assert result.claims[0].citation_ids == [f"C{index:03}" for index in range(1, 8)]
    assert [citation.evidence_id for citation in result.citations] == [unit.evidence_id for unit in units]


def test_renderer_uses_snapshot_sources_order_and_dedup_without_freeform_limitation(evidence):
    value = draft(second=True)
    value.limitation = "Unverified normative statement that must not be rendered."
    review = critic()
    review.claim_verdicts.append(review.claim_verdicts[0].model_copy(update={"claim_id": "second"}))
    snapshot = SnapshotInfo(id=evidence.snapshot_id, captured_at=datetime.now(timezone.utc), version_count=1)
    labels = {evidence.units[0].document_version_id: "2026 edition"}
    result = render_answer(value, review, evidence, snapshot, version_labels=labels, repair_attempts=1)
    assert [claim.claim_id for claim in result.claims] == ["first", "second"]
    assert [claim.citation_ids for claim in result.claims] == [["C001"], ["C001"]]
    assert len(result.citations) == 1 and result.citations[0].pdf_pages == [12]
    assert result.citations[0].printed_page_labels == ["10"]
    assert result.citations[0].document_title == "Actual snapshot title"
    assert result.citations[0].structural_path == ["Section IV", "3.2.15"]
    assert result.citations[0].source_url == f"/api/v1/versions/{evidence.units[0].document_version_id}/source"
    assert value.limitation not in result.text and result.validation.repair_used
    assert result == render_answer(value, review, evidence, snapshot, version_labels=labels, repair_attempts=1)
    review.question_match = "partial"
    with pytest.raises(ValidationFailure):
        render_answer(value, review, evidence, snapshot, version_labels=labels, repair_attempts=1)


@pytest.mark.parametrize("code", list(RefusalCode))
def test_exact_refusal_renderer(code):
    snapshot = SnapshotInfo(id=uuid4(), captured_at=datetime.now(timezone.utc), version_count=0)
    run_id = uuid4()
    result = render_refusal(run_id, snapshot, code)
    assert result.text == REFUSAL_MESSAGES[code]
    assert result == render_refusal(run_id, snapshot, code)

@pytest.mark.parametrize("kwargs,policy", [
    ({}, "render"),
    ({"match": "partial"}, "repair"),
    ({"missing": ("A mandatory condition",)}, "repair"),
    ({"verdict": "partially_supported", "issue": "missing_condition", "reason": "MISSING_CONDITION"}, "repair"),
    ({"verdict": "unsupported", "issue": "irrelevant_evidence", "reason": "UNSUPPORTED"}, "refuse"),
    ({"verdict": "contradicted", "issue": "wrong_value", "reason": "WRONG_VALUE"}, "refuse"),
    ({"verdict": "partially_supported", "issue": "wrong_scope", "reason": "WRONG_SCOPE"}, "refuse"),
    ({"match": "no"}, "refuse"),
    ({"globals": ("Unresolved source conflict",)}, "refuse"),
])
def test_critic_storage_policy_matches_sql_policy_order(kwargs, policy):
    assert critic_storage_policy(critic(**kwargs)) == policy

