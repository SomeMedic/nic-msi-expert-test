"""Numeric claim literals must be present in that claim's cited evidence."""
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from expert_agent.llm.types import sha256
from expert_agent.rendering import render_answer
from expert_agent.validation import ValidationFailure, aggregate, precheck_draft
from expert_contracts.internal import EvidencePack, EvidenceUnit
from expert_contracts.model import CriticInternalResult, DraftAnswer
from expert_contracts.runs import SnapshotInfo
from expert_contracts.sources import SourceSpan


def evidence(*texts: str) -> EvidencePack:
    units = []
    for index, text in enumerate(texts, start=1):
        units.append(EvidenceUnit(evidence_id=f"E{index:03}", document_version_id=uuid4(),
            index_generation_id=uuid4(), canonical_node_id=uuid4(), source_chunk_ids=(uuid4(),),
            document_title="Fixture", structural_path=("Section",), excerpt=text,
            source_spans=(SourceSpan(pdf_page=index, block_id=f"b{index}", start_offset=0, end_offset=len(text)),),
            content_hash=sha256(text)))
    return EvidencePack(pack_id=uuid4(), run_id=uuid4(), snapshot_id=uuid4(), units=tuple(units),
                        llm_token_count=100, manifest_hash="a" * 64)


def draft(text: str, citations: list[str]) -> DraftAnswer:
    return DraftAnswer(disposition="answer", limitation=None,
                       claims=[{"claim_id": "c1", "text": text, "evidence_ids": citations}])


def supported_critic() -> CriticInternalResult:
    return CriticInternalResult(claim_verdicts=[{"claim_id": "c1", "verdict": "supported",
        "reason_code": "SUPPORTED", "explanation": "Fixture support", "evidence_ids": ["E001"],
        "issue_type": "none"}], question_match="yes", missing_answer_parts=[], global_issues=[])


def test_numeric_literals_normalize_decimal_comma_and_space_thousands_without_float_rounding():
    pack = evidence("Порог составляет 10 000,5 единицы, коэффициент равен 2,75.")
    precheck_draft(draft("Порог составляет 10000.5 единицы, коэффициент равен 2.75.", ["E001"]), pack)


def test_split_table_numbers_match_claim_with_units_without_trailing_space_capture():
    pack = evidence("Минимальная ставка: 120", "Максимальная ставка: 540")
    precheck_draft(draft("Минимальная ставка 120 рублей, максимальная ставка 540 рублей.", ["E001", "E002"]), pack)


@pytest.mark.parametrize("claim_text", [
    "Порог составляет 10000 единиц.",
    "Коэффициент равен 2,750.",
])
def test_numeric_precheck_does_not_assume_ambiguous_separator_conversion(claim_text):
    pack = evidence("Порог составляет 10,000 единиц, коэффициент равен 2,75.")
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        precheck_draft(draft(claim_text, ["E001"]), pack)


def test_digit_spaces_are_not_merged_unless_they_are_valid_thousands_groups():
    pack = evidence("Пункты 12 54 перечислены отдельно.")
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED") as caught:
        precheck_draft(draft("Пункт 1254 указан в источнике.", ["E001"]), pack)
    assert str(caught.value) == "VERIFICATION_FAILED"


def test_valid_nbsp_thousands_group_matches_plain_digits():
    pack = evidence("Порог составляет 1\u00a0200 единиц.")
    precheck_draft(draft("Порог составляет 1200 единиц.", ["E001"]), pack)


@pytest.mark.parametrize(("source", "claim"), [
    ("Диапазон составляет 12-54 единицы.", "Диапазон составляет 12–54 единицы."),
    ("Диапазон составляет 12–54 единицы.", "Диапазон составляет 12-54 единицы."),
])
def test_hyphen_and_dash_ranges_are_not_interpreted_as_negative_missing_values(source, claim):
    pack = evidence(source)
    precheck_draft(draft(claim, ["E001"]), pack)


def test_numeric_literals_adjacent_to_unit_suffixes_are_checked():
    pack = evidence("Размер составляет 12м, доля составляет 3%.")
    precheck_draft(draft("Размер составляет 12 м, доля составляет 3%.", ["E001"]), pack)


def test_numeric_literal_from_other_evidence_does_not_support_wrong_claim_citation():
    pack = evidence("Минимальная ставка составляет 120 рублей.", "Максимальная ставка составляет 540 рублей.")
    value = draft("Минимальная ставка составляет 540 рублей.", ["E001"])
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        precheck_draft(value, pack)


def test_invented_numeric_value_fails_before_supported_critic_can_render():
    pack = evidence("Срок регистрации составляет 3 рабочих дня.")
    value = draft("Срок регистрации составляет 5 рабочих дней.", ["E001"])
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        aggregate(value, supported_critic(), pack, repair_attempts=0)
    with pytest.raises(ValidationFailure, match="VERIFICATION_FAILED"):
        render_answer(value, supported_critic(), pack,
            SnapshotInfo(id=pack.snapshot_id, captured_at=datetime.now(timezone.utc), version_count=1),
            version_labels={pack.units[0].document_version_id: "fixture"}, repair_attempts=0)


def test_blank_template_form_claim_is_not_globally_rejected_when_cited_excerpt_contains_number():
    pack = evidence("Поле 3: код заявителя.")
    precheck_draft(draft("Форма содержит поле 3 для кода заявителя.", ["E001"]), pack)


def test_blank_template_style_form_claim_without_numeric_value_still_passes_citation_precheck():
    pack = evidence("Поле для кода заявителя.")
    precheck_draft(draft("Форма содержит поле для кода заявителя.", ["E001"]), pack)
