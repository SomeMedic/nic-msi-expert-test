"""Deterministic publication policy; citation membership never substitutes for Critic."""
from collections.abc import Mapping
from dataclasses import dataclass
import re
import unicodedata
from typing import Literal

from pydantic import ValidationError

from expert_contracts.internal import EvidencePack
from expert_contracts.model import CriticInternalResult, DraftAnswer, ReasonCode

from .llm.prompts import render_evidence
from .llm.types import LlmError

_THOUSANDS_SEPARATOR = r"[ \u00a0\u202f\u2009'’]"
_NUMERIC_LITERAL = re.compile(
    rf"(?<!\d)[+-]?(?:\d{{1,3}}(?:{_THOUSANDS_SEPARATOR}\d{{3}})+(?:[,.]\d+)?|\d+(?:[,.]\d+)?)(?!\d)"
)
_VALID_THOUSANDS = re.compile(rf"^\d{{1,3}}(?:{_THOUSANDS_SEPARATOR}\d{{3}})+(?:[,.]\d+)?$")
_DIGIT_SPACE = re.compile(rf"(?<=\d){_THOUSANDS_SEPARATOR}(?=\d)")


class ValidationFailure(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class PolicyDecision:
    status: Literal["confirmed", "partially_confirmed", "hallucinated"]
    action: Literal["render", "repair", "refuse"]
    reason: Literal["SUPPORTED", "UNSUPPORTED_CLAIM", "QUESTION_MISMATCH", "GLOBAL_ISSUE",
                    "UNSAFE_PARTIAL", "INCOMPLETE_ANSWER", "REPAIR_EXHAUSTED",
                    "INCOMPLETE_TABLE_VARIANT"]


def critic_storage_policy(critic: CriticInternalResult) -> Literal["render", "repair", "refuse"]:
    """Mirror SQL agent._policy for the immutable raw Critic artifact."""
    unsafe_partial = any(
        item.verdict == "partially_supported"
        and (item.issue_type not in {"missing_condition", "incomplete_support"}
             or item.reason_code not in {ReasonCode.MISSING_CONDITION, ReasonCode.INCOMPLETE_SUPPORT})
        for item in critic.claim_verdicts
    )
    if (any(item.verdict in {"unsupported", "contradicted"} for item in critic.claim_verdicts)
            or critic.question_match == "no" or critic.global_issues or unsafe_partial):
        return "refuse"
    if (critic.question_match == "partial" or critic.missing_answer_parts
            or any(item.verdict == "partially_supported" for item in critic.claim_verdicts)):
        return "repair"
    return "render"


def precheck_draft(draft: DraftAnswer, evidence: EvidencePack, *, bindings_json: str | None = None) -> None:
    """Called after persisted pack/source identity and current access checks.

    Invalid JSON shape is a technical failure. A fabricated source in a valid
    draft is a verification refusal and must not get a schema-only retry.
    """
    try:
        DraftAnswer.model_validate(draft.model_dump(mode="json"))
        EvidencePack.model_validate(evidence.model_dump(mode="json"))
    except ValidationError:
        raise ValidationFailure("OUTPUT_SCHEMA_INVALID") from None
    try:
        draft.validate_binding({unit.evidence_id for unit in evidence.units})
        _validate_no_repeated_claim_text(draft)
        _validate_claim_numeric_literals(draft, evidence)
        if bindings_json is not None:
            _validate_claim_table_context(draft, evidence, bindings_json)
    except ValueError:
        raise ValidationFailure("VERIFICATION_FAILED") from None


_WORD = re.compile(r"\w+", re.UNICODE)
_QUOTED = re.compile(r"[\"«“](.*?)[\"»”]")


def enrich_table_variant_missing_parts(question: str, draft: DraftAnswer, critic: CriticInternalResult,
                                       evidence: EvidencePack,
                                       bindings_json: str | None) -> CriticInternalResult:
    """Return a repair-facing Critic copy with deterministic table-variant gaps."""
    missing = _table_variant_missing_parts(question, draft, evidence, bindings_json)
    if not missing:
        return critic
    merged = [*critic.missing_answer_parts]
    for item in missing:
        if item not in merged:
            merged.append(item)
    return critic.model_copy(update={"question_match": "partial", "missing_answer_parts": merged})


def _table_variant_missing_parts(question: str, draft: DraftAnswer, evidence: EvidencePack,
                                 bindings_json: str | None) -> list[str]:
    if bindings_json is None:
        return []
    try:
        projection = render_evidence(evidence, bindings_json)
    except LlmError:
        raise ValidationFailure("EVIDENCE_PACK_MISMATCH") from None
    options = _projection_records(projection, "table_row_options")
    groups = _projection_records(projection, "table_variant_groups")
    if not options or not groups:
        return []
    by_id = {unit.evidence_id: unit for unit in evidence.units}
    cited = {evidence_id for claim in draft.claims for evidence_id in claim.evidence_ids}
    messages: list[str] = []
    for group in groups:
        indices = [index for index in group.get("row_option_indices", [])
                   if type(index) is int and 0 <= index < len(options)]
        if len(indices) < 2:
            continue
        group_value_ids = {evidence_id for index in indices
                           for evidence_id in options[index].get("value_evidence_ids", [])}
        if not cited.intersection(group_value_ids):
            continue
        required_indices = _question_selected_variant_indices(question, options, group, indices, by_id) or indices
        for index in required_indices:
            option = options[index]
            complete = set(option.get("complete_row_citation_ids", []))
            if complete <= cited:
                continue
            missing = [evidence_id for evidence_id in [*option.get("subject_evidence_ids", []),
                                                       *option.get("value_evidence_ids", [])]
                       if evidence_id not in cited]
            if not missing:
                missing = [evidence_id for evidence_id in option.get("complete_row_citation_ids", [])
                           if evidence_id not in cited]
            if missing:
                messages.append("Неполный вариант строки таблицы"
                                f" row={option.get('row')}: добавь отдельный claim с условием и значениями этой"
                                f" строки из table_row_options; используй evidence_ids {', '.join(missing)}.")
    return messages[:12]


def _projection_records(projection: dict, name: str) -> list[dict]:
    fields = projection.get(f"{name}_fields")
    rows = projection.get(name)
    if not isinstance(fields, list) or not isinstance(rows, list):
        return []
    records: list[dict] = []
    for row in rows:
        if isinstance(row, list) and len(row) == len(fields):
            records.append(dict(zip(fields, row, strict=True)))
    return records


def _question_selected_variant_indices(question: str, options: list[dict], group: dict, indices: list[int],
                                       by_id: Mapping[str, object]) -> list[int]:
    question_words = set(_WORD.findall(_normalize_text(question)))
    quoted = [_normalize_text(item) for item in _QUOTED.findall(question)]
    base = group.get("base_row_option_index")
    subject_words = {index: _row_subject_words(options[index], by_id) for index in indices}
    subject_texts = {index: _normalize_text(_row_subject_text(options[index], by_id)) for index in indices}
    selected: list[int] = []
    for index in indices:
        subject_text = subject_texts[index]
        if subject_text and any(subject_text == item for item in quoted):
            selected.append(index)
            continue
        if index == base:
            continue
        peer_words = set().union(*(words for peer, words in subject_words.items() if peer != index))
        distinctive = subject_words[index] - peer_words
        if distinctive and distinctive <= question_words:
            selected.append(index)
    return selected


def _row_subject_words(option: dict, by_id: Mapping[str, object]) -> set[str]:
    return set(_WORD.findall(_normalize_text(_row_subject_text(option, by_id))))


def _row_subject_text(option: dict, by_id: Mapping[str, object]) -> str:
    return " ".join(getattr(by_id[evidence_id], "excerpt", "")
                    for evidence_id in option.get("subject_evidence_ids", []) if evidence_id in by_id)


def _normalize_text(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")


def _validate_no_repeated_claim_text(draft: DraftAnswer) -> None:
    seen: dict[tuple[str, frozenset[str]], str] = {}
    for claim in draft.claims:
        # Exact text only: wording, whitespace, Unicode bytes, punctuation,
        # numbers and cited source sets stay unchanged.
        key = (claim.text, frozenset(claim.evidence_ids))
        first_claim_id = seen.get(key)
        if first_claim_id is not None and first_claim_id != claim.claim_id:
            raise ValueError("Repeated draft claim text with identical evidence set")
        seen[key] = claim.claim_id


def _validate_claim_table_context(draft: DraftAnswer, evidence: EvidencePack, bindings_json: str) -> None:
    # Render only after authenticating the immutable manifest, exactly as the LLM
    # sees it. Missing context blocks publication; it never adds model citations.
    try:
        projection = render_evidence(evidence, bindings_json)
    except LlmError:
        raise ValidationFailure("EVIDENCE_PACK_MISMATCH") from None
    groups = [dict(zip(projection["citation_groups_fields"], row, strict=True))
              for row in projection["citation_groups"]]
    source_numbers = {unit.evidence_id: _numeric_literals(unit.excerpt) for unit in evidence.units}
    for claim in draft.claims:
        numbers = _numeric_literals(claim.text)
        cited = set(claim.evidence_ids)
        for group in groups:
            triggers = cited.intersection(group["trigger_evidence_ids"])
            if (any(numbers.intersection(source_numbers[identifier]) for identifier in triggers)
                    and not set(group["required_evidence_ids"]) <= cited):
                raise ValueError("Numeric table claim lacks its source-derived row context")


def _validate_claim_numeric_literals(draft: DraftAnswer, evidence: EvidencePack) -> None:
    source_numbers = {unit.evidence_id: _numeric_literals(unit.excerpt) for unit in evidence.units}
    for claim in draft.claims:
        claim_numbers = _numeric_literals(claim.text)
        if not claim_numbers:
            continue
        cited_numbers = set().union(*(source_numbers[identifier] for identifier in claim.evidence_ids))
        if not claim_numbers <= cited_numbers:
            raise ValueError("Claim numeric literals not found in cited excerpts")


def _numeric_literals(text: str) -> set[str]:
    return {_canonical_numeric_literal(match[0]) for match in _NUMERIC_LITERAL.finditer(text)}


def _canonical_numeric_literal(value: str) -> str:
    compact = _DIGIT_SPACE.sub("", value) if _VALID_THOUSANDS.fullmatch(value.lstrip("+-")) else value
    sign = compact[0] if compact[:1] in {"-", "+"} else ""
    body = compact[1:] if sign else compact
    if sign == "+":
        sign = ""
    comma_count, dot_count = body.count(","), body.count(".")
    if comma_count and not dot_count:
        body = _canonical_single_separator(body, ",")
    elif dot_count and not comma_count:
        body = _canonical_single_separator(body, ".")
    return sign + body


def _canonical_single_separator(value: str, separator: str) -> str:
    parts = value.split(separator)
    if len(parts) == 2 and len(parts[1]) != 3:
        return ".".join(parts)
    if len(parts) > 2 and all(len(part) == 3 for part in parts[1:]):
        return "".join(parts)
    return value


def aggregate(draft: DraftAnswer, critic: CriticInternalResult, evidence: EvidencePack,
              *, repair_attempts: int, bindings_json: str | None = None,
              question: str = "") -> PolicyDecision:
    if type(repair_attempts) is not int or repair_attempts not in (0, 1):
        raise ValidationFailure("GENERATION_INVALID")
    precheck_draft(draft, evidence, bindings_json=bindings_json)
    if draft.disposition != "answer" or not draft.claims:
        # Initial insufficient evidence is handled before Critic by the graph;
        # an empty repair can never exploit all([]) to become confirmed.
        raise ValidationFailure("VERIFICATION_FAILED")
    try:
        CriticInternalResult.model_validate(critic.model_dump(mode="json"))
        critic.validate_binding(draft, {unit.evidence_id for unit in evidence.units})
    except (ValidationError, ValueError):
        raise ValidationFailure("OUTPUT_SCHEMA_INVALID") from None
    if any((item.verdict == "supported") != (item.reason_code == ReasonCode.SUPPORTED)
           for item in critic.claim_verdicts):
        raise ValidationFailure("OUTPUT_SCHEMA_INVALID")
    if any(item.verdict in {"unsupported", "contradicted"} for item in critic.claim_verdicts):
        return PolicyDecision("hallucinated", "refuse", "UNSUPPORTED_CLAIM")
    if critic.question_match == "no":
        return PolicyDecision("hallucinated", "refuse", "QUESTION_MISMATCH")
    if critic.global_issues:
        # Global issues are currently free-text in the accepted role schema.
        # Their severity cannot safely be inferred by parsing explanation prose.
        return PolicyDecision("hallucinated", "refuse", "GLOBAL_ISSUE")
    partials = [item for item in critic.claim_verdicts if item.verdict == "partially_supported"]
    safe_issues = {"missing_condition", "incomplete_support"}
    safe_reasons = {ReasonCode.MISSING_CONDITION, ReasonCode.INCOMPLETE_SUPPORT}
    if any(item.issue_type not in safe_issues or item.reason_code not in safe_reasons for item in partials):
        return PolicyDecision("hallucinated", "refuse", "UNSAFE_PARTIAL")
    variant_missing = _table_variant_missing_parts(question, draft, evidence, bindings_json)
    if partials or critic.question_match == "partial" or critic.missing_answer_parts or variant_missing:
        if repair_attempts:
            if variant_missing:
                return PolicyDecision("partially_confirmed", "refuse", "INCOMPLETE_TABLE_VARIANT")
            return PolicyDecision("partially_confirmed", "refuse", "REPAIR_EXHAUSTED")
        if variant_missing:
            return PolicyDecision("partially_confirmed", "repair", "INCOMPLETE_TABLE_VARIANT")
        return PolicyDecision("partially_confirmed", "repair", "INCOMPLETE_ANSWER")
    return PolicyDecision("confirmed", "render", "SUPPORTED")

