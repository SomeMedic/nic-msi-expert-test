"""Model-facing role schemas from the verified P00 spike.

All fields, including nullable fields and empty lists, are required in structured
output. Application membership checks run after decoding; no public DTO uses these.
"""
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field, model_validator

from .common import NonBlank, ShortID, StrictDTO, unique

IssueText = Annotated[NonBlank, Field(max_length=1000)]


class ReasonCode(StrEnum):
    SUPPORTED = "SUPPORTED"
    MISSING_CONDITION = "MISSING_CONDITION"
    INCOMPLETE_SUPPORT = "INCOMPLETE_SUPPORT"
    WRONG_VALUE = "WRONG_VALUE"
    WRONG_SCOPE = "WRONG_SCOPE"
    IRRELEVANT_EVIDENCE = "IRRELEVANT_EVIDENCE"
    CITATION_MISMATCH = "CITATION_MISMATCH"
    UNSUPPORTED = "UNSUPPORTED"
    CONTRADICTED = "CONTRADICTED"
    OTHER = "OTHER"


class RouteDecision(StrictDTO):
    classification: Literal["in_scope", "out_of_scope", "uncertain"]
    matched_descriptor_ids: list[ShortID] = Field(max_length=12)
    reason: NonBlank = Field(max_length=500)

    @model_validator(mode="after")
    def unique_descriptors(self):
        unique(self.matched_descriptor_ids, "descriptor IDs")
        return self

    def validate_binding(self, descriptor_ids: set[str]) -> None:
        if not set(self.matched_descriptor_ids) <= descriptor_ids:
            raise ValueError("unknown routing descriptor")


class DraftClaim(StrictDTO):
    claim_id: ShortID
    text: NonBlank = Field(max_length=1800)
    evidence_ids: list[ShortID] = Field(min_length=1, max_length=10)

    @model_validator(mode="after")
    def unique_evidence(self):
        unique(self.evidence_ids, "evidence IDs")
        return self


class DraftAnswer(StrictDTO):
    disposition: Literal["answer", "insufficient_evidence"]
    claims: list[DraftClaim] = Field(max_length=12)
    limitation: NonBlank | None = Field(max_length=500)

    @model_validator(mode="after")
    def consistent_claims(self):
        if (self.disposition == "answer") != bool(self.claims):
            raise ValueError("draft disposition and claims disagree")
        unique([claim.claim_id for claim in self.claims], "claim IDs")
        return self

    def validate_binding(self, evidence_ids: set[str]) -> None:
        if any(not set(claim.evidence_ids) <= evidence_ids for claim in self.claims):
            raise ValueError("draft referenced unknown evidence")


class ClaimVerdict(StrictDTO):
    claim_id: ShortID
    verdict: Literal["supported", "partially_supported", "unsupported", "contradicted"]
    reason_code: ReasonCode
    explanation: NonBlank = Field(max_length=1000)
    evidence_ids: list[ShortID] = Field(max_length=10)
    issue_type: Literal["none", "missing_condition", "incomplete_support", "wrong_value", "wrong_scope", "irrelevant_evidence", "citation_mismatch", "other"]

    @model_validator(mode="after")
    def consistent_support(self):
        unique(self.evidence_ids, "verdict evidence IDs")
        if self.verdict in {"supported", "partially_supported"} and not self.evidence_ids:
            raise ValueError("supported verdict requires evidence")
        if self.verdict == "supported" and self.issue_type != "none":
            raise ValueError("supported verdict cannot have an issue")
        if self.verdict != "supported" and self.issue_type == "none":
            raise ValueError("non-supported verdict requires an issue")
        return self


class CriticInternalResult(StrictDTO):
    claim_verdicts: list[ClaimVerdict] = Field(max_length=12)
    question_match: Literal["yes", "partial", "no"] = Field(description=(
        "Does the draft answer the user's question? yes means the question is fully addressed, "
        "not that the draft agrees with its premise. A source-supported correction or refutation "
        "of a false premise is yes. partial means a requested part is missing; no means the draft "
        "does not address the question."
    ))
    missing_answer_parts: list[IssueText] = Field(max_length=12)
    global_issues: list[IssueText] = Field(max_length=12)

    @model_validator(mode="after")
    def unique_claim_verdicts(self):
        unique([verdict.claim_id for verdict in self.claim_verdicts], "verdict claim IDs")
        return self

    def validate_binding(self, draft: DraftAnswer, evidence_ids: set[str]) -> None:
        draft.validate_binding(evidence_ids)
        if {v.claim_id for v in self.claim_verdicts} != {c.claim_id for c in draft.claims}:
            raise ValueError("critic must cover exactly the draft claims")
        if any(not set(v.evidence_ids) <= evidence_ids for v in self.claim_verdicts):
            raise ValueError("critic referenced unknown evidence")
