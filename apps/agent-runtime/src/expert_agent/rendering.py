"""Render verified claims and snapshot metadata without any further model rewrite."""
from collections.abc import Mapping
from uuid import UUID, uuid5

from expert_contracts.errors import REFUSAL_MESSAGES, RefusalCode
from expert_contracts.internal import EvidencePack
from expert_contracts.model import CriticInternalResult, DraftAnswer
from expert_contracts.runs import FinalAnswer, PublicClaim, RefusalResult, SnapshotInfo, ValidationSummary
from expert_contracts.sources import CitationDTO

from expert_agent.validation import ValidationFailure, aggregate


def render_answer(draft: DraftAnswer, critic: CriticInternalResult, evidence: EvidencePack,
                  snapshot: SnapshotInfo, *, version_labels: Mapping[UUID, str | None],
                  repair_attempts: int, bindings_json: str | None = None,
                  question: str = "") -> FinalAnswer:
    """Return plain text; frontend must render it as text, never trusted HTML.

    The caller supplies metadata from the immutable snapshot and must perform
    current source/revocation checks; the finalizer repeats them in PostgreSQL.
    The model's optional free-form limitation is not a separately verified claim
    and is deliberately excluded from the public answer.
    """
    decision = aggregate(draft, critic, evidence, repair_attempts=repair_attempts, bindings_json=bindings_json,
                         question=question)
    if decision.action != "render" or snapshot.id != evidence.snapshot_id:
        raise ValidationFailure("VERIFICATION_FAILED")
    by_id = {unit.evidence_id: unit for unit in evidence.units}
    citations: dict[str, CitationDTO] = {}
    public_claims = []
    paragraphs = []
    for claim in draft.claims:
        citation_ids = []
        for evidence_id in claim.evidence_ids:
            if evidence_id not in citations:
                unit = by_id[evidence_id]
                if unit.document_version_id not in version_labels:
                    raise ValidationFailure("SOURCE_UNAVAILABLE")
                labels = list(dict.fromkeys(span.printed_page_label for span in unit.source_spans
                                            if span.printed_page_label is not None))
                citations[evidence_id] = CitationDTO(citation_id=f"C{len(citations) + 1:03}",
                    evidence_id=evidence_id, document_title=unit.document_title,
                    version_label=version_labels[unit.document_version_id],
                    structural_path=list(unit.structural_path),
                    pdf_pages=sorted({span.pdf_page for span in unit.source_spans}),
                    printed_page_labels=labels,
                    source_url=f"/api/v1/versions/{unit.document_version_id}/source")
            citation_ids.append(citations[evidence_id].citation_id)
        public_claims.append(PublicClaim(claim_id=claim.claim_id, text=claim.text, citation_ids=citation_ids))
        paragraphs.append(claim.text + " " + " ".join(f"[{key}]" for key in citation_ids))
    paragraphs.append(f"Ответ сформирован по загруженной базе на момент {snapshot.captured_at.isoformat(timespec='microseconds')}; "
                      "автоматическая проверка не заменяет экспертную оценку.")
    return FinalAnswer(result_id=uuid5(evidence.run_id, "final-result"), text="\n\n".join(paragraphs),
        claims=public_claims, citations=list(citations.values()), snapshot=snapshot,
        validation=ValidationSummary(claim_count=len(public_claims), supported_count=len(public_claims),
                                     repair_used=bool(repair_attempts)))


def render_refusal(run_id: UUID, snapshot: SnapshotInfo, code: RefusalCode) -> RefusalResult:
    return RefusalResult(result_id=uuid5(run_id, "final-result"), code=code,
                         text=REFUSAL_MESSAGES[code], snapshot=snapshot)
