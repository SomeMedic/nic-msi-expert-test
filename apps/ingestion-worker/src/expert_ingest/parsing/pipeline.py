"""Local parse orchestration. A parse artifact is never a ready index generation."""
from pathlib import Path

from .dto import Diagnostic, ParsedArtifact, ParseRequest
from .normalize import Normalizer
from .primary import DocumentParser, apply_region_reviews, enrich_symbols, reassess_table_layout
from .quality import ArtifactValidator
from .structure import StructureBuilder


def parse_document(path: Path, request: ParseRequest, *, artifacts_path: Path | None = None,
                   asset_lock_path: Path | None = None) -> ParsedArtifact:
    # This entry runs only after the caller has installed the parser sandbox.
    from .tables import enrich_tables

    document = DocumentParser().parse(path, request)
    document = enrich_symbols(apply_region_reviews(document, path, request.region_reviews), path)
    document = reassess_table_layout(enrich_tables(document, path))
    fallback = {page.pdf_page for page in document.pages if page.decision in {"need_ocr", "need_layout_reparse"}}
    fallback.update(d.pdf_page for d in document.diagnostics if d.pdf_page is not None
                    and d.severity == "critical" and ("TABLE" in d.code or "LAYOUT" in d.code))
    if fallback:
        if artifacts_path is None or asset_lock_path is None:
            document = document.model_copy(update={"diagnostics": document.diagnostics + (
                Diagnostic(code="FALLBACK_ASSETS_NOT_CONFIGURED", severity="critical"),)})
        else:
            from .docling import enrich_docling
            # Neighbours supply table continuation and reading-order context.
            selected = tuple(sorted({q for p in fallback for q in (p - 1, p, p + 1) if 1 <= q <= len(document.pages)}))
            ocr_pages = {page.pdf_page for page in document.pages if page.decision == "need_ocr"}
            # Force-full-page OCR must never rewrite text-bearing neighbour pages.
            # A table crossing these batches stays unresolved unless each source
            # region has independent provenance; the adapter guards partial tables.
            for pages, use_ocr in ((tuple(p for p in selected if p not in ocr_pages), False),
                                   (tuple(p for p in selected if p in ocr_pages), True)):
                if pages:
                    document = enrich_docling(document, path, pages, artifacts_path=artifacts_path,
                                              asset_lock_path=asset_lock_path, use_ocr=use_ocr,
                                              timeout_seconds=min(300, request.limits.wall_seconds))
            document = reassess_table_layout(document)
    canonical = StructureBuilder().build(Normalizer().normalize(document))
    report = ArtifactValidator(trusted_region_reviews=request.region_reviews).validate(canonical)
    return ParsedArtifact(document=canonical, quality_report=report)
