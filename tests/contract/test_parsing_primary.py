import hashlib
from uuid import uuid4

import pymupdf
import pytest

from expert_ingest.parsing.dto import ParseRequest, ParserLimits
from expert_ingest.parsing.primary import DocumentParser, ParseFailure


def parse(path, **limits):
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Synthetic unknown source",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(), limits=ParserLimits(**limits))
    return DocumentParser().parse(path, request)


def test_unknown_pdf_raw_geometry_and_physical_pages(tmp_path):
    path = tmp_path / "unrelated-random.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=300, height=400)
        page.insert_text((30, 70), "1. Keep source number 73.")
        page.set_cropbox(pymupdf.Rect(10, 10, 290, 390))
        page.set_rotation(90)
        pdf.new_page(width=300, height=400)
        pdf.save(path)
    result = parse(path)
    assert [p.pdf_page for p in result.pages] == [1, 2]
    assert result.pages[0].rotation == 90
    assert (result.pages[0].width, result.pages[0].height) == (280, 380)
    assert result.pages[1].decision == "intentional_blank"
    assert result.blocks[0].text == "1. Keep source number 73."
    for block in result.blocks:
        assert block.bbox[2] <= 280 and block.bbox[3] <= 380
        assert "".join(c.text for c in block.chars) == block.text


def test_image_scan_requests_ocr_and_does_not_invent_text(tmp_path):
    path = tmp_path / "new-raster.pdf"
    with pymupdf.open() as source:
        page = source.new_page(width=200, height=100)
        page.insert_text((15, 35), "Pressure 17 + 3")
        pixmap = page.get_pixmap()
        with pymupdf.open() as scan:
            scan.new_page(width=200, height=100).insert_image(pymupdf.Rect(0, 0, 200, 100), pixmap=pixmap)
            scan.save(path)
    result = parse(path)
    assert result.pages[0].decision == "need_ocr"
    assert not result.blocks
    assert any(d.code == "OCR_REQUIRED" for d in result.diagnostics)


def test_mixed_pdf_fallback_ocr_never_covers_text_neighbors(tmp_path, monkeypatch):
    from expert_ingest.parsing import docling
    from expert_ingest.parsing.pipeline import parse_document
    path = tmp_path / "unknown-mixed.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=500, height=500)
        for row in range(4):
            page.insert_text((30, 60 + row * 25), f"Left condition {row}")
            page.insert_text((280, 60 + row * 25), f"Right condition {row}")
        pdf.new_page(width=500, height=500).insert_text((30, 100), "Ordinary neighbor 2")
        with pymupdf.open() as raster:
            scan = raster.new_page(width=500, height=500)
            scan.insert_text((30, 100), "Unknown scanned condition")
            pdf.new_page(width=500, height=500).insert_image(scan.rect, pixmap=scan.get_pixmap())
        pdf.new_page(width=500, height=500).insert_text((30, 100), "Ordinary neighbor 4")
        pdf.save(path)
    calls = []
    def fallback(document, source, pages, **options):
        calls.append((pages, options["use_ocr"]))
        return document
    monkeypatch.setattr(docling, "enrich_docling", fallback)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Mixed unknown PDF",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    artifact = parse_document(path, request, artifacts_path=tmp_path, asset_lock_path=tmp_path / "unused.json")
    assert calls == [((1, 2, 4), False), ((3,), True)]
    assert artifact.quality_report.status == "failed"
    assert "OCR_REQUIRED" in {d.code for d in artifact.quality_report.diagnostics}


def test_two_columns_require_layout_review(tmp_path):
    path = tmp_path / "unknown-columns.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=500, height=500)
        for i in range(4):
            page.insert_text((30, 50 + i * 25), f"Left source {i}")
            page.insert_text((280, 50 + i * 25), f"Right source {i}")
        pdf.save(path)
    result = parse(path)
    assert result.pages[0].decision == "need_layout_reparse"
    assert [b.text.split()[0] for b in result.blocks] == ["Left"] * 4 + ["Right"] * 4


def test_vector_only_stroke_is_never_an_intentionally_blank_page(tmp_path):
    path = tmp_path / "unknown-drawing.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=200, height=200)
        page.draw_line((30, 60), (170, 60), width=1)
        pdf.save(path)
    result = parse(path)
    assert result.pages[0].decision == "need_layout_reparse"
    assert len(result.pages[0].vector_regions) == 1
    assert "VECTOR_CONTENT_UNRESOLVED" in {d.code for d in result.diagnostics}


def test_running_header_and_signature_do_not_create_body_columns(tmp_path):
    path = tmp_path / "unknown-letter.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=500, height=500)
        for i in range(4):
            page.insert_text((20, 12 + i * 10), "Left header", fontsize=7)
            page.insert_text((330, 12 + i * 10), "Right header", fontsize=7)
        for i in range(4):
            page.insert_text((20, 120 + i * 25), "Short text ending")
            page.insert_text((330, 310 + i * 25), "Signed")
        pdf.save(path)
    result = parse(path)
    assert result.pages[0].decision == "accept_text"


def test_separately_positioned_words_are_not_column_gutters(tmp_path):
    from expert_ingest.parsing.normalize import Normalizer
    from expert_ingest.parsing.structure import StructureBuilder
    path = tmp_path / "word-fragments.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=500, height=400)
        for row in range(3):
            x = 30.0
            for word in [f"{row + 1}.", "Separate", "positioned", "words", "keep", "their", "source", "order."]:
                page.insert_text((x, 100 + row * 30), word + " ")
                x += pymupdf.get_text_length(word + " ", fontsize=11) + 4
        pdf.save(path)
    raw = parse(path)
    assert raw.pages[0].decision == "accept_text"
    tree = StructureBuilder().build(Normalizer().normalize(raw))
    assert [node.number for node in tree.nodes if node.node_type == "clause"] == ["1", "2", "3"]
    assert tree.nodes[1].own_body == "1. Separate positioned words keep their source order."


def test_unknown_russian_pdf_pipeline_keeps_critical_symbols(tmp_path):
    from expert_ingest.parsing.pipeline import parse_document
    path = tmp_path / "new-russian-generated.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=500, height=400)
        page.insert_htmlbox(pymupdf.Rect(25, 70, 470, 250),
                            "<p>Статья 8 Проверка</p><p>1. Не более 19 м³ ± 4.</p>")
        pdf.save(path)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Новая тестовая инструкция",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    artifact = parse_document(path, request)
    assert artifact.quality_report.status == "passed", artifact.quality_report
    assert any("Не более 19 м³ ± 4." in n.own_body for n in artifact.document.nodes)


def test_repeated_margins_exclude_only_headers_preserving_body_and_notes(tmp_path):
    from expert_ingest.parsing.normalize import Normalizer
    from expert_ingest.parsing.quality import ArtifactValidator
    from expert_ingest.parsing.structure import StructureBuilder
    path = tmp_path / "independent-repeated-margins.pdf"
    with pymupdf.open() as pdf:
        for number in range(1, 4):
            page = pdf.new_page(width=400, height=400)
            page.insert_text((25, 20), "Repeated metadata", fontsize=8)
            page.insert_text((25, 130), "Repeated metadata", fontsize=8)
            page.insert_text((25, 170), f"{number}. Body clause continues.", fontsize=8)
            page.insert_text((25, 380), "1) Mandatory footnote", fontsize=6)
            page.insert_text((25, 395), f"Page {number}", fontsize=6)
        pdf.save(path)
    raw = parse(path)
    doc = StructureBuilder().build(Normalizer().normalize(raw))
    assert len(doc.exclusions) == 6
    assert all(e.evidence_pages == (1, 2, 3) for e in doc.exclusions)
    canonical = " ".join(n.own_body for n in doc.nodes)
    assert canonical.count("Repeated metadata") == 3
    assert canonical.count("Mandatory footnote") == 3
    assert "Page 1" not in canonical
    assert ArtifactValidator().validate(doc).status == "passed"
    assert raw.blocks == doc.blocks
    forged = doc.model_copy(update={"exclusions": ()})
    assert "BOILERPLATE_EXCLUSION_INVALID" in {d.code for d in ArtifactValidator().validate(forged).diagnostics}


def symbol_artifact(tmp_path):
    from expert_ingest.parsing.pipeline import parse_document
    width, height = 19, 23
    pixels = bytearray([255] * (width * height))
    for y in (8, 9, 17, 18):
        for x in range(3, 16):
            pixels[y * width + x] = 0
    for y in range(4, 14):
        for x in (9, 10):
            pixels[y * width + x] = 0
    glyph = pymupdf.Pixmap(pymupdf.csGRAY, width, height, bytes(pixels), False)
    path = tmp_path / "unknown-inline-raster.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=300, height=200)
        page.insert_text((30, 90), "Pressure 14")
        page.insert_image(pymupdf.Rect(100, 78.5, 109.5, 90), pixmap=glyph)
        pdf.save(path)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Synthetic raster evidence",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return parse_document(path, request)


def test_qualified_raster_proof_is_explicit_and_replayed(tmp_path):
    artifact = symbol_artifact(tmp_path)
    assert artifact.quality_report.status == "passed", artifact.quality_report
    proof = artifact.document.symbol_proofs[0]
    assert proof.classification == "±" and proof.crop_encoding == "luma8"
    assert proof.may_insert_as_pdf_text_layer is False
    assert any(b.kind == "symbol" and b.text == "±" for b in artifact.document.blocks)
    assert all("±" not in b.text for b in artifact.document.blocks if b.kind == "text")


def test_superscript_three_uses_source_baseline_and_keeps_raw_three(tmp_path):
    from expert_ingest.parsing.pipeline import parse_document
    path = tmp_path / "unknown-superscript.pdf"
    with pymupdf.open() as pdf:
        page = pdf.new_page(width=400, height=300)
        page.insert_text((30, 120), "1. Measure m", fontsize=14)
        x = 30 + pymupdf.get_text_length("1. Measure m", fontsize=14)
        page.insert_text((x, 114), "3", fontsize=9)
        page.insert_text((30, 160), "2. Ordinary 3 stays unchanged", fontsize=14)
        pdf.save(path)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Synthetic typography",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    artifact = parse_document(path, request)
    assert artifact.quality_report.status == "passed", artifact.quality_report
    assert any(p.classification == "³" and p.method == "typography_text_layer" for p in artifact.document.symbol_proofs)
    assert any("³" in n.own_body for n in artifact.document.nodes)
    assert all("³" not in block.text for block in artifact.document.blocks)
    assert any("Ordinary 3 stays unchanged" in n.own_body for n in artifact.document.nodes)


def reviewed_graphic_case(tmp_path):
    from expert_ingest.parsing.dto import RegionReview
    path = tmp_path / "new-image-review.pdf"
    with pymupdf.open() as raster:
        label = raster.new_page(width=190, height=50)
        label.draw_rect(pymupdf.Rect(3, 3, 35, 40), fill=(0.3, 0.7, 0.5))
        label.insert_text((40, 25), "Archive branding", fontsize=12)
        with pymupdf.open() as pdf:
            page = pdf.new_page(width=400, height=400)
            page.insert_image(pymupdf.Rect(30, 30, 220, 80), pixmap=label.get_pixmap())
            page.insert_text((30, 130), "1. The source condition remains 27.")
            pdf.save(path)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Fresh reviewed image",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    with pymupdf.open(path) as pdf:
        crop = pdf[0].get_pixmap(dpi=144, clip=pymupdf.Rect(30, 30, 220, 80), colorspace=pymupdf.csGRAY)
    review = RegionReview(source_sha256=request.source_sha256, pdf_page=1, bbox=(30, 30, 220, 80),
                          render_dpi=144, crop_sha256=hashlib.sha256(bytes(crop.samples)).hexdigest())
    return path, request, review


def test_reviewed_image_requires_exact_source_and_separate_trust(tmp_path):
    from expert_ingest.parsing.pipeline import parse_document
    from expert_ingest.parsing.quality import ArtifactValidator
    path, request, review = reviewed_graphic_case(tmp_path)
    assert parse_document(path, request).quality_report.status == "failed"
    reviewed_request = request.model_copy(update={"region_reviews": (review,)})
    artifact = parse_document(path, reviewed_request)
    assert artifact.quality_report.status == "passed", artifact.quality_report
    assert any(d.code == "REVIEWED_NON_NORMATIVE_GRAPHIC" and d.severity == "warning"
               for d in artifact.quality_report.diagnostics)
    assert "The source condition remains 27" in " ".join(n.own_body for n in artifact.document.nodes)
    assert "Archive branding" not in " ".join(n.own_body for n in artifact.document.nodes)
    assert ArtifactValidator().validate(artifact.document).status == "failed"
    assert ArtifactValidator(trusted_region_reviews=(review,)).validate(artifact.document) == artifact.quality_report
    renamed = tmp_path / "independent-name.pdf"
    renamed.write_bytes(path.read_bytes())
    assert parse_document(renamed, reviewed_request) == artifact


def test_review_does_not_transfer_to_new_bytes_or_incorrect_crop(tmp_path):
    from expert_ingest.parsing.pipeline import parse_document
    path, request, review = reviewed_graphic_case(tmp_path)
    wrong_crop = review.model_copy(update={"crop_sha256": "b" * 64})
    artifact = parse_document(path, request.model_copy(update={"region_reviews": (wrong_crop,)}))
    assert artifact.quality_report.status == "failed"
    assert "REGION_REVIEW_CROP_MISMATCH" in {d.code for d in artifact.quality_report.diagnostics}
    changed_path = tmp_path / "changed-source.pdf"
    with pymupdf.open(path) as pdf:
        pdf[0].insert_text((30, 160), "2. A new applicable condition.")
        pdf.save(changed_path)
    changed_request = request.model_copy(update={"source_sha256": hashlib.sha256(changed_path.read_bytes()).hexdigest(),
                                                  "region_reviews": (review,)})
    assert parse_document(changed_path, changed_request).quality_report.status == "failed"


@pytest.mark.parametrize("case", ["corrupt", "encrypted", "size", "pages", "hash"])
def test_bounded_rejections_use_safe_codes(tmp_path, case):
    path = tmp_path / "input.pdf"
    with pymupdf.open() as pdf:
        pdf.new_page()
        pdf.new_page()
        if case == "encrypted":
            pdf.save(path, encryption=pymupdf.PDF_ENCRYPT_AES_256, owner_pw="synthetic-owner", user_pw="synthetic-user")
        else:
            pdf.save(path)
    if case == "corrupt":
        path.write_bytes(b"not a PDF with sensitive-like input")
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Synthetic",
                           source_sha256="f" * 64 if case == "hash" else hashlib.sha256(path.read_bytes()).hexdigest(),
                           limits=ParserLimits(max_bytes=5) if case == "size" else
                           ParserLimits(max_pages=1) if case == "pages" else ParserLimits())
    with pytest.raises(ParseFailure) as error:
        DocumentParser().parse(path, request)
    assert str(error.value) in {"PDF_INVALID", "GENERATION_INVALID", "SIZE_LIMIT_EXCEEDED"}


def test_cancellation_guard_is_invoked_between_pages(tmp_path):
    path = tmp_path / "cancel.pdf"
    with pymupdf.open() as pdf:
        pdf.new_page()
        pdf.save(path)
    request = ParseRequest(version_id=uuid4(), parse_generation_id=uuid4(), title="Synthetic",
                           source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    calls = []
    def cancel():
        calls.append(True)
        raise ParseFailure("DEADLINE_EXCEEDED")
    with pytest.raises(ParseFailure, match="DEADLINE_EXCEEDED"):
        DocumentParser().parse(path, request, cancel)
    assert calls == [True]
