"""Bounded PyMuPDF extraction. No network, OCR, persistence or document-specific rules."""
from __future__ import annotations

from collections.abc import Callable
import hashlib
import base64
import json
import math
from pathlib import Path
from typing import Any, Literal, cast

from .dto import Decision, Diagnostic, ImageRegion, Page, ParsedDocument, ParserManifest, ParseRequest, RawBlock, RawChar, RegionReview, RegionReviewEvidence, SymbolProof


class ParseFailure(Exception):
    """Safe machine code only; vendor errors never cross the process boundary."""

    def __init__(self, code: str):
        if code not in {"PDF_INVALID", "SIZE_LIMIT_EXCEEDED", "GENERATION_INVALID", "DEADLINE_EXCEEDED"}:
            code = "PDF_INVALID"
        self.code = code
        super().__init__(code)


def _clip(rect: Any, width: float, height: float) -> tuple[float, float, float, float] | None:
    x0, y0, x1, y1 = (float(value) for value in rect)
    clipped = (max(0.0, x0), max(0.0, y0), min(width, x1), min(height, y1))
    if clipped[2] <= clipped[0] or clipped[3] <= clipped[1]:
        return None
    return clipped


def _drawing_region(drawing: Any, width: float, height: float) -> tuple[float, float, float, float] | None:
    # Straight PDF strokes have a zero-area path rect but visible stroke width.
    x0, y0, x1, y1 = (float(value) for value in drawing["rect"])
    pad = max(float(drawing.get("width") or 0), 0.1) / 2
    return _clip((x0 - pad, y0 - pad, x1 + pad, y1 + pad), width, height)


def _reading_order(blocks: list[RawBlock], width: float, height: float) -> tuple[list[RawBlock], bool]:
    """Detect a central gutter conservatively; ambiguity still needs layout review."""
    body = [b for b in blocks if b.bbox[1] >= height * 0.1 and b.bbox[3] <= height * 0.9]
    left = [b for b in body if b.bbox[2] < width * 0.53 and b.bbox[0] < width * 0.4]
    right = [b for b in body if b.bbox[0] > width * 0.47]
    # Distant signatures, running headers and short line endings are not columns.
    paired_y = []
    for a in left:
        for b in right:
            overlap = min(a.bbox[3], b.bbox[3]) - max(a.bbox[1], b.bbox[1])
            if b.bbox[0] - a.bbox[2] <= width * 0.03 or overlap <= min(a.bbox[3] - a.bbox[1], b.bbox[3] - b.bbox[1]) * 0.4:
                continue
            # Justified PDF words can be separate lines. A column gutter must be empty.
            if any(other.block_id not in {a.block_id, b.block_id} and other.bbox[0] < b.bbox[0]
                   and other.bbox[2] > a.bbox[2]
                   and min(other.bbox[3], a.bbox[3]) - max(other.bbox[1], a.bbox[1]) > overlap * 0.4
                   for other in body):
                continue
            paired_y.append((a.bbox[1] + a.bbox[3]) / 2)
    distinct_rows: list[float] = []
    for y in sorted(paired_y):
        if not distinct_rows or y - distinct_rows[-1] > 6:
            distinct_rows.append(y)
    two_columns = len(distinct_rows) >= 3
    if not two_columns:
        ordered = sorted(blocks, key=lambda b: (round(b.bbox[1], 1), b.bbox[0], b.original_ordinal))
    else:
        left = [b for b in blocks if b.bbox[2] < width * 0.53 and b.bbox[0] < width * 0.4]
        right = [b for b in blocks if b.bbox[0] > width * 0.47]
        left_ids = {b.block_id for b in left}
        right_ids = {b.block_id for b in right}
        wide = sorted((b for b in blocks if b.block_id not in left_ids | right_ids), key=lambda b: b.bbox[1])
        ordered = []
        pending = list(left + right)
        for separator in wide:
            band = [b for b in pending if b.bbox[1] < separator.bbox[1]]
            ordered.extend(sorted(band, key=lambda b: (b.block_id in right_ids, b.bbox[1], b.bbox[0])))
            pending = [b for b in pending if b not in band]
            ordered.append(separator)
        ordered.extend(sorted(pending, key=lambda b: (b.block_id in right_ids, b.bbox[1], b.bbox[0])))
    return [b.model_copy(update={"ordinal": index}) for index, b in enumerate(ordered)], bool(two_columns)


class DocumentParser:
    def parse(self, local_path: Path, request: ParseRequest,
              cancellation: Callable[[], None] | None = None) -> ParsedDocument:
        """Caller must apply OS isolation and wall/memory limits before invoking."""
        import pymupdf

        path = Path(local_path)
        limits = request.limits
        try:
            size = path.stat().st_size
            if size <= 0 or size > limits.max_bytes:
                raise ParseFailure("SIZE_LIMIT_EXCEEDED")
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                total = 0
                while piece := stream.read(1024 * 1024):
                    total += len(piece)
                    if total > limits.max_bytes:
                        raise ParseFailure("SIZE_LIMIT_EXCEEDED")
                    digest.update(piece)
            if total != size or digest.hexdigest() != request.source_sha256:
                raise ParseFailure("GENERATION_INVALID")
            document = pymupdf.open(path)
        except ParseFailure:
            raise
        except Exception:
            raise ParseFailure("PDF_INVALID") from None

        pages: list[Page] = []
        blocks: list[RawBlock] = []
        diagnostics: list[Diagnostic] = []
        characters = 0
        try:
            with document:
                if not document.is_pdf or document.needs_pass or document.page_count < 1:
                    raise ParseFailure("PDF_INVALID")
                if document.page_count > limits.max_pages:
                    raise ParseFailure("SIZE_LIMIT_EXCEEDED")
                for page_index in range(document.page_count):
                    if cancellation:
                        cancellation()
                    page = document[page_index]
                    number = page_index + 1
                    width, height = float(page.cropbox.width), float(page.cropbox.height)
                    raw = page.get_text("rawdict", sort=False, flags=pymupdf.TEXTFLAGS_RAWDICT & ~pymupdf.TEXT_PRESERVE_IMAGES)
                    page_blocks: list[RawBlock] = []
                    for block_index, block in enumerate(raw["blocks"]):
                        if block.get("type") != 0:
                            continue
                        for line_index, line in enumerate(block.get("lines", [])):
                            block_id = f"p{number}:b{block_index}:l{line_index}"
                            chars: list[RawChar] = []
                            pieces: list[str] = []
                            offset = 0
                            for span in line.get("spans", []):
                                for char in span.get("chars", []):
                                    text = char["c"]
                                    if not text:
                                        continue
                                    bbox = _clip(char["bbox"], width, height)
                                    if bbox is None:
                                        diagnostics.append(Diagnostic(code="TEXT_OUTSIDE_PAGE", severity="critical",
                                                                      pdf_page=number, block_id=block_id))
                                    else:
                                        chars.append(RawChar(offset=offset, text=text, bbox=bbox,
                                                             font=span.get("font", ""), size=span.get("size", 0),
                                                             flags=span.get("flags", 0), origin=char.get("origin", (0, 0))))
                                    pieces.append(text)
                                    offset += len(text)
                            text = "".join(pieces)
                            if not text:
                                continue
                            characters += len(text)
                            if characters > limits.max_characters or len(blocks) + len(page_blocks) >= limits.max_blocks:
                                raise ParseFailure("SIZE_LIMIT_EXCEEDED")
                            bbox = _clip(line["bbox"], width, height)
                            if bbox is None:
                                bbox = (0.0, 0.0, width, height)
                            if tuple(line.get("dir", (1, 0))) != (1, 0):
                                diagnostics.append(Diagnostic(code="NON_HORIZONTAL_TEXT", severity="critical",
                                                              pdf_page=number, block_id=block_id, bbox=bbox))
                            if any(c == "\ufffd" or (ord(c) < 32 and c not in "\t\n\r") for c in text):
                                diagnostics.append(Diagnostic(code="DAMAGED_TEXT_GLYPHS", severity="critical",
                                                              pdf_page=number, block_id=block_id, bbox=bbox))
                            page_blocks.append(RawBlock(block_id=block_id, pdf_page=number, text=text, bbox=bbox,
                                                        chars=tuple(chars), ordinal=len(page_blocks),
                                                        original_ordinal=len(page_blocks), group_id=f"p{number}:b{block_index}"))
                    page_blocks, multi_column = _reading_order(page_blocks, width, height)
                    images: list[ImageRegion] = []
                    for image_index, info in enumerate(page.get_image_info()):
                        bbox = _clip(info["bbox"], width, height)
                        if bbox:
                            images.append(ImageRegion(region_id=f"p{number}:image{image_index}", pdf_page=number,
                                                      bbox=bbox, pixel_width=info["width"], pixel_height=info["height"]))
                    vectors = tuple(rect for drawing in page.get_drawings()
                                    if (rect := _drawing_region(drawing, width, height)))
                    decision: Decision
                    if not page_blocks and not images and not vectors:
                        decision = "intentional_blank"
                    elif not page_blocks and images:
                        decision = "need_ocr"
                        diagnostics.append(Diagnostic(code="OCR_REQUIRED", severity="critical", pdf_page=number))
                    elif not page_blocks:
                        decision = "need_layout_reparse"
                        diagnostics.append(Diagnostic(code="VECTOR_CONTENT_UNRESOLVED", severity="critical", pdf_page=number))
                    elif multi_column:
                        decision = "need_layout_reparse"
                        diagnostics.append(Diagnostic(code="MULTICOLUMN_READING_ORDER", severity="critical", pdf_page=number))
                    else:
                        decision = "accept_text"
                    # Visible raster content cannot silently disappear from a text-only artifact.
                    if images and page_blocks:
                        diagnostics.extend(Diagnostic(code="UNRESOLVED_IMAGE_REGION", severity="critical", pdf_page=number,
                                                      block_id=image.region_id, bbox=image.bbox) for image in images)
                    label = page.get_label() or None
                    pages.append(Page(pdf_page=number, width=width, height=height, rotation=page.rotation,
                                      cropbox=(page.cropbox.x0, page.cropbox.y0, page.cropbox.x1, page.cropbox.y1),
                                      printed_page_label=label,
                                      block_ids=tuple(b.block_id for b in page_blocks), images=tuple(images),
                                      vector_regions=vectors, decision=decision))
                    blocks.extend(page_blocks)
        except ParseFailure:
            raise
        except Exception:
            raise ParseFailure("PDF_INVALID") from None
        return ParsedDocument(version_id=request.version_id, parse_generation_id=request.parse_generation_id,
                              source_sha256=request.source_sha256, title=request.title, pages=tuple(pages),
                              blocks=tuple(blocks), diagnostics=tuple(diagnostics),
                              manifest=ParserManifest(pymupdf_version=pymupdf.VersionBind, limits=limits))


def reassess_table_layout(document: ParsedDocument) -> ParsedDocument:
    """Table columns do not imply ambiguous ordinary-body reading order."""
    owned: dict[str, list[tuple[int, int]]] = {}
    for table in document.tables:
        for span in table.owned_source_spans:
            owned.setdefault(span.block_id, []).append((span.start_offset, span.end_offset))
    resolved_pages = set()
    for page in document.pages:
        if not any(d.code == "MULTICOLUMN_READING_ORDER" and d.pdf_page == page.pdf_page for d in document.diagnostics):
            continue
        body = []
        for block in document.blocks:
            if block.pdf_page != page.pdf_page:
                continue
            remaining = [c for c in block.chars if not any(start <= c.offset < end for start, end in owned.get(block.block_id, ()))]
            if not any(c.text.strip() for c in remaining):
                continue
            bbox = (min(c.bbox[0] for c in remaining), min(c.bbox[1] for c in remaining),
                    max(c.bbox[2] for c in remaining), max(c.bbox[3] for c in remaining))
            body.append(block.model_copy(update={"bbox": bbox}))
        _, ambiguous = _reading_order(body, page.width, page.height)
        if not ambiguous:
            resolved_pages.add(page.pdf_page)
    diagnostics = tuple(d for d in document.diagnostics
                        if not (d.code == "MULTICOLUMN_READING_ORDER" and d.pdf_page in resolved_pages))
    pages = tuple(p.model_copy(update={"decision": "accept_text"}) if p.pdf_page in resolved_pages else p for p in document.pages)
    return document.model_copy(update={"pages": pages, "diagnostics": diagnostics})


def typography_anchors(document: ParsedDocument, block: RawBlock, char: RawChar) -> tuple[tuple[RawBlock, RawChar], ...]:
    own = tuple((block, peer) for peer in block.chars if peer.offset != char.offset and peer.text.strip())
    if len(own) >= 2:
        return own[:1000]
    neighbours = [(other, peer) for other in document.blocks if other.pdf_page == block.pdf_page and other.kind == "text"
                  for peer in other.chars if peer.text.strip() and (other.block_id != block.block_id or peer.offset != char.offset)
                  and 0 <= char.bbox[0] - peer.bbox[2] <= max(peer.size, 1) * 2
                  and abs(char.origin[1] - peer.origin[1]) <= max(peer.size, 1)]
    if not neighbours:
        return ()
    neighbour = min(neighbours, key=lambda item: char.bbox[0] - item[1].bbox[2])[0]
    return tuple((neighbour, peer) for peer in neighbour.chars if peer.text.strip())[:1000]


def symbol_features(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def apply_region_reviews(document: ParsedDocument, path: Path,
                         trusted_reviews: tuple[RegionReview, ...]) -> ParsedDocument:
    """Bind explicit trusted visual dispositions to newly rendered source pixels."""
    import pymupdf

    selected = [r for r in trusted_reviews if r.source_sha256 == document.source_sha256]
    if not selected:
        return document
    evidence = []
    diagnostics = list(document.diagnostics)
    reviewed_regions = set()
    with pymupdf.open(path) as pdf:
        for review in selected:
            matching = [image for page in document.pages for image in page.images
                        if image.pdf_page == review.pdf_page and image.bbox == review.bbox]
            if len(matching) != 1:
                diagnostics.append(Diagnostic(code="REGION_REVIEW_IDENTITY_INVALID", severity="critical",
                                              pdf_page=review.pdf_page))
                continue
            region = matching[0]
            scale = review.render_dpi / 72
            width = math.ceil(region.bbox[2] * scale) - math.floor(region.bbox[0] * scale)
            height = math.ceil(region.bbox[3] * scale) - math.floor(region.bbox[1] * scale)
            if width > 4096 or height > 4096 or width * height > 4_000_000:
                diagnostics.append(Diagnostic(code="REGION_REVIEW_RENDER_LIMIT", severity="critical",
                                              pdf_page=region.pdf_page, block_id=region.region_id))
                continue
            source_page = pdf[region.pdf_page - 1]
            source_page.set_rotation(0)
            pixmap = source_page.get_pixmap(dpi=review.render_dpi, clip=pymupdf.Rect(region.bbox),
                                            colorspace=pymupdf.csGRAY, alpha=False)
            pixels = bytes(pixmap.samples)
            if (pixmap.stride != pixmap.width or len(pixels) != pixmap.width * pixmap.height
                    or (pixmap.width, pixmap.height) != (width, height)
                    or hashlib.sha256(pixels).hexdigest() != review.crop_sha256):
                diagnostics.append(Diagnostic(code="REGION_REVIEW_CROP_MISMATCH", severity="critical",
                                              pdf_page=region.pdf_page, block_id=region.region_id))
                continue
            evidence.append(RegionReviewEvidence(review=review, region_id=region.region_id,
                                                  crop_base64=base64.b64encode(pixels).decode("ascii"),
                                                  crop_width=width, crop_height=height))
            reviewed_regions.add(region.region_id)
            diagnostics.append(Diagnostic(code="REVIEWED_NON_NORMATIVE_GRAPHIC", severity="warning",
                                          pdf_page=region.pdf_page, block_id=region.region_id, bbox=region.bbox))
    reviewed_pages = {p.pdf_page for p in document.pages if p.images and not p.block_ids and not p.vector_regions
                      and all(image.region_id in reviewed_regions for image in p.images)}
    diagnostics = [d for d in diagnostics if not (
        (d.code == "UNRESOLVED_IMAGE_REGION" and d.block_id in reviewed_regions)
        or (d.code == "OCR_REQUIRED" and d.pdf_page in reviewed_pages))]
    pages = tuple(p.model_copy(update={"decision": "accept_text"}) if p.pdf_page in reviewed_pages else p
                  for p in document.pages)
    return document.model_copy(update={"region_reviews": tuple(evidence), "diagnostics": tuple(diagnostics), "pages": pages})


def enrich_symbols(document: ParsedDocument, path: Path) -> ParsedDocument:
    """Create explicit image transcriptions; original extracted blocks remain unchanged."""
    import pymupdf
    from .symbols import classify_luma_pixels, resolve_typographic_three
    from .normalize import source_span

    proofs = []
    added = []
    resolved_regions = set()
    with pymupdf.open(path) as pdf:
        for page in document.pages:
            source_page = pdf[page.pdf_page - 1]
            source_page.set_rotation(0)
            for region in page.images:
                if any(item.region_id == region.region_id for item in document.region_reviews):
                    continue
                width = math.ceil((region.bbox[2] - region.bbox[0]) * document.manifest.limits.symbol_render_dpi / 72)
                height = math.ceil((region.bbox[3] - region.bbox[1]) * document.manifest.limits.symbol_render_dpi / 72)
                if width > 2048 or height > 2048 or width * height > 1_000_000:
                    continue
                pixmap = source_page.get_pixmap(dpi=document.manifest.limits.symbol_render_dpi, clip=pymupdf.Rect(region.bbox),
                                                colorspace=pymupdf.csGRAY, alpha=False)
                pixels = pixmap.samples
                if pixmap.stride != pixmap.width or len(pixels) > 1_000_000:
                    continue
                result = classify_luma_pixels(pixels, pixmap.width, pixmap.height)
                if result.status != "resolved" or result.symbol not in {"+", "−", "±"}:
                    continue
                block_id = f"symbol:{region.region_id}"
                proof = SymbolProof(region_id=region.region_id, block_id=block_id, pdf_page=region.pdf_page,
                                    bbox=region.bbox, classification=cast(Literal["+", "−", "±"], result.symbol), source_sha256=document.source_sha256,
                                    crop_sha256=hashlib.sha256(pixels).hexdigest(), crop_base64=base64.b64encode(pixels).decode("ascii"),
                                    crop_encoding="luma8", crop_width=pixmap.width, crop_height=pixmap.height,
                                    render_dpi=document.manifest.limits.symbol_render_dpi,
                                    method="raster_topology", features=symbol_features(result.features),
                                    reason_codes=result.reason_codes, supported_profile=result.supported_profile)
                proofs.append(proof)
                added.append(RawBlock(block_id=block_id, pdf_page=region.pdf_page, text=result.symbol, bbox=region.bbox,
                                      chars=(RawChar(offset=0, text=result.symbol, bbox=region.bbox,
                                                     font="raster-symbol", size=region.bbox[3] - region.bbox[1],
                                                     origin=(region.bbox[0], region.bbox[3])),),
                                      ordinal=0, backend="symbol", kind="symbol"))
                resolved_regions.add(region.region_id)
    for block in document.blocks:
        if block.kind != "text":
            continue
        for char in block.chars:
            if char.text != "3":
                continue
            anchors = typography_anchors(document, block, char)
            result = resolve_typographic_three(char.model_dump(), (peer.model_dump() for _, peer in anchors))
            if result.status == "resolved" and result.symbol == "³":
                proofs.append(SymbolProof(region_id=f"typography:{block.block_id}:{char.offset}", block_id=block.block_id,
                                          pdf_page=block.pdf_page, bbox=char.bbox, classification="³", source_sha256=document.source_sha256,
                                          method="typography_text_layer", source_offset=char.offset,
                                          anchor_spans=tuple(source_span(owner, peer.offset, peer.offset + len(peer.text))
                                                             for owner, peer in anchors),
                                          features=symbol_features(result.features), reason_codes=result.reason_codes,
                                          supported_profile=result.supported_profile))
    blocks = []
    pages = []
    for page in document.pages:
        own = [b for b in (*document.blocks, *added) if b.pdf_page == page.pdf_page]
        ordered, _ = _reading_order(own, page.width, page.height)
        blocks.extend(ordered)
        pages.append(page.model_copy(update={"block_ids": tuple(b.block_id for b in ordered)}))
    diagnostics = tuple(d for d in document.diagnostics
                        if not (d.code == "UNRESOLVED_IMAGE_REGION" and d.block_id in resolved_regions))
    return document.model_copy(update={"blocks": tuple(blocks), "pages": tuple(pages), "symbol_proofs": tuple(proofs),
                                       "diagnostics": diagnostics,
                                       "manifest": document.manifest.model_copy(update={"symbol_adapter_version": "qualified-symbol-v1"})})
