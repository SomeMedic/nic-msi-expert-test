"""Fail-closed artifact validation; diagnostics do not claim calibrated OCR accuracy."""
from __future__ import annotations

import math
import base64
import hashlib
import unicodedata

from expert_contracts.sources import SourceSpan

from .dto import CanonicalDocument, CharMapSegment, Diagnostic, ParsedDocument, QualityReport, RegionReview, RegionReviewRegistry
from .normalize import classify_boilerplate, content_hash, source_span


class ExtractionAssessor:
    def assess(self, document: ParsedDocument):
        return tuple(page.decision for page in document.pages)


class ArtifactValidator:
    def __init__(self, *, trusted_region_reviews: tuple[RegionReview, ...] = ()):
        self.trusted_region_reviews = RegionReviewRegistry(reviews=trusted_region_reviews).reviews

    def validate(self, document: CanonicalDocument) -> QualityReport:
        diagnostics = list(document.diagnostics)
        seen = {(d.code, d.pdf_page, d.block_id) for d in diagnostics}

        def fail(code: str, page: int | None = None, block: str | None = None):
            key = (code, page, block)
            if key not in seen:
                diagnostics.append(Diagnostic(code=code, severity="critical", pdf_page=page, block_id=block))
                seen.add(key)

        pages = {p.pdf_page: p for p in document.pages}
        raw = {b.block_id: b for b in document.blocks}
        coverage = {key: bytearray(len(block.text)) for key, block in raw.items()}
        if sorted(pages) != list(range(1, len(document.pages) + 1)):
            fail("PAGE_REGISTRY_INVALID")
        if len(raw) != len(document.blocks):
            fail("DUPLICATE_RAW_BLOCK")
        if not any(block.text.strip() for block in document.blocks):
            fail("NO_USABLE_TEXT")
        for page in document.pages:
            if page.decision not in {"accept_text", "intentional_blank"}:
                fail("PAGE_EXTRACTION_UNRESOLVED", page.pdf_page)
            expected = {b.block_id for b in document.blocks if b.pdf_page == page.pdf_page}
            if set(page.block_ids) != expected or len(set(page.block_ids)) != len(page.block_ids):
                fail("PAGE_BLOCK_REGISTRY_INVALID", page.pdf_page)
        for block in document.blocks:
            block_page = pages.get(block.pdf_page)
            if block_page is None or block.bbox[2] > block_page.width or block.bbox[3] > block_page.height:
                fail("RAW_BLOCK_GEOMETRY_INVALID", block.pdf_page, block.block_id)
            char_offsets = {i for c in block.chars for i in range(c.offset, c.offset + len(c.text))}
            if any(i not in char_offsets for i, c in enumerate(block.text) if not c.isspace()):
                fail("RAW_CHAR_COVERAGE_INVALID", block.pdf_page, block.block_id)
            if block.kind == "ocr" and any(c.isdigit() or c in "+−±³²=%<>" for c in block.text):
                fail("UNVERIFIED_OCR_SYMBOLS", block.pdf_page, block.block_id)

        def resolve(span: SourceSpan) -> str | None:
            block = raw.get(span.block_id)
            if block is None or span.pdf_page != block.pdf_page:
                fail("SOURCE_SPAN_IDENTITY_INVALID", span.pdf_page, span.block_id)
                return None
            page = pages.get(span.pdf_page)
            if page is None or span.end_offset <= span.start_offset:
                fail("SOURCE_SPAN_INVALID", span.pdf_page, span.block_id)
                return None
            try:
                span.validate_source(block_text=block.text, pdf_page_count=len(document.pages),
                                     page_size=(page.width, page.height))
                if span.bbox and not all(math.isfinite(v) for v in span.bbox):
                    raise ValueError("non-finite source bbox")
                relevant_chars = [c for c in block.chars if c.offset < span.end_offset
                                  and c.offset + len(c.text) > span.start_offset]
                if span.bbox and any(c.bbox[0] < span.bbox[0] - 0.01 or c.bbox[1] < span.bbox[1] - 0.01
                                     or c.bbox[2] > span.bbox[2] + 0.01 or c.bbox[3] > span.bbox[3] + 0.01
                                     for c in relevant_chars):
                    raise ValueError("source bbox does not contain referenced glyphs")
            except ValueError:
                fail("SOURCE_SPAN_INVALID", span.pdf_page, span.block_id)
                return None
            return block.text[span.start_offset:span.end_offset]

        verified_typography = set()
        verified_raster = set()
        seen_proofs = set()
        images = {image.region_id: image for page in document.pages for image in page.images}
        from .primary import symbol_features, typography_anchors
        from .symbols import classify_luma_pixels, resolve_typographic_three
        for proof in document.symbol_proofs:
            key = (proof.method, proof.region_id)
            proof_block = raw.get(proof.block_id)
            if key in seen_proofs or proof.source_sha256 != document.source_sha256 or proof_block is None:
                fail("SYMBOL_PROOF_IDENTITY_INVALID", proof.pdf_page, proof.block_id)
                continue
            seen_proofs.add(key)
            try:
                if proof.method == "raster_topology":
                    region = images.get(proof.region_id)
                    if (region is None or region.pdf_page != proof.pdf_page or region.bbox != proof.bbox
                            or proof_block.kind != "symbol" or proof_block.backend != "symbol" or proof_block.text != proof.classification
                            or proof_block.pdf_page != region.pdf_page or proof_block.bbox != region.bbox
                            or proof.crop_base64 is None or proof.source_offset is not None or proof.anchor_spans):
                        raise ValueError("invalid image binding")
                    if (proof.render_dpi != document.manifest.limits.symbol_render_dpi or proof.crop_encoding != "luma8"
                            or proof.crop_width is None or proof.crop_height is None):
                        raise ValueError("invalid crop rendering profile")
                    scale = proof.render_dpi / 72
                    expected_width = math.ceil(region.bbox[2] * scale) - math.floor(region.bbox[0] * scale)
                    expected_height = math.ceil(region.bbox[3] * scale) - math.floor(region.bbox[1] * scale)
                    if (proof.crop_width, proof.crop_height) != (expected_width, expected_height):
                        raise ValueError("crop dimensions differ from source region")
                    crop = base64.b64decode(proof.crop_base64, validate=True)
                    if (len(crop) > 1_000_000 or len(crop) != proof.crop_width * proof.crop_height
                            or hashlib.sha256(crop).hexdigest() != proof.crop_sha256):
                        raise ValueError("invalid crop checksum")
                    result = classify_luma_pixels(crop, proof.crop_width, proof.crop_height)
                else:
                    if (proof_block.kind != "text" or proof.source_offset is None or proof.classification != "³"
                            or proof.crop_base64 is not None or proof.crop_sha256 is not None or proof.render_dpi is not None
                            or proof.crop_encoding is not None or proof.crop_width is not None or proof.crop_height is not None):
                        raise ValueError("invalid typography source")
                    char = next(c for c in proof_block.chars if c.offset == proof.source_offset and c.text == "3")
                    if char.bbox != proof.bbox or proof_block.pdf_page != proof.pdf_page:
                        raise ValueError("invalid typography region")
                    anchors = typography_anchors(document, proof_block, char)
                    expected_anchors = tuple(source_span(owner, peer.offset, peer.offset + len(peer.text)) for owner, peer in anchors)
                    if proof.anchor_spans != expected_anchors:
                        raise ValueError("invalid typography anchors")
                    result = resolve_typographic_three(char.model_dump(), (peer.model_dump() for _, peer in anchors))
                if (result.status != "resolved" or result.symbol != proof.classification or result.source_kind != proof.method
                        or result.supported_profile != proof.supported_profile or result.reason_codes != proof.reason_codes
                        or symbol_features(result.features) != proof.features):
                    raise ValueError("symbol qualification replay differs")
            except Exception:
                fail("SYMBOL_PROOF_INVALID", proof.pdf_page, proof.block_id)
                continue
            if proof.method == "raster_topology":
                verified_raster.add(proof.region_id)
            else:
                verified_typography.add((proof.block_id, proof.source_offset))
        verified_reviews = set()
        for evidence in document.region_reviews:
            review = evidence.review
            region = images.get(evidence.region_id)
            if (review not in self.trusted_region_reviews or review.source_sha256 != document.source_sha256
                    or region is None or review.pdf_page != region.pdf_page or review.bbox != region.bbox
                    or evidence.region_id in verified_reviews or evidence.region_id in verified_raster):
                fail("REGION_REVIEW_NOT_TRUSTED", review.pdf_page, evidence.region_id)
                continue
            try:
                pixels = base64.b64decode(evidence.crop_base64, validate=True)
                scale = review.render_dpi / 72
                width = math.ceil(region.bbox[2] * scale) - math.floor(region.bbox[0] * scale)
                height = math.ceil(region.bbox[3] * scale) - math.floor(region.bbox[1] * scale)
                if ((width, height) != (evidence.crop_width, evidence.crop_height)
                        or len(pixels) > 4_000_000 or len(pixels) != width * height
                        or hashlib.sha256(pixels).hexdigest() != review.crop_sha256):
                    raise ValueError("reviewed crop differs from trusted registry")
            except Exception:
                fail("REGION_REVIEW_CROP_MISMATCH", review.pdf_page, evidence.region_id)
                continue
            verified_reviews.add(evidence.region_id)
            warning_key = ("REVIEWED_NON_NORMATIVE_GRAPHIC", review.pdf_page, evidence.region_id)
            if warning_key not in seen:
                diagnostics.append(Diagnostic(code=warning_key[0], severity="warning", pdf_page=review.pdf_page,
                                              block_id=evidence.region_id, bbox=review.bbox))
                seen.add(warning_key)
        for image in images.values():
            if image.region_id not in verified_raster | verified_reviews:
                fail("UNRESOLVED_IMAGE_REGION", image.pdf_page, image.region_id)
        for block in document.blocks:
            if block.kind == "symbol" and not any(p.block_id == block.block_id and p.region_id in verified_raster
                                                   for p in document.symbol_proofs if p.method == "raster_topology"):
                fail("SYMBOL_PROOF_REQUIRED", block.pdf_page, block.block_id)
            if any(char.text == "3" and char.flags & 1 and (block.block_id, char.offset) not in verified_typography
                   for char in block.chars):
                fail("UNRESOLVED_TYPOGRAPHY", block.pdf_page, block.block_id)

        def check_mapping(text: str, mapping: tuple[CharMapSegment, ...]):
            cursor = 0
            for segment in mapping:
                if segment.canonical_start != cursor or segment.canonical_end > len(text):
                    fail("CANONICAL_MAP_COVERAGE_INVALID")
                cursor = segment.canonical_end
                target = text[segment.canonical_start:segment.canonical_end]
                values = [resolve(span) for span in segment.source_spans]
                if any(value is None for value in values):
                    continue
                source = "".join(value for value in values if value is not None)
                if segment.mapping == "exact":
                    valid = target == source
                elif segment.operation == "nfc":
                    valid = target == unicodedata.normalize("NFC", source)
                elif segment.operation in {"whitespace", "line_join"}:
                    valid = bool(source) and source.isspace() and target == " "
                elif segment.operation == "format_separator":
                    valid = not segment.source_spans and target.isspace()
                elif segment.operation == "typography":
                    valid = (target == "³" and source == "3" and len(segment.source_spans) == 1
                             and (segment.source_spans[0].block_id, segment.source_spans[0].start_offset) in verified_typography)
                else:
                    # Other semantic transformations need an independently qualified adapter.
                    valid = False
                if not valid:
                    fail("CANONICAL_TEXT_NOT_SOURCE_DERIVED")
                for span in segment.source_spans:
                    counters = coverage[span.block_id]
                    for offset in range(span.start_offset, span.end_offset):
                        if counters[offset] and not raw[span.block_id].text[offset].isspace():
                            fail("DUPLICATE_SOURCE_OWNERSHIP", span.pdf_page, span.block_id)
                        counters[offset] = 1
            if cursor != len(text):
                fail("CANONICAL_MAP_COVERAGE_INVALID")

        def check_context(ref):
            values = [resolve(span) for span in ref.source_spans]
            if any(value is None for value in values):
                return
            # Context may combine raw line/cell slices with whitespace, never new tokens.
            source_parts = []
            for span, value in zip(ref.source_spans, values, strict=True):
                source_parts.append("".join("³" if char == "3" and (span.block_id, span.start_offset + i) in verified_typography else char
                                            for i, char in enumerate(value or "")))
            source = " ".join(source_parts)
            def compact(value):
                return "".join(unicodedata.normalize("NFC", value).split())
            if compact(ref.text) != compact(source):
                fail("CONTEXT_NOT_SOURCE_DERIVED")

        def interval_union(spans):
            grouped: dict[tuple[int, str], list[tuple[int, int]]] = {}
            for span in spans:
                grouped.setdefault((span.pdf_page, span.block_id), []).append((span.start_offset, span.end_offset))
            result = set()
            for identity, intervals in grouped.items():
                start, end = sorted(intervals)[0]
                for next_start, next_end in sorted(intervals)[1:]:
                    if next_start <= end:
                        end = max(end, next_end)
                    else:
                        result.add((*identity, start, end))
                        start, end = next_start, next_end
                result.add((*identity, start, end))
            return result

        nodes = {n.id: n for n in document.nodes}
        roots = [n for n in document.nodes if n.parent_id is None]
        if len(nodes) != len(document.nodes) or len(roots) != 1 or roots[0].node_type != "document":
            fail("TREE_ROOT_INVALID")
        visited = set()
        table_ids = set()
        for node in document.nodes:
            if node.parent_id is not None:
                parent = nodes.get(node.parent_id)
                if parent is None or parent.id not in visited or parent.level + 1 != node.level:
                    fail("TREE_PARENT_INVALID")
            elif node.level != 0:
                fail("TREE_ROOT_INVALID")
            visited.add(node.id)
            if not 1 <= node.page_start <= node.page_end <= len(document.pages):
                fail("TREE_PAGE_RANGE_INVALID")
            if node.content_hash != content_hash(node.own_body, node.table):
                fail("NODE_CONTENT_HASH_INVALID")
            check_mapping(node.own_body, node.char_map)
            for span in node.source_spans:
                resolve(span)
            expected_spans = (node.table.owned_source_spans if node.table else
                              tuple(span for segment in node.char_map for span in segment.source_spans))
            if interval_union(expected_spans) != interval_union(node.source_spans):
                fail("NODE_SOURCE_SPANS_DIFFER_FROM_BODY")
            for ref in node.context_refs:
                check_context(ref)
            if node.table is None:
                continue
            table = node.table
            if table.id in table_ids:
                fail("DUPLICATE_TABLE_OWNERSHIP")
            table_ids.add(table.id)
            cells = {c.id: c for c in table.cells}
            if len(cells) != len(table.cells):
                fail("TABLE_CELL_ID_INVALID")
            if table.kind == "blank_template":
                from .tables import is_blank_template
                if not is_blank_template(table, table.context_refs):
                    fail("TABLE_TEMPLATE_CLASSIFICATION_INVALID")
            occupied = set()
            for cell in table.cells:
                if cell.column + cell.column_span > table.column_count or cell.row + cell.row_span > 10_000:
                    fail("TABLE_GRID_INVALID")
                if cell.row_span * cell.column_span > 100_000:
                    fail("TABLE_GRID_INVALID")
                    continue
                for row_number in range(cell.row, cell.row + cell.row_span):
                    for col in range(cell.column, cell.column + cell.column_span):
                        if (row_number, col) in occupied:
                            fail("TABLE_GRID_INVALID")
                        occupied.add((row_number, col))
                check_mapping(cell.text, cell.char_map)
            max_row = max(c.row + c.row_span for c in table.cells)
            if len(occupied) != max_row * table.column_count:
                fail("TABLE_GRID_INCOMPLETE")
            mapped_spans = tuple(span for cell in table.cells for segment in cell.char_map for span in segment.source_spans)
            if interval_union(mapped_spans) != interval_union(table.owned_source_spans):
                fail("TABLE_SOURCE_OWNERSHIP_INVALID")
            owned_cells = set()
            owning_rows = set()
            for row in table.rows:
                if row.row_index in owning_rows:
                    fail("TABLE_ROW_OWNERSHIP_INVALID")
                owning_rows.add(row.row_index)
                for cell_id in row.cell_ids:
                    if cell_id not in cells or cell_id in owned_cells or cells[cell_id].row != row.row_index:
                        fail("TABLE_ROW_OWNERSHIP_INVALID")
                    owned_cells.add(cell_id)
                if any(cells[c].role == "data" and cells[c].text for c in row.cell_ids if c in cells):
                    if not any(ref.role == "header" for ref in row.context_refs + table.context_refs):
                        fail("TABLE_HEADER_CONTEXT_MISSING")
                for ref in row.context_refs:
                    check_context(ref)
            if owned_cells != set(cells):
                fail("TABLE_ROW_OWNERSHIP_INVALID")
            for ref in table.context_refs:
                check_context(ref)
            previous_end = -1
            for group in sorted(table.row_groups, key=lambda g: g.first_row):
                if group.first_row <= previous_end or group.last_row >= max_row:
                    fail("TABLE_ROW_GROUP_INVALID")
                previous_end = group.last_row
                for ref in group.context_refs:
                    check_context(ref)
        if table_ids != {table.id for table in document.tables}:
            fail("TABLE_TREE_COVERAGE_INVALID")
        expected_exclusions = classify_boilerplate(document)
        if expected_exclusions != document.exclusions:
            fail("BOILERPLATE_EXCLUSION_INVALID")
        else:
            for exclusion in expected_exclusions:
                for span in exclusion.source_spans:
                    if resolve(span) is None:
                        continue
                    for offset in range(span.start_offset, span.end_offset):
                        if coverage[span.block_id][offset] and not raw[span.block_id].text[offset].isspace():
                            fail("EXCLUDED_TEXT_IN_CANONICAL_BODY", span.pdf_page, span.block_id)
                        coverage[span.block_id][offset] = 1
        for block_id, counters in coverage.items():
            if any(not counters[i] for i, char in enumerate(raw[block_id].text) if not char.isspace()):
                fail("UNMAPPED_SOURCE_TEXT", raw[block_id].pdf_page, block_id)
        critical = sum(d.severity == "critical" for d in diagnostics)
        return QualityReport(status="failed" if critical else "passed", complete_page_count=len(pages),
                             critical_count=critical, warning_count=len(diagnostics) - critical,
                             diagnostics=tuple(diagnostics))
