"""Pure source-mapped projections with exact FRIDA admission and zero overlap.

No storage, model inference, legal interpretation, OCR repair or publication lives here.
Failed projections expose diagnostics only, never a deceptively complete partial index.
"""
from __future__ import annotations

from bisect import bisect_right
import hashlib
import json
import math
import re
import unicodedata
from typing import Literal
from uuid import UUID

from pydantic import Field

from expert_contracts.common import SHA256
from expert_contracts.sources import SourceSpan
from .dto import (
    CanonicalDocument, CanonicalNode, CharMapSegment, ContextRef, Diagnostic, FrozenDTO,
    BBox, ParserManifest, RawBlock, Table,
)
from .tokenizer import DOCUMENT_PREFIX, MODEL_ID, REVISION, TokenizerBudget


class ChunkingConfig(FrozenDTO):
    max_input_tokens: int = Field(default=512, ge=8, le=512, strict=True)
    descriptor_max_tokens: int = Field(default=192, ge=8, le=512, strict=True)
    max_chunks: int = Field(default=100_000, ge=1, le=100_000, strict=True)
    max_tokenizer_calls: int = Field(default=1_000_000, ge=1, le=1_000_000, strict=True)
    overlap: Literal[0] = 0


class CanonicalRange(FrozenDTO):
    owner_kind: Literal["node_body", "table_cell"]
    text_owner_id: str
    start: int = Field(ge=0)
    end: int = Field(gt=0)


class TemplateCellSlot(FrozenDTO):
    """Geometry of an existing form slot; an empty slot carries no invented value."""

    cell_id: str
    row: int = Field(ge=0)
    column: int = Field(ge=0)
    row_span: int = Field(ge=1)
    column_span: int = Field(ge=1)
    role: Literal["header", "unit", "data", "note"]
    empty: bool
    pdf_page: int | None = Field(default=None, ge=1)
    bbox: BBox | None = None


class TableProjectionType(FrozenDTO):
    node_id: UUID
    table_id: str
    kind: Literal["data", "blank_template"]


class ChunkDraft(FrozenDTO):
    node_id: UUID
    chunk_index: int = Field(ge=0)
    canonical_ranges: tuple[CanonicalRange, ...]
    source_text: str = Field(min_length=1, repr=False)
    header_text: str
    embedding_text: str = Field(min_length=1, repr=False)
    source_spans: tuple[SourceSpan, ...] = Field(min_length=1)
    context_refs: tuple[ContextRef, ...]
    input_tokens: int = Field(ge=1, le=512)
    content_hash: SHA256
    table_rows: tuple[int, ...] = ()
    table_kind: Literal["data", "blank_template"] | None = None
    template_grid: tuple[TemplateCellSlot, ...] = ()
    requires_expansion: bool = False


class RoutingDescriptorDraft(FrozenDTO):
    node_id: UUID
    text: str = Field(min_length=1, repr=False)
    source_spans: tuple[SourceSpan, ...] = Field(min_length=1)
    input_tokens: int = Field(ge=1, le=512)


class ProjectionManifest(FrozenDTO):
    schema_version: Literal["p04.chunks.v2"] = "p04.chunks.v2"
    source_sha256: SHA256
    version_id: UUID
    parse_generation_id: UUID
    parser: ParserManifest
    config: ChunkingConfig
    tokenizer_model_id: str
    tokenizer_revision: str
    tokenizer_fingerprint: str
    document_prefix: Literal["search_document: "] = "search_document: "
    rendering: Literal["header-newline-context-newline-body-v1"] = "header-newline-context-newline-body-v1"
    splitting: Literal["paragraph-sentence-map-unicode-zero-overlap-v1"] = "paragraph-sentence-map-unicode-zero-overlap-v1"
    descriptors: Literal["mapped-source-intro-v1"] = "mapped-source-intro-v1"
    lexical: Literal["canonical-header-source-v1"] = "canonical-header-source-v1"
    shortened_header_nodes: tuple[UUID, ...] = ()
    table_types: tuple[TableProjectionType, ...] = ()
    recipe_hash: SHA256


class ChunkProjection(FrozenDTO):
    status: Literal["passed", "failed"]
    error_code: Literal["GENERATION_INVALID", "TOKEN_LIMIT_EXCEEDED"] | None = None
    chunks: tuple[ChunkDraft, ...] = ()
    descriptors: tuple[RoutingDescriptorDraft, ...] = ()
    manifest: ProjectionManifest
    diagnostics: tuple[Diagnostic, ...] = ()


class _Failure(Exception):
    def __init__(self, code: Literal["GENERATION_INVALID", "TOKEN_LIMIT_EXCEEDED"], diagnostic: str):
        self.code, self.diagnostic = code, diagnostic


def _fail(diagnostic: str, code: Literal["GENERATION_INVALID", "TOKEN_LIMIT_EXCEEDED"] = "GENERATION_INVALID") -> None:
    raise _Failure(code, diagnostic)


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":"), default=str).encode()).hexdigest()


def _unique_spans(spans) -> tuple[SourceSpan, ...]:
    return tuple(dict.fromkeys(spans))


def _refs(refs) -> tuple[ContextRef, ...]:
    return tuple(dict.fromkeys(refs))


def _slice_spans(mapping: tuple[CharMapSegment, ...], start: int, end: int) -> tuple[SourceSpan, ...]:
    result = []
    for segment in mapping:
        if segment.canonical_end <= start:
            continue
        if segment.canonical_start >= end:
            break
        left, right = max(start, segment.canonical_start), min(end, segment.canonical_end)
        if segment.mapping == "exact":
            span = segment.source_spans[0]
            offset = span.start_offset - segment.canonical_start
            result.append(span.model_copy(update={"start_offset": offset + left, "end_offset": offset + right}))
        else:
            if left != segment.canonical_start or right != segment.canonical_end:
                _fail("ATOMIC_MAP_SPLIT")
            result.extend(segment.source_spans)
    return _unique_spans(result)


def _boundaries(text: str, mapping: tuple[CharMapSegment, ...]) -> list[int]:
    """Codepoints, extended combining sequences and normalization groups remain whole."""
    boundaries = [0]
    for segment in mapping:
        if segment.mapping == "exact":
            for offset in range(segment.canonical_start + 1, segment.canonical_end + 1):
                if (offset < len(text) and (unicodedata.combining(text[offset])
                        or text[offset] in {"\ufe0e", "\ufe0f", "\u200d"}
                        or text[offset - 1] == "\u200d"
                        or 0x1F3FB <= ord(text[offset]) <= 0x1F3FF)):
                    continue
                boundaries.append(offset)
        else:
            boundaries.append(segment.canonical_end)
    return sorted(offset for offset in set(boundaries) if offset == len(text) or offset == 0
                  or not (unicodedata.combining(text[offset]) or text[offset] in {"\ufe0e", "\ufe0f", "\u200d"}
                          or text[offset - 1] == "\u200d" or 0x1F3FB <= ord(text[offset]) <= 0x1F3FF))


class _Projector:
    def __init__(self, document: CanonicalDocument, budget: TokenizerBudget, config: ChunkingConfig):
        self.document, self.budget, self.config = document, budget, config
        self.nodes = {node.id: node for node in document.nodes}
        self.blocks: dict[tuple[int, str], RawBlock] = {}
        self.pages = {page.pdf_page: page for page in document.pages}
        self.chunks: list[ChunkDraft] = []
        self.descriptors: list[RoutingDescriptorDraft] = []
        self.shortened: set[UUID] = set()
        self.calls = 0
        self.cache: dict[str, int] = {}
        self.ownership: dict[tuple[int, str], list[tuple[int, int]]] = {}
        self.ordinals: dict[UUID, int] = {}
        self.recipe_value: dict | None = None

    def count(self, text: str) -> int:
        if text not in self.cache:
            self.calls += 1
            if self.calls > self.config.max_tokenizer_calls:
                _fail("CHUNKING_RESOURCE_LIMIT")
            count = self.budget.document_tokens(text)
            if type(count) is not int or count < 1:
                _fail("TOKENIZER_INVALID_COUNT")
            if len(self.cache) >= 512:
                self.cache.clear()
            if len(text) <= 8192:
                self.cache[text] = count
            return count
        return self.cache[text]

    def span(self, span: SourceSpan) -> RawBlock:
        block = self.blocks.get((span.pdf_page, span.block_id))
        page = self.pages.get(span.pdf_page)
        if block is None or page is None or span.end_offset <= span.start_offset:
            _fail("SOURCE_SPAN_UNRESOLVED")
        assert block is not None and page is not None
        try:
            span.validate_source(block_text=block.text, pdf_page_count=len(self.pages),
                                 page_size=(page.width, page.height))
        except ValueError:
            _fail("SOURCE_SPAN_OUT_OF_BOUNDS")
        if span.bbox is not None and not all(math.isfinite(v) for v in span.bbox):
            _fail("SOURCE_SPAN_OUT_OF_BOUNDS")
        return block

    def mapping(self, text: str, mapping: tuple[CharMapSegment, ...]) -> None:
        cursor = 0
        for segment in mapping:
            if segment.canonical_start != cursor or segment.canonical_end > len(text):
                _fail("CANONICAL_MAP_INCOMPLETE")
            value = text[segment.canonical_start:segment.canonical_end]
            for span in segment.source_spans:
                block = self.span(span)
                if segment.mapping == "exact" and block.text[span.start_offset:span.end_offset] != value:
                    _fail("EXACT_MAP_TEXT_DIFFERS")
                self.ownership.setdefault((span.pdf_page, span.block_id), []).append(
                    (span.start_offset, span.end_offset))
            if not segment.source_spans and not value.isspace():
                _fail("SOURCELESS_NORMATIVE_TEXT")
            cursor = segment.canonical_end
        if cursor != len(text):
            _fail("CANONICAL_MAP_INCOMPLETE")

    def context(self, refs: tuple[ContextRef, ...]) -> None:
        for ref in refs:
            if not ref.text.strip():
                _fail("EMPTY_CONTEXT")
            for span in ref.source_spans:
                self.span(span)

    def validate(self) -> None:
        if any(d.severity == "critical" for d in self.document.diagnostics):
            _fail("CRITICAL_PARSER_DIAGNOSTIC")
        if (not self.nodes or len(self.nodes) != len(self.document.nodes)
                or sorted(self.pages) != list(range(1, len(self.document.pages) + 1))
                or len(self.pages) != len(self.document.pages)):
            _fail("CANONICAL_TREE_INVALID")
        if any(p.decision not in {"accept_text", "intentional_blank"} for p in self.document.pages):
            _fail("UNRESOLVED_PAGE_DECISION")
        for block in self.document.blocks:
            key = (block.pdf_page, block.block_id)
            if key in self.blocks or block.pdf_page not in self.pages:
                _fail("RAW_REGISTRY_INVALID")
            self.blocks[key] = block
        roots = [n for n in self.document.nodes if n.parent_id is None]
        if len(roots) != 1 or roots[0].node_type != "document":
            _fail("CANONICAL_TREE_INVALID")
        tables = set()
        for node in self.document.nodes:
            path: set[UUID] = set()
            current = node
            while current.parent_id is not None:
                if current.id in path or current.parent_id not in self.nodes or len(path) > 100:
                    _fail("CANONICAL_TREE_INVALID")
                path.add(current.id)
                current = self.nodes[current.parent_id]
            if node.page_start > node.page_end or node.page_start not in self.pages or node.page_end not in self.pages:
                _fail("NODE_PAGE_INVALID")
            self.mapping(node.own_body, node.char_map)
            self.context(node.context_refs)
            for span in node.source_spans:
                self.span(span)
            if node.table:
                if node.table.id in tables:
                    _fail("DUPLICATE_TABLE_OWNER")
                tables.add(node.table.id)
                self.validate_table(node.table)
        if {t.id for t in self.document.tables} - tables:
            _fail("TABLE_WITHOUT_CANONICAL_OWNER")
        for intervals in self.ownership.values():
            previous = -1
            for start, end in sorted(intervals):
                if start < previous:
                    _fail("DUPLICATE_SOURCE_OWNERSHIP")
                previous = end

    def validate_table(self, table: Table) -> None:
        cells = {cell.id: cell for cell in table.cells}
        if len(cells) != len(table.cells) or len({r.row_index for r in table.rows}) != len(table.rows):
            _fail("TABLE_GRID_INVALID")
        if tuple(r.row_index for r in table.rows) != tuple(sorted(r.row_index for r in table.rows)):
            _fail("TABLE_ROW_ORDER_INVALID")
        if any(page not in self.pages for page in table.pdf_pages):
            _fail("TABLE_PAGE_INVALID")
        self.context(table.context_refs)
        for cell in table.cells:
            if cell.column + cell.column_span > table.column_count:
                _fail("TABLE_GRID_INVALID")
            self.mapping(cell.text, cell.char_map)
        seen = set()
        for row in table.rows:
            if len(set(row.cell_ids)) != len(row.cell_ids) or any(cid not in cells for cid in row.cell_ids):
                _fail("TABLE_CELL_UNRESOLVED")
            self.context(row.context_refs)
            for cid in row.cell_ids:
                cell = cells[cid]
                if cell.row != row.row_index or cid in seen:
                    _fail("TABLE_ROW_CELL_MISMATCH")
                seen.add(cid)
        if set(cells) != seen:
            _fail("TABLE_DATA_COVERAGE_INCOMPLETE")
        if table.kind == "blank_template":
            headers = [c for c in table.cells if c.role in {"header", "unit"} and c.text.strip()]
            # Classification/form cues were independently qualified by the parser.
            # A form can have source row labels alongside empty value slots, so a
            # completely empty row is not mandatory here. No observed data is admitted.
            empty_slots = [cell for cell in table.cells if not cell.text.strip()]
            if (not headers or any(c.text.strip() and c.role not in {"header", "unit"} for c in table.cells)
                    or not any(cell.row > min(c.row for c in headers) for cell in empty_slots)):
                _fail("TABLE_TEMPLATE_CLASSIFICATION_INVALID")
        elif not any(c.role == "data" and c.text.strip() for c in table.cells):
            _fail("TABLE_WITHOUT_READABLE_DATA")

    def header_options(self, node: CanonicalNode) -> list[str]:
        path = []
        current = node
        while current.parent_id is not None:
            current = self.nodes[current.parent_id]
            label = " ".join(p for p in (current.number, current.title) if p)
            if label:
                path.append(label)
        path.reverse()
        own = " ".join(p for p in (node.number, node.title) if p)
        parts = list(dict.fromkeys(p for p in (self.document.title, *path, own) if p))
        options = [" / ".join(parts)]
        # Distant ancestors and operator metadata are optional; the current number is retained.
        while len(parts) > 1:
            parts.pop(0)
            options.append(" / ".join(parts))
        options.append(node.number or "")
        return list(dict.fromkeys(options))

    @staticmethod
    def render(header: str, refs: tuple[ContextRef, ...], body: str) -> str:
        return "\n".join(part for part in (header, *(ref.text for ref in refs), body) if part)

    def fit_render(self, node: CanonicalNode, body: str, refs: tuple[ContextRef, ...], limit: int):
        options = self.header_options(node)
        for selected_refs in (refs, tuple(r for r in refs if r.required)):
            for header in options:
                rendered = self.render(header, selected_refs, body)
                if self.count(rendered) <= limit:
                    return header, selected_refs, rendered
        return None

    def prefix_end(self, text: str, start: int, boundaries: list[int], fits) -> int:
        """Find a verified fitting complete interval, without assuming maximal BPE monotonicity."""
        lo = bisect_right(boundaries, start)
        # Bound candidate text per tokenization, except for one explicitly atomic group.
        hi = max(lo, bisect_right(boundaries, start + 8192) - 1)
        hi = min(hi, len(boundaries) - 1)
        best = start
        while lo <= hi:
            mid = (lo + hi) // 2
            end = boundaries[mid]
            if fits(text[start:end]):
                best = end
                lo = mid + 1
            else:
                hi = mid - 1
        if best == start:
            first = boundaries[bisect_right(boundaries, start)]
            if fits(text[start:first]):
                best = first
        return best

    def body_chunks(self, node: CanonicalNode) -> None:
        text = node.own_body
        if not text.strip():
            return
        refs = _refs(node.context_refs)
        whole = self.fit_render(node, text, refs, self.config.max_input_tokens)
        if whole:
            self.add(node, text, (("node_body", str(node.id), 0, len(text)),),
                     _slice_spans(node.char_map, 0, len(text)), whole)
            return
        boundaries = _boundaries(text, node.char_map)
        boundary_set = set(boundaries)
        paragraphs = [m.end() for m in re.finditer(r"\n[ \t]*\n|\n", text) if m.end() in boundary_set]
        sentences = [m.end() for m in re.finditer(r"(?<=[.!?;])\s+", text) if m.end() in boundary_set]
        words = [m.end() for m in re.finditer(r"\s+", text) if m.end() in boundary_set]
        start = 0
        while start < len(text):
            def fits(body):
                return self.fit_render(node, body, refs, self.config.max_input_tokens) is not None
            end = self.prefix_end(text, start, boundaries, fits)
            if end == start:
                _fail("ATOMIC_CONTEXT_OR_TEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")
            if end < len(text):
                for preferred in (paragraphs, sentences, words):
                    choices = preferred[bisect_right(preferred, start):bisect_right(preferred, end)]
                    selected = next((candidate for candidate in reversed(choices)
                                     if fits(text[start:candidate])), None)
                    if selected is not None:
                        end = selected
                        break
                # Do not strand a short introducer at a forced character boundary.
                match = re.search(r"(?:\bне|\bесли|\bпри условии|\bза исключением)\s*$", text[start:end], re.I)
                if match and match.start() > 0:
                    safe = start + match.start()
                    safe = boundaries[bisect_right(boundaries, safe) - 1]
                    if safe > start and fits(text[start:safe]):
                        end = safe
            # Include trailing whitespace in this source interval when it fits, avoiding empty chunks.
            if text[end:].isspace() and fits(text[start:]):
                end = len(text)
            body = text[start:end]
            rendered = self.fit_render(node, body, refs, self.config.max_input_tokens)
            if rendered is None or not body.strip():
                _fail("ATOMIC_CONTEXT_OR_TEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")
            self.add(node, body, (("node_body", str(node.id), start, end),),
                     _slice_spans(node.char_map, start, end), rendered, partial=True)
            start = end

    def add(self, node, source, ranges, spans, rendered, *, rows=(), partial=False, table_kind=None, grid=()):
        if len(self.chunks) >= self.config.max_chunks:
            _fail("CHUNKING_RESOURCE_LIMIT")
        header, refs, embedding = rendered
        if header != self.header_options(node)[0]:
            self.shortened.add(node.id)
        if not spans:
            _fail("EMPTY_SOURCE_PROJECTION")
        canonical_ranges = tuple(CanonicalRange(owner_kind=kind, text_owner_id=owner, start=start, end=end)
                                 for kind, owner, start, end in ranges)
        identity = {
            "recipe": self.recipe(), "node": node.id, "source": source,
            "ranges": [r.model_dump(mode="json") for r in canonical_ranges],
            "spans": [s.model_dump(mode="json") for s in spans], "header": header,
            "context": [r.model_dump(mode="json") for r in refs], "partial": partial, "rows": rows,
            "table_kind": table_kind, "template_grid": [cell.model_dump(mode="json") for cell in grid],
        }
        ordinal = self.ordinals.get(node.id, 0)
        self.ordinals[node.id] = ordinal + 1
        self.chunks.append(ChunkDraft(
            node_id=node.id, chunk_index=ordinal, canonical_ranges=canonical_ranges,
            source_text=source, source_spans=spans, header_text=header, embedding_text=embedding,
            context_refs=refs, input_tokens=self.count(embedding), content_hash=_hash(identity),
            table_rows=rows, requires_expansion=partial,
            table_kind=table_kind, template_grid=grid,
        ))

    def template_chunk(self, node: CanonicalNode, table: Table) -> None:
        labels = sorted((cell for cell in table.cells if cell.text.strip()), key=lambda c: (c.row, c.column))
        source = " | ".join(cell.text for cell in labels)
        ranges = tuple(("table_cell", cell.id, 0, len(cell.text)) for cell in labels)
        spans = _unique_spans(span for cell in labels for span in _slice_spans(cell.char_map, 0, len(cell.text)))
        refs = _refs((*node.context_refs, *table.context_refs, *(ref for row in table.rows for ref in row.context_refs)))
        rendered = self.fit_render(node, source, refs, self.config.max_input_tokens)
        if rendered is None:
            _fail("TABLE_TEMPLATE_CONTEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")
        grid = tuple(TemplateCellSlot(cell_id=cell.id, row=cell.row, column=cell.column,
                                     row_span=cell.row_span, column_span=cell.column_span,
                                     role=cell.role, empty=not cell.text.strip(),
                                     pdf_page=cell.pdf_page, bbox=cell.bbox)
                     for cell in sorted(table.cells, key=lambda c: (c.row, c.column)))
        self.add(node, source, ranges, spans, rendered, rows=tuple(row.row_index for row in table.rows),
                 partial=True, table_kind="blank_template", grid=grid)

    def table_chunks(self, node: CanonicalNode) -> None:
        table = node.table
        assert table is not None
        if table.kind == "blank_template":
            self.template_chunk(node, table)
            return
        cells = {cell.id: cell for cell in table.cells}
        retained_context: list[SourceSpan] = []
        for row in table.rows:
            row_cells = [cells[cid] for cid in row.cell_ids if cells[cid].role == "data"]
            if not row_cells:
                continue
            refs = list(_refs((*node.context_refs, *table.context_refs, *row.context_refs)))
            if not any(ref.role == "header" and ref.required for ref in refs):
                _fail("TABLE_REQUIRED_HEADER_MISSING")
            body, ranges = [], []
            spans: list[SourceSpan] = []
            for cell in sorted(row_cells, key=lambda c: c.column):
                body.append(cell.text)
                if cell.text:
                    ranges.append(("table_cell", cell.id, 0, len(cell.text)))
                    spans.extend(_slice_spans(cell.char_map, 0, len(cell.text)))
            if not ranges:
                _fail("TABLE_ROW_WITHOUT_OWNED_TEXT")
            source = " | ".join(body)
            rendered = self.fit_render(node, source, _refs(refs), self.config.max_input_tokens)
            if rendered is None:
                _fail("TABLE_ROW_CONTEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")
            retained_context.extend(s for ref in rendered[1] for s in ref.source_spans)
            self.add(node, source, ranges, _unique_spans(spans), rendered, rows=(row.row_index,), table_kind="data")
        # Header/unit/note owning cells need an explicit route into the projection's source context.
        # The parser owns per-row applicability; this checks that none disappears from every row.
        for cell in table.cells:
            if cell.role == "data" or not cell.text.strip():
                continue
            for source_span in _slice_spans(cell.char_map, 0, len(cell.text)):
                intervals = sorted((s.start_offset, s.end_offset) for s in retained_context
                                   if (s.pdf_page, s.block_id) == (source_span.pdf_page, source_span.block_id))
                cursor = source_span.start_offset
                for start, end in intervals:
                    if start > cursor:
                        break
                    cursor = max(cursor, end)
                if cursor < source_span.end_offset:
                    _fail("TABLE_CONTEXT_COVERAGE_INCOMPLETE")

    def routing(self) -> None:
        for node in self.document.nodes:
            if node.node_type not in {"document", "section", "chapter", "article"}:
                continue
            # A source intro, including a source heading if present, avoids routing solely on metadata.
            candidate = node
            if not candidate.own_body.strip():
                candidate = next((n for n in self.document.nodes if n.own_body.strip()
                                  and self.descendant(n, node.id)), node)
            if not candidate.own_body.strip():
                continue
            text = candidate.own_body
            boundaries = _boundaries(text, candidate.char_map)
            limit = min(self.config.descriptor_max_tokens, self.config.max_input_tokens)
            # Descriptors are non-citable search aids, and never inherit normative claim authority.
            end = self.prefix_end(text, 0, boundaries,
                                  lambda value: self.fit_render(node, value, (), limit) is not None)
            if not end:
                _fail("ROUTING_ATOMIC_SOURCE_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")
            intro = text[:end]
            spans = _slice_spans(candidate.char_map, 0, end)
            if intro.strip() and spans:
                rendered = self.fit_render(node, intro, (), limit)
                assert rendered is not None
                self.descriptors.append(RoutingDescriptorDraft(
                    node_id=node.id, text=rendered[2], source_spans=spans, input_tokens=self.count(rendered[2])))
        if not self.descriptors:
            if not self.chunks:
                _fail("EMPTY_GENERATION")
            chunk = self.chunks[0]
            self.descriptors.append(RoutingDescriptorDraft(
                node_id=chunk.node_id, text=chunk.embedding_text,
                source_spans=_unique_spans((*chunk.source_spans,
                                            *(s for r in chunk.context_refs for s in r.source_spans))),
                input_tokens=chunk.input_tokens,
            ))

    def descendant(self, node: CanonicalNode, parent_id: UUID) -> bool:
        while node.parent_id is not None:
            if node.parent_id == parent_id:
                return True
            node = self.nodes[node.parent_id]
        return False

    def recipe(self):
        if self.recipe_value is not None:
            return self.recipe_value
        self.recipe_value = {
            "schema": "p04.chunks.v2", "version_id": self.document.version_id,
            "parse_generation_id": self.document.parse_generation_id, "sha256": self.document.source_sha256,
            "parser": self.document.manifest.model_dump(mode="json"),
            "config": self.config.model_dump(mode="json"), "tokenizer": self.budget.fingerprint,
            "revision": self.budget.revision, "prefix": DOCUMENT_PREFIX,
            "rendering": "header-newline-context-newline-body-v1",
            "split": "paragraph-sentence-map-unicode-zero-overlap-v1", "descriptor": "mapped-source-intro-v1",
            "lexical": "canonical-header-source-v1",
            "tables": [(str(node.id), node.table.id, node.table.kind)
                       for node in self.document.nodes if node.table is not None],
        }
        return self.recipe_value

    def manifest(self) -> ProjectionManifest:
        return ProjectionManifest(
            source_sha256=self.document.source_sha256, version_id=self.document.version_id,
            parse_generation_id=self.document.parse_generation_id, parser=self.document.manifest,
            config=self.config, tokenizer_model_id=self.budget.model_id, tokenizer_revision=self.budget.revision,
            tokenizer_fingerprint=self.budget.fingerprint, recipe_hash=_hash(self.recipe()),
            shortened_header_nodes=tuple(sorted(self.shortened, key=str)),
            table_types=tuple(TableProjectionType(node_id=node.id, table_id=node.table.id, kind=node.table.kind)
                              for node in self.document.nodes if node.table is not None),
        )


def project_chunks(document: CanonicalDocument, tokenizer_budget: TokenizerBudget,
                   chunking_config: ChunkingConfig | None = None) -> ChunkProjection:
    """Produce deterministic complete drafts, or a typed failure with no partial output."""
    worker = _Projector(document, tokenizer_budget, chunking_config or ChunkingConfig())
    try:
        if tokenizer_budget.model_id != MODEL_ID or tokenizer_budget.revision != REVISION:
            _fail("TOKENIZER_RECIPE_MISMATCH")
        worker.validate()
        for node in document.nodes:
            worker.body_chunks(node)
            if node.table:
                worker.table_chunks(node)
        if not worker.chunks:
            _fail("EMPTY_GENERATION")
        worker.routing()
        return ChunkProjection(status="passed", chunks=tuple(worker.chunks),
                               descriptors=tuple(worker.descriptors), manifest=worker.manifest(),
                               diagnostics=tuple(d for d in document.diagnostics if d.severity == "warning"))
    except _Failure as exc:
        return ChunkProjection(status="failed", error_code=exc.code, manifest=worker.manifest(),
                               diagnostics=(Diagnostic(code=exc.diagnostic, severity="critical"),))
