"""Generic table geometry, exact character ownership and conservative continuation.

Geometry tolerances are engineering settings, not confidence or quality scores.
Vendor text is checked against source characters and never replaces their values.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from statistics import median
from typing import Any, Literal

from expert_contracts.sources import SourceSpan

from .dto import (
    BBox, Cell, CharMapSegment, ContextRef, Diagnostic, ParsedDocument, RawBlock,
    RawChar, Row, Table, TablePageSegment, TableRowGroup,
)

TABLE_ADAPTER_VERSION = "exact-char-grid-v1"
_NUMBER = re.compile(r"[+−±-]?\d+(?:[.,]\d+)?|[²³%‰]")
_NOTE = re.compile(r"^(?:Примечани[ея]|Примечание:|Note\b|[*†‡])", re.I)
_UNIT = re.compile(r"(?:%|‰|\b(?:кг|мг|км|мм|см|м|руб\w*|тонн\w*|штук|сут\w*|год\w*)\b|[²³])", re.I)
_VALUE = re.compile(r"^(?:(?:от|до|свыше|более|менее)\s+)?[+−±-]?\d+(?:[.,]\d+)?(?:\s|$)", re.I)
ContextRole = Literal["scope", "header", "unit", "note", "caption"]


@dataclass(frozen=True)
class CellGeometry:
    """Ephemeral vendor boundary; artifact cells always use the shared DTO."""

    row: int
    column: int
    row_span: int
    column_span: int
    bbox: tuple[float, float, float, float] | None
    expected_text: str | None = None
    header: bool = False
    row_group: bool = False


def _id(*parts: object) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()[:32]


def _compact(text: str) -> str:
    return re.sub(r"\s+", "", text)


def _inside(box: tuple[float, float, float, float], char: RawChar) -> bool:
    x = (char.bbox[0] + char.bbox[2]) / 2
    y = (char.bbox[1] + char.bbox[3]) / 2
    return box[0] <= x < box[2] and box[1] <= y < box[3]


def _union(boxes: Sequence[tuple[float, float, float, float]]) -> BBox:
    return (min(b[0] for b in boxes), min(b[1] for b in boxes),
            max(b[2] for b in boxes), max(b[3] for b in boxes))


def _diag(code: str, page: int, bbox: BBox | None = None, *, warning: bool = False) -> Diagnostic:
    return Diagnostic(code=code, severity="warning" if warning else "critical", pdf_page=page, bbox=bbox)


def mapped_characters(selected: Sequence[tuple[RawBlock, RawChar]]) -> tuple[str, tuple[CharMapSegment, ...]]:
    """Preserve exact raw intervals; inserted inter-line spaces have no source claim."""
    # A page-level column decision is not the reading order inside one cell.
    lines: list[list[tuple[RawBlock, RawChar]]] = []
    for item in sorted(selected, key=lambda item: (item[0].pdf_page, item[1].bbox[1], item[1].bbox[0])):
        block, char = item
        matches = []
        for i, line in enumerate(lines):
            reference = line[0]
            if reference[0].pdf_page != block.pdf_page:
                continue
            a, b = reference[1].bbox, char.bbox
            overlap = min(a[3], b[3]) - max(a[1], b[1])
            if overlap >= 0.5 * min(a[3] - a[1], b[3] - b[1]):
                matches.append((abs((a[1] + a[3]) - (b[1] + b[3])), i))
        if matches:
            lines[min(matches)[1]].append(item)
        else:
            lines.append([item])
    ordered = [item for line in lines for item in sorted(line, key=lambda item: (
        (item[1].bbox[0] + item[1].bbox[2]) / 2, item[0].block_id, item[1].offset))]
    groups: list[tuple[RawBlock, int, int, list[BBox]]] = []
    for block, char in ordered:
        if groups and groups[-1][0].block_id == block.block_id and groups[-1][2] == char.offset:
            old, start, _, boxes = groups.pop()
            groups.append((old, start, char.offset + len(char.text), [*boxes, char.bbox]))
        else:
            groups.append((block, char.offset, char.offset + len(char.text), [char.bbox]))
    text = ""
    segments: list[CharMapSegment] = []
    previous: RawBlock | None = None
    for block, start, end, boxes in groups:
        inline_symbol = previous is not None and (block.kind == "symbol" or previous.kind == "symbol") and (
            min(block.bbox[3], previous.bbox[3]) > max(block.bbox[1], previous.bbox[1]))
        if text and not inline_symbol and not text[-1].isspace() and not block.text[start:end][:1].isspace():
            segments.append(CharMapSegment(canonical_start=len(text), canonical_end=len(text) + 1,
                                            mapping="normalized_group", operation="format_separator"))
            text += " "
        span = SourceSpan(pdf_page=block.pdf_page, block_id=block.block_id,
                          start_offset=start, end_offset=end, bbox=_union(boxes))
        segments.append(CharMapSegment(canonical_start=len(text), canonical_end=len(text) + end - start,
                                        source_spans=(span,), mapping="exact"))
        text += block.text[start:end]
        previous = block
    return text, tuple(segments)


def _cell_ref(cell: Cell, role: ContextRole = "header") -> ContextRef | None:
    spans = tuple(span for segment in cell.char_map for span in segment.source_spans)
    if not cell.text.strip() or not spans:
        return None
    return ContextRef(role=role, text=cell.text, source_spans=spans)


def _row_context(cells: Sequence[Cell], context: tuple[ContextRef, ...],
                 groups: tuple[TableRowGroup, ...] = ()) -> tuple[Row, ...]:
    rows: dict[int, list[Cell]] = defaultdict(list)
    for cell in cells:
        rows[cell.row].append(cell)
    result = []
    header_rows = sorted({c.row for c in cells if c.role == "header"})
    header_starts = [r for i, r in enumerate(header_rows) if i == 0 or r != header_rows[i - 1] + 1]
    for row_index, own in sorted(rows.items()):
        references = list(context)
        header_start = max((r for r in header_starts if r <= row_index), default=0)
        for header in cells:
            if header.role in ("header", "unit") and header_start <= header.row <= row_index:
                ref = _cell_ref(header, header.role)
                if ref is not None:
                    references.append(ref)
                if header.role == "header" and _UNIT.search(header.text):
                    unit = _cell_ref(header, "unit")
                    if unit is not None:
                        references.append(unit)
            if header.role == "data" and header.row < row_index < header.row + header.row_span:
                inherited = _cell_ref(header, "scope")
                if inherited is not None:
                    references.append(inherited)
        for group in groups:
            if group.first_row <= row_index <= group.last_row:
                references.extend(group.context_refs)
        result.append(Row(row_index=row_index, cell_ids=tuple(c.id for c in sorted(own, key=lambda c: c.column)),
                          context_refs=tuple(dict.fromkeys(references))))
    return tuple(result)


def map_table(document: ParsedDocument, *, page_number: int, bbox: BBox,
              geometry: Sequence[CellGeometry], column_count: int,
              backend: str) -> tuple[Table | None, tuple[Diagnostic, ...]]:
    """Map a vendor grid to immutable raw characters; retain diagnostics on failure."""
    if not geometry or not 0 < column_count <= 1000 or len(geometry) > 100_000:
        return None, (_diag("AMBIGUOUS_TABLE_GRID", page_number, bbox),)
    diagnostics: list[Diagnostic] = []
    occupied: set[tuple[int, int]] = set()
    for spec in geometry:
        if (spec.row < 0 or spec.column < 0 or spec.row_span < 1 or spec.column_span < 1
                or spec.column + spec.column_span > column_count or spec.row + spec.row_span > 10_000):
            return None, (_diag("AMBIGUOUS_TABLE_GRID", page_number, bbox),)
        for row in range(spec.row, spec.row + spec.row_span):
            for col in range(spec.column, spec.column + spec.column_span):
                if (row, col) in occupied:
                    return None, (_diag("AMBIGUOUS_TABLE_GRID", page_number, bbox),)
                occupied.add((row, col))
    assignments: dict[int, list[tuple[RawBlock, RawChar]]] = defaultdict(list)
    for block in document.blocks:
        if block.pdf_page != page_number:
            continue
        for char in block.chars:
            if not _inside(bbox, char):
                continue
            matches = [i for i, spec in enumerate(geometry) if spec.bbox is not None and _inside(spec.bbox, char)]
            if len(matches) == 1:
                assignments[matches[0]].append((block, char))
            elif char.text.strip():
                diagnostics.append(_diag("AMBIGUOUS_TABLE_CHAR_OWNERSHIP", page_number, char.bbox))
    table_id = "table-" + _id(document.parse_generation_id, page_number, bbox, backend)
    cells: list[Cell] = []
    for i, spec in enumerate(geometry):
        text, char_map = mapped_characters(assignments[i])
        # Qualified symbol blocks are independent source records. Vendor PDF
        # text cannot contain an image-backed symbol, so compare its text-layer
        # portion separately without mutating either source representation.
        vendor_source_text, _ = mapped_characters([(b, c) for b, c in assignments[i] if b.kind != "symbol"])
        if spec.expected_text is not None and _compact(vendor_source_text) != _compact(spec.expected_text):
            diagnostics.append(_diag("TABLE_CELL_TEXT_MISMATCH", page_number, spec.bbox))
        if any(block.kind == "ocr" for block, _ in assignments[i]) and _NUMBER.search(text):
            diagnostics.append(_diag("UNVERIFIED_OCR_SYMBOLS", page_number, spec.bbox))
        role: Literal["header", "unit", "data", "note"] = "header" if spec.header else "note" if spec.row_group else "data"
        cells.append(Cell(id="cell-" + _id(table_id, spec.row, spec.column), row=spec.row, column=spec.column,
                          row_span=spec.row_span, column_span=spec.column_span, role=role,
                          text=text, char_map=char_map, bbox=spec.bbox, pdf_page=page_number))
    max_row = max(c.row + c.row_span for c in cells)
    group_cells = sorted((cell for cell, spec in zip(cells, geometry, strict=True) if spec.row_group), key=lambda c: c.row)
    groups = tuple(TableRowGroup(first_row=cell.row, last_row=(group_cells[i + 1].row - 1
                            if i + 1 < len(group_cells) else max_row - 1),
                            context_refs=(ref,)) for i, cell in enumerate(group_cells)
                   if (ref := _cell_ref(cell, "scope")) is not None)
    if not groups:
        header_rows = sorted({c.row for c in cells if c.role == "header"})
        starts = [r for i, r in enumerate(header_rows) if i == 0 or r != header_rows[i - 1] + 1]
        if len(starts) > 1:
            groups = tuple(TableRowGroup(first_row=start, last_row=(starts[i + 1] - 1 if i + 1 < len(starts) else max_row - 1),
                                         context_refs=()) for i, start in enumerate(starts))
    if len(occupied) != max_row * column_count:
        diagnostics.append(_diag("INCOMPLETE_TABLE_GRID", page_number, bbox))
    owned = tuple(span for cell in cells for segment in cell.char_map for span in segment.source_spans)
    table = Table(id=table_id, cells=tuple(cells), rows=_row_context(cells, (), groups), column_count=column_count,
                  row_groups=groups,
                  source_block_ids=tuple(dict.fromkeys(span.block_id for span in owned)), owned_source_spans=owned,
                  pdf_pages=(page_number,), page_segments=(TablePageSegment(pdf_page=page_number, bbox=bbox,
                                                                           first_row=0, last_row=max_row - 1),))
    return table, tuple(dict.fromkeys(diagnostics))


def _boundaries(values: Sequence[float]) -> list[float]:
    result: list[float] = []
    for value in sorted(values):
        if not result or value - result[-1] > 0.5:
            result.append(value)
    return result


def _nearest(value: float, boundaries: list[float]) -> int:
    return min(range(len(boundaries)), key=lambda i: abs(value - boundaries[i]))


def _primary_geometry(vendor: Any) -> tuple[list[CellGeometry], int]:
    # The vendor object remains local to this adapter and never enters an artifact.
    row_boxes = vendor.rows
    boxes = [tuple(box) for row in row_boxes for box in row.cells if box is not None]
    xs = _boundaries([edge for box in boxes for edge in (box[0], box[2])])
    ys = _boundaries([edge for box in boxes for edge in (box[1], box[3])])
    extracted = vendor.extract()
    first_numeric = next((i for i, row in enumerate(extracted)
                          if any(_VALUE.match((value or "").strip()) for value in row)), len(extracted))
    first_blank = next((i for i, row in enumerate(extracted) if i > 0 and not any(value for value in row)), len(extracted))
    if first_numeric == len(extracted):
        first_numeric = first_blank
    header_rows = set(range(first_numeric))
    initial = extracted[0] if extracted else []
    for i in range(first_numeric + 1, len(extracted) - 1):
        row = extracted[i]
        # A later complete nonnumeric band repeating a prior column heading
        # starts a new header scope. Values/filenames/page numbers are irrelevant.
        if (all(value and not _VALUE.match(value.strip()) for value in row)
                and any(_compact(value or "") == _compact(initial[col] or "")
                        for col, value in enumerate(row) if col < len(initial))
                and any(_VALUE.match((value or "").strip()) for value in extracted[i + 1])):
            header_rows.add(i)
    specs = []
    seen = set()
    for source_row, row in enumerate(row_boxes):
        for source_col, box in enumerate(row.cells):
            if box is None or tuple(box) in seen:
                continue
            seen.add(tuple(box))
            left, right = _nearest(box[0], xs), _nearest(box[2], xs)
            top, bottom = _nearest(box[1], ys), _nearest(box[3], ys)
            specs.append(CellGeometry(top, left, bottom - top, right - left, tuple(box),
                                      extracted[source_row][source_col], top in header_rows,
                                      row_group=right - left == len(xs) - 1 and top >= first_numeric))
    return specs, len(xs) - 1


def _outside_blocks(document: ParsedDocument, tables: Sequence[Table]) -> list[RawBlock]:
    owned: dict[str, set[int]] = defaultdict(set)
    for table in tables:
        for span in table.owned_source_spans:
            owned[span.block_id].update(range(span.start_offset, span.end_offset))
    return [block for block in document.blocks
            if any(char.text.strip() and char.offset not in owned[block.block_id] for char in block.chars)]


def _boilerplate(block: RawBlock, document: ParsedDocument) -> bool:
    page = next(p for p in document.pages if p.pdf_page == block.pdf_page)
    if not (block.bbox[3] < page.height * 0.11 or block.bbox[1] > page.height * 0.88):
        return False
    signature = re.sub(r"\d+", "#", _compact(block.text))
    return sum(1 for other in document.blocks if other.pdf_page != block.pdf_page
               and re.sub(r"\d+", "#", _compact(other.text)) == signature) >= 1


def _context(document: ParsedDocument, table: Table, outside: list[RawBlock]) -> tuple[ContextRef, ...]:
    first = table.page_segments[0]
    before = [b for b in outside if b.pdf_page == first.pdf_page and b.bbox[3] <= first.bbox[1]
              and not _boilerplate(b, document)]
    page = next(p for p in document.pages if p.pdf_page == first.pdf_page)
    if not before and first.bbox[1] < page.height * 0.25:
        before = [b for b in outside if b.pdf_page == first.pdf_page - 1 and not _boilerplate(b, document)]
    selected: list[tuple[RawBlock, ContextRole]] = []
    if before:
        nearest = max(before, key=lambda b: (b.pdf_page, b.bbox[1], b.bbox[0]))
        group = nearest.group_id or nearest.block_id
        caption = nearest.text.isupper() or re.match(r"^Таблица\b", nearest.text, re.I)
        selected.extend((b, "caption" if caption else "scope")
                        for b in before if (b.group_id or b.block_id) == group)
        if caption:
            top = min(b.bbox[1] for b, _ in selected)
            for block in sorted(before, key=lambda b: b.bbox[1], reverse=True):
                if block.bbox[1] >= top:
                    continue
                if not block.text.isupper() or top - block.bbox[3] > 10:
                    break
                selected.append((block, "caption"))
                top = block.bbox[1]
        elif not any(re.match(r"^\d+(?:\.\d+)*[.)]\s", b.text.strip()) for b, _ in selected):
            # PDF producers may split a single lead-in across several raw
            # groups. Continue only through adjacent lines to its clause start.
            top = min(b.bbox[1] for b, _ in selected)
            own_ids = {b.block_id for b, _ in selected}
            for block in sorted(before, key=lambda b: (b.bbox[1], b.bbox[0]), reverse=True):
                if block.block_id in own_ids or block.bbox[1] > top:
                    continue
                if top - block.bbox[3] > 1.5 * (block.bbox[3] - block.bbox[1]):
                    break
                selected.append((block, "scope"))
                top = min(top, block.bbox[1])
                if re.match(r"^\d+(?:\.\d+)*[.)]\s", block.text.strip()) or len(selected) >= 64:
                    break
    last = table.page_segments[-1]
    after = sorted((b for b in outside if b.pdf_page == last.pdf_page and b.bbox[1] >= last.bbox[3]
                    and not _boilerplate(b, document)), key=lambda b: b.ordinal)
    if after and _NOTE.match(after[0].text.strip()):
        group = after[0].group_id or after[0].block_id
        selected.extend((b, "note") for b in after if (b.group_id or b.block_id) == group)
    refs = []
    for block, role in sorted(selected, key=lambda item: (item[0].pdf_page, item[0].bbox[1], item[0].bbox[0])):
        if block.text.strip():
            refs.append(ContextRef(role=role, text=block.text, source_spans=(SourceSpan(
                pdf_page=block.pdf_page, block_id=block.block_id, start_offset=0,
                end_offset=len(block.text), bbox=block.bbox),)))
    return tuple(refs)


def _can_continue(document: ParsedDocument, left: Table, right: Table, outside: list[RawBlock]) -> bool:
    a, b = left.page_segments[-1], right.page_segments[0]
    if b.pdf_page != a.pdf_page + 1 or left.column_count != right.column_count:
        return False
    if any(c.role == "header" for c in right.cells):
        return False  # Repeated headers alone cannot distinguish adjacent similar tables.
    page_a = next(p for p in document.pages if p.pdf_page == a.pdf_page)
    page_b = next(p for p in document.pages if p.pdf_page == b.pdf_page)
    if a.bbox[3] < page_a.height * 0.75 or b.bbox[1] > page_b.height * 0.25:
        return False
    def anchors(table: Table, page_number: int) -> list[float]:
        result = []
        for col in range(table.column_count):
            boxes = [c.bbox for c in table.cells if c.pdf_page == page_number and c.column == col
                     and c.column_span == 1 and c.role == "data" and c.bbox and c.text.strip()]
            if not boxes:
                return []
            result.append(median(box[0] if col == 0 else (box[0] + box[2]) / 2 for box in boxes))
        return result
    edges_a, edges_b = anchors(left, a.pdf_page), anchors(right, b.pdf_page)
    if not edges_a or not edges_b:
        return False
    if len(edges_a) != len(edges_b) or any(abs(x / page_a.width - y / page_b.width) > 0.01
                                         for x, y in zip(edges_a, edges_b, strict=True)):
        return False
    intervening = [block for block in outside if
                   ((block.pdf_page == a.pdf_page and block.bbox[1] >= a.bbox[3])
                    or (block.pdf_page == b.pdf_page and block.bbox[3] <= b.bbox[1]))
                   and not _boilerplate(block, document)]
    return not any(block.text.strip() for block in intervening)


def is_blank_template(table: Table, context: Sequence[ContextRef]) -> bool:
    """Require a visible form cue plus unfilled geometry, never missing data alone."""
    headers = [c for c in table.cells if c.role in {"header", "unit"} and c.text.strip()]
    if not headers or any(c.text.strip() for c in table.cells if c.role not in {"header", "unit"}):
        return False
    form_cue = any(re.search(r"_{3,}|\b(?:форма|бланк|шаблон|template|form)\b", ref.text, re.I) for ref in context)
    if not form_cue:
        return False
    header_end = max(c.row + c.row_span for c in headers)
    if any(row.row_index >= header_end and all(not c.text.strip() for c in table.cells if c.id in row.cell_ids)
           for row in table.rows):
        return True
    lower = [c for c in table.cells if c.row > 0]
    labels = [c for c in lower if c.text.strip()]
    if not labels or not any(re.search(r"\.{3}|…", c.text) for c in labels):
        return False
    if any(re.search(r"\d", c.text) and re.fullmatch(r"[\s\d.,+±−%-]+", c.text) for c in labels):
        return False
    last_label_column = max(c.column + c.column_span for c in labels)
    return table.column_count - last_label_column >= 2 and all(
        not c.text.strip() for c in lower if c.column >= last_label_column)


def finish_tables(document: ParsedDocument, tables: Sequence[Table],
                  diagnostics: Sequence[Diagnostic] = ()) -> ParsedDocument:
    """Attach source context, merge only proven headerless page fragments, validate ownership."""
    ordered = sorted(tables, key=lambda t: (t.pdf_pages[0], t.page_segments[0].bbox[1]))
    outside = _outside_blocks(document, ordered)
    merged: list[Table] = []
    for table in ordered:
        if merged and _can_continue(document, merged[-1], table, outside):
            previous = merged.pop()
            offset = max(c.row + c.row_span for c in previous.cells)
            cells = previous.cells + tuple(c.model_copy(update={"row": c.row + offset}) for c in table.cells)
            groups = previous.row_groups + tuple(g.model_copy(update={
                "first_row": g.first_row + offset, "last_row": g.last_row + offset}) for g in table.row_groups)
            merged.append(previous.model_copy(update={
                "cells": cells, "rows": _row_context(cells, (), groups), "row_groups": groups,
                "source_block_ids": tuple(dict.fromkeys(previous.source_block_ids + table.source_block_ids)),
                "owned_source_spans": previous.owned_source_spans + table.owned_source_spans,
                "pdf_pages": previous.pdf_pages + table.pdf_pages,
                "page_segments": previous.page_segments + tuple(s.model_copy(update={
                    "first_row": s.first_row + offset, "last_row": s.last_row + offset}) for s in table.page_segments),
                "continuation_evidence": previous.continuation_evidence + (
                    "adjacent_pages:aligned_grid:headerless_fragment:body_boundaries:no_intervening_text",),
            }))
        else:
            merged.append(table)
    # These checks depend on the final merged representation and must be
    # recomputed after each fallback/range instead of retaining stale failures.
    recomputed = {"TABLE_HEADER_UNRESOLVED", "TABLE_DATA_UNRESOLVED"}
    issues = [d for d in document.diagnostics if d.code not in recomputed] + list(diagnostics)
    ownership: set[tuple[str, int]] = set()
    result = []
    for table in merged:
        context = _context(document, table, outside)
        if not any(c.role == "header" and c.text.strip() for c in table.cells):
            issues.append(_diag("TABLE_HEADER_UNRESOLVED", table.pdf_pages[0], table.page_segments[0].bbox))
        blank_template = is_blank_template(table, context)
        if not blank_template and not any(c.role == "data" and c.text.strip() for c in table.cells):
            issues.append(_diag("TABLE_DATA_UNRESOLVED", table.pdf_pages[0], table.page_segments[0].bbox))
        for span in table.owned_source_spans:
            for index in range(span.start_offset, span.end_offset):
                key = (span.block_id, index)
                if key in ownership:
                    issues.append(_diag("AMBIGUOUS_TABLE_CHAR_OWNERSHIP", span.pdf_page, span.bbox))
                ownership.add(key)
        result.append(table.model_copy(update={"kind": "blank_template" if blank_template else "data",
            "context_refs": context, "rows": _row_context(table.cells, context, table.row_groups)}))
    return document.model_copy(update={"tables": tuple(result), "diagnostics": tuple(dict.fromkeys(issues))})


def enrich_tables(document: ParsedDocument, path: Path, pages: tuple[int, ...] | None = None) -> ParsedDocument:
    """Read table geometry from the original PDF; never delete or rewrite raw blocks."""
    import pymupdf

    tables: list[Table] = []
    diagnostics: list[Diagnostic] = []
    with pymupdf.open(path) as pdf:
        for page_info in document.pages:
            if pages is not None and page_info.pdf_page not in pages:
                continue
            page = pdf[page_info.pdf_page - 1]
            page.set_rotation(0)  # In-memory only; raw coordinates use unrotated CropBox.
            try:
                detected = page.find_tables()
                for vendor in detected.tables:
                    geometry, columns = _primary_geometry(vendor)
                    table, problems = map_table(document, page_number=page_info.pdf_page,
                                                bbox=tuple(vendor.bbox), geometry=geometry,
                                                column_count=columns, backend="pymupdf")
                    diagnostics.extend(problems)
                    if table is not None:
                        tables.append(table)
            except Exception:
                diagnostics.append(_diag("TABLE_DETECTOR_FAILED", page_info.pdf_page))
    enriched = finish_tables(document, tables, diagnostics)
    return enriched.model_copy(update={"manifest": enriched.manifest.model_copy(update={
        "table_adapter_version": TABLE_ADAPTER_VERSION})})
