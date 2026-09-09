"""Public, bounded projections of a pinned canonical parse; no parser internals."""
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, Field, model_validator

from .common import ApiPath, Count, NonBlank, PositiveInt, StrictDTO, unique
from .documents import QualitySummary
from .sources import SourceSpan


CanonicalNodeType = Literal[
    "document", "section", "chapter", "article", "clause", "subclause", "paragraph",
    "appendix", "table", "figure", "footnote", "editorial_note", "unknown",
]
PDFPage = Annotated[int, Field(ge=1, le=500, strict=True)]
Cursor = Annotated[NonBlank, Field(max_length=500)]
Identifier = Annotated[NonBlank, Field(max_length=200)]


def _bbox(value: tuple[float, float, float, float]):
    x0, y0, x1, y1 = value
    if min(value) < 0 or x1 <= x0 or y1 <= y0:
        raise ValueError("PDF bounding box must have ordered nonnegative coordinates and positive area")
    return value


PDFBoundingBox = Annotated[tuple[float, float, float, float], AfterValidator(_bbox)]


class CanonicalTreeNode(StrictDTO):
    node_id: UUID
    parent_id: UUID | None
    node_type: CanonicalNodeType
    ordinal: Count
    number: str | None = Field(max_length=200)
    title: str | None = Field(max_length=2000)
    page_start: PDFPage
    page_end: PDFPage
    has_children: bool = Field(strict=True)

    @model_validator(mode="after")
    def node_bounds(self):
        if self.parent_id == self.node_id:
            raise ValueError("tree node cannot parent itself")
        if self.page_end < self.page_start:
            raise ValueError("tree node page range must be ordered")
        return self


class CanonicalTreePage(StrictDTO):
    version_id: UUID
    parse_generation_id: UUID
    parent_id: UUID | None
    items: list[CanonicalTreeNode] = Field(max_length=100)
    next_cursor: Cursor | None

    @model_validator(mode="after")
    def child_bindings(self):
        unique([item.node_id for item in self.items], "tree node IDs")
        if any(item.parent_id != self.parent_id for item in self.items):
            raise ValueError("tree children must match the selected parent")
        positions = [(item.ordinal, item.node_id) for item in self.items]
        if positions != sorted(positions):
            raise ValueError("tree children must follow ordinal and node ID order")
        if self.next_cursor is not None and not self.items:
            raise ValueError("empty tree page cannot advertise more children")
        return self


class QualityDiagnostic(StrictDTO):
    ordinal: PositiveInt
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,79}$")
    severity: Literal["warning", "critical"]
    pdf_page: PDFPage | None
    block_id: Identifier | None
    bbox: PDFBoundingBox | None

    @model_validator(mode="after")
    def source_location(self):
        if self.bbox is not None and self.pdf_page is None:
            raise ValueError("diagnostic bounding box requires its physical PDF page")
        return self


class ParseQualityPage(StrictDTO):
    version_id: UUID
    parse_generation_id: UUID
    summary: QualitySummary
    items: list[QualityDiagnostic] = Field(max_length=100)
    next_cursor: Cursor | None
    source_url: ApiPath

    @model_validator(mode="after")
    def diagnostic_order_and_source(self):
        ordinals = [item.ordinal for item in self.items]
        unique(ordinals, "diagnostic ordinals")
        if ordinals != sorted(ordinals):
            raise ValueError("diagnostics must retain their original report order")
        if self.next_cursor is not None and not self.items:
            raise ValueError("empty quality page cannot advertise more diagnostics")
        if self.source_url != f"/api/v1/versions/{self.version_id}/source":
            raise ValueError("quality source URL must identify the selected version")
        if self.summary.status in ("passed", "warning") and any(item.severity == "critical" for item in self.items):
            raise ValueError("critical diagnostics require failed quality status")
        if sum(item.severity == "warning" for item in self.items) > self.summary.warning_count:
            raise ValueError("page warning count exceeds the complete report count")
        return self


class StructuredTableContext(StrictDTO):
    role: Literal["scope", "header", "unit", "note", "caption"]
    text: NonBlank = Field(max_length=2_000_000)
    source_spans: list[SourceSpan] = Field(min_length=1, max_length=1000)
    required: bool = Field(strict=True)


class StructuredTableCell(StrictDTO):
    id: Identifier
    row: Count
    column: Count
    row_span: PositiveInt = Field(le=10_000)
    column_span: PositiveInt = Field(le=1000)
    role: Literal["header", "unit", "data", "note"]
    text: str = Field(max_length=2_000_000)
    pdf_page: PDFPage | None
    bbox: PDFBoundingBox | None
    source_spans: list[SourceSpan] = Field(max_length=1000)

    @model_validator(mode="after")
    def source_location(self):
        if self.bbox is not None and self.pdf_page is None:
            raise ValueError("table cell bounding box requires its physical PDF page")
        if self.text.strip() and not self.source_spans:
            raise ValueError("nonempty table cell must retain its mapped source spans")
        unique([span.model_dump_json() for span in self.source_spans], "cell source spans")
        return self


class StructuredTableRow(StrictDTO):
    row_index: Count
    cell_ids: list[Identifier] = Field(max_length=1000)
    context_refs: list[StructuredTableContext] = Field(max_length=1000)

    @model_validator(mode="after")
    def cell_references_unique(self):
        unique(self.cell_ids, "row cell IDs")
        return self


class StructuredTablePageSegment(StrictDTO):
    pdf_page: PDFPage
    bbox: PDFBoundingBox
    first_row: Count
    last_row: Count

    @model_validator(mode="after")
    def row_bounds(self):
        if self.last_row < self.first_row:
            raise ValueError("table page segment row range must be ordered")
        return self


class StructuredTablePage(StrictDTO):
    version_id: UUID
    parse_generation_id: UUID
    node_id: UUID
    table_id: Identifier
    kind: Literal["data", "blank_template"]
    column_count: PositiveInt = Field(le=1000)
    total_rows: PositiveInt = Field(le=100_000)
    row_start: Count
    row_end: Count
    rows: list[StructuredTableRow] = Field(min_length=1, max_length=50)
    cells: list[StructuredTableCell] = Field(min_length=1, max_length=1000)
    context_refs: list[StructuredTableContext] = Field(max_length=1000)
    page_segments: list[StructuredTablePageSegment] = Field(max_length=500)
    pdf_pages: list[PDFPage] = Field(min_length=1, max_length=500)
    source_url: ApiPath
    next_cursor: Cursor | None

    @model_validator(mode="after")
    def window_and_grid(self):
        if not (self.row_start <= self.row_end < self.total_rows) or self.row_end - self.row_start >= 50:
            raise ValueError("table window must contain 1 to 50 rows inside the original grid")
        if [row.row_index for row in self.rows] != list(range(self.row_start, self.row_end + 1)):
            raise ValueError("table rows must cover the selected logical grid window exactly")
        unique([cell.id for cell in self.cells], "table cell IDs")
        unique(self.pdf_pages, "table PDF pages")
        if self.pdf_pages != sorted(self.pdf_pages):
            raise ValueError("table PDF pages must be ordered")
        if self.source_url != f"/api/v1/versions/{self.version_id}/source":
            raise ValueError("table source URL must identify the selected version")
        if (self.next_cursor is None) != (self.row_end == self.total_rows - 1):
            raise ValueError("table cursor must correspond to the remaining grid rows")
        pages = set(self.pdf_pages)
        occupied: set[tuple[int, int]] = set()
        anchored: dict[int, set[str]] = {row.row_index: set() for row in self.rows}
        for cell in self.cells:
            if cell.column + cell.column_span > self.column_count or cell.row + cell.row_span > self.total_rows:
                raise ValueError("table cell exceeds its original grid")
            if cell.row > self.row_end or cell.row + cell.row_span <= self.row_start:
                raise ValueError("table page cannot include a cell outside its selected window")
            if cell.pdf_page is not None and cell.pdf_page not in pages:
                raise ValueError("cell physical PDF page must belong to the table")
            if any(span.pdf_page not in pages for span in cell.source_spans):
                raise ValueError("cell source span page must belong to the table")
            if cell.row in anchored:
                anchored[cell.row].add(cell.id)
            for row_index in range(max(cell.row, self.row_start), min(cell.row + cell.row_span, self.row_end + 1)):
                for column in range(cell.column, cell.column + cell.column_span):
                    position = (row_index, column)
                    if position in occupied:
                        raise ValueError("table cells overlap inside the selected window")
                    occupied.add(position)
        if len(occupied) != len(self.rows) * self.column_count:
            raise ValueError("table cells must preserve the complete selected grid")
        if any(set(row.cell_ids) != anchored[row.row_index] for row in self.rows):
            raise ValueError("row cell IDs must identify exactly the cells anchored in that row")
        for segment in self.page_segments:
            if segment.pdf_page not in pages or segment.last_row >= self.total_rows:
                raise ValueError("table page segment exceeds its original grid or PDF pages")
            if segment.first_row > self.row_end or segment.last_row < self.row_start:
                raise ValueError("table page segments must intersect the selected row window")
        if len(self.model_dump_json().encode("utf-8")) > 8 * 1024 * 1024:
            raise ValueError("selected table projection exceeds 8 MiB; open the original PDF")
        return self
