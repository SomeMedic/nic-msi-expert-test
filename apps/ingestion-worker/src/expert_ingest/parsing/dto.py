"""Closed, immutable, vendor-free parse artifacts with Unicode source offsets."""
from __future__ import annotations

import math
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from expert_contracts.common import SHA256
from expert_contracts.sources import SourceSpan


def _bbox(value: tuple[float, float, float, float]):
    if (not all(math.isfinite(v) for v in value) or min(value) < 0
            or value[2] <= value[0] or value[3] <= value[1]):
        raise ValueError("invalid PDF region")
    return value


BBox = Annotated[tuple[float, float, float, float], AfterValidator(_bbox)]
Index = Annotated[int, Field(ge=0, le=10_000_000, strict=True)]
PageNumber = Annotated[int, Field(ge=1, le=500, strict=True)]
Identifier = Annotated[str, Field(min_length=1, max_length=200)]
Text = Annotated[str, Field(max_length=2_000_000)]
NodeType = Literal["document", "section", "chapter", "article", "clause", "subclause",
                   "paragraph", "appendix", "table", "figure", "footnote", "editorial_note", "unknown"]
Decision = Literal["accept_text", "need_layout_reparse", "need_ocr", "intentional_blank", "rejected"]


class FrozenDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False, validate_default=True)


class ParserLimits(FrozenDTO):
    max_bytes: int = Field(default=50 * 1024 * 1024, ge=1, le=50 * 1024 * 1024, strict=True)
    max_pages: int = Field(default=500, ge=1, le=500, strict=True)
    max_characters: int = Field(default=10_000_000, ge=1, le=10_000_000, strict=True)
    max_blocks: int = Field(default=100_000, ge=1, le=100_000, strict=True)
    max_artifact_bytes: int = Field(default=256 * 1024 * 1024, ge=1, le=256 * 1024 * 1024, strict=True)
    wall_seconds: int = Field(default=480, ge=1, le=480, strict=True)
    render_dpi: int = Field(default=216, ge=72, le=300, strict=True)
    symbol_render_dpi: int = Field(default=576, ge=72, le=600, strict=True)
    max_page_pixels: int = Field(default=40_000_000, ge=1, le=40_000_000, strict=True)


class RegionReview(FrozenDTO):
    schema_version: Literal["p04.region-review.v1"] = "p04.region-review.v1"
    source_sha256: SHA256
    pdf_page: PageNumber
    bbox: BBox
    crop_sha256: SHA256
    render_dpi: int = Field(ge=72, le=300, strict=True)
    disposition: Literal["non_normative_graphic"] = "non_normative_graphic"
    reviewer: Literal["codex-visual-review"] = "codex-visual-review"
    method: Literal["visual_inspection"] = "visual_inspection"


class RegionReviewRegistry(FrozenDTO):
    schema_version: Literal["p04.region-reviews.v1"] = "p04.region-reviews.v1"
    reviews: tuple[RegionReview, ...] = Field(default=(), max_length=1000)

    @model_validator(mode="after")
    def unique_regions(self):
        identities = {(r.source_sha256, r.pdf_page, r.bbox) for r in self.reviews}
        if len(identities) != len(self.reviews):
            raise ValueError("duplicate reviewed region")
        return self


class RegionReviewEvidence(FrozenDTO):
    review: RegionReview
    region_id: Identifier
    crop_base64: str = Field(max_length=5_400_000, repr=False)
    crop_width: int = Field(ge=1, le=4096, strict=True)
    crop_height: int = Field(ge=1, le=4096, strict=True)


class ParseRequest(FrozenDTO):
    version_id: UUID
    parse_generation_id: UUID
    source_sha256: SHA256
    title: str = Field(min_length=1, max_length=500)
    limits: ParserLimits = Field(default_factory=ParserLimits)
    region_reviews: tuple[RegionReview, ...] = Field(default=(), max_length=1000)


class Diagnostic(FrozenDTO):
    code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,79}$")
    severity: Literal["critical", "warning"]
    pdf_page: PageNumber | None = None
    block_id: Identifier | None = None
    bbox: BBox | None = None


class RawChar(FrozenDTO):
    offset: Index
    text: str = Field(min_length=1, max_length=8, repr=False)
    bbox: BBox
    font: str = Field(default="", max_length=200)
    size: float = Field(default=0, ge=0, le=10000)
    flags: Index = 0
    origin: tuple[float, float] = (0, 0)


class RawBlock(FrozenDTO):
    block_id: Identifier
    pdf_page: PageNumber
    text: Text = Field(repr=False)
    bbox: BBox
    chars: tuple[RawChar, ...] = Field(default=(), max_length=100_000, repr=False)
    ordinal: Index
    original_ordinal: Index = 0
    group_id: Identifier | None = None
    backend: Literal["pymupdf", "docling", "easyocr", "tesseract", "symbol"] = "pymupdf"
    kind: Literal["text", "ocr", "symbol"] = "text"

    @model_validator(mode="after")
    def char_bounds(self):
        previous = -1
        for char in self.chars:
            if char.offset <= previous or self.text[char.offset:char.offset + len(char.text)] != char.text:
                raise ValueError("raw character offsets do not resolve")
            previous = char.offset
        return self


class ImageRegion(FrozenDTO):
    region_id: Identifier
    pdf_page: PageNumber
    bbox: BBox
    sha256: SHA256 | None = None
    pixel_width: int = Field(default=0, ge=0, le=1_000_000, strict=True)
    pixel_height: int = Field(default=0, ge=0, le=1_000_000, strict=True)


class Page(FrozenDTO):
    pdf_page: PageNumber
    width: float = Field(gt=0, le=100000)
    height: float = Field(gt=0, le=100000)
    rotation: Literal[0, 90, 180, 270] = 0
    cropbox: BBox
    printed_page_label: str | None = Field(default=None, max_length=100)
    block_ids: tuple[Identifier, ...] = Field(default=(), max_length=100_000)
    images: tuple[ImageRegion, ...] = Field(default=(), max_length=10_000)
    vector_regions: tuple[BBox, ...] = Field(default=(), max_length=100_000)
    decision: Decision = "accept_text"


class CharMapSegment(FrozenDTO):
    canonical_start: Index
    canonical_end: Index
    source_spans: tuple[SourceSpan, ...] = Field(default=(), max_length=1000)
    mapping: Literal["exact", "normalized_group"]
    operation: Literal["nfc", "whitespace", "line_join", "format_separator", "dehyphenate", "typography"] | None = None

    @model_validator(mode="after")
    def mapping_bounds(self):
        if self.canonical_end <= self.canonical_start:
            raise ValueError("canonical mapping must have positive length")
        if any(s.end_offset <= s.start_offset for s in self.source_spans):
            raise ValueError("source mapping must have positive length")
        if self.mapping == "exact":
            if len(self.source_spans) != 1 or self.operation is not None:
                raise ValueError("exact mapping requires one unchanged source interval")
            span = self.source_spans[0]
            if span.end_offset - span.start_offset != self.canonical_end - self.canonical_start:
                raise ValueError("exact mapping length differs")
        elif not self.source_spans and self.operation != "format_separator":
            raise ValueError("only formatting separators can lack source intervals")
        return self


class ContextRef(FrozenDTO):
    role: Literal["scope", "header", "unit", "note", "caption"]
    text: Text = Field(min_length=1, repr=False)
    source_spans: tuple[SourceSpan, ...] = Field(min_length=1, max_length=1000)
    required: bool = True


class Cell(FrozenDTO):
    id: Identifier
    row: Index
    column: Index
    row_span: int = Field(default=1, ge=1, le=10000, strict=True)
    column_span: int = Field(default=1, ge=1, le=1000, strict=True)
    role: Literal["header", "unit", "data", "note"]
    text: Text = Field(repr=False)
    char_map: tuple[CharMapSegment, ...] = Field(default=(), max_length=100_000)
    bbox: BBox | None = None
    pdf_page: PageNumber | None = None


class Row(FrozenDTO):
    row_index: Index
    cell_ids: tuple[Identifier, ...] = Field(min_length=1, max_length=1000)
    context_refs: tuple[ContextRef, ...] = Field(default=(), max_length=1000)


class TablePageSegment(FrozenDTO):
    pdf_page: PageNumber
    bbox: BBox
    first_row: Index
    last_row: Index


class TableRowGroup(FrozenDTO):
    first_row: Index
    last_row: Index
    context_refs: tuple[ContextRef, ...] = Field(default=(), max_length=1000)

    @model_validator(mode="after")
    def ordered_rows(self):
        if self.last_row < self.first_row:
            raise ValueError("row group range must be ordered")
        return self


class Table(FrozenDTO):
    id: Identifier
    kind: Literal["data", "blank_template"] = "data"
    cells: tuple[Cell, ...] = Field(min_length=1, max_length=100_000)
    rows: tuple[Row, ...] = Field(min_length=1, max_length=100_000)
    column_count: int = Field(ge=1, le=1000, strict=True)
    source_block_ids: tuple[Identifier, ...] = Field(default=(), max_length=100_000)
    owned_source_spans: tuple[SourceSpan, ...] = Field(default=(), max_length=100_000)
    pdf_pages: tuple[PageNumber, ...] = Field(min_length=1, max_length=500)
    page_segments: tuple[TablePageSegment, ...] = Field(default=(), max_length=500)
    context_refs: tuple[ContextRef, ...] = Field(default=(), max_length=1000)
    continuation_evidence: tuple[str, ...] = Field(default=(), max_length=500)
    row_groups: tuple[TableRowGroup, ...] = Field(default=(), max_length=10_000)


class ParserRuntimeProfile(FrozenDTO):
    enforced: bool = Field(strict=True)
    platform: str = Field(min_length=1, max_length=32)
    no_new_privs: bool = Field(strict=True)
    seccomp_network_denied: bool = Field(strict=True)
    process_creation_denied: bool = Field(strict=True)
    landlock_abi: int = Field(ge=0, le=100, strict=True)
    memory_bytes: int = Field(ge=1, le=16 * 1024**3, strict=True)
    cpu_seconds: int = Field(ge=1, le=7200, strict=True)
    file_bytes: int = Field(ge=1, le=268_435_456, strict=True)
    open_files: int = Field(ge=1, le=1024, strict=True)
    processes: int = Field(ge=1, le=256, strict=True)
    cpu_count: int = Field(ge=1, le=2, strict=True)
    memory_max_bytes: int | None = Field(default=None, ge=1, strict=True)
    swap_max_bytes: int | None = Field(default=None, ge=0, strict=True)
    requested_profile_sha256: SHA256 | None = None


class ParserManifest(FrozenDTO):
    schema_version: Literal["p04.parse.v1"] = "p04.parse.v1"
    parser_version: str = Field(default="pymupdf-primary-v1", max_length=200)
    pymupdf_version: str = Field(default="unknown", max_length=100)
    normalizer_version: str = Field(default="mapped-nfc-v1", max_length=100)
    structure_version: str = Field(default="generic-tree-v1", max_length=100)
    coordinate_system: Literal["unrotated_cropbox_top_left_pt"] = "unrotated_cropbox_top_left_pt"
    limits: ParserLimits = Field(default_factory=ParserLimits)
    table_adapter_version: str | None = Field(default=None, max_length=100)
    docling_version: str | None = Field(default=None, max_length=100)
    fallback_pages: tuple[PageNumber, ...] = Field(default=(), max_length=500)
    asset_fingerprints: tuple[AssetFingerprint, ...] = Field(default=(), max_length=100)
    symbol_adapter_version: str | None = Field(default=None, max_length=100)
    parser_fingerprint: SHA256 | None = None
    runtime_profile: ParserRuntimeProfile | None = None


class AssetFingerprint(FrozenDTO):
    name: str = Field(min_length=1, max_length=200, pattern=r"^[a-zA-Z0-9_.-]+$")
    sha256: SHA256


class ParsedDocument(FrozenDTO):
    version_id: UUID
    parse_generation_id: UUID
    source_sha256: SHA256
    title: str = Field(min_length=1, max_length=500)
    pages: tuple[Page, ...] = Field(min_length=1, max_length=500)
    blocks: tuple[RawBlock, ...] = Field(default=(), max_length=100_000, repr=False)
    tables: tuple[Table, ...] = Field(default=(), max_length=10_000)
    diagnostics: tuple[Diagnostic, ...] = Field(default=(), max_length=100_000)
    manifest: ParserManifest = Field(default_factory=ParserManifest)
    symbol_proofs: tuple[SymbolProof, ...] = Field(default=(), max_length=10_000)
    region_reviews: tuple[RegionReviewEvidence, ...] = Field(default=(), max_length=1000)


class SymbolProof(FrozenDTO):
    region_id: Identifier
    block_id: Identifier
    pdf_page: PageNumber
    bbox: BBox
    classification: Literal["+", "−", "±", "³"]
    source_sha256: SHA256
    crop_sha256: SHA256 | None = None
    crop_base64: str | None = Field(default=None, max_length=2_800_000, repr=False)
    crop_encoding: Literal["luma8"] | None = None
    crop_width: int | None = Field(default=None, ge=1, le=2048, strict=True)
    crop_height: int | None = Field(default=None, ge=1, le=2048, strict=True)
    render_dpi: int | None = Field(default=None, ge=72, le=600, strict=True)
    adapter_version: Literal["qualified-symbol-v1"] = "qualified-symbol-v1"
    method: Literal["raster_topology", "typography_text_layer"]
    # Canonical JSON keeps nested qualification features immutable and replayable.
    features: str = Field(max_length=20000, repr=False)
    reason_codes: tuple[str, ...] = Field(max_length=100)
    supported_profile: str = Field(max_length=200)
    may_insert_as_pdf_text_layer: Literal[False] = False
    source_offset: Index | None = None
    anchor_spans: tuple[SourceSpan, ...] = Field(default=(), max_length=1000)


class CanonicalNode(FrozenDTO):
    id: UUID
    parent_id: UUID | None = None
    node_type: NodeType
    ordinal: Index
    number: str | None = Field(default=None, max_length=200)
    title: str | None = Field(default=None, max_length=2000)
    own_body: Text = Field(default="", repr=False)
    char_map: tuple[CharMapSegment, ...] = Field(default=(), max_length=100_000)
    context_refs: tuple[ContextRef, ...] = Field(default=(), max_length=1000)
    table: Table | None = None
    level: int = Field(default=0, ge=0, le=100, strict=True)
    structural_path: tuple[str, ...] = Field(default=(), max_length=100)
    page_start: PageNumber = 1
    page_end: PageNumber = 1
    source_spans: tuple[SourceSpan, ...] = Field(default=(), max_length=100_000)
    content_hash: SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


class CanonicalDocument(ParsedDocument):
    nodes: tuple[CanonicalNode, ...] = Field(default=(), max_length=100_000)
    exclusions: tuple[SourceExclusion, ...] = Field(default=(), max_length=100_000)


class SourceExclusion(FrozenDTO):
    reason: Literal["repeated_header", "repeated_footer"]
    source_spans: tuple[SourceSpan, ...] = Field(min_length=1, max_length=1000)
    evidence_pages: tuple[PageNumber, ...] = Field(min_length=3, max_length=500)


class QualityReport(FrozenDTO):
    schema_version: Literal["p04.quality.v1"] = "p04.quality.v1"
    status: Literal["passed", "failed"]
    complete_page_count: int = Field(ge=0, le=500, strict=True)
    critical_count: Index
    warning_count: Index
    diagnostics: tuple[Diagnostic, ...] = Field(default=(), max_length=100_000)

    @model_validator(mode="after")
    def counts_match(self):
        critical = sum(item.severity == "critical" for item in self.diagnostics)
        warning = len(self.diagnostics) - critical
        if critical != self.critical_count or warning != self.warning_count:
            raise ValueError("quality counts differ from diagnostics")
        if (self.status == "passed") != (critical == 0):
            raise ValueError("quality status differs from critical diagnostics")
        return self


class ParsedArtifact(FrozenDTO):
    schema_version: Literal["p04.artifact.v1"] = "p04.artifact.v1"
    document: CanonicalDocument
    quality_report: QualityReport


ParserManifest.model_rebuild()
ParsedDocument.model_rebuild()
CanonicalDocument.model_rebuild()
