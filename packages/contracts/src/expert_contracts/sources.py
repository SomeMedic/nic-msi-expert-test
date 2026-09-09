"""Public canonical source descriptors; never accepts a caller object key."""
from typing import Literal
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from .common import ApiPath, Count, NonBlank, PositiveInt, SHA256, ShortID, StrictDTO, unique


class SourceSpan(StrictDTO):
    model_config = ConfigDict(frozen=True)
    pdf_page: PositiveInt
    printed_page_label: str | None = Field(default=None, max_length=100)
    block_id: NonBlank = Field(max_length=200)
    start_offset: Count
    end_offset: Count
    bbox: tuple[float, float, float, float] | None = None

    @model_validator(mode="after")
    def valid_interval(self):
        if self.end_offset < self.start_offset:
            raise ValueError("source offsets use half-open [start,end) intervals")
        if self.bbox is not None:
            x0, y0, x1, y1 = self.bbox
            if x0 < 0 or y0 < 0 or x1 <= x0 or y1 <= y0:
                raise ValueError("bbox must have positive area and ordered nonnegative coordinates")
        return self

    def validate_source(self, *, block_text: str, pdf_page_count: int, page_size: tuple[float, float] | None = None) -> None:
        """Check source-dependent bounds after resolving block_id in the pinned tree."""
        if self.pdf_page > pdf_page_count or self.end_offset > len(block_text):
            raise ValueError("source span exceeds resolved page/block bounds")
        if page_size is not None and self.bbox is not None:
            if self.bbox[2] > page_size[0] or self.bbox[3] > page_size[1]:
                raise ValueError("bbox exceeds resolved PDF page bounds")


class SourceDescriptor(StrictDTO):
    version_id: UUID
    source_url: ApiPath
    original_filename: NonBlank = Field(max_length=500)
    media_type: Literal["application/pdf"] = "application/pdf"
    size_bytes: PositiveInt
    sha256: SHA256
    page_count: PositiveInt | None = None


class CitationDTO(StrictDTO):
    citation_id: ShortID
    evidence_id: ShortID
    document_title: NonBlank = Field(max_length=500)
    version_label: str | None = Field(default=None, max_length=200)
    structural_path: list[NonBlank] = Field(max_length=32)
    pdf_pages: list[PositiveInt] = Field(min_length=1, max_length=500)
    printed_page_labels: list[str] = Field(max_length=500)
    source_url: ApiPath

    @model_validator(mode="after")
    def pages_unique(self):
        unique(self.pdf_pages, "PDF pages")
        if self.pdf_pages != sorted(self.pdf_pages):
            raise ValueError("PDF pages must be ordered")
        return self


class PublicEvidence(StrictDTO):
    evidence_id: ShortID
    run_id: UUID
    document_version_id: UUID
    document_title: NonBlank = Field(max_length=500)
    version_label: str | None = Field(default=None, max_length=200)
    structural_path: list[NonBlank] = Field(max_length=32)
    excerpt: NonBlank = Field(max_length=30000)
    source_spans: list[SourceSpan] = Field(min_length=1, max_length=500)
    source_url: ApiPath
