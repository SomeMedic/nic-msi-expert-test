"""Pinned CPU-only Docling fallback with verified local assets and source checks.

This adapter disables vendor networking/downloads. OS isolation, wall-clock
termination and memory limits remain the parser runner's responsibility.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
from typing import Any

from .dto import AssetFingerprint, Diagnostic, ParsedDocument, Table
from .tables import CellGeometry, TABLE_ADAPTER_VERSION, _inside, finish_tables, map_table

DOCLING_VERSION = "2.66.0"


def _local_file(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("PARSER_ASSET_PATH_INVALID")
    target = (root / path).resolve(strict=True)
    if not target.is_relative_to(root) or not target.is_file():
        raise ValueError("PARSER_ASSET_PATH_INVALID")
    return target


def verify_assets(artifacts_path: Path, asset_lock_path: Path, *, use_ocr: bool) -> tuple[AssetFingerprint, ...]:
    """Verify every declared model file before importing/initializing the converter."""
    root = artifacts_path.resolve(strict=True)
    lock = json.loads(asset_lock_path.read_text(encoding="utf-8"))
    if lock.get("docling_version") != DOCLING_VERSION or not root.is_dir():
        raise ValueError("PARSER_ASSET_VERSION_MISMATCH")
    fingerprints = []
    models = lock.get("hf_models", [])
    if {item.get("model_id") for item in models} != {
        "docling-project/docling-layout-heron", "docling-project/docling-models",
    }:
        raise ValueError("PARSER_ASSET_MANIFEST_INVALID")
    for model in models:
        directory = Path(model["local_path"]).name
        for entry in model["files"]:
            source = _local_file(root, str(Path(directory) / entry["path"]))
            data = source.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            git_digest = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            if (len(data) != entry["size_bytes"] or
                    (entry.get("sha256") and digest != entry["sha256"]) or
                    (entry.get("git_blob_sha1") and git_digest != entry["git_blob_sha1"]) or
                    not (entry.get("sha256") or entry.get("git_blob_sha1"))):
                raise ValueError("PARSER_ASSET_HASH_MISMATCH")
            fingerprints.append(AssetFingerprint(name="asset-" + hashlib.sha256(
                str(source.relative_to(root)).encode()).hexdigest()[:24], sha256=digest))
    if use_ocr:
        entries = lock.get("ocr_archives", [])
        if {item["member"]["path"] for item in entries} != {"craft_mlt_25k.pth", "cyrillic_g2.pth"}:
            raise ValueError("PARSER_OCR_ASSETS_MISSING")
        for entry in entries:
            member = entry["member"]
            source = _local_file(root, str(Path("EasyOcr") / member["path"]))
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            if source.stat().st_size != member["size_bytes"] or digest != member["sha256"]:
                raise ValueError("PARSER_ASSET_HASH_MISMATCH")
            fingerprints.append(AssetFingerprint(name=member["path"], sha256=digest))
    return tuple(fingerprints)


def _ranges(pages: Sequence[int]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for page in sorted(set(pages)):
        if ranges and page == ranges[-1][1] + 1:
            ranges[-1] = (ranges[-1][0], page)
        else:
            ranges.append((page, page))
    return ranges


def _converter(artifacts_path: Path, *, use_ocr: bool, timeout_seconds: int, num_threads: int) -> Any:
    # Imports are local: merely importing parsing DTOs never loads torch/Docling.
    from docling.datamodel.accelerator_options import AcceleratorOptions
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import EasyOcrOptions, PdfPipelineOptions, TableStructureOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    options = PdfPipelineOptions(
        artifacts_path=artifacts_path.resolve(),
        accelerator_options=AcceleratorOptions(device="cpu", num_threads=num_threads),
        enable_remote_services=False, allow_external_plugins=False,
        do_ocr=use_ocr, do_table_structure=True,
        table_structure_options=TableStructureOptions(do_cell_matching=True),
        do_picture_classification=False, do_picture_description=False,
        do_code_enrichment=False, do_formula_enrichment=False,
        document_timeout=timeout_seconds,
        ocr_options=EasyOcrOptions(lang=["ru", "en"], download_enabled=False,
                                  force_full_page_ocr=use_ocr,
                                  model_storage_directory=str((artifacts_path / "EasyOcr").resolve())),
    )
    return DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)},
    )


def _parent_heading(document: ParsedDocument, page_number: int, bbox: tuple[float, float, float, float],
                    geometry: list[CellGeometry]) -> list[CellGeometry]:
    """Recover one omitted parent heading only from an unambiguous raw line.

    The line must precede a contiguous multi-column child header band inside
    the table region. Numeric or partially assigned lines remain unresolved.
    """
    headers = sorted((c for c in geometry if c.row == 0 and c.header and c.bbox), key=lambda c: c.column)
    if len(headers) < 2 or any(a.column + a.column_span != b.column for a, b in zip(headers, headers[1:])):
        return geometry
    boxes = [c.bbox for c in headers if c.bbox is not None]
    top = min(box[1] for box in boxes)
    left, right = min(box[0] for box in boxes), max(box[2] for box in boxes)
    candidates = []
    for block in document.blocks:
        if block.pdf_page != page_number or not block.text.strip() or re.search(r"\d", block.text):
            continue
        if not all(_inside(bbox, char) for char in block.chars):
            continue
        if any(_inside(cell.bbox, char) for cell in geometry if cell.bbox for char in block.chars):
            continue
        center = (block.bbox[0] + block.bbox[2]) / 2
        if (block.bbox[3] <= top and top - block.bbox[3] <= 3 * (block.bbox[3] - block.bbox[1])
                and left <= block.bbox[0] < block.bbox[2] <= right
                and (boxes[0][0] + boxes[0][2]) / 2 < center < (boxes[-1][0] + boxes[-1][2]) / 2):
            candidates.append(block)
    if len(candidates) != 1:
        return geometry
    block = candidates[0]
    parent = CellGeometry(0, headers[0].column, 1,
                          headers[-1].column + headers[-1].column_span - headers[0].column,
                          block.bbox, expected_text=block.text, header=True)
    return [parent, *(replace(c, row=c.row + 1) for c in geometry)]


def _coalesce_wrapped_rows(geometry: list[CellGeometry]) -> list[CellGeometry]:
    """Reunite complementary rows only when values lie inside a tall label's Y range."""
    result = geometry
    row = 0
    while row < max((c.row for c in result), default=0):
        upper = [c for c in result if c.row == row and (c.expected_text or "").strip()]
        lower = [c for c in result if c.row == row + 1 and (c.expected_text or "").strip()]
        upper_columns = {col for c in upper for col in range(c.column, c.column + c.column_span)}
        lower_columns = {col for c in lower for col in range(c.column, c.column + c.column_span)}
        labels = [c for c in upper if c.bbox and re.search(r"[^\W\d_]", c.expected_text or "")]
        if (upper and lower and not (upper_columns & lower_columns)
                and all(not c.header and c.row_span == 1 for c in [*upper, *lower])
                and all(c.bbox and re.fullmatch(r"[\s\d.,+±−%-]+", c.expected_text or "") for c in lower)
                and any(all(c.bbox and label.bbox and label.bbox[1] < (c.bbox[1] + c.bbox[3]) / 2 < label.bbox[3]
                            for c in lower) for label in labels)):
            result = [replace(c, row=c.row - 1 if c.row > row else c.row) for c in result
                      if c.row not in {row, row + 1} or (c.expected_text or "").strip()]
        else:
            row += 1
    return result


def _overlaps(left: Table, right: Table) -> bool:
    return any(a.pdf_page == b.pdf_page and a.bbox[0] < b.bbox[2] and b.bbox[0] < a.bbox[2]
               and a.bbox[1] < b.bbox[3] and b.bbox[1] < a.bbox[3]
               for a in left.page_segments for b in right.page_segments)


def _source_points(table: Table) -> set[tuple[str, int]]:
    return {(span.block_id, i) for span in table.owned_source_spans for i in range(span.start_offset, span.end_offset)}


def map_docling_document(document: ParsedDocument, converted: Any, pages: Sequence[int],
                         *, use_ocr: bool = False) -> ParsedDocument:
    """Reconcile layout cells with the existing raw registry, never vendor prose."""
    requested = set(pages)
    candidates: list[Table] = []
    diagnostics: list[Diagnostic] = []
    mapped_pages: set[int] = set()
    for vendor in converted.tables:
        provenance = vendor.prov
        if not provenance or len({p.page_no for p in provenance}) != 1:
            for requested_page in requested:
                diagnostics.append(Diagnostic(code="DOCLING_MULTI_PAGE_CELL_IDENTITY_UNRESOLVED",
                                              severity="critical", pdf_page=requested_page))
            continue
        page_number = provenance[0].page_no
        if page_number not in requested:
            continue
        page = next((p for p in document.pages if p.pdf_page == page_number), None)
        converted_page = converted.pages.get(page_number)
        if page is None or converted_page is None:
            diagnostics.append(Diagnostic(code="DOCLING_PAGE_IDENTITY_MISMATCH", severity="critical", pdf_page=page_number))
            continue
        if (abs(converted_page.size.width - page.width) > 1 or abs(converted_page.size.height - page.height) > 1):
            diagnostics.append(Diagnostic(code="DOCLING_PAGE_GEOMETRY_MISMATCH", severity="critical", pdf_page=page_number))
            continue
        box = provenance[0].bbox.to_top_left_origin(page.height)
        geometry = []
        for cell in vendor.data.table_cells:
            rect = cell.bbox.to_top_left_origin(page.height) if cell.bbox is not None else None
            geometry.append(CellGeometry(
                row=cell.start_row_offset_idx, column=cell.start_col_offset_idx,
                row_span=cell.row_span, column_span=cell.col_span,
                bbox=None if rect is None else (rect.l, rect.t, rect.r, rect.b),
                expected_text=cell.text, header=cell.column_header and not re.fullmatch(r"[\d\s.,+±−%-]+", cell.text),
                row_group=cell.row_section,
            ))
        geometry = _coalesce_wrapped_rows(geometry)
        geometry = _parent_heading(document, page_number, (box.l, box.t, box.r, box.b), geometry)
        # Missing grid slots are explicit empty cells, never zeros. Any actual
        # visible characters left outside mapped cells still fail ownership.
        occupied = {(row, col) for cell in geometry
                    for row in range(cell.row, cell.row + cell.row_span)
                    for col in range(cell.column, cell.column + cell.column_span)}
        for row in range(max((c.row + c.row_span for c in geometry), default=vendor.data.num_rows)):
            for col in range(vendor.data.num_cols):
                if (row, col) not in occupied:
                    geometry.append(CellGeometry(row, col, 1, 1, None, expected_text="",
                                                  header=any(c.row == row and c.header for c in geometry)))
        table, problems = map_table(document, page_number=page_number, bbox=(box.l, box.t, box.r, box.b),
                                    geometry=geometry, column_count=vendor.data.num_cols, backend="docling")
        primary = [t for t in document.tables if table is not None and _overlaps(t, table)]
        valid_primary = bool(primary) and all(any(c.role == "header" and c.text.strip() for c in t.cells)
            and not any(d.severity == "critical" and "TABLE" in d.code and d.pdf_page in t.pdf_pages
                        for d in document.diagnostics) for t in primary)
        if table is not None and valid_primary:
            # Source-proven primary grids retain their merged headers and page
            # identity. Additional fallback characters cannot silently disappear.
            owned = set().union(*(_source_points(t) for t in primary))
            registry = {b.block_id: b for b in document.blocks}
            extra = {point for point in _source_points(table) - owned if registry[point[0]].text[point[1]].strip()}
            if extra:
                diagnostics.append(Diagnostic(code="TABLE_CANDIDATE_OWNERSHIP_DISAGREEMENT", severity="critical",
                                              pdf_page=page_number))
            continue
        if any(set(t.pdf_pages) - requested for t in primary):
            diagnostics.append(Diagnostic(code="DOCLING_PARTIAL_TABLE_RANGE", severity="critical", pdf_page=page_number))
            continue
        diagnostics.extend(problems)
        if use_ocr:
            diagnostics.append(Diagnostic(code="UNVERIFIED_OCR_SYMBOLS", severity="critical", pdf_page=page_number))
        if table is not None:
            candidates.append(table)
            mapped_pages.add(page_number)
    # Keep primary candidates if the fallback produced no table on that page.
    kept = [table for table in document.tables if not any(_overlaps(table, candidate) for candidate in candidates)]
    table_codes = {"TABLE_HEADER_UNRESOLVED", "TABLE_DETECTOR_FAILED", "TABLE_CELL_TEXT_MISMATCH",
                   "AMBIGUOUS_TABLE_GRID", "AMBIGUOUS_TABLE_CHAR_OWNERSHIP", "INCOMPLETE_TABLE_GRID"}
    base = document.model_copy(update={"diagnostics": tuple(d for d in document.diagnostics
        if not (d.pdf_page in mapped_pages and d.code in table_codes))})
    return finish_tables(base, [*kept, *candidates], diagnostics)


def enrich_docling(document: ParsedDocument, path: Path, pages: tuple[int, ...], *,
                   artifacts_path: Path, asset_lock_path: Path, use_ocr: bool = False,
                   timeout_seconds: int = 300, num_threads: int = 2) -> ParsedDocument:
    """Run requested original page ranges locally; caller includes continuation context."""
    if not pages:
        return document
    if (any(p < 1 or p > len(document.pages) for p in pages)
            or not 1 <= timeout_seconds <= document.manifest.limits.wall_seconds or not 1 <= num_threads <= 8):
        raise ValueError("PARSER_FALLBACK_LIMIT_INVALID")
    if version("docling") != DOCLING_VERSION:
        raise ValueError("PARSER_BACKEND_VERSION_MISMATCH")
    fingerprints = verify_assets(artifacts_path, asset_lock_path, use_ocr=use_ocr)
    os.environ.update({"HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1", "HF_DATASETS_OFFLINE": "1",
                       "HF_HUB_DISABLE_TELEMETRY": "1", "DO_NOT_TRACK": "1", "ANONYMIZED_TELEMETRY": "False"})
    converter = _converter(artifacts_path, use_ocr=use_ocr, timeout_seconds=timeout_seconds, num_threads=num_threads)
    result = document
    for start, end in _ranges(pages):
        converted = converter.convert(path.resolve(strict=True), page_range=(start, end),
                                      max_num_pages=document.manifest.limits.max_pages,
                                      max_file_size=document.manifest.limits.max_bytes)
        if str(converted.status.value) != "success":
            result = result.model_copy(update={"diagnostics": result.diagnostics + (
                Diagnostic(code="DOCLING_CONVERSION_INCOMPLETE", severity="critical", pdf_page=start),)})
        result = map_docling_document(result, converted.document, tuple(range(start, end + 1)), use_ocr=use_ocr)
    return result.model_copy(update={"manifest": result.manifest.model_copy(update={
        "table_adapter_version": TABLE_ADAPTER_VERSION, "docling_version": DOCLING_VERSION,
        "fallback_pages": tuple(sorted(set(result.manifest.fallback_pages) | set(pages))),
        "asset_fingerprints": fingerprints,
    })})
