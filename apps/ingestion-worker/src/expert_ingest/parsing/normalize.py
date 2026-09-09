"""Conservative NFC and whitespace normalization with reversible raw anchors."""
from __future__ import annotations

import hashlib
import json
import math
import re
import unicodedata
from typing import Literal
from uuid import uuid5

from expert_contracts.sources import SourceSpan

from .dto import CanonicalDocument, CanonicalNode, CharMapSegment, ParsedDocument, RawBlock, SourceExclusion, Table


def content_hash(text: str, table: Table | None = None) -> str:
    payload = text if table is None else json.dumps(table.model_dump(mode="json", exclude_none=False),
                                                  ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                                                  allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def source_span(block: RawBlock, start: int, end: int) -> SourceSpan:
    if not 0 <= start < end <= len(block.text):
        raise ValueError("invalid raw slice")
    chars = [c for c in block.chars if c.offset < end and c.offset + len(c.text) > start]
    bbox = ((min(c.bbox[0] for c in chars), min(c.bbox[1] for c in chars),
             max(c.bbox[2] for c in chars), max(c.bbox[3] for c in chars)) if chars else block.bbox)
    return SourceSpan(pdf_page=block.pdf_page, block_id=block.block_id,
                      start_offset=start, end_offset=end, bbox=bbox)


def classify_boilerplate(document: ParsedDocument) -> tuple[SourceExclusion, ...]:
    """Require repeated content AND position; body text and marked notes never qualify."""
    if len(document.pages) < 3:
        return ()
    pages = {page.pdf_page: page for page in document.pages}
    table_blocks = {span.block_id for table in document.tables for span in table.owned_source_spans}
    candidates: dict[tuple[Literal["repeated_header", "repeated_footer"], str, int, int], list[RawBlock]] = {}
    for block in document.blocks:
        page = pages.get(block.pdf_page)
        text = block.text.strip()
        if page is None or block.kind != "text" or block.block_id in table_blocks or not text or len(text) > 500:
            continue
        if re.match(r"^(?:\d+[).]\s|[*†‡]|Примечани[ея]\b|Сноска\b)", text, re.IGNORECASE):
            continue
        reason: Literal["repeated_header", "repeated_footer"]
        if block.bbox[3] <= page.height * 0.09:
            reason = "repeated_header"
        elif block.bbox[1] >= page.height * 0.91:
            reason = "repeated_footer"
        else:
            continue
        signature = re.sub(r"\d+", "#", " ".join(unicodedata.normalize("NFC", text).split()))
        key = (reason, signature, round(block.bbox[0] / page.width * 100), round(block.bbox[1] / page.height * 100))
        candidates.setdefault(key, []).append(block)
    required = max(3, math.ceil(len(document.pages) * 0.6))
    exclusions: list[SourceExclusion] = []
    for (reason, _, _, _), blocks in candidates.items():
        evidence = tuple(sorted({b.pdf_page for b in blocks}))
        if len(evidence) < required:
            continue
        exclusions.extend(SourceExclusion(reason=reason, source_spans=(source_span(block, 0, len(block.text)),),
                                          evidence_pages=evidence) for block in blocks)
    return tuple(sorted(exclusions, key=lambda item: (item.source_spans[0].pdf_page, item.source_spans[0].block_id)))


def normalize_slice(block: RawBlock, start: int = 0, end: int | None = None,
                    typography_offsets: frozenset[int] = frozenset()) -> tuple[str, tuple[CharMapSegment, ...]]:
    """No spelling, number, sign or discretionary-hyphen repair is performed."""
    end = len(block.text) if end is None else end
    if not 0 <= start <= end <= len(block.text):
        raise ValueError("invalid normalization slice")
    while start < end and block.text[start].isspace():
        start += 1
    while end > start and block.text[end - 1].isspace():
        end -= 1
    output: list[str] = []
    segments: list[CharMapSegment] = []
    cursor = start
    target = 0
    while cursor < end:
        stop = cursor + 1
        operation: Literal["nfc", "whitespace", "typography"] | None = None
        if block.text[cursor].isspace():
            while stop < end and block.text[stop].isspace():
                stop += 1
            normalized = " "
            operation = "whitespace" if block.text[cursor:stop] != normalized else None
        else:
            while stop < end and unicodedata.combining(block.text[stop]):
                stop += 1
            raw = block.text[cursor:stop]
            normalized = unicodedata.normalize("NFC", raw)
            operation = "nfc" if normalized != raw else None
            if cursor in typography_offsets and raw == "3":
                normalized, operation = "³", "typography"
        span = source_span(block, cursor, stop)
        # Merge adjacent unchanged segments; normalized groups stay atomic.
        if (operation is None and segments and segments[-1].mapping == "exact"
                and segments[-1].source_spans[0].end_offset == cursor):
            previous = segments.pop()
            span = source_span(block, previous.source_spans[0].start_offset, stop)
            segments.append(CharMapSegment(canonical_start=previous.canonical_start,
                                           canonical_end=target + len(normalized), source_spans=(span,), mapping="exact"))
        else:
            segments.append(CharMapSegment(canonical_start=target, canonical_end=target + len(normalized),
                                           source_spans=(span,), mapping="exact" if operation is None else "normalized_group",
                                           operation=operation))
        output.append(normalized)
        target += len(normalized)
        cursor = stop
    return "".join(output), tuple(segments)


def append_mapped(left_text: str, left_map: tuple[CharMapSegment, ...], right_text: str,
                  right_map: tuple[CharMapSegment, ...], *, separator_text: str = " ") -> tuple[str, tuple[CharMapSegment, ...]]:
    if not left_text:
        return right_text, right_map
    if not right_text:
        return left_text, left_map
    if separator_text not in {"", " "}:
        raise ValueError("only a formatting space can be inserted")
    offset = len(left_text) + len(separator_text)
    separator = ((CharMapSegment(canonical_start=len(left_text), canonical_end=offset,
                                 mapping="normalized_group", operation="format_separator"),) if separator_text else ())
    shifted = tuple(s.model_copy(update={"canonical_start": s.canonical_start + offset,
                                         "canonical_end": s.canonical_end + offset}) for s in right_map)
    return left_text + separator_text + right_text, left_map + separator + shifted


class Normalizer:
    def normalize(self, document: ParsedDocument) -> CanonicalDocument:
        typography = {(proof.block_id, proof.source_offset) for proof in document.symbol_proofs
                      if proof.method == "typography_text_layer" and proof.classification == "³"}
        registry = {block.block_id: block for block in document.blocks}

        def transform_context(ref):
            before, after = [], []
            for span in ref.source_spans:
                value = registry[span.block_id].text[span.start_offset:span.end_offset]
                before.append(value)
                after.append("".join("³" if (span.block_id, span.start_offset + i) in typography and c == "3" else c
                                     for i, c in enumerate(value)))
            if before == after:
                return ref
            if "".join("".join(before).split()) != "".join(ref.text.split()):
                return ref
            return ref.model_copy(update={"text": " ".join(after)})

        def transform_cell(cell):
            text = list(cell.text)
            mapping = []
            for segment in cell.char_map:
                if segment.mapping != "exact":
                    mapping.append(segment)
                    continue
                span = segment.source_spans[0]
                offsets = sorted(offset for owner, offset in typography if owner == span.block_id and offset is not None
                                 and span.start_offset <= offset < span.end_offset)
                cursor = span.start_offset
                block = registry[span.block_id]
                for offset in offsets:
                    target = segment.canonical_start + offset - span.start_offset
                    if offset > cursor:
                        mapping.append(CharMapSegment(canonical_start=segment.canonical_start + cursor - span.start_offset,
                                                      canonical_end=target, mapping="exact",
                                                      source_spans=(source_span(block, cursor, offset),)))
                    mapping.append(CharMapSegment(canonical_start=target, canonical_end=target + 1, mapping="normalized_group",
                                                  operation="typography", source_spans=(source_span(block, offset, offset + 1),)))
                    text[target] = "³"
                    cursor = offset + 1
                if cursor < span.end_offset:
                    mapping.append(CharMapSegment(canonical_start=segment.canonical_start + cursor - span.start_offset,
                                                  canonical_end=segment.canonical_end, mapping="exact",
                                                  source_spans=(source_span(block, cursor, span.end_offset),)))
            return cell.model_copy(update={"text": "".join(text), "char_map": tuple(mapping)})

        if typography:
            document = document.model_copy(update={"tables": tuple(table.model_copy(update={
                "cells": tuple(transform_cell(cell) for cell in table.cells),
                "rows": tuple(row.model_copy(update={"context_refs": tuple(transform_context(ref) for ref in row.context_refs)})
                              for row in table.rows),
                "context_refs": tuple(transform_context(ref) for ref in table.context_refs),
                "row_groups": tuple(group.model_copy(update={"context_refs": tuple(transform_context(ref) for ref in group.context_refs)})
                                    for group in table.row_groups),
            }) for table in document.tables)})
        exclusions = classify_boilerplate(document)
        excluded_blocks = {span.block_id for exclusion in exclusions for span in exclusion.source_spans}
        owned: dict[str, list[tuple[int, int]]] = {}
        for table in document.tables:
            for span in table.owned_source_spans:
                owned.setdefault(span.block_id, []).append((span.start_offset, span.end_offset))
        nodes: list[CanonicalNode] = []
        for block in document.blocks:
            if block.block_id in excluded_blocks:
                continue
            mask = [False] * len(block.text)
            for start, end in owned.get(block.block_id, []):
                if not 0 <= start < end <= len(mask):
                    raise ValueError("table ownership exceeds raw source")
                mask[start:end] = [True] * (end - start)
            start = 0
            fragment = 0
            while start < len(mask):
                if mask[start]:
                    start += 1
                    continue
                end = start + 1
                while end < len(mask) and not mask[end]:
                    end += 1
                text, mapping = normalize_slice(block, start, end, frozenset(offset for owner, offset in typography
                                                                            if owner == block.block_id and offset is not None))
                if text:
                    nodes.append(CanonicalNode(id=uuid5(document.parse_generation_id, f"{block.block_id}:{fragment}"),
                                               node_type="paragraph", ordinal=len(nodes), own_body=text, char_map=mapping,
                                               page_start=block.pdf_page, page_end=block.pdf_page,
                                               source_spans=tuple(s for item in mapping for s in item.source_spans),
                                               content_hash=content_hash(text)))
                start = end
                fragment += 1
        # Collinear PDF word fragments are one physical line. Use geometry, not expected legal numbers.
        by_id = {b.block_id: b for b in document.blocks}
        joined: list[CanonicalNode] = []
        for node in nodes:
            previous = joined[-1] if joined else None
            a = by_id[previous.source_spans[-1].block_id] if previous else None
            b = by_id[node.source_spans[0].block_id]
            scale = max((c.size for c in b.chars), default=b.bbox[3] - b.bbox[1]) or 10
            gap = b.bbox[0] - a.bbox[2] if a else -1
            aligned = (a is not None and a.pdf_page == b.pdf_page and 0 <= gap < scale * 1.5
                       and abs((a.bbox[1] + a.bbox[3]) - (b.bbox[1] + b.bbox[3])) < scale * 0.35)
            if previous and a and aligned:
                left_end = previous.source_spans[-1].end_offset
                right_start = node.source_spans[0].start_offset
                explicit_space = a.text[left_end:].isspace() or b.text[:right_start].isspace()
                separator = " " if explicit_space or gap > scale * 0.15 else ""
                body, mapping = append_mapped(previous.own_body, previous.char_map, node.own_body, node.char_map,
                                               separator_text=separator)
                joined[-1] = previous.model_copy(update={"own_body": body, "char_map": mapping,
                                                        "source_spans": previous.source_spans + node.source_spans,
                                                        "content_hash": content_hash(body)})
            else:
                joined.append(node)
        return CanonicalDocument(**document.model_dump(), nodes=tuple(joined), exclusions=exclusions)
