"""Pure chunker behavior on generated source maps; no corpus/retrieval quality claims."""
from __future__ import annotations

import hashlib
import json
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest

from expert_contracts.sources import SourceSpan
from expert_ingest.parsing.chunking import ChunkingConfig, project_chunks
from expert_ingest.parsing.dto import (
    CanonicalDocument, CanonicalNode, Cell, CharMapSegment, ContextRef, Diagnostic,
    Page, RawBlock, Row, Table,
)
from expert_ingest.parsing.tokenizer import (
    DOCUMENT_PREFIX, MODEL_ID, REVISION, TOKENIZER_FILES, FridaTokenizerBudget, TokenizerUnavailable,
)


ROOT = UUID(int=1)
NODE = UUID(int=2)


class CharacterBudget:
    """Explicit test double: six synthetic prefix/special tokens plus source codepoints."""

    model_id, revision, fingerprint = MODEL_ID, REVISION, "c" * 64

    def document_tokens(self, text):
        return 6 + len(text)


def span(block, start=0, end=None):
    return SourceSpan(pdf_page=block.pdf_page, block_id=block.block_id,
                      start_offset=start, end_offset=len(block.text) if end is None else end, bbox=block.bbox)


def mapped(text, block_id="body", page=1):
    block = RawBlock(block_id=block_id, pdf_page=page, text=text, bbox=(1, 1, 300, 30), ordinal=0)
    mapping = ((CharMapSegment(canonical_start=0, canonical_end=len(text),
                               mapping="exact", source_spans=(span(block),)),) if text else ())
    return block, mapping


def document(text="Текст нормы.", *, refs=(), more_blocks=(), number=None, node_type="clause"):
    block, mapping = mapped(text)
    root = CanonicalNode(id=ROOT, node_type="document", ordinal=0)
    node = CanonicalNode(id=NODE, parent_id=ROOT, node_type=node_type, ordinal=1,
                         number=number, own_body=text, char_map=mapping, context_refs=refs)
    return CanonicalDocument(
        version_id=UUID(int=3), parse_generation_id=UUID(int=4), source_sha256="a" * 64,
        title="Операторский заголовок", pages=(Page(pdf_page=1, width=500, height=800,
        cropbox=(0, 0, 500, 800), block_ids=("body",)),), blocks=(block, *more_blocks), nodes=(root, node),
    )


def project(doc, limit=80):
    return project_chunks(doc, CharacterBudget(), ChunkingConfig(max_input_tokens=limit,
                                                                descriptor_max_tokens=limit))


def require_failed(result, diagnostic, code="GENERATION_INVALID"):
    assert result.status == "failed", result
    assert result.error_code == code
    assert result.chunks == result.descriptors == ()
    assert result.diagnostics[0].code == diagnostic


def test_source_header_and_prefix_have_distinct_ownership():
    doc = document()
    result = project(doc)
    assert result.status == "passed"
    chunk = result.chunks[0]
    assert chunk.source_text == "Текст нормы."
    assert doc.title in chunk.header_text
    assert chunk.embedding_text == doc.title + "\nТекст нормы."
    assert DOCUMENT_PREFIX not in chunk.embedding_text
    assert chunk.input_tokens == CharacterBudget().document_tokens(chunk.embedding_text)
    assert chunk.canonical_ranges[0].start == 0
    assert chunk.source_spans[0].bbox == doc.blocks[0].bbox
    assert result == project(doc)


def test_long_clause_exact_coverage_stable_ordinals_and_partial_flag():
    text = "Первое условие действует.\n\nВторое условие действует. " * 15
    result = project(document(text), 65)
    assert result.status == "passed"
    assert len(result.chunks) > 2
    assert "".join(c.source_text for c in result.chunks) == text
    assert [c.chunk_index for c in result.chunks] == list(range(len(result.chunks)))
    assert all(c.requires_expansion and c.input_tokens <= 65 for c in result.chunks)
    for previous, current in zip(result.chunks, result.chunks[1:]):
        assert previous.canonical_ranges[-1].end == current.canonical_ranges[0].start
        assert previous.source_spans[-1].end_offset == current.source_spans[0].start_offset


def test_separate_articles_are_not_merged_and_source_hash_binds_version():
    doc = document("Первая норма.", node_type="article")
    second_block, second_map = mapped("Вторая норма.", "second")
    second = CanonicalNode(id=UUID(int=5), parent_id=ROOT, node_type="article", ordinal=2,
                           own_body=second_block.text, char_map=second_map)
    doc = doc.model_copy(update={"nodes": (*doc.nodes, second), "blocks": (*doc.blocks, second_block)})
    result = project(doc)
    assert result.status == "passed"
    assert [c.node_id for c in result.chunks] == [NODE, second.id]
    assert all("Первая" not in c.source_text or "Вторая" not in c.source_text for c in result.chunks)
    assert {d.node_id for d in result.descriptors} == {ROOT, NODE, second.id}
    changed = project(doc.model_copy(update={"version_id": UUID(int=6)}))
    assert changed.chunks[0].content_hash != result.chunks[0].content_hash


def test_long_optional_headers_shorten_but_current_number_and_required_scope_survive():
    block, _ = mapped("Если давление выше нормы", "scope")
    ref = ContextRef(role="scope", text=block.text, source_spans=(span(block),))
    doc = document("Испытание запрещено.", refs=(ref,), more_blocks=(block,), number="3.2.15")
    doc = doc.model_copy(update={"title": "Длинное название " * 25})
    result = project(doc, 70)
    assert result.status == "passed"
    chunk = result.chunks[0]
    assert chunk.header_text == "3.2.15"
    assert chunk.context_refs == (ref,)
    assert ref.text in chunk.embedding_text
    assert NODE in result.manifest.shortened_header_nodes


def test_oversized_required_context_fails_without_partial_result():
    block, _ = mapped("Условие " * 20, "scope")
    ref = ContextRef(role="scope", text=block.text, source_spans=(span(block),))
    require_failed(project(document(refs=(ref,), more_blocks=(block,)), 40),
                   "ATOMIC_CONTEXT_OR_TEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")


def test_oversized_optional_context_can_be_omitted():
    block, _ = mapped("Необязательный caption " * 20, "caption")
    ref = ContextRef(role="caption", text=block.text, source_spans=(span(block),), required=False)
    result = project(document(refs=(ref,), more_blocks=(block,)))
    assert result.status == "passed"
    assert not result.chunks[0].context_refs


def test_atomic_normalization_maps_keep_all_noncontiguous_raw_sources():
    one, _ = mapped("норма-", "one")
    two, _ = mapped("тив", "two")
    body = "норматив"
    group = CharMapSegment(canonical_start=0, canonical_end=len(body), mapping="normalized_group",
                           operation="dehyphenate", source_spans=(span(one), span(two)))
    doc = document(body)
    doc = doc.model_copy(update={"blocks": (one, two), "nodes": (doc.nodes[0], doc.nodes[1].model_copy(
        update={"char_map": (group,)}))})
    result = project(doc, 15)
    assert result.status == "passed"
    assert result.chunks[0].source_text == body
    assert result.chunks[0].source_spans == (span(one), span(two))
    require_failed(project(doc, 13), "ATOMIC_CONTEXT_OR_TEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")


def test_unicode_offsets_keep_combining_emoji_and_atomic_group_whole():
    text = "А±³ 😀 е\u0301 👩\u200d🔬 " * 12
    result = project(document(text), 23)
    assert result.status == "passed"
    assert "".join(c.source_text for c in result.chunks) == text
    for chunk in result.chunks:
        interval = chunk.canonical_ranges[0]
        assert chunk.source_text == text[interval.start:interval.end]
        assert not chunk.source_text.startswith(("\u0301", "\u200d"))
        assert not chunk.source_text.endswith("\u200d")


def test_short_negation_introducer_is_not_stranded():
    text = "Устройство не допускается использовать. " * 4
    result = project(document(text), 20)
    assert result.status == "passed"
    assert all(not c.source_text.rstrip().endswith(" не") for c in result.chunks)
    assert "".join(c.source_text for c in result.chunks) == text


@pytest.mark.parametrize("kind,diagnostic", [
    ("unknown_block", "SOURCE_SPAN_UNRESOLVED"),
    ("wrong_page", "SOURCE_SPAN_UNRESOLVED"),
    ("out_of_bounds", "SOURCE_SPAN_OUT_OF_BOUNDS"),
    ("wrong_text", "EXACT_MAP_TEXT_DIFFERS"),
    ("gap", "CANONICAL_MAP_INCOMPLETE"),
])
def test_invalid_source_maps_fail_closed(kind, diagnostic):
    doc = document("Норма")
    original = doc.nodes[1].char_map[0]
    anchor = original.source_spans[0]
    if kind == "unknown_block":
        anchor = anchor.model_copy(update={"block_id": "missing"})
    elif kind == "wrong_page":
        anchor = anchor.model_copy(update={"pdf_page": 2})
    elif kind == "out_of_bounds":
        anchor = anchor.model_copy(update={"start_offset": 1, "end_offset": 6})
    elif kind == "wrong_text":
        doc = doc.model_copy(update={"blocks": (doc.blocks[0].model_copy(update={"text": "Иное!"}),)})
    else:
        original = original.model_copy(update={"canonical_start": 1})
    node = doc.nodes[1].model_copy(update={"char_map": (original.model_copy(update={"source_spans": (anchor,)}),)})
    require_failed(project(doc.model_copy(update={"nodes": (doc.nodes[0], node)})), diagnostic)


def test_duplicate_body_ownership_and_critical_quality_are_rejected():
    doc = document()
    duplicate = doc.nodes[1].model_copy(update={"id": UUID(int=8), "ordinal": 2})
    require_failed(project(doc.model_copy(update={"nodes": (*doc.nodes, duplicate)})), "DUPLICATE_SOURCE_OWNERSHIP")
    require_failed(project(doc.model_copy(update={"diagnostics": (Diagnostic(
        code="UNCERTAIN_MATH_GLYPH", severity="critical"),)})), "CRITICAL_PARSER_DIAGNOSTIC")


@pytest.mark.parametrize("kind", ["cycle", "orphan", "two_roots"])
def test_invalid_trees_are_rejected(kind):
    doc = document()
    parent = {"cycle": NODE, "orphan": UUID(int=55), "two_roots": None}[kind]
    node = doc.nodes[1].model_copy(update={"parent_id": parent})
    require_failed(project(doc.model_copy(update={"nodes": (doc.nodes[0], node)})), "CANONICAL_TREE_INVALID")


def table_document(*, long_row=False, long_header=False):
    doc = document("")
    header_block, header_map = mapped("Объём м³" if not long_header else "Заголовок " * 30, "header")
    note_block, _ = mapped("При температуре 20 °C", "note")
    unit_block, unit_map = mapped("м³", "unit")
    data_blocks, cells, rows = [], [
        Cell(id="h", row=0, column=0, column_span=2, role="header", text=header_block.text, char_map=header_map),
        Cell(id="u", row=1, column=0, column_span=2, role="unit", text=unit_block.text, char_map=unit_map),
    ], [Row(row_index=0, cell_ids=("h",)), Row(row_index=1, cell_ids=("u",))]
    refs = (ContextRef(role="header", text=header_block.text, source_spans=(span(header_block),)),
            ContextRef(role="unit", text=unit_block.text, source_spans=(span(unit_block),)),
            ContextRef(role="note", text=note_block.text, source_spans=(span(note_block),)))
    for row_index in (2, 3):
        ids = []
        for column, text in enumerate((f"{row_index},5", "±0,2" if not long_row else "9" * 120)):
            cid = f"d{row_index}{column}"
            block, mapping = mapped(text, cid, 2 if row_index == 3 else 1)
            data_blocks.append(block)
            cells.append(Cell(id=cid, row=row_index, column=column, role="data", text=text, char_map=mapping))
            ids.append(cid)
        rows.append(Row(row_index=row_index, cell_ids=tuple(ids), context_refs=refs))
    table = Table(id="table", cells=tuple(cells), rows=tuple(rows), column_count=2, pdf_pages=(1, 2))
    node = doc.nodes[1].model_copy(update={"node_type": "table", "table": table, "page_end": 2})
    page2 = doc.pages[0].model_copy(update={"pdf_page": 2})
    return doc.model_copy(update={"blocks": (header_block, unit_block, note_block, *data_blocks),
                                  "pages": (*doc.pages, page2), "nodes": (doc.nodes[0], node), "tables": (table,)})


def test_table_rows_repeat_complete_source_context_with_cell_local_ranges():
    doc = table_document()
    result = project(doc, 80)
    assert result.status == "passed", result
    assert len(result.chunks) == 2
    assert [c.table_rows for c in result.chunks] == [(2,), (3,)]
    for chunk in result.chunks:
        assert [r.role for r in chunk.context_refs] == ["header", "unit", "note"]
        assert "Объём м³" in chunk.embedding_text and "При температуре" in chunk.embedding_text
        assert "Объём" not in chunk.source_text
        assert all(r.owner_kind == "table_cell" and r.start == 0 for r in chunk.canonical_ranges)
    assert {s.pdf_page for c in result.chunks for s in c.source_spans} == {1, 2}
    assert len({r.text_owner_id for c in result.chunks for r in c.canonical_ranges}) == 4


@pytest.mark.parametrize("kind", ["row", "header"])
def test_oversized_table_row_or_required_header_fails_completely(kind):
    require_failed(project(table_document(long_row=kind == "row", long_header=kind == "header"), 70),
                   "TABLE_ROW_CONTEXT_TOO_LONG", "TOKEN_LIMIT_EXCEEDED")


def test_header_only_table_is_not_a_searchable_result():
    doc = table_document()
    table = doc.tables[0].model_copy(update={"cells": doc.tables[0].cells[:2], "rows": doc.tables[0].rows[:2]})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_WITHOUT_READABLE_DATA")


def test_mandatory_table_header_cannot_be_missing():
    doc = table_document()
    table = doc.tables[0].model_copy(update={"rows": tuple(r.model_copy(update={"context_refs": ()})
                                                         for r in doc.tables[0].rows)})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_REQUIRED_HEADER_MISSING")


def test_table_unit_cell_cannot_disappear_from_all_context_refs():
    doc = table_document()
    table = doc.tables[0].model_copy(update={"rows": tuple(r.model_copy(update={
        "context_refs": tuple(ref for ref in r.context_refs if ref.role != "unit")}) for r in doc.tables[0].rows)})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_CONTEXT_COVERAGE_INCOMPLETE")


def test_repeated_merged_cell_id_is_not_second_ownership():
    doc = table_document()
    table = doc.tables[0]
    rows = (*table.rows[:3], table.rows[3].model_copy(update={"cell_ids": ("d20", "d31")}))
    table = table.model_copy(update={"rows": rows})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_ROW_CELL_MISMATCH")


def test_body_table_duplicate_ownership_is_rejected():
    doc = table_document()
    cell = doc.tables[0].cells[2]
    node = doc.nodes[1].model_copy(update={"own_body": cell.text, "char_map": cell.char_map})
    require_failed(project(doc.model_copy(update={"nodes": (doc.nodes[0], node)})), "DUPLICATE_SOURCE_OWNERSHIP")


def template_document(*, kind="blank_template", long_header=False):
    doc = table_document(long_header=long_header)
    original = doc.tables[0]
    cells = tuple(cell.model_copy(update={"text": "", "char_map": ()}) if cell.role == "data" else cell
                  for cell in original.cells)
    table = original.model_copy(update={"kind": kind, "cells": cells})
    node = doc.nodes[1].model_copy(update={"table": table})
    return doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})


def test_blank_template_preserves_labels_empty_slots_and_kind_without_filled_values():
    doc = template_document()
    result = project(doc, 160)
    assert result.status == "passed"
    assert len(result.chunks) == 1
    chunk = result.chunks[0]
    assert chunk.table_kind == "blank_template"
    assert chunk.source_text == "Объём м³ | м³"
    assert all(c.role in {"header", "unit"} for c in doc.tables[0].cells if c.id in
               {r.text_owner_id for r in chunk.canonical_ranges})
    assert len(chunk.template_grid) == 6
    assert sum(slot.empty for slot in chunk.template_grid) == 4
    assert all("text" not in type(slot).model_fields for slot in chunk.template_grid)
    assert chunk.requires_expansion
    assert chunk.table_rows == (0, 1, 2, 3)
    assert result.manifest.schema_version == "p04.chunks.v2"
    assert result.manifest.table_types[0].kind == "blank_template"
    assert not any(value in chunk.embedding_text for value in ("2,5", "3,5", "±0,2"))
    assert result == project(doc, 160)


def test_empty_data_table_does_not_implicitly_become_template():
    require_failed(project(template_document(kind="data")), "TABLE_WITHOUT_READABLE_DATA")


def test_template_with_observed_data_is_not_accepted_as_blank():
    doc = table_document()
    table = doc.tables[0].model_copy(update={"kind": "blank_template"})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_TEMPLATE_CLASSIFICATION_INVALID")


def test_template_without_actual_blank_slots_is_rejected():
    doc = template_document()
    table = doc.tables[0].model_copy(update={"cells": doc.tables[0].cells[:2], "rows": doc.tables[0].rows[:2]})
    node = doc.nodes[1].model_copy(update={"table": table})
    require_failed(project(doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node)})),
                   "TABLE_TEMPLATE_CLASSIFICATION_INVALID")


def test_template_preserves_all_required_context_or_fails_atomically():
    require_failed(project(template_document(long_header=True), 40), "TABLE_TEMPLATE_CONTEXT_TOO_LONG",
                   "TOKEN_LIMIT_EXCEEDED")


def test_template_with_row_labels_and_empty_value_slots_preserves_structure():
    doc = template_document()
    extra_blocks, replacements = [], {}
    for row in (2, 3):
        block, mapping = mapped(f"Позиция {row} ...", f"label{row}")
        extra_blocks.append(block)
        replacements[f"d{row}0"] = {"text": block.text, "char_map": mapping, "role": "header"}
    table = doc.tables[0].model_copy(update={"cells": tuple(cell.model_copy(update=replacements[cell.id])
        if cell.id in replacements else cell for cell in doc.tables[0].cells)})
    node = doc.nodes[1].model_copy(update={"table": table})
    doc = doc.model_copy(update={"tables": (table,), "nodes": (doc.nodes[0], node), "blocks": (*doc.blocks, *extra_blocks)})
    result = project(doc, 200)
    assert result.status == "passed"
    chunk = result.chunks[0]
    assert "Позиция 2 ..." in chunk.source_text
    assert sum(slot.empty for slot in chunk.template_grid) == 2
    assert chunk.table_kind == "blank_template"


def test_synthetic_separator_has_no_fake_source_span():
    left, _ = mapped("Первая", "left")
    right, _ = mapped("норма", "right")
    doc = document("Первая норма")
    mapping = (
        CharMapSegment(canonical_start=0, canonical_end=6, mapping="exact", source_spans=(span(left),)),
        CharMapSegment(canonical_start=6, canonical_end=7, mapping="normalized_group", operation="format_separator"),
        CharMapSegment(canonical_start=7, canonical_end=12, mapping="exact", source_spans=(span(right),)),
    )
    node = doc.nodes[1].model_copy(update={"char_map": mapping})
    doc = doc.model_copy(update={"blocks": (left, right), "nodes": (doc.nodes[0], node)})
    result = project(doc)
    assert result.status == "passed"
    assert result.chunks[0].source_spans == (span(left), span(right))
    bad = node.model_copy(update={"own_body": "ПерваяXнорма"})
    require_failed(project(doc.model_copy(update={"nodes": (doc.nodes[0], bad)})), "SOURCELESS_NORMATIVE_TEXT")


def test_uninformative_root_uses_mapped_intro_for_routing():
    doc = document("Правила испытания сосудов под давлением.")
    result = project(doc)
    assert result.descriptors[0].node_id == ROOT
    assert "Правила испытания" in result.descriptors[0].text
    assert result.descriptors[0].source_spans == result.chunks[0].source_spans


def test_explicit_resource_caps_fail_without_partial_index():
    doc = document("Длинная норма. " * 100)
    result = project_chunks(doc, CharacterBudget(), ChunkingConfig(max_input_tokens=30, max_chunks=1))
    require_failed(result, "CHUNKING_RESOURCE_LIMIT")
    result = project_chunks(doc, CharacterBudget(), ChunkingConfig(max_tokenizer_calls=1))
    require_failed(result, "CHUNKING_RESOURCE_LIMIT")


def test_tokenizer_hash_or_revision_mismatch_fails_before_ml_import(tmp_path):
    specs = []
    for name in TOKENIZER_FILES:
        data = b"{}"
        (tmp_path / name).write_bytes(data)
        specs.append({"path": name, "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    model = {"model_id": MODEL_ID, "revision": REVISION, "tokenizer_revision": REVISION,
             "contract": {"document_prefix": DOCUMENT_PREFIX, "query_prefix": "search_query: ",
                          "max_input_tokens": 512, "silent_truncation": False}, "files": specs}
    lock = tmp_path / "lock.json"
    lock.write_text(json.dumps({"models": {"embedding": model}}))
    (tmp_path / "tokenizer.json").write_bytes(b"tampered")
    with pytest.raises(TokenizerUnavailable, match="LOCAL_TOKENIZER_UNAVAILABLE"):
        FridaTokenizerBudget.from_local(tmp_path, lock)


def test_frida_tokenizer_uses_local_autotokenizer_after_hash_verification(tmp_path, monkeypatch):
    specs = []
    for name in TOKENIZER_FILES:
        data = f"locked {name}".encode()
        (tmp_path / name).write_bytes(data)
        specs.append({"path": name, "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    model = {"model_id": MODEL_ID, "revision": REVISION, "tokenizer_revision": REVISION,
             "contract": {"document_prefix": DOCUMENT_PREFIX, "query_prefix": "search_query: ",
                          "max_input_tokens": 512, "silent_truncation": False}, "files": specs}
    lock = tmp_path / "lock.json"
    lock.write_text(json.dumps({"models": {"embedding": model}}), encoding="utf-8")
    calls = []

    class FakeTokenizer:
        is_fast = True

        def num_special_tokens_to_add(self, *, pair=False):
            assert pair is False
            return 2

    class FakeAutoTokenizer:
        @staticmethod
        def from_pretrained(path, **kwargs):
            calls.append((path, kwargs))
            return FakeTokenizer()

    monkeypatch.setitem(sys.modules, "transformers", SimpleNamespace(AutoTokenizer=FakeAutoTokenizer))

    budget = FridaTokenizerBudget.from_local(tmp_path, lock)

    assert budget.fingerprint
    assert calls == [(str(tmp_path.resolve()), {"local_files_only": True, "trust_remote_code": False})]
    model["revision"] = "wrong"
    lock.write_text(json.dumps({"models": {"embedding": model}}))
    with pytest.raises(TokenizerUnavailable, match="LOCAL_TOKENIZER_UNAVAILABLE"):
        FridaTokenizerBudget.from_local(tmp_path, lock)
