"""Synthetic retrieval contracts; no model-quality or expert-label claims."""
from dataclasses import FrozenInstanceError, replace
import json
from uuid import uuid4

import pytest

from expert_contracts.internal import SnapshotItem
from expert_contracts.sources import SourceSpan
from expert_contracts.retrieval_sources import RegistryOwnerRequest, canonical_owner_hash
from expert_agent.retrieval.expansion import build_evidence_pack, expand_candidates
from expert_agent.retrieval.fusion import fuse_rrf
from expert_agent.retrieval.types import (
    CandidateBatch, ChannelHit, Chunk, FusedHit, Node, RetrievalConfig, RetrievalError,
    Snapshot, SourceBlock, SourceIdentity, SourceRegistry, canonical_json, prepare_query,
)


def attest(nodes):
    values = []
    for node in nodes:
        owners = [("node_body", str(node.id), node.canonical_text, json.loads(node.metadata_json)["char_map"])] if node.canonical_text else []
        if node.table_json:
            owners.extend(("table_cell", c["id"], c["text"], c["char_map"]) for c in json.loads(node.table_json)["cells"] if c["text"])
        for kind, owner_id, text, mappings in owners:
            owner = RegistryOwnerRequest(node_id=node.id, owner_kind=kind, text_owner_id=owner_id)
            values.append((node.id, kind, owner_id, canonical_owner_hash(owner, text, mappings)))
    return tuple(values)


def fixture():
    run, snap, logical, version, parsed, index, publication, root, leaf = (uuid4() for _ in range(9))
    spans = (SourceSpan(pdf_page=1, block_id="scope", start_offset=0, end_offset=23),
             SourceSpan(pdf_page=2, block_id="body", start_offset=0, end_offset=24))
    texts = ("Только для сухой породы.", "Объём не более 17 литров.")
    spans = tuple(s.model_copy(update={"end_offset": len(t)}) for s, t in zip(spans, texts, strict=True))
    nodes = []
    for node_id, parent, span, text in ((root, None, spans[0], texts[0]), (leaf, root, spans[1], texts[1])):
        metadata = {"schema_version": "p04.node.v1", "char_map": [{"canonical_start": 0, "canonical_end": len(text),
            "mapping": "exact", "operation": None, "source_spans": [span.model_dump(mode="json")]}], "context_refs": []}
        nodes.append(Node(node_id, parsed, parent, "clause", text, ("Раздел",), (span,), canonical_json(metadata), None))
    projection = {"schema_version": "p04.chunks.v2", "canonical_ranges": [
        {"owner_kind": "node_body", "text_owner_id": str(leaf), "start": 0, "end": len(texts[1])}],
        "context_refs": [], "table_rows": [], "table_kind": None, "template_grid": [], "requires_expansion": False}
    chunk = Chunk(uuid4(), index, parsed, version, leaf, texts[1], "Заголовок", (spans[1],), "a" * 64,
                  canonical_json(projection), "Заголовок\nТолько для сухой породы.\nОбъём не более 17 литров.")
    snapshot = Snapshot(run, "principal", snap, 3, "f" * 64, (SnapshotItem(snapshot_id=snap, logical_document_id=logical,
        document_version_id=version, parse_generation_id=parsed, index_generation_id=index, publication_id=publication),))
    identity = SourceIdentity(parsed, version, uuid4(), "b" * 64, "Историческое название", "c" * 64)
    registry = SourceRegistry(parsed, version, "b" * 64, identity.artifact_object_id, "c" * 64, tuple(SourceBlock(s.block_id, s.pdf_page, t)
        for s, t in zip(spans, texts, strict=True)), ((600, 800), (600, 800)), attest(nodes))
    return snapshot, chunk, tuple(nodes), (identity,), {parsed: registry}


def test_question_preserves_original_numbers_negation_and_unicode():
    text = "  Пункт 999.42: не более 17 м³?\n"
    query = prepare_query(text)
    assert query.original_question == text
    assert query.search_question == "Пункт 999.42: не более 17 м³?"
    with pytest.raises(FrozenInstanceError):
        query.search_question = "changed"


@pytest.mark.parametrize("question", ["", "   ", "а" * 4001])
def test_query_outer_admission(question):
    with pytest.raises(RetrievalError, match="QUERY_INVALID"):
        prepare_query(question)


def test_rrf_first_occurrence_missing_channel_stable_ties_and_version_identity():
    _, one, *_ = fixture()
    two = replace(one, id=uuid4(), index_generation_id=uuid4())
    batch = CandidateBatch((ChannelHit(one, 0.1), ChannelHit(one, 0.1), ChannelHit(two, 0.2)),
                           (ChannelHit(two, 0.9),), "current", 3)
    result = fuse_rrf(batch, RetrievalConfig())
    assert result[0].chunk == two and result[0].rrf_score == 1 / 63 + 1 / 61
    assert result[1].dense_rank == 1 and result[1].lexical_rank is None
    tied = CandidateBatch((ChannelHit(one, 0), ChannelHit(two, 0)), (ChannelHit(two, 1), ChannelHit(one, 1)), "current", 3)
    assert [h.chunk.id for h in fuse_rrf(tied, RetrievalConfig())] == sorted([one.id, two.id], key=str)


def test_rrf_rejects_same_identity_with_different_source():
    _, one, *_ = fixture()
    with pytest.raises(RetrievalError, match="RETRIEVAL_CONTRACT_INVALID"):
        fuse_rrf(CandidateBatch((ChannelHit(one, 0),), (ChannelHit(replace(one, source_text="forged"), 1),), "current", 3), RetrievalConfig())


def test_expansion_preserves_parent_condition_and_frozen_pack_exact_provenance():
    snapshot, chunk, nodes, identities, registries = fixture()
    expanded = expand_candidates((FusedHit(chunk, .1, 1, None),), nodes, identities, registries)
    assert {f.text for f in expanded[0].fragments} == {node.canonical_text for node in nodes}
    packed = build_evidence_pack(snapshot, expanded, nodes, identities, token_budget=10000, count_tokens=len)
    assert len(packed.pack.units) == 2
    assert all(unit.document_title == "Историческое название" for unit in packed.pack.units)
    assert {unit.source_spans[0].pdf_page for unit in packed.pack.units} == {1, 2}
    assert all(unit.source_chunk_ids == (chunk.id,) for unit in packed.pack.units)
    assert "origin_hit_lineage" in packed.bindings_json
    assert packed == build_evidence_pack(snapshot, expanded, nodes, identities, token_budget=10000, count_tokens=len)
    with pytest.raises(Exception):
        packed.pack.units[0].excerpt = "changed"


def test_minimum_legal_unit_does_not_fit_never_truncates_condition():
    snapshot, chunk, nodes, identities, registries = fixture()
    expanded = expand_candidates((FusedHit(chunk, .1, 1, None),), nodes, identities, registries)
    with pytest.raises(RetrievalError, match="ATOMIC_CONTEXT_TOO_LARGE"):
        build_evidence_pack(snapshot, expanded, nodes, identities, token_budget=10, count_tokens=len)


@pytest.mark.parametrize("fault", ["source_text", "wrong_page", "wrong_parse", "missing_block", "range"])
def test_expansion_rejects_untrusted_source_or_binding(fault):
    _, chunk, nodes, identities, registries = fixture()
    registry = registries[chunk.parse_generation_id]
    if fault == "source_text":
        chunk = replace(chunk, source_text="invented")
    elif fault == "wrong_page":
        registry = replace(registry, blocks=(registry.blocks[0], replace(registry.blocks[1], pdf_page=1)))
    elif fault == "wrong_parse":
        registry = replace(registry, parse_generation_id=uuid4())
    elif fault == "missing_block":
        registry = replace(registry, blocks=registry.blocks[:1])
    else:
        projection = json.loads(chunk.projection_json)
        projection["canonical_ranges"][0]["end"] = 999
        chunk = replace(chunk, projection_json=canonical_json(projection))
    with pytest.raises(RetrievalError):
        expand_candidates((FusedHit(chunk, .1, 1, None),), nodes, identities, {chunk.parse_generation_id: registry})


def test_exact_duplicate_evidence_deduplicates_without_losing_origin_hits():
    snapshot, chunk, nodes, identities, registries = fixture()
    other = replace(chunk, id=uuid4())
    hits = tuple(FusedHit(c, .1, i, None) for i, c in enumerate((chunk, other), 1))
    expanded = expand_candidates(hits, nodes, identities, registries)
    packed = build_evidence_pack(snapshot, expanded, nodes, identities, token_budget=10000, count_tokens=len)
    assert len(packed.pack.units) == 2
    assert all(set(unit.source_chunk_ids) == {chunk.id, other.id} for unit in packed.pack.units)


def test_partially_overlapping_hits_merge_canonical_intervals_not_text_hashes():
    snapshot, chunk, nodes, identities, registries = fixture()
    parts = []
    for start, end in ((0, 15), (10, len(chunk.source_text))):
        projection = json.loads(chunk.projection_json)
        projection["canonical_ranges"][0].update(start=start, end=end)
        parts.append(replace(chunk, id=uuid4(), source_text=chunk.source_text[start:end],
                             source_spans=(chunk.source_spans[0].model_copy(update={"start_offset": start, "end_offset": end}),),
                             projection_json=canonical_json(projection)))
    expanded = expand_candidates(tuple(FusedHit(c, .1, i, None) for i, c in enumerate(parts, 1)), nodes, identities, registries)
    packed = build_evidence_pack(snapshot, expanded, nodes, identities, token_budget=10000, count_tokens=len)
    body = [unit for unit in packed.pack.units if unit.canonical_node_id == chunk.node_id]
    assert len(body) == 1 and body[0].excerpt == chunk.source_text
    assert body[0].source_spans == chunk.source_spans
    assert set(body[0].source_chunk_ids) == {part.id for part in parts}
