"""Source-owned expansion and atomic evidence packing; no legal/model inference."""
from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
import json
import re
from typing import Any, Literal
import unicodedata
from uuid import UUID, uuid5

from expert_contracts.internal import EvidencePack, EvidenceUnit
from expert_contracts.sources import SourceSpan
from expert_contracts.retrieval_sources import RegistryOwnerRequest, canonical_owner_hash

from .types import (
    ExpandedCandidate, Expansion, FusedHit, Node, PackedEvidence, RetrievalConfig,
    RetrievalError, Snapshot, SourceIdentity, SourceRegistry, canonical_json, content_hash,
)


def _span_key(span: SourceSpan) -> tuple[int, str, int, int]:
    return span.pdf_page, span.block_id, span.start_offset, span.end_offset


def _unique_spans(spans) -> tuple[SourceSpan, ...]:
    return tuple({_span_key(span): span for span in spans}.values())


def _contains(outer: SourceSpan, inner: SourceSpan) -> bool:
    return (outer.pdf_page, outer.block_id) == (inner.pdf_page, inner.block_id) and (
        outer.start_offset <= inner.start_offset < inner.end_offset <= outer.end_offset)


def _merged_spans(spans) -> tuple[SourceSpan, ...]:
    by_block: dict[tuple[int, str], list[SourceSpan]] = {}
    for span in spans:
        by_block.setdefault((span.pdf_page, span.block_id), []).append(span)
    result: list[SourceSpan] = []
    for values in by_block.values():
        merged: list[SourceSpan] = []
        for span in sorted(values, key=lambda value: (value.start_offset, value.end_offset)):
            if merged and span.start_offset <= merged[-1].end_offset:
                previous = merged[-1]
                bbox = previous.bbox if previous.bbox == span.bbox else None
                merged[-1] = previous.model_copy(update={"end_offset": max(previous.end_offset, span.end_offset), "bbox": bbox})
            else:
                merged.append(span)
        result.extend(merged)
    return tuple(result)


def _coalesced(fragments: list[Expansion]):
    """Merge overlapping canonical slices only within one pinned text owner."""
    groups: dict[tuple[Any, ...], list[Expansion]] = {}
    for fragment in fragments:
        binding = json.loads(fragment.owner_ranges_json)
        key = (fragment.chunk.index_generation_id, fragment.node_id, binding["owner_kind"], binding["text_owner_id"])
        groups.setdefault(key, []).append(fragment)
    result: list[tuple[str, tuple[SourceSpan, ...], list[Expansion]]] = []
    for values in groups.values():
        clusters: list[list[Any]] = []
        for fragment in sorted(values, key=lambda value: (json.loads(value.owner_ranges_json)["start"], json.loads(value.owner_ranges_json)["end"])):
            binding = json.loads(fragment.owner_ranges_json)
            start, end = binding["start"], binding["end"]
            if end - start != len(fragment.text):
                raise RetrievalError("SOURCE_RANGE_INVALID")
            if clusters and start <= clusters[-1][1]:
                current = clusters[-1]
                overlap_end = min(current[1], end)
                if current[2][start - current[0]:overlap_end - current[0]] != fragment.text[:overlap_end - start]:
                    raise RetrievalError("SOURCE_TEXT_MISMATCH")
                current[2] += fragment.text[max(0, current[1] - start):]
                current[1] = max(current[1], end)
                current[3].extend(fragment.spans)
                current[4].append(fragment)
            else:
                clusters.append([start, end, fragment.text, list(fragment.spans), [fragment]])
        result.extend((item[2], _merged_spans(item[3]), item[4]) for item in clusters)
    # A canonical owner must not steal intervals from another owning source node.
    claimed: dict[tuple[UUID, str], list[tuple[int, int]]] = {}
    for _, spans, matches in result:
        for span in spans:
            source_key = (matches[0].chunk.parse_generation_id, span.block_id)
            intervals = claimed.setdefault(source_key, [])
            if any(max(start, span.start_offset) < min(end, span.end_offset) for start, end in intervals):
                raise RetrievalError("SOURCE_OWNERSHIP_OVERLAP")
            intervals.append((span.start_offset, span.end_offset))
    return result


@dataclass(frozen=True)
class _Owner:
    node: Node
    owner_id: str
    kind: Literal["node_body", "table_cell"]
    text: str
    mappings: tuple[dict[str, Any], ...]


def _owners(nodes: tuple[Node, ...]) -> tuple[_Owner, ...]:
    owners = []
    for node in nodes:
        metadata = json.loads(node.metadata_json)
        if metadata.get("schema_version") != "p04.node.v1":
            raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
        if node.canonical_text:
            owners.append(_Owner(node, str(node.id), "node_body", node.canonical_text, tuple(metadata["char_map"])))
        if node.table_json:
            for cell in json.loads(node.table_json)["cells"]:
                if cell["text"]:
                    owners.append(_Owner(node, cell["id"], "table_cell", cell["text"], tuple(cell["char_map"])))
    return tuple(owners)


def _slice(owner: _Owner, start: int, end: int, registry: SourceRegistry) -> tuple[str, tuple[SourceSpan, ...]]:
    if not 0 <= start < end <= len(owner.text):
        raise RetrievalError("SOURCE_RANGE_INVALID")
    blocks = {block.id: block for block in registry.blocks}
    if len(blocks) != len(registry.blocks):
        raise RetrievalError("SOURCE_REGISTRY_INVALID")
    spans = []
    position = start
    for segment in owner.mappings:
        left, right = max(start, segment["canonical_start"]), min(end, segment["canonical_end"])
        if left >= right:
            continue
        if left != position:
            raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
        position = right
        source = [SourceSpan.model_validate(value) for value in segment["source_spans"]]
        if segment["mapping"] == "exact":
            if len(source) != 1:
                raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
            span = source[0]
            source = [span.model_copy(update={
                "start_offset": span.start_offset + left - segment["canonical_start"],
                "end_offset": span.start_offset + right - segment["canonical_start"],
            })]
        elif left != segment["canonical_start"] or right != segment["canonical_end"]:
            raise RetrievalError("NORMALIZED_GROUP_CUT")
        if not source and segment.get("operation") != "format_separator":
            raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
        for span in source:
            block = blocks.get(span.block_id)
            if block is None or block.pdf_page != span.pdf_page or not 1 <= span.pdf_page <= len(registry.page_sizes):
                raise RetrievalError("SOURCE_BINDING_INVALID")
            try:
                span.validate_source(block_text=block.text, pdf_page_count=len(registry.page_sizes),
                                     page_size=registry.page_sizes[span.pdf_page - 1])
            except ValueError:
                raise RetrievalError("SOURCE_RANGE_INVALID") from None
            if segment["mapping"] == "exact" and owner.text[left:right] != block.text[span.start_offset:span.end_offset]:
                raise RetrievalError("SOURCE_TEXT_MISMATCH")
        spans.extend(source)
    if position != end or not spans:
        raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
    return owner.text[start:end], _unique_spans(spans)


def _reference_ranges(ref: dict[str, Any], owners: tuple[_Owner, ...], registry: SourceRegistry) -> list[tuple[_Owner, int, int]]:
    anchors = tuple(SourceSpan.model_validate(span) for span in ref["source_spans"])
    blocks = {block.id: block for block in registry.blocks}
    if len(blocks) != len(registry.blocks):
        raise RetrievalError("SOURCE_REGISTRY_INVALID")
    for anchor in anchors:
        block = blocks.get(anchor.block_id)
        if block is None or block.pdf_page != anchor.pdf_page or not 1 <= anchor.pdf_page <= len(registry.page_sizes):
            raise RetrievalError("SOURCE_BINDING_INVALID")
        try:
            anchor.validate_source(block_text=block.text, pdf_page_count=len(registry.page_sizes),
                                   page_size=registry.page_sizes[anchor.pdf_page - 1])
        except ValueError:
            raise RetrievalError("SOURCE_RANGE_INVALID") from None
        if not block.text[anchor.start_offset:anchor.end_offset].strip():
            raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
    result = []
    covered = []
    for owner in owners:
        for mapping in owner.mappings:
            source = tuple(SourceSpan.model_validate(value) for value in mapping["source_spans"])
            if not source:
                continue
            if mapping["mapping"] == "exact":
                if len(source) != 1:
                    raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
                span = source[0]
                for anchor in anchors:
                    if (span.pdf_page, span.block_id) != (anchor.pdf_page, anchor.block_id):
                        continue
                    start, end = max(span.start_offset, anchor.start_offset), min(span.end_offset, anchor.end_offset)
                    if start < end:
                        result.append((owner, mapping["canonical_start"] + start - span.start_offset,
                                       mapping["canonical_start"] + end - span.start_offset))
                        covered.append(span.model_copy(update={"start_offset": start, "end_offset": end}))
            elif all(any(_contains(anchor, span) for anchor in anchors) for span in source):
                result.append((owner, mapping["canonical_start"], mapping["canonical_end"]))
                covered.extend(source)
            elif any((anchor.pdf_page, anchor.block_id) == (span.pdf_page, span.block_id)
                     and max(anchor.start_offset, span.start_offset) < min(anchor.end_offset, span.end_offset)
                     for anchor in anchors for span in source):
                raise RetrievalError("NORMALIZED_GROUP_CUT")
    for anchor in anchors:
        intervals = sorted((max(s.start_offset, anchor.start_offset), min(s.end_offset, anchor.end_offset))
                           for s in covered if (s.pdf_page, s.block_id) == (anchor.pdf_page, anchor.block_id)
                           and max(s.start_offset, anchor.start_offset) < min(s.end_offset, anchor.end_offset))
        if not intervals:
            raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
        raw = blocks[anchor.block_id].text
        leading_end, trailing_start = len(raw) - len(raw.lstrip()), len(raw.rstrip())

        def trimmed_boundary(left: int, right: int) -> bool:
            # Only authenticated raw-block trim margins may lack canonical
            # ownership. Interior whitespace and atomic mapping cuts still fail.
            # Evidence retains exact mapped text/spans; raw refs remain immutable.
            return raw[left:right].isspace() and (right <= leading_end or left >= trailing_start)

        position = anchor.start_offset
        for left, right in intervals:
            if left > position and not trimmed_boundary(position, left):
                raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
            position = max(position, right)
        if position < anchor.end_offset and not trimmed_boundary(position, anchor.end_offset):
            raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
    if not result:
        raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
    return result


def _grid(table: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"cell_id": cell["id"], **{key: cell.get(key) for key in (
        "row", "column", "row_span", "column_span", "role", "pdf_page", "bbox")},
        "empty": not bool(cell["text"])} for cell in sorted(table["cells"], key=lambda c: (c["row"], c["column"]))]


def _normalized_subject(text: str) -> str:
    value = unicodedata.normalize("NFC", text).casefold().replace("ё", "е")
    return re.sub(r"\s+", " ", value).strip(" \t\r\n.,;:()[]{}")


def _strict_whole_word_prefix(left: str, right: str) -> bool:
    if not left or left == right or not right.startswith(left):
        return False
    if right[len(left)].isalnum() or right[len(left)] == "_":
        return False
    return True


def _subject_prefix_match(left: str, right: str) -> bool:
    first, second = _normalized_subject(left), _normalized_subject(right)
    return _strict_whole_word_prefix(first, second) or _strict_whole_word_prefix(second, first)


def _row_subject(table: dict[str, Any], row_index: int) -> str | None:
    for cell in sorted(table["cells"], key=lambda c: c["column"]):
        if cell["row"] == row_index and cell["role"] == "data" and cell["text"].strip():
            return cell["text"]
    return None


def _contrast_rows(table: dict[str, Any], selected_rows: set[int]) -> set[int]:
    if table["kind"] == "blank_template":
        return set()
    data_rows = [row["row_index"] for row in table["rows"] if _row_subject(table, row["row_index"]) is not None]
    positions = {row: index for index, row in enumerate(data_rows)}
    result = set()
    for row in selected_rows & set(data_rows):
        subject = _row_subject(table, row)
        if subject is None:
            continue
        for offset in (-1, 1):
            neighbor_index = positions[row] + offset
            if not 0 <= neighbor_index < len(data_rows):
                continue
            neighbor = data_rows[neighbor_index]
            neighbor_subject = _row_subject(table, neighbor)
            if neighbor_subject is not None and _subject_prefix_match(subject, neighbor_subject):
                result.add(neighbor)
    return result


def _projected_text(node: Node, projection: dict[str, Any], owned_text: list[str]) -> str:
    if projection["table_kind"] is None:
        return "".join(owned_text)
    if node.table_json is None:
        raise RetrievalError("SOURCE_BINDING_INVALID")
    table = json.loads(node.table_json)
    if table["kind"] != projection["table_kind"]:
        raise RetrievalError("SOURCE_BINDING_INVALID")
    if table["kind"] == "blank_template":
        cells = sorted((c for c in table["cells"] if c["text"]), key=lambda c: (c["row"], c["column"]))
        if projection["template_grid"] != _grid(table):
            raise RetrievalError("SOURCE_BINDING_INVALID")
    else:
        # Empty data cells contribute separators, but own no characters. Restore
        # their positions from the pinned grid instead of inventing a value.
        cells = sorted((c for c in table["cells"] if c["row"] in projection["table_rows"] and c["role"] == "data"),
                       key=lambda c: (c["row"], c["column"]))
    ranges = [{"owner_kind": "table_cell", "text_owner_id": c["id"], "start": 0, "end": len(c["text"])}
              for c in cells if c["text"]]
    if ranges != projection["canonical_ranges"]:
        raise RetrievalError("SOURCE_BINDING_INVALID")
    return " | ".join(c["text"] for c in cells)


def expand_candidates(hits: tuple[FusedHit, ...], nodes: tuple[Node, ...],
                      identities: tuple[SourceIdentity, ...], registries: Mapping[UUID, SourceRegistry],
                      config: RetrievalConfig | None = None) -> tuple[ExpandedCandidate, ...]:
    config = config or RetrievalConfig()
    by_node = {node.id: node for node in nodes}
    by_parse = {identity.parse_generation_id: identity for identity in identities}
    owners = _owners(nodes)
    owner_lookup = {(owner.node.id, owner.kind, owner.owner_id): owner for owner in owners}
    for source_identity in identities:
        registry = registries.get(source_identity.parse_generation_id)
        if registry is None or (registry.parse_generation_id, registry.document_version_id, registry.source_sha256) != (
                source_identity.parse_generation_id, source_identity.document_version_id, source_identity.source_sha256):
            raise RetrievalError("SOURCE_REGISTRY_INVALID")
        if (registry.artifact_object_id != source_identity.artifact_object_id or
                registry.artifact_sha256 != source_identity.artifact_sha256):
            raise RetrievalError("SOURCE_ARTIFACT_UNVERIFIED")
        attested = {(node_id, kind, owner_id): digest for node_id, kind, owner_id, digest in registry.owner_hashes}
        expected = {}
        for source_owner in owners:
            if source_owner.node.parse_generation_id == source_identity.parse_generation_id:
                request = RegistryOwnerRequest(node_id=source_owner.node.id, owner_kind=source_owner.kind, text_owner_id=source_owner.owner_id)
                expected[(source_owner.node.id, source_owner.kind, source_owner.owner_id)] = canonical_owner_hash(request, source_owner.text, list(source_owner.mappings))
        if attested != expected or len(attested) != len(registry.owner_hashes):
            raise RetrievalError("SOURCE_OWNER_UNVERIFIED")
    output = []
    for hit in hits:
        chunk = hit.chunk
        node = by_node.get(chunk.node_id)
        identity = by_parse.get(chunk.parse_generation_id)
        if node is None or node.parse_generation_id != chunk.parse_generation_id or identity is None or identity.document_version_id != chunk.document_version_id:
            raise RetrievalError("SOURCE_BINDING_INVALID")
        registry = registries[chunk.parse_generation_id]
        projection = json.loads(chunk.projection_json)
        if projection.get("schema_version") != "p04.chunks.v2":
            raise RetrievalError("SOURCE_MAP_UNAVAILABLE")
        ancestry = [node]
        seen = {node.id}
        while ancestry[-1].parent_id:
            parent = by_node.get(ancestry[-1].parent_id)
            if parent is None or parent.id in seen or parent.parse_generation_id != node.parse_generation_id:
                raise RetrievalError("SOURCE_TREE_INVALID")
            seen.add(parent.id)
            ancestry.append(parent)
            if len(ancestry) > config.max_ancestor_depth:
                raise RetrievalError("EXPANSION_LIMIT")
        relevant = [n for n in nodes if n.id in seen or n.parent_id in seen
                    and n.node_type in {"table", "footnote", "editorial_note"}]
        available = tuple(owner for owner in owners if owner.node.parse_generation_id == chunk.parse_generation_id)
        selected: dict[tuple[UUID, Literal["node_body", "table_cell"], str], list[tuple[int, int, str]]] = {}

        def select(owner: _Owner, start: int, end: int, relation: str) -> None:
            selected.setdefault((owner.node.id, owner.kind, owner.owner_id), []).append((start, end, relation))

        owned_text = []
        owned_spans: list[SourceSpan] = []
        for value in projection["canonical_ranges"]:
            owner = owner_lookup.get((node.id, value["owner_kind"], value["text_owner_id"]))
            if owner is None:
                raise RetrievalError("SOURCE_BINDING_INVALID")
            text, spans = _slice(owner, value["start"], value["end"], registry)
            owned_text.append(text)
            owned_spans.extend(spans)
            select(owner, value["start"], value["end"], "template" if projection["table_kind"] == "blank_template" else "hit")
        if _projected_text(node, projection, owned_text) != chunk.source_text or {_span_key(s) for s in _unique_spans(owned_spans)} != {_span_key(s) for s in chunk.source_spans}:
            raise RetrievalError("SOURCE_TEXT_MISMATCH")
        references = list(projection["context_refs"])
        for related in relevant:
            metadata = json.loads(related.metadata_json)
            references.extend(metadata["context_refs"])
            if related.canonical_text and (related.id != node.id or projection["requires_expansion"]):
                owner = owner_lookup[(related.id, "node_body", str(related.id))]
                select(owner, 0, len(owner.text), "note" if related.node_type in {"footnote", "editorial_note"} else "scope")
            if related.table_json:
                table = json.loads(related.table_json)
                if related.id == node.id and projection["table_kind"] != table["kind"]:
                    raise RetrievalError("SOURCE_BINDING_INVALID")
                references.extend(table["context_refs"])
                chosen_rows = set(projection["table_rows"]) if related.id == node.id else {row["row_index"] for row in table["rows"]}
                if related.id == node.id:
                    chosen_rows.update(_contrast_rows(table, set(projection["table_rows"])))
                chosen_cells = set()
                for row in table["rows"]:
                    if row["row_index"] in chosen_rows:
                        references.extend(row["context_refs"])
                        chosen_cells.update(row["cell_ids"])
                for group in table["row_groups"]:
                    if any(group["first_row"] <= row <= group["last_row"] for row in chosen_rows):
                        references.extend(group["context_refs"])
                for cell in table["cells"]:
                    # Headers, units and notes remain attached; all cells of each
                    # selected data row are retained as one legal context unit.
                    if cell["text"] and (cell["id"] in chosen_cells or cell["role"] in {"header", "unit", "note"}):
                        owner = owner_lookup[(related.id, "table_cell", cell["id"])]
                        role = "template" if table["kind"] == "blank_template" else cell["role"] if cell["role"] != "data" else "table"
                        select(owner, 0, len(owner.text), role)
        for ref in references:
            # Optional contexts are also retained if provided. No truncation or
            # dropping of a required condition is used to satisfy a token limit.
            for owner, start, end in _reference_ranges(ref, available, registry):
                select(owner, start, end, ref["role"])
        fragments = []
        for key, intervals in selected.items():
            owner = owner_lookup[key]
            merged: list[list[Any]] = []
            for start, end, relation in sorted(intervals):
                if merged and start <= merged[-1][1]:
                    merged[-1][1] = max(merged[-1][1], end)
                else:
                    merged.append([start, end, relation])
            for start, end, relation in merged:
                text, spans = _slice(owner, start, end, registry)
                table = json.loads(owner.node.table_json) if owner.node.table_json else None
                binding = {"owner_kind": owner.kind, "text_owner_id": owner.owner_id, "start": start, "end": end,
                           "table_kind": table["kind"] if table else None,
                           "table_id": table["id"] if table else None,
                           "cell": next((slot for slot in _grid(table) if slot["cell_id"] == owner.owner_id), None) if table else None,
                           "template_grid": _grid(table) if table and table["kind"] == "blank_template" else []}
                fragments.append(Expansion(chunk, owner.node.id, text, spans, relation, True, canonical_json(binding)))
        if not fragments:
            raise RetrievalError("REQUIRED_CONTEXT_UNRESOLVED")
        output.append(ExpandedCandidate(hit, tuple(fragments)))
    return tuple(output)


def _materialize(snapshot: Snapshot, fragments: list[Expansion], nodes: tuple[Node, ...],
                 identities: tuple[SourceIdentity, ...], config: RetrievalConfig) -> PackedEvidence:
    by_node, by_parse = {n.id: n for n in nodes}, {i.parse_generation_id: i for i in identities}
    units, bindings = [], []
    for index, (text, spans, matches) in enumerate(_coalesced(fragments), 1):
        if index > config.max_units or len(text) > config.max_excerpt_characters or len(spans) > 500:
            raise RetrievalError("CONTEXT_UNIT_LIMIT")
        fragment = matches[0]
        identity, node = by_parse[fragment.chunk.parse_generation_id], by_node[fragment.node_id]
        if identity.artifact_sha256 is None:
            raise RetrievalError("SOURCE_ARTIFACT_UNVERIFIED")
        alias = f"E{index:03}"
        origins = tuple(sorted({match.chunk.id for match in matches}, key=str))
        units.append(EvidenceUnit(evidence_id=alias, document_version_id=identity.document_version_id,
            index_generation_id=fragment.chunk.index_generation_id, canonical_node_id=node.id,
            source_chunk_ids=origins, document_title=identity.document_title,
            structural_path=node.structural_path, excerpt=text, source_spans=spans,
            content_hash=content_hash(text)))
        bindings.append({"evidence_id": alias, "parse_generation_id": str(identity.parse_generation_id),
            "artifact_object_id": str(identity.artifact_object_id), "artifact_sha256": identity.artifact_sha256,
            "relations": sorted({match.relation for match in matches}),
            "owner_ranges": [json.loads(match.owner_ranges_json) for match in matches],
            "source_chunk_semantics": "origin_hit_lineage; ownership is canonical_node and exact mapped ranges"})
    manifest = canonical_json({"run_id": str(snapshot.run_id), "snapshot_id": str(snapshot.snapshot_id),
                               "units": [u.model_dump(mode="json") for u in units], "bindings": bindings})
    digest = content_hash(manifest)
    pack = EvidencePack(pack_id=uuid5(snapshot.run_id, digest), run_id=snapshot.run_id,
                        snapshot_id=snapshot.snapshot_id, units=tuple(units), llm_token_count=0, manifest_hash=digest)
    pack.validate_binding(snapshot.items, node_generations={n.id: n.parse_generation_id for n in nodes},
                          chunk_generations={f.chunk.id: f.chunk.index_generation_id for f in fragments})
    return PackedEvidence(pack, canonical_json(bindings), ())


def _packing_steps(snapshot, candidates, nodes, identities, token_budget, config):
    """Shared packing state; consumers supply measured complete-prompt token counts."""
    if token_budget <= 0:
        raise RetrievalError("CONTEXT_BUDGET_INVALID")
    chosen: list[Expansion] = []
    accepted = _materialize(snapshot, chosen, nodes, identities, config)
    dropped = [candidate.hit.chunk.id for candidate in candidates[config.max_context_candidates:]]
    for candidate in candidates[:config.max_context_candidates]:
        proposed = chosen + list(candidate.fragments)
        try:
            trial = _materialize(snapshot, proposed, nodes, identities, config)
        except RetrievalError as error:
            if error.code != "CONTEXT_UNIT_LIMIT":
                raise
            dropped.append(candidate.hit.chunk.id)
            continue
        tokens = yield trial
        if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
            raise RetrievalError("TOKEN_COUNTER_INVALID")
        if tokens > token_budget:
            dropped.append(candidate.hit.chunk.id)
        else:
            chosen = proposed
            accepted = replace(trial, pack=trial.pack.model_copy(update={"llm_token_count": tokens}))
    if not accepted.pack.units and candidates:
        raise RetrievalError("ATOMIC_CONTEXT_TOO_LARGE")
    return replace(accepted, dropped_candidate_ids=tuple(dropped))


def build_evidence_pack(snapshot: Snapshot, candidates: tuple[ExpandedCandidate, ...], nodes: tuple[Node, ...],
                        identities: tuple[SourceIdentity, ...], *, token_budget: int,
                        count_tokens: Callable[[str], int], config: RetrievalConfig | None = None) -> PackedEvidence:
    """Pure local/test interface; production uses exact role-message async counting."""
    steps = _packing_steps(snapshot, candidates, nodes, identities, token_budget, config or RetrievalConfig())
    try:
        trial = next(steps)
        while True:
            rendering = canonical_json({"units": [u.model_dump(mode="json") for u in trial.pack.units],
                                         "bindings": json.loads(trial.bindings_json)})
            trial = steps.send(count_tokens(rendering))
    except StopIteration as finished:
        return finished.value


async def build_evidence_pack_async(snapshot: Snapshot, candidates: tuple[ExpandedCandidate, ...], nodes: tuple[Node, ...],
                                   identities: tuple[SourceIdentity, ...], *, token_budget: int,
                                   count_evidence: Callable[[EvidencePack, str], Awaitable[int]],
                                   config: RetrievalConfig | None = None) -> PackedEvidence:
    """Count exact complete Drafter messages; bookkeeping is not model input.

    The callback renders the same question/templates/units/bindings used by the
    final role request. token_budget excludes reserved output and safety margin.
    No estimate or body-only fallback is provided on token service failure.
    """
    steps = _packing_steps(snapshot, candidates, nodes, identities, token_budget, config or RetrievalConfig())
    try:
        trial = next(steps)
        while True:
            trial = steps.send(await count_evidence(trial.pack, trial.bindings_json))
    except StopIteration as finished:
        return finished.value
