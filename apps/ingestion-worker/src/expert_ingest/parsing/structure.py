"""Generic contextual legal hierarchy, preserving uncertain text as paragraphs."""
from __future__ import annotations

import re
from typing import cast
from uuid import UUID, uuid5

from .dto import CanonicalDocument, CanonicalNode, ContextRef, Diagnostic, Table
from .normalize import append_mapped, content_hash

_HEADING = re.compile(r"^(Раздел|Глава|Статья)\s+([0-9]+(?:\.[0-9]+)*|[IVXLCDM]+)\b", re.IGNORECASE)
_APPENDIX = re.compile(r"^Приложение(?:\s+№?\s*([0-9]+|[А-ЯA-Z]))?(?:\s|$)", re.IGNORECASE)
_CLAUSE = re.compile(r"^(\d{1,4}(?:\.\d{1,4}){0,9})[.)]\s+")
_LETTER = re.compile(r"^([а-яa-z])\)\s+", re.IGNORECASE)
_NOTE = re.compile(r"^(Примечани[ея]|Сноска)(?:\s|[.:])", re.IGNORECASE)


class StructureBuilder:
    def build(self, document: CanonicalDocument) -> CanonicalDocument:
        root_id = uuid5(document.parse_generation_id, "document-root")
        root = CanonicalNode(id=root_id, node_type="document", ordinal=0, title=document.title,
                             page_end=len(document.pages), content_hash=content_hash(""))
        output = [root]
        positions = {root.id: 0}
        scope = root_id
        headings: dict[str, UUID] = {}
        numbered: dict[str, UUID] = {}
        current_clause: UUID | None = None
        diagnostics = list(document.diagnostics)
        raw = {b.block_id: b for b in document.blocks}

        events: list[tuple[int, int, str, object]] = []
        for node in document.nodes:
            first = node.source_spans[0] if node.source_spans else None
            block = raw[first.block_id] if first else None
            events.append((node.page_start, block.ordinal if block else 0, "text", node))
        for table in document.tables:
            first_page = min(table.pdf_pages)
            table_blocks = [raw[s.block_id] for s in table.owned_source_spans
                            if s.block_id in raw and s.pdf_page == first_page]
            events.append((first_page, min((b.ordinal for b in table_blocks), default=0), "table", table))
        events.sort(key=lambda event: (event[0], event[1], event[2]))

        def parent_node(parent_id: UUID) -> CanonicalNode:
            return output[positions[parent_id]]

        def add(node: CanonicalNode, parent: UUID, kind: str, number: str | None = None,
                title: str | None = None):
            ancestor = parent_node(parent)
            path_item = title or (f"{kind} {number}" if number else kind)
            result = node.model_copy(update={"parent_id": parent, "node_type": kind, "number": number,
                                            "title": title, "level": ancestor.level + 1,
                                            "ordinal": len(output),
                                            "structural_path": ancestor.structural_path + (path_item,)})
            positions[result.id] = len(output)
            output.append(result)
            return result

        for _, _, event_kind, value in events:
            if event_kind == "table":
                table = cast(Table, value)
                refs = table.context_refs
                if current_clause:
                    lead = parent_node(current_clause)
                    if lead.own_body and lead.source_spans:
                        refs += (ContextRef(role="scope", text=lead.own_body, source_spans=lead.source_spans),)
                node = CanonicalNode(id=uuid5(document.parse_generation_id, f"table:{table.id}"),
                                     node_type="table", ordinal=0, table=table, context_refs=refs,
                                     page_start=min(table.pdf_pages), page_end=max(table.pdf_pages),
                                     source_spans=table.owned_source_spans, content_hash=content_hash("", table))
                add(node, current_clause or scope, "table")
                continue
            node = cast(CanonicalNode, value)
            text = node.own_body
            appendix = _APPENDIX.match(text)
            heading = _HEADING.match(text)
            clause = _CLAUSE.match(text)
            letter = _LETTER.match(text)
            if appendix:
                result = add(node, root_id, "appendix", appendix.group(1), text[:2000])
                scope, headings, numbered, current_clause = result.id, {}, {}, None
            elif heading:
                kind = {"раздел": "section", "глава": "chapter", "статья": "article"}[heading.group(1).lower()]
                parent = root_id if kind == "section" else headings.get("section", scope)
                if kind == "article":
                    parent = headings.get("chapter", parent)
                result = add(node, parent, kind, heading.group(2), text[:2000])
                headings[kind] = result.id
                if kind == "section":
                    headings.pop("chapter", None)
                    headings.pop("article", None)
                elif kind == "chapter":
                    headings.pop("article", None)
                numbered, current_clause = {}, None
            elif clause:
                number = clause.group(1)
                container = headings.get("article", headings.get("chapter", headings.get("section", scope)))
                dotted_parent = number.rsplit(".", 1)[0] if "." in number else None
                parent = numbered.get(dotted_parent, container) if dotted_parent else container
                if dotted_parent and dotted_parent not in numbered:
                    diagnostics.append(Diagnostic(code="UNRESOLVED_NUMBER_PARENT", severity="warning", pdf_page=node.page_start))
                result = add(node, parent, "subclause" if dotted_parent else "clause", number)
                # Repeated numbering is preserved; it is not guessed to be a continuation.
                if number in numbered:
                    diagnostics.append(Diagnostic(code="REPEATED_NUMBER_IN_SCOPE", severity="warning", pdf_page=node.page_start))
                numbered[number] = result.id
                current_clause = result.id
            elif letter and current_clause:
                add(node, current_clause, "subclause", letter.group(1))
            elif _NOTE.match(text):
                add(node, current_clause or scope, "footnote")
            else:
                previous = output[-1]
                previous_block = raw.get(previous.source_spans[-1].block_id) if previous.source_spans else None
                this_block = raw.get(node.source_spans[0].block_id) if node.source_spans else None
                same_group = (previous_block is not None and this_block is not None
                              and previous_block.group_id is not None
                              and previous_block.group_id == this_block.group_id)
                can_join = (previous.node_type in {"clause", "subclause", "paragraph", "footnote"}
                            and (same_group or previous.id == current_clause)
                            and previous.table is None)
                if can_join:
                    body, mapping = append_mapped(previous.own_body, previous.char_map, text, node.char_map)
                    output[-1] = previous.model_copy(update={"own_body": body, "char_map": mapping,
                                                            "source_spans": previous.source_spans + node.source_spans,
                                                            "page_end": node.page_end, "content_hash": content_hash(body)})
                else:
                    add(node, current_clause or headings.get("article", headings.get("chapter", headings.get("section", scope))),
                        "paragraph")
        return document.model_copy(update={"nodes": tuple(output), "diagnostics": tuple(diagnostics)})
