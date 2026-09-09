"""Source-derived table context for answer-role citation guidance."""
from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
import re
import unicodedata

from expert_contracts.internal import EvidencePack, EvidenceUnit

SOURCE_KEYS = ("parse_generation_id", "artifact_object_id", "artifact_sha256", "source_chunk_semantics")


def table_row_options(pack: EvidencePack, bindings: list[dict], citation_groups: list[dict] | None = None) -> list[dict]:
    """Return same-source table rows as explicit subject/value citation choices."""
    order = {unit.evidence_id: index for index, unit in enumerate(pack.units)}
    units = {unit.evidence_id: unit for unit in pack.units}
    table_indices = _table_indices(bindings)
    citation_groups = table_citation_groups(pack, bindings) if citation_groups is None else citation_groups
    by_row: dict[tuple[tuple[object, ...], tuple[object, object], tuple[str, ...], int], list[tuple[str, dict]]] = {}
    seen_row_cells: set[tuple[object, ...]] = set()

    for binding in bindings:
        evidence_id = binding["evidence_id"]
        unit = units[evidence_id]
        identity = _source_identity(binding, unit)
        for owner in binding["owner_ranges"]:
            table_id = owner.get("table_id")
            table_kind = owner.get("table_kind")
            cell = owner.get("cell")
            if table_id is None or cell is None or table_kind == "blank_template":
                continue
            if cell.get("role") != "data" or cell.get("empty") is True or type(cell.get("row")) is not int:
                continue
            row_key = (identity, (table_id, table_kind), tuple(unit.structural_path), cell["row"])
            cell_key = (*row_key, evidence_id, _cell_identity(cell))
            if cell_key in seen_row_cells:
                continue
            seen_row_cells.add(cell_key)
            by_row.setdefault(row_key, []).append((evidence_id, cell))

    options: list[dict] = []
    for (_identity, table_key, _table_path, row), cells in by_row.items():
        ordered_cells = sorted(cells, key=lambda item: order[item[0]])
        values = [evidence_id for evidence_id, _cell in ordered_cells
                  if _looks_like_numeric_value(units[evidence_id].excerpt.strip())]
        subjects = _row_option_subjects(ordered_cells, values, units, order)
        values = [evidence_id for evidence_id in values if evidence_id not in subjects]
        if subjects and values:
            complete = _complete_row_citation_ids(subjects, values, citation_groups, order)
            options.append({"table_index": table_indices[table_key], "row": row,
                            "subject_evidence_ids": subjects, "value_evidence_ids": values,
                            "complete_row_citation_ids": complete})
    return options


def table_variant_groups(pack: EvidencePack, bindings: list[dict], options: list[dict]) -> list[dict]:
    """Expose strict subject refinements for semantic review, not applicability.

    A shared subject phrase is only a comparison hint. The question and quoted
    row conditions still determine which variants an answer must cover.
    """
    units = {unit.evidence_id: unit for unit in pack.units}
    by_id = {binding["evidence_id"]: binding for binding in bindings}
    table_indices = _table_indices(bindings)
    buckets: dict[tuple, list[tuple[int, tuple[str, ...]]]] = {}
    for index, option in enumerate(options):
        subject_ids = option["subject_evidence_ids"]
        first = units[subject_ids[0]]
        identity = _source_identity(by_id[first.evidence_id], first)
        path = tuple(first.structural_path)
        evidence_ids = [*subject_ids, *option["value_evidence_ids"]]
        if any(_source_identity(by_id[eid], units[eid]) != identity
               or tuple(units[eid].structural_path) != path for eid in evidence_ids):
            continue
        columns: set[int] = set()
        for evidence_id in option["value_evidence_ids"]:
            for owner in by_id[evidence_id]["owner_ranges"]:
                cell = owner.get("cell")
                table_key = (owner.get("table_id"), owner.get("table_kind"))
                if (cell and cell.get("role") == "data" and cell.get("row") == option["row"]
                        and table_indices.get(table_key) == option["table_index"]):
                    columns.update(_cell_columns(cell))
        if not columns:
            continue
        text = " ".join(units[eid].excerpt for eid in subject_ids)
        normalized = unicodedata.normalize("NFKC", text).casefold().replace("ё", "е")
        words = tuple(re.findall(r"\w+", normalized))
        if len(words) >= 2:
            key = (identity, path, option["table_index"], tuple(sorted(columns)))
            buckets.setdefault(key, []).append((index, words))

    groups: list[dict] = []
    for rows in buckets.values():
        covered: set[int] = set()
        for base_index, base_words in sorted(rows, key=lambda item: (len(item[1]), item[0])):
            if base_index in covered:
                continue
            refined = [index for index, words in rows if len(words) > len(base_words)
                       and any(words[start:start + len(base_words)] == base_words
                               for start in range(len(words) - len(base_words) + 1))]
            if refined:
                members = sorted([base_index, *refined])
                groups.append({"base_row_option_index": base_index, "row_option_indices": members})
                covered.update(members)
    return sorted(groups, key=lambda group: group["base_row_option_index"])


def _row_option_subjects(cells: list[tuple[str, dict]], values: list[str], units: dict[str, EvidenceUnit],
                         order: dict[str, int]) -> list[str]:
    subjects: set[str] = set()
    for value_id, value_cell in cells:
        if value_id not in values:
            continue
        row = value_cell.get("row")
        columns = _cell_columns(value_cell)
        if type(row) is int and columns:
            subjects.update(_same_row_subjects(cells, value_id, row, columns, units))
    return _pack_ordered(subjects, order)


def _complete_row_citation_ids(subjects: list[str], values: list[str], citation_groups: list[dict],
                               order: dict[str, int]) -> list[str]:
    required = {*subjects, *values}
    value_ids = set(values)
    for group in citation_groups:
        if value_ids & set(group.get("trigger_evidence_ids", [])):
            required.update(group.get("required_evidence_ids", []))
    leading = list(dict.fromkeys([*subjects, *values]))
    context = [evidence_id for evidence_id in _pack_ordered(required, order) if evidence_id not in set(leading)]
    return [*leading, *context]


def table_citation_groups(pack: EvidencePack, bindings: list[dict]) -> list[dict]:
    """Return per-data-cell source context that must be cited with table values.

    The groups are deterministic guidance derived only from authenticated source
    ownership. They do not change generated citations and never add neighboring
    numeric/data values as required context for a trigger cell.
    """
    order = {unit.evidence_id: index for index, unit in enumerate(pack.units)}
    units = {unit.evidence_id: unit for unit in pack.units}
    table_indices = _table_indices(bindings)
    by_table: dict[tuple[tuple[object, ...], tuple[object, object], tuple[str, ...]], list[tuple[str, dict]]] = {}
    seen_table_cells: set[tuple[object, ...]] = set()
    scopes: dict[tuple[object, ...], list[str]] = {}

    for binding in bindings:
        evidence_id = binding["evidence_id"]
        unit = units[evidence_id]
        identity = _source_identity(binding, unit)
        for owner in binding["owner_ranges"]:
            table_id = owner.get("table_id")
            table_kind = owner.get("table_kind")
            cell = owner.get("cell")
            if table_id is not None and cell is not None and table_kind != "blank_template":
                source_table_key = (identity, (table_id, table_kind), tuple(unit.structural_path))
                cell_key = (*source_table_key, evidence_id, _cell_identity(cell))
                if cell_key in seen_table_cells:
                    continue
                seen_table_cells.add(cell_key)
                by_table.setdefault(source_table_key, []).append((evidence_id, cell))
            elif "scope" in binding.get("relations", ()) and evidence_id not in scopes.setdefault(identity, []):
                scopes[identity].append(evidence_id)

    groups: list[dict] = []
    for (identity, table_key, table_path), cells in by_table.items():
        data_cells = [(evidence_id, cell) for evidence_id, cell in cells
                      if cell.get("role") == "data" and cell.get("empty") is not True]
        for trigger_id, data_cell in sorted(data_cells, key=lambda item: order[item[0]]):
            data_row = data_cell.get("row")
            columns = _cell_columns(data_cell)
            if type(data_row) is not int or not columns:
                continue
            headers = _applicable_preceding_headers(cells, data_row, columns)
            row_subjects = _same_row_subjects(cells, trigger_id, data_row, columns, units)
            matching_scopes = _matching_scopes(scopes.get(identity, []), table_path, units)
            required = _pack_ordered((*(evidence_id for evidence_id, _ in headers), *row_subjects, *matching_scopes),
                                     order)
            if required:
                groups.append({"group_id": f"G{len(groups) + 1:03}", "kind": "table_row_context",
                               "table_index": table_indices[table_key], "row": data_row,
                               "trigger_evidence_ids": [trigger_id],
                               "row_subject_evidence_ids": row_subjects,
                               "required_evidence_ids": required})
    return groups


def _pack_ordered(evidence_ids: Iterable[str], order: dict[str, int]) -> list[str]:
    return sorted(set(evidence_ids), key=order.__getitem__)


def _cell_identity(cell: dict) -> tuple[object, ...]:
    copied = deepcopy(cell)
    return tuple(_freeze(copied))


def _freeze(value):
    if isinstance(value, dict):
        return tuple((key, _freeze(item)) for key, item in sorted(value.items()))
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _table_indices(bindings: Iterable[dict]) -> dict[tuple[object, object], int]:
    indices: dict[tuple[object, object], int] = {}
    for binding in bindings:
        for owner in binding["owner_ranges"]:
            table_id = owner.get("table_id")
            if table_id is not None:
                key = (table_id, owner.get("table_kind"))
                if key not in indices:
                    indices[key] = len(indices)
    return indices


def _source_identity(binding: dict, unit: EvidenceUnit) -> tuple[object, ...]:
    return (*tuple(binding.get(key) for key in SOURCE_KEYS), str(unit.document_version_id), str(unit.index_generation_id))


def _applicable_preceding_headers(cells: list[tuple[str, dict]], data_row: int,
                                  columns: set[int]) -> list[tuple[str, dict]]:
    return [(evidence_id, cell) for evidence_id, cell in cells
            if (cell.get("role") in {"header", "unit"}
                and type(cell.get("row")) is int
                and cell["row"] < data_row
                and _intersects(columns, cell))]


def _same_row_subjects(cells: list[tuple[str, dict]], trigger_id: str, data_row: int, columns: set[int],
                       units: dict[str, EvidenceUnit]) -> list[str]:
    subjects: list[str] = []
    min_column = min(columns)
    for evidence_id, cell in cells:
        if evidence_id == trigger_id or cell.get("role") != "data" or cell.get("empty") is True:
            continue
        cell_columns = _cell_columns(cell)
        if not cell_columns or max(cell_columns) >= min_column:
            continue
        cell_row = cell.get("row")
        row_span = cell.get("row_span", 1)
        if type(cell_row) is not int or type(row_span) is not int or row_span <= 0:
            continue
        if not (cell_row <= data_row < cell_row + row_span):
            continue
        text = units[evidence_id].excerpt.strip()
        if text and _looks_like_textual_subject(text) and evidence_id not in subjects:
            subjects.append(evidence_id)
    return subjects


def _matching_scopes(scope_ids: list[str], table_path: tuple[str, ...], units: dict[str, EvidenceUnit]) -> list[str]:
    matching = [evidence_id for evidence_id in scope_ids
                if _is_path_prefix(list(units[evidence_id].structural_path), list(table_path))]
    if not matching:
        return []
    depth = max(len(units[evidence_id].structural_path) for evidence_id in matching)
    return [evidence_id for evidence_id in matching if len(units[evidence_id].structural_path) == depth]


def _cell_columns(cell: dict) -> set[int]:
    column = cell.get("column")
    span = cell.get("column_span", 1)
    return set(range(column, column + span)) if type(column) is int and type(span) is int and span > 0 else set()


def _intersects(columns: set[int], cell: dict) -> bool:
    return bool(columns & _cell_columns(cell))


def _is_path_prefix(prefix: list[str], path: list[str]) -> bool:
    return len(prefix) < len(path) and path[:len(prefix)] == prefix


def _looks_like_textual_subject(text: str) -> bool:
    normalized = text.strip().lower()
    if not normalized or normalized[0].isdigit() or normalized.startswith(("не более", "не менее", "до ", "от ", "свыше ")):
        return False
    return any(char.isalpha() for char in normalized)


def _looks_like_numeric_value(text: str) -> bool:
    return any(char.isdigit() for char in text)
