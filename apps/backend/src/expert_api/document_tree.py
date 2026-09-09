"""Bounded public views of one immutable parse; no parser runtime or artifact access."""
from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager
import hashlib
import json
import re
from typing import Any
from uuid import UUID

from psycopg.errors import QueryCanceled
from psycopg.rows import dict_row
from pydantic import ValidationError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.library import quality_summary
from expert_contracts.auth import ApplicationRole
from expert_contracts.errors import ErrorCode
from expert_contracts.structure import CanonicalTreePage, ParseQualityPage, StructuredTablePage

VIEW_MAX_BYTES = 8 * 1024 * 1024


def view_scope(kind: str, principal: Principal, version_id: UUID, generation_id: UUID, target: UUID | None = None) -> str:
    return hashlib.sha256(json.dumps([kind, principal.subject, str(version_id), str(generation_id),
                                     str(target) if target else None], separators=(",", ":")).encode()).hexdigest()


def encode_position(scope: str, ordinal: int, identity: UUID | None = None) -> str:
    value = [1, scope, ordinal, str(identity) if identity else None]
    return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).decode().rstrip("=")


def decode_position(value: str, scope: str, *, with_identity: bool = False) -> tuple[int, UUID | None]:
    try:
        if len(value) > 500 or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError()
        data = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if (not isinstance(data, list) or len(data) != 4 or type(data[0]) is not int or data[0] != 1
                or data[1] != scope or type(data[2]) is not int or not 0 <= data[2] <= 2_147_483_647):
            raise ValueError()
        if with_identity:
            if not isinstance(data[3], str):
                raise ValueError()
            identity = UUID(data[3])
        else:
            if data[3] is not None:
                raise ValueError()
            identity = None
        return data[2], identity
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "cursor"}) from None


def _limit(value: int, maximum: int) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "limit"})


def _size_error() -> ApiError:
    return ApiError(ErrorCode.SIZE_LIMIT_EXCEEDED, details={"max_bytes": VIEW_MAX_BYTES})


def _distinct(items: list[dict[str, Any]], maximum: int = 1000) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = json.dumps(item, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        if key not in seen:
            seen.add(key)
            result.append(item)
            if len(result) > maximum:
                raise _size_error()
    return result


def project_table(table: dict[str, Any], *, version_id: UUID, generation_id: UUID, node_id: UUID,
                  start: int, limit: int, scope: str) -> StructuredTablePage:
    """Preserve an entire row window and its required context, or reject its size.

    Cells spanning the window retain their original anchor row. Rows only own
    cells anchored there; inherited headers and notes remain explicit context.
    """
    _limit(limit, 50)
    total = max(cell["row"] + cell["row_span"] for cell in table["cells"])
    if type(start) is not int or not 0 <= start < total:
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "cursor"})
    end = min(total - 1, start + limit - 1)
    chosen = [cell for cell in table["cells"] if cell["row"] <= end and cell["row"] + cell["row_span"] > start]
    if len(chosen) > 1000:
        raise _size_error()
    chosen.sort(key=lambda cell: (cell["row"], cell["column"], cell["id"]))
    cells = []
    for cell in chosen:
        value = {key: cell[key] for key in ("id", "row", "column", "row_span", "column_span", "role", "text", "pdf_page", "bbox")}
        value["source_spans"] = _distinct([span for segment in cell["char_map"] for span in segment["source_spans"]])
        cells.append(value)
    source_rows = {row["row_index"]: row for row in table["rows"]}
    rows = []
    # A limit is an engineering memory/transport cap, never a text truncation rule.
    encoded_size = len(json.dumps(cells, ensure_ascii=False).encode())
    for index in range(start, end + 1):
        original = source_rows.get(index, {"cell_ids": [], "context_refs": []})
        context = list(original["context_refs"])
        for group in table["row_groups"]:
            if group["first_row"] <= index <= group["last_row"]:
                context.extend(group["context_refs"])
        row = {"row_index": index, "cell_ids": original["cell_ids"], "context_refs": _distinct(context)}
        encoded_size += len(json.dumps(row, ensure_ascii=False).encode())
        if encoded_size > VIEW_MAX_BYTES:
            raise _size_error()
        rows.append(row)
    payload = {
        "version_id": version_id, "parse_generation_id": generation_id, "node_id": node_id,
        "table_id": table["id"], "kind": table["kind"], "column_count": table["column_count"],
        "total_rows": total, "row_start": start, "row_end": end, "rows": rows, "cells": cells,
        "context_refs": table["context_refs"], "pdf_pages": table["pdf_pages"],
        "page_segments": [segment for segment in table["page_segments"]
                          if segment["first_row"] <= end and segment["last_row"] >= start],
        "source_url": f"/api/v1/versions/{version_id}/source",
        "next_cursor": encode_position(scope, end + 1) if end + 1 < total else None,
    }
    if len(json.dumps(payload, default=str, separators=(",", ":"), ensure_ascii=False).encode()) > VIEW_MAX_BYTES:
        raise _size_error()
    return StructuredTablePage.model_validate(payload)


class DocumentTreeService:
    def __init__(self, pool, *, operation_timeout_seconds: float = 10):
        if not 0 < operation_timeout_seconds <= 30:
            raise ValueError("DOCUMENT_VIEW_TIMEOUT_INVALID")
        self.pool = pool
        self.operation_timeout_seconds = operation_timeout_seconds
        self.statement_timeout_ms = min(5000, max(1, int(operation_timeout_seconds * 1000)))

    @asynccontextmanager
    async def _read(self, version_id: UUID, generation_id: UUID, principal: Principal, *, quality: bool = False):
        principal.require(ApplicationRole.VIEWER)
        with database_failures():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    async with self.pool.connection() as connection, connection.transaction():
                        await connection.execute("SELECT set_config('statement_timeout',%s,true)", (str(self.statement_timeout_ms),))
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute("""SELECT g.status,d.security_revoked_at,d.purge_plan_id FROM knowledge.parse_generations g
                                JOIN app.document_versions v ON v.id=g.document_version_id AND v.source_sha256=g.source_sha256
                                JOIN app.logical_documents d ON d.id=v.logical_document_id WHERE v.id=%s AND g.id=%s""",
                                (version_id, generation_id))
                            row = await cursor.fetchone()
                            if row is None:
                                raise ApiError(ErrorCode.NOT_FOUND)
                            if row["security_revoked_at"] is not None:
                                raise ApiError(ErrorCode.SOURCE_REVOKED)
                            if row.get("purge_plan_id") is not None:
                                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
                            if row["status"] not in (("ready", "failed") if quality else ("ready",)):
                                raise ApiError(ErrorCode.GENERATION_INVALID)
                            yield cursor
                            # READ COMMITTED gives this final query fresh revocation
                            # state; generation content itself is immutable.
                            await cursor.execute("""SELECT d.security_revoked_at,d.purge_plan_id FROM app.document_versions v
                                JOIN app.logical_documents d ON d.id=v.logical_document_id WHERE v.id=%s""", (version_id,))
                            current = await cursor.fetchone()
                            if current is None:
                                raise ApiError(ErrorCode.NOT_FOUND)
                            if current["security_revoked_at"] is not None:
                                raise ApiError(ErrorCode.SOURCE_REVOKED)
                            if current.get("purge_plan_id") is not None:
                                raise ApiError(ErrorCode.SOURCE_UNAVAILABLE)
            except (TimeoutError, QueryCanceled):
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
            except (ValidationError, ValueError, KeyError, TypeError):
                raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def tree(self, version_id: UUID, generation_id: UUID, principal: Principal, *, parent_id: UUID | None = None,
                   cursor: str | None = None, limit: int = 50) -> CanonicalTreePage:
        _limit(limit, 100)
        scope = view_scope("tree", principal, version_id, generation_id, parent_id)
        position = decode_position(cursor, scope, with_identity=True) if cursor is not None else None
        async with self._read(version_id, generation_id, principal) as query:
            if parent_id is not None:
                await query.execute("SELECT id FROM knowledge.document_nodes WHERE parse_generation_id=%s AND id=%s", (generation_id, parent_id))
                if await query.fetchone() is None:
                    raise ApiError(ErrorCode.NOT_FOUND)
            conditions = ""
            params: list[Any] = [generation_id, parent_id]
            if position:
                conditions = "AND (n.ordinal,n.id)>(%s,%s)"
                params.extend(position)
            params.append(limit + 1)
            await query.execute(f"""SELECT n.id AS node_id,n.parent_id,n.node_type,n.ordinal,n.number,n.title,n.page_start,n.page_end,
                EXISTS(SELECT 1 FROM knowledge.document_nodes child WHERE child.parse_generation_id=n.parse_generation_id
                    AND child.parent_id=n.id) AS has_children FROM knowledge.document_nodes n
                WHERE n.parse_generation_id=%s AND n.parent_id IS NOT DISTINCT FROM %s {conditions}
                ORDER BY n.ordinal,n.id LIMIT %s""", params)
            values = list(await query.fetchall())
            page = values[:limit]
            result = CanonicalTreePage(version_id=version_id, parse_generation_id=generation_id, parent_id=parent_id,
                items=page, next_cursor=encode_position(scope, page[-1]["ordinal"], page[-1]["node_id"]) if len(values) > limit else None)
        return result

    async def quality(self, version_id: UUID, generation_id: UUID, principal: Principal, *, cursor: str | None = None,
                      limit: int = 50) -> ParseQualityPage:
        _limit(limit, 100)
        scope = view_scope("quality", principal, version_id, generation_id)
        after = decode_position(cursor, scope)[0] if cursor is not None else 0
        async with self._read(version_id, generation_id, principal, quality=True) as query:
            await query.execute("""SELECT status,jsonb_build_object('status',coalesce(quality_report->>'status',
                CASE WHEN status='failed' THEN 'failed' ELSE 'pending' END),
                'warning_count',coalesce(quality_report->'warning_count','0'::jsonb),
                'diagnostics',coalesce((SELECT jsonb_agg(jsonb_build_object('code',code)) FROM
                    (SELECT DISTINCT item->>'code' AS code FROM jsonb_array_elements(coalesce(quality_report->'diagnostics','[]'::jsonb)) item
                     WHERE item->>'code' ~ '^[A-Z][A-Z0-9_]{0,79}$' ORDER BY code LIMIT 100) codes),'[]'::jsonb)) AS report
                FROM knowledge.parse_generations WHERE id=%s""", (generation_id,))
            meta = await query.fetchone()
            await query.execute("""SELECT ordinal,item->>'code' AS code,item->>'severity' AS severity,
                (item->>'pdf_page')::integer AS pdf_page,item->>'block_id' AS block_id,item->'bbox' AS bbox
                FROM knowledge.parse_generations g,
                LATERAL jsonb_array_elements(coalesce(g.quality_report->'diagnostics','[]'::jsonb)) WITH ORDINALITY AS d(item,ordinal)
                WHERE g.id=%s AND ordinal>%s ORDER BY ordinal LIMIT %s""", (generation_id, after, limit + 1))
            values = list(await query.fetchall())
            page = values[:limit]
            result = ParseQualityPage(version_id=version_id, parse_generation_id=generation_id,
                summary=quality_summary(meta["report"], meta["status"]), items=page,
                next_cursor=encode_position(scope, page[-1]["ordinal"]) if len(values) > limit else None,
                source_url=f"/api/v1/versions/{version_id}/source")
        return result

    async def table(self, version_id: UUID, generation_id: UUID, node_id: UUID, principal: Principal, *, cursor: str | None = None,
                    limit: int = 25) -> StructuredTablePage:
        _limit(limit, 50)
        scope = view_scope("table", principal, version_id, generation_id, node_id)
        start = decode_position(cursor, scope)[0] if cursor is not None else 0
        async with self._read(version_id, generation_id, principal) as query:
            # SQL009 already bounds one accepted node batch to 8MiB. The read
            # guard also protects this boundary from legacy or privileged data.
            await query.execute("""SELECT CASE WHEN octet_length(table_data::text)<=%s THEN table_data ELSE NULL END AS body
                FROM knowledge.document_nodes WHERE parse_generation_id=%s AND id=%s AND node_type='table' AND table_data IS NOT NULL""",
                (VIEW_MAX_BYTES, generation_id, node_id))
            row = await query.fetchone()
            if row is None:
                raise ApiError(ErrorCode.NOT_FOUND)
            if row["body"] is None:
                raise _size_error()
            result = project_table(row["body"], version_id=version_id, generation_id=generation_id,
                node_id=node_id, start=start, limit=limit, scope=scope)
        return result
