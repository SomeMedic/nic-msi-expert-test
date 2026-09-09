"""Bounded, side-effect-free library projections from authoritative PostgreSQL state."""
from __future__ import annotations

import base64
import hashlib
import json
import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from psycopg.rows import dict_row
from pydantic import ValidationError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_contracts.documents import (
    DocumentDetail, DocumentList, DocumentMetadata, DocumentSummary, GenerationSummary, LibraryProfile,
    QualitySummary, VersionDetail, VersionSummary,
)
from expert_contracts.errors import ErrorCode
from expert_contracts.sources import SourceDescriptor

LibraryStatus = Literal["all", "active", "archived", "staging", "failed"]
_PUBLICATION = """CASE WHEN p.id IS NULL THEN NULL ELSE jsonb_build_object(
 'publication_id',p.id,'document_version_id',p.document_version_id,
 'index_generation_id',p.index_generation_id,'published_at',p.published_at,'retired_at',p.retired_at) END"""
_DOCUMENT = f"""SELECT d.id AS document_id,d.canonical_title,d.document_type,d.document_number,d.authority,
 {_PUBLICATION} AS current_publication,
 (SELECT count(*) FROM app.document_versions v WHERE v.logical_document_id=d.id) AS version_count,
 d.created_at,d.archived_at,d.security_revoked_at IS NOT NULL AS security_revoked,
 d.purge_plan_id,d.purged_at,app.document_purge_status(d.id) AS purge_status
 FROM app.logical_documents d LEFT JOIN app.publications p ON p.id=d.current_publication_id"""
_VERSION = """SELECT v.*,d.document_type,d.document_number,d.authority,d.security_revoked_at
 FROM app.document_versions v JOIN app.logical_documents d ON d.id=v.logical_document_id"""
_QUALITY = """jsonb_build_object('status',coalesce(quality_report->>'status',
 CASE WHEN status='failed' THEN 'failed' ELSE 'pending' END),
 'warning_count',coalesce(quality_report->'warning_count','0'::jsonb),
 'diagnostics',coalesce((SELECT jsonb_agg(jsonb_build_object('code',code)) FROM
 (SELECT DISTINCT item->>'code' AS code FROM jsonb_array_elements(coalesce(quality_report->'diagnostics','[]'::jsonb)) item
 WHERE item->>'code' ~ '^[A-Z][A-Z0-9_]{0,99}$' ORDER BY code LIMIT 100) codes),'[]'::jsonb))"""


def encode_cursor(created_at: datetime, identity: UUID, scope: str) -> str:
    payload = [1, created_at.isoformat(), str(identity), hashlib.sha256(scope.encode()).hexdigest()]
    return base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode().rstrip("=")


def decode_cursor(value: str, scope: str) -> tuple[datetime, UUID]:
    try:
        if len(value) > 500 or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
            raise ValueError()
        data = json.loads(base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True))
        if (not isinstance(data, list) or len(data) != 4 or type(data[0]) is not int or data[0] != 1
                or not isinstance(data[1], str) or not isinstance(data[2], str)
                or data[3] != hashlib.sha256(scope.encode()).hexdigest()):
            raise ValueError()
        timestamp = datetime.fromisoformat(data[1])
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError()
        return timestamp, UUID(data[2])
    except (ValueError, TypeError, UnicodeError):
        raise ApiError(ErrorCode.INVALID_REQUEST, details={"field": "cursor"}) from None


def version_summary(row: dict[str, Any]) -> VersionSummary:
    metadata = row["source_metadata"] or {
        "title": row["source_title"], "legal_status": row["legal_status"], "approved_at": row["approved_at"],
        **{key: row[key] for key in ("document_type", "document_number", "authority", "version_label",
                                   "edition_at", "effective_from", "effective_to")},
    }
    return VersionSummary(
        version_id=row["id"], document_id=row["logical_document_id"], metadata=DocumentMetadata.model_validate(metadata),
        publication_status=row["publication_status"], created_at=row["created_at"],
        published_at=row["published_at"], deactivated_at=row["deactivated_at"],
    )


def quality_summary(report: dict[str, Any], status: str) -> QualitySummary:
    # Only reason codes and counts leave this boundary, never diagnostic text.
    codes = sorted({item["code"] for item in report.get("diagnostics", [])
                    if isinstance(item, dict) and isinstance(item.get("code"), str)
                    and re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", item["code"])})[:100]
    warnings = report.get("warning_count", 0)
    quality = report.get("status", "failed" if status == "failed" else "pending")
    if quality == "passed" and warnings:
        quality = "warning"
    return QualitySummary(status=quality, reason_codes=codes, warning_count=warnings)


class LibraryService:
    def __init__(self, pool):
        self.pool = pool

    async def _rows(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(sql, params)
                    return list(await cursor.fetchall())

    async def profile(self, principal: Principal) -> LibraryProfile:
        # One statement gives coherent global counts, independent of any cursor
        # page. Eligibility matches agent.capture_snapshot, including both ready
        # generations. The date describes the currently eligible publications.
        rows = await self._rows("""SELECT
            (SELECT count(*) FROM app.logical_documents) AS logical_document_count,
            (SELECT count(*) FROM app.document_versions) AS version_count,
            count(*) AS eligible_document_count,max(p.published_at) AS last_publication_at
            FROM app.logical_documents d JOIN app.publications p ON p.id=d.current_publication_id
            JOIN app.document_versions v ON v.id=p.document_version_id
            JOIN knowledge.index_generations g ON g.id=p.index_generation_id
            JOIN knowledge.parse_generations pg ON pg.id=g.parse_generation_id
            WHERE d.archived_at IS NULL AND d.security_revoked_at IS NULL
            AND v.legal_status='active' AND v.publication_status='published'
            AND p.retired_at IS NULL AND g.status='ready' AND pg.status='ready'""")
        try:
            return LibraryProfile.model_validate(rows[0])
        except (IndexError, ValidationError):
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def list(self, principal: Principal, *, cursor: str | None = None, limit: int = 50,
                   q: str = "", status: LibraryStatus = "all") -> DocumentList:
        if not 1 <= limit <= 100 or len(q) > 500 or status not in {"all", "active", "archived", "staging", "failed"}:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        q = q.strip()
        scope = json.dumps(["documents", principal.subject, q, status], separators=(",", ":"))
        conditions = ["true"]
        params: list[Any] = []
        if q:
            conditions.append("strpos(lower(d.canonical_title),lower(%s))>0")
            params.append(q)
        if status == "active":
            conditions.append("d.archived_at IS NULL AND d.security_revoked_at IS NULL AND EXISTS("
                              "SELECT 1 FROM app.document_versions v WHERE v.id=p.document_version_id "
                              "AND v.legal_status='active' AND v.publication_status='published' AND p.retired_at IS NULL)")
        elif status == "archived":
            conditions.append("(d.archived_at IS NOT NULL OR EXISTS(SELECT 1 FROM app.document_versions v "
                              "WHERE v.id=coalesce(p.document_version_id,(SELECT latest.id FROM app.document_versions latest "
                              "WHERE latest.logical_document_id=d.id ORDER BY latest.created_at DESC,latest.id DESC LIMIT 1)) "
                              "AND (v.legal_status='archived' "
                              "OR v.publication_status='deactivated')))")
        elif status == "staging":
            conditions.append("EXISTS(SELECT 1 FROM app.document_versions v WHERE v.logical_document_id=d.id "
                              "AND v.publication_status='staging')")
        elif status == "failed":
            conditions.append("EXISTS(SELECT 1 FROM app.document_versions v JOIN LATERAL (SELECT status "
                              "FROM app.ingestion_jobs j WHERE j.version_id=v.id ORDER BY j.created_at DESC,j.id DESC "
                              "LIMIT 1) latest ON true WHERE v.logical_document_id=d.id AND latest.status='failed')")
        if cursor is not None:
            conditions.append("(d.created_at,d.id)<(%s,%s)")
            params.extend(decode_cursor(cursor, scope))
        params.append(limit + 1)
        rows = await self._rows(_DOCUMENT + " WHERE " + " AND ".join(conditions)
                                + " ORDER BY d.created_at DESC,d.id DESC LIMIT %s", tuple(params))
        selected = rows[:limit]
        next_cursor = encode_cursor(selected[-1]["created_at"], selected[-1]["document_id"], scope) if len(rows) > limit else None
        try:
            return DocumentList(items=[DocumentSummary.model_validate(row) for row in selected], next_cursor=next_cursor)
        except ValidationError:
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def document(self, document_id: UUID, principal: Principal, *, versions_cursor: str | None = None,
                       versions_limit: int = 50) -> DocumentDetail:
        if not 1 <= versions_limit <= 100:
            raise ApiError(ErrorCode.INVALID_REQUEST)
        scope = f"versions:{principal.subject}:{document_id}"
        after = decode_cursor(versions_cursor, scope) if versions_cursor is not None else None
        # One repeatable-read snapshot keeps the count, current publication and page coherent.
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(_DOCUMENT + " WHERE d.id=%s", (document_id,))
                    doc = await cursor.fetchone()
                    if doc is None:
                        raise ApiError(ErrorCode.NOT_FOUND)
                    if doc["security_revoked"]:
                        raise ApiError(ErrorCode.SOURCE_REVOKED)
                    clause = " AND (v.created_at,v.id)<(%s,%s)" if after else ""
                    await cursor.execute(_VERSION + " WHERE v.logical_document_id=%s" + clause
                                         + " ORDER BY v.created_at DESC,v.id DESC LIMIT %s",
                                         (document_id, *(after or ()), versions_limit + 1))
                    rows = list(await cursor.fetchall())
        selected = rows[:versions_limit]
        next_cursor = encode_cursor(selected[-1]["created_at"], selected[-1]["id"], scope) if len(rows) > versions_limit else None
        try:
            return DocumentDetail(**doc, versions=[version_summary(row) for row in selected], next_versions_cursor=next_cursor)
        except ValidationError:
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def version(self, version_id: UUID, principal: Principal, *, document_id: UUID | None = None) -> VersionDetail:
        with database_failures():
            async with self.pool.connection() as connection, connection.transaction():
                await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                async with connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute(_VERSION + " WHERE v.id=%s" + (" AND d.id=%s" if document_id else ""),
                                         (version_id, document_id) if document_id else (version_id,))
                    row = await cursor.fetchone()
                    if row is None:
                        raise ApiError(ErrorCode.NOT_FOUND)
                    if row["security_revoked_at"] is not None:
                        raise ApiError(ErrorCode.SOURCE_REVOKED)
                    await cursor.execute(f"SELECT {_PUBLICATION} AS publication FROM app.publications p "
                                         "JOIN app.logical_documents d ON d.current_publication_id=p.id "
                                         "WHERE p.document_version_id=%s", (version_id,))
                    publication = await cursor.fetchone()
                    # Project counts/codes in PG: 100 full diagnostic reports can
                    # otherwise transfer hundreds of MiB into a library request.
                    await cursor.execute(f"""SELECT id AS generation_id,'parse' AS kind,status,created_at,completed_at,
                        node_count,NULL::integer AS chunk_count,NULL::integer AS routing_count,{_QUALITY} AS quality_report
                        FROM knowledge.parse_generations WHERE document_version_id=%s
                        UNION ALL SELECT id,'index',status,created_at,completed_at,NULL,chunk_count,routing_count,NULL
                        FROM knowledge.index_generations WHERE document_version_id=%s
                        ORDER BY created_at DESC,generation_id DESC LIMIT 100""", (version_id, version_id))
                    generations = list(await cursor.fetchall())
                    await cursor.execute("SELECT physical_page_count FROM knowledge.parse_generations "
                                         "WHERE document_version_id=%s AND status='ready' "
                                         "ORDER BY created_at DESC,id DESC LIMIT 1", (version_id,))
                    page = await cursor.fetchone()
        try:
            return VersionDetail(**version_summary(row).model_dump(),
                source=SourceDescriptor(version_id=version_id, source_url=f"/api/v1/versions/{version_id}/source",
                    original_filename=row["original_filename"], media_type="application/pdf", size_bytes=row["content_size"],
                    sha256=row["source_sha256"], page_count=page["physical_page_count"] if page else None),
                current_publication=publication["publication"] if publication else None,
                generations=[GenerationSummary(**{k: v for k, v in generation.items() if k != "quality_report"},
                    quality=quality_summary(generation["quality_report"], generation["status"])
                    if generation["quality_report"] is not None else None) for generation in generations])
        except ValidationError:
            raise ApiError(ErrorCode.INTERNAL_ERROR) from None
