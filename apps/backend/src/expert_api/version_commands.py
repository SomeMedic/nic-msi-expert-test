"""Operator publication commands through reviewed PostgreSQL routines only."""
from __future__ import annotations

import asyncio
from uuid import UUID

from psycopg.errors import QueryCanceled
from psycopg.rows import dict_row
from pydantic import ValidationError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_api.library import _DOCUMENT, version_summary
from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import (
    ArchiveDocumentRequest, DeactivateRequest, DocumentSummary, PublicationInfo, PublishRequest, VersionSummary,
)
from expert_contracts.errors import ErrorCode


class VersionCommandService:
    def __init__(self, pool, *, operation_timeout_seconds: float = 10):
        if not 0 < operation_timeout_seconds <= 30:
            raise ValueError("VERSION_COMMAND_TIMEOUT_INVALID")
        self.pool = pool
        self.operation_timeout_seconds = operation_timeout_seconds
        self.statement_timeout_ms = min(5000, max(1, int(operation_timeout_seconds * 1000)))

    async def publish(self, version_id: UUID, principal: Principal, payload: PublishRequest) -> PublicationInfo:
        principal.require(ApplicationRole.OPERATOR)
        with database_failures():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    async with self.pool.connection() as connection, connection.transaction():
                        await connection.execute("SELECT set_config('statement_timeout',%s,true)", (str(self.statement_timeout_ms),))
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute("SELECT app.publish_version(%s,%s,%s,%s,NULL,NULL,NULL) AS publication_id",
                                (version_id, payload.index_generation_id, payload.expected_current_publication_id, payload.operation_id))
                            result = await cursor.fetchone()
                            await cursor.execute("""SELECT p.id AS publication_id,p.document_version_id,p.index_generation_id,
                                p.published_at,p.retired_at,d.security_revoked_at FROM app.publications p
                                JOIN app.document_versions v ON v.id=p.document_version_id
                                JOIN app.logical_documents d ON d.id=v.logical_document_id
                                WHERE p.id=%s AND v.id=%s""", (result["publication_id"], version_id))
                            row = await cursor.fetchone()
                            if row is None:
                                raise ApiError(ErrorCode.INTERNAL_ERROR)
                            # publish_version owns the catalog lock until this TX
                            # commits. Its replay branch precedes revoke validation,
                            # so even an old operation replay must pass this check.
                            if row.pop("security_revoked_at") is not None:
                                raise ApiError(ErrorCode.SOURCE_REVOKED)
                            return PublicationInfo.model_validate(row)
            except (TimeoutError, QueryCanceled):
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
            except ValidationError:
                raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def deactivate(self, version_id: UUID, principal: Principal, payload: DeactivateRequest) -> VersionSummary:
        principal.require(ApplicationRole.OPERATOR)
        with database_failures():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    async with self.pool.connection() as connection, connection.transaction():
                        await connection.execute("SELECT set_config('statement_timeout',%s,true)", (str(self.statement_timeout_ms),))
                        await connection.execute("SELECT app.deactivate_version(%s,%s,%s)", (version_id, principal.subject, payload.reason))
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute("""SELECT v.*,d.document_type,d.document_number,d.authority
                                FROM app.document_versions v JOIN app.logical_documents d ON d.id=v.logical_document_id
                                WHERE v.id=%s""", (version_id,))
                            row = await cursor.fetchone()
                            if row is None:
                                raise ApiError(ErrorCode.INTERNAL_ERROR)
                            return version_summary(row)
            except (TimeoutError, QueryCanceled):
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
            except ValidationError:
                raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def archive(self, document_id: UUID, principal: Principal,
                      payload: ArchiveDocumentRequest) -> DocumentSummary:
        principal.require(ApplicationRole.OPERATOR)
        with database_failures():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    async with self.pool.connection() as connection, connection.transaction():
                        await connection.execute("SELECT set_config('statement_timeout',%s,true)",
                                                 (str(self.statement_timeout_ms),))
                        await connection.execute("SELECT app.archive_document(%s,%s,%s,%s,%s)", (
                            document_id, principal.subject, payload.expected_current_publication_id,
                            payload.operation_id, payload.reason,
                        ))
                        # The routine retains catalog/document locks until commit;
                        # this response belongs to that same atomic command.
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute(_DOCUMENT + " WHERE d.id=%s", (document_id,))
                            row = await cursor.fetchone()
                            if row is None:
                                raise ApiError(ErrorCode.INTERNAL_ERROR)
                            if row["security_revoked"]:
                                raise ApiError(ErrorCode.SOURCE_REVOKED)
                            return DocumentSummary.model_validate(row)
            except (TimeoutError, QueryCanceled):
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
            except ValidationError:
                raise ApiError(ErrorCode.INTERNAL_ERROR) from None
