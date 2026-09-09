"""Admin source purge commands; SQL owns plans, reference checks and the saga."""
from __future__ import annotations

import asyncio
from typing import TypeVar
from uuid import UUID

from psycopg.errors import QueryCanceled
from psycopg.rows import dict_row
from pydantic import BaseModel, ValidationError

from expert_api.auth import Principal
from expert_api.errors import ApiError, database_failures
from expert_clients.settings import Settings
from expert_contracts.auth import ApplicationRole
from expert_contracts.documents import PurgeAccepted, PurgePlan, PurgeRequest
from expert_contracts.errors import ErrorCode
from expert_contracts.purge import PurgeStatus

M = TypeVar("M", bound=BaseModel)


class PurgeService:
    def __init__(self, settings: Settings, pool, *, operation_timeout_seconds: float = 10):
        if not 0 < operation_timeout_seconds <= 30:
            raise ValueError("PURGE_COMMAND_TIMEOUT_INVALID")
        self.settings, self.pool = settings, pool
        self.operation_timeout_seconds = operation_timeout_seconds

    async def _call(self, query: str, parameters: tuple, model: type[M]) -> M:
        with database_failures():
            try:
                async with asyncio.timeout(self.operation_timeout_seconds):
                    async with self.pool.connection() as connection, connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='5s'")
                        async with connection.cursor(row_factory=dict_row) as cursor:
                            await cursor.execute(query, parameters)
                            row = await cursor.fetchone()
                            if row is None or row["value"] is None:
                                raise ApiError(ErrorCode.NOT_FOUND)
                            return model.model_validate(row["value"])
            except (TimeoutError, QueryCanceled):
                raise ApiError(ErrorCode.DEADLINE_EXCEEDED) from None
            except ValidationError:
                raise ApiError(ErrorCode.INTERNAL_ERROR) from None

    async def plan(self, document_id: UUID, principal: Principal) -> PurgePlan:
        principal.require(ApplicationRole.ADMIN)
        value = await self._call("SELECT app.plan_document_purge(%s,%s,%s,%s) AS value", (
            document_id, principal.subject, self.settings.source_purge_retention_seconds,
            self.settings.source_purge_plan_ttl_seconds,
        ), PurgePlan)
        if value.document_id != document_id:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return value

    async def accept(self, document_id: UUID, principal: Principal, payload: PurgeRequest) -> PurgeAccepted:
        principal.require(ApplicationRole.ADMIN)
        value = await self._call("SELECT app.accept_document_purge(%s,%s,%s,%s) AS value", (
            document_id, payload.plan_id, payload.plan_version, principal.subject,
        ), PurgeAccepted)
        if value.document_id != document_id or value.plan_id != payload.plan_id:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return value

    async def status(self, document_id: UUID, plan_id: UUID, principal: Principal) -> PurgeStatus:
        principal.require(ApplicationRole.ADMIN)
        value = await self._call("SELECT app.get_document_purge(%s,%s,%s) AS value", (
            document_id, plan_id, principal.subject,
        ), PurgeStatus)
        if value.document_id != document_id or value.plan_id != plan_id:
            raise ApiError(ErrorCode.INTERNAL_ERROR)
        return value
