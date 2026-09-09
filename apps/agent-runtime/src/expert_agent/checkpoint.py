"""Lease-bound adapter for the pinned PostgreSQL saver, using one transaction.

No pool-owned vendor saver, setup, deletion, or sync mutation surface is exposed.
The application migration owns DDL; every vendor read/write is bound to this run
and uses the same connection as the database guard before and after the operation.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Coroutine, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import wraps
from typing import Any
from uuid import UUID

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import ChannelVersions, Checkpoint, CheckpointMetadata, CheckpointTuple, DeltaChannelHistory
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.postgres.base import BasePostgresSaver
from psycopg import Error as PostgresError
from psycopg_pool import AsyncConnectionPool


class ExecutionError(RuntimeError):
    """Allowlisted code only; driver messages and private checkpoint data stay local."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class ExecutionToken:
    run_id: UUID
    owner: UUID
    epoch: int

    def __post_init__(self) -> None:
        if (not isinstance(self.run_id, UUID) or not isinstance(self.owner, UUID)
                or type(self.epoch) is not int or not 1 <= self.epoch < 2**63):
            raise ValueError("Invalid execution token")

    @property
    def parameters(self) -> tuple[UUID, UUID, int]:
        return self.run_id, self.owner, self.epoch


_GUARD_ERRORS = frozenset({"RUN_NOT_FOUND", "STALE_EXECUTION", "CANCEL_REQUESTED", "RUN_NOT_RUNNING",
    "DEADLINE_EXCEEDED", "LEASE_EXPIRED", "SOURCE_REVOKED", "EXECUTION_BINDING_IMMUTABLE",
    "SCHEMA_RETRY_EXHAUSTED", "SCHEMA_RETRY_FORBIDDEN", "REPAIR_EXHAUSTED", "REPAIR_FORBIDDEN",
    "CAPACITY_EXCEEDED", "IDEMPOTENCY_CONFLICT", "STEP_IDENTITY_MISMATCH", "EVIDENCE_PACK_MISMATCH"})


def database_error(error: PostgresError) -> ExecutionError:
    if error.sqlstate in {"40001", "40P01"}:
        return ExecutionError("DATABASE_RETRYABLE")
    code = error.diag.message_primary
    return ExecutionError(code if code in _GUARD_ERRORS else "DATABASE_UNAVAILABLE")


def retry_database[**P, T](operation: Callable[P, Awaitable[T]]) -> Callable[P, Coroutine[Any, Any, T]]:
    """Retry only an entire rolled-back local DB operation, at most three times.

    Callers must not include HTTP/inference inside this boundary. The enclosing
    run deadline and cancellation still bound pool acquisition and backoff.
    """
    @wraps(operation)
    async def call(*args: P.args, **kwargs: P.kwargs) -> T:
        for attempt in range(3):
            try:
                return await operation(*args, **kwargs)
            except ExecutionError as error:
                if error.code != "DATABASE_RETRYABLE":
                    raise
                if attempt == 2:
                    raise ExecutionError("DATABASE_UNAVAILABLE") from None
                await asyncio.sleep(0.01 * (attempt + 1))
        raise ExecutionError("DATABASE_UNAVAILABLE")
    return call


@asynccontextmanager
async def fenced_transaction(pool: AsyncConnectionPool, token: ExecutionToken, *, repeatable_read: bool = False):
    """Hold catalog/run locks only for a bounded local database operation."""
    try:
        async with pool.connection() as connection:
            async with connection.transaction():
                if repeatable_read:
                    await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                await connection.execute("SET LOCAL search_path=agent,pg_catalog,public")
                await connection.execute("SET LOCAL statement_timeout='10s'")
                await connection.execute("SET LOCAL lock_timeout='5s'")
                await connection.execute("SELECT agent.guard_run_write(%s,%s,%s)", token.parameters)
                yield connection
                # A lease/deadline may expire while the local write is in flight.
                # Roll back all checkpoint/blob/pending-write changes in that case.
                await connection.execute("SELECT agent.guard_run_write(%s,%s,%s)", token.parameters)
    except PostgresError as error:
        raise database_error(error) from None


class FencedCheckpointAdapter(BasePostgresSaver):
    """Immutable execution authority is never taken from mutable graph config."""

    def __init__(self, pool: AsyncConnectionPool, token: ExecutionToken):
        super().__init__()
        self._pool, self._token = pool, token

    def _validate(self, config: RunnableConfig | None) -> None:
        values = config.get("configurable", {}) if config is not None else {}
        if values.get("thread_id") != str(self._token.run_id) or values.get("checkpoint_ns", "") != "":
            raise ExecutionError("CHECKPOINT_RUN_MISMATCH")

    @asynccontextmanager
    async def _vendor(self):
        async with fenced_transaction(self._pool, self._token) as connection:
            yield AsyncPostgresSaver(connection, serde=self.serde)

    @retry_database
    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        self._validate(config)
        async with self._vendor() as vendor:
            return await vendor.aget_tuple(config)

    async def alist(self, config: RunnableConfig | None, *, filter: dict[str, Any] | None = None,
                    before: RunnableConfig | None = None, limit: int | None = None) -> AsyncIterator[CheckpointTuple]:
        self._validate(config)
        if before is not None:
            self._validate(before)
        count = 100 if limit is None else limit
        if type(count) is not int or not 1 <= count <= 100:
            raise ExecutionError("CHECKPOINT_LIST_LIMIT_INVALID")
        # Materialize the bounded result before yielding so an idle consumer
        # cannot retain a transaction/run lock across arbitrary application work.
        @retry_database
        async def read():
            async with self._vendor() as vendor:
                return [row async for row in vendor.alist(config, filter=filter, before=before, limit=count)]
        rows = await read()
        for row in rows:
            yield row

    @retry_database
    async def aput(self, config: RunnableConfig, checkpoint: Checkpoint, metadata: CheckpointMetadata,
                   new_versions: ChannelVersions) -> RunnableConfig:
        self._validate(config)
        # The vendor requires an explicit root namespace even though LangGraph
        # read APIs accept a thread-only config. Do not mutate caller state.
        config = {**config, "configurable": {**config["configurable"], "checkpoint_ns": ""}}
        async with self._vendor() as vendor:
            return await vendor.aput(config, checkpoint, metadata, new_versions)

    @retry_database
    async def aput_writes(self, config: RunnableConfig, writes: Sequence[tuple[str, Any]],
                          task_id: str, task_path: str = "") -> None:
        self._validate(config)
        async with self._vendor() as vendor:
            await vendor.aput_writes(config, writes, task_id, task_path)

    @retry_database
    async def aget_delta_channel_history(self, *, config: RunnableConfig,
                                         channels: Sequence[str]) -> Mapping[str, DeltaChannelHistory]:
        self._validate(config)
        async with self._vendor() as vendor:
            return await vendor.aget_delta_channel_history(config=config, channels=channels)
