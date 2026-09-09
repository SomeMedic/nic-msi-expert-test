"""Bounded durable execution coordination; graph/model work is an injected callback.

PostgreSQL owns every run, lease, deadline and terminal outcome. A process owns
one local slot until its callback and active HTTP requests have drained. Shutdown
leaves an unfinished lease for checkpoint recovery rather than inventing a result.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
import logging
from typing import Any
from uuid import UUID, uuid4, uuid5

from psycopg import AsyncConnection, Error as PostgresError
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from expert_clients.settings import ConfigurationError, Settings
from expert_contracts.internal import ExecutionStatus, StartRunRequest, StartRunResponse

from .checkpoint import ExecutionError, ExecutionToken

logger = logging.getLogger(__name__)
TERMINAL = frozenset({"completed", "refused", "failed", "cancelled"})
_ERRORS = frozenset({"DEPENDENCY_UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL_ERROR", "MODEL_UNAVAILABLE",
    "MODEL_TIMEOUT", "CAPACITY_EXCEEDED", "OUTPUT_SCHEMA_INVALID", "TOKEN_LIMIT_EXCEEDED",
    "SOURCE_REVOKED", "SOURCE_UNAVAILABLE", "GENERATION_INVALID"})
_CONTROL = frozenset({"RUN_NOT_FOUND", "STALE_EXECUTION", "LEASE_EXPIRED", "CANCEL_REQUESTED",
                      "RUN_NOT_RUNNING", "TERMINAL_CONFLICT"})
_READ = """SELECT id,status,current_stage,execution_epoch,execution_owner,lease_until,heartbeat_at,
 cancel_requested_at,configuration_fingerprint,principal_id,
 extract(epoch FROM deadline_at-clock_timestamp())::double precision AS remaining_seconds,
 coalesce(lease_until>clock_timestamp(),false) AS lease_live FROM agent.runs WHERE id=%s"""


def _safe_code(error: BaseException) -> str:
    if isinstance(error, TimeoutError):
        return "DEADLINE_EXCEEDED"
    code = getattr(error, "code", None)
    if not isinstance(code, str):
        return "INTERNAL_ERROR"
    code = {"DATABASE_UNAVAILABLE": "DEPENDENCY_UNAVAILABLE", "RUN_DEADLINE_EXCEEDED": "DEADLINE_EXCEEDED",
            "RUN_CANCELLED": "CANCEL_REQUESTED", "RETRIEVAL_DATABASE_UNAVAILABLE": "DEPENDENCY_UNAVAILABLE",
            "DEPENDENCY_TIMEOUT": "DEPENDENCY_UNAVAILABLE", "MODEL_PROFILE_MISMATCH": "GENERATION_INVALID",
            "INDEX_RECIPE_MISMATCH": "GENERATION_INVALID", "SOURCE_REGISTRY_INVALID": "SOURCE_UNAVAILABLE",
            "SOURCE_ARTIFACT_CHANGED": "SOURCE_UNAVAILABLE"}.get(code, code)
    return code if code in _ERRORS | _CONTROL else "INTERNAL_ERROR"


async def _drain(*tasks: asyncio.Future) -> None:
    """Cancellation does not release a slot while child cleanup is still running."""
    for task in tasks:
        if not task.done():
            task.cancel()
    joined = asyncio.gather(*tasks, return_exceptions=True)
    interrupted = False
    while not joined.done():
        try:
            await asyncio.shield(joined)
        except asyncio.CancelledError:
            interrupted = True
    if interrupted:
        raise asyncio.CancelledError


class _AdmissionPermit:
    """One session lock on an idle autocommit connection, never a long transaction."""

    def __init__(self, pool: AsyncConnectionPool, connection: AsyncConnection):
        self.pool, self.connection = pool, connection

    async def check(self) -> None:
        try:
            async with asyncio.timeout(3):
                await self.connection.execute("SELECT 1")
        except (PostgresError, TimeoutError):
            raise ExecutionError("DEPENDENCY_UNAVAILABLE") from None

    async def release(self) -> None:
        try:
            async with asyncio.timeout(3):
                async with self.connection.cursor(row_factory=dict_row) as cursor:
                    await cursor.execute("SELECT pg_advisory_unlock(92608,5) AS released")
                    row = await cursor.fetchone()
                    if row is None or not row["released"]:
                        await self.connection.close()
        except (PostgresError, TimeoutError):
            # A session with uncertain lock state cannot reenter the pool.
            await self.connection.close()
        finally:
            await self.pool.putconn(self.connection)


async def _release(permit: _AdmissionPermit) -> None:
    task = asyncio.create_task(permit.release())
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
    task.result()
    if interrupted:
        raise asyncio.CancelledError


class _RunStore:
    """Short, role-restricted SQL operations; never holds a lock during inference."""

    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    async def permit(self) -> _AdmissionPermit:
        connection = None
        try:
            async with asyncio.timeout(5):
                connection = await self.pool.getconn()
                if not connection.autocommit or connection.info.transaction_status != TransactionStatus.IDLE:
                    raise ExecutionError("DEPENDENCY_UNAVAILABLE")
                row = await self._row(connection, "SELECT pg_try_advisory_lock(92608,5) AS acquired")
                if row is None or not row["acquired"]:
                    await self.pool.putconn(connection)
                    connection = None
                    raise ExecutionError("CAPACITY_EXCEEDED")
                return _AdmissionPermit(self.pool, connection)
        except BaseException as error:
            if connection is not None:
                # Covers cancellation after the server took the lock but before
                # the caller received its result. Closing releases session locks.
                await connection.close()
                await self.pool.putconn(connection)
            if isinstance(error, (PostgresError, TimeoutError)):
                raise ExecutionError("DEPENDENCY_UNAVAILABLE") from None
            raise

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncConnection]:
        try:
            async with asyncio.timeout(5):
                async with self.pool.connection() as connection:
                    async with connection.transaction():
                        await connection.execute("SET LOCAL statement_timeout='3s'")
                        await connection.execute("SET LOCAL lock_timeout='2s'")
                        yield connection
        except PostgresError as error:
            code = error.diag.message_primary
            raise ExecutionError(code if code in _ERRORS | _CONTROL else "DEPENDENCY_UNAVAILABLE") from None
        except TimeoutError:
            raise ExecutionError("DEPENDENCY_UNAVAILABLE") from None

    @staticmethod
    async def _row(connection: AsyncConnection, query: str, args: tuple = ()) -> dict[str, Any] | None:
        async with connection.cursor(row_factory=dict_row) as cursor:
            await cursor.execute(query, args)
            return await cursor.fetchone()

    async def state(self, run_id: UUID) -> dict[str, Any]:
        async with self._transaction() as connection:
            row = await self._row(connection, _READ, (run_id,))
        if row is None:
            raise ExecutionError("RUN_NOT_FOUND")
        return row

    async def acquire(self, run_id: UUID, owner: UUID, lease: int, *, local_busy: bool) -> dict[str, Any]:
        async with self._transaction() as connection:
            # Dedicated global admission lock precedes catalog/run locks. Its
            # transaction ends before the callback is scheduled.
            locked = await self._row(connection, "SELECT pg_try_advisory_xact_lock(92608,4) AS acquired")
            if not locked or not locked["acquired"]:
                raise ExecutionError("CAPACITY_EXCEEDED")
            current = await self._row(connection, _READ, (run_id,))
            if current is None:
                raise ExecutionError("RUN_NOT_FOUND")
            eligible = (current["status"] not in TERMINAL and current["cancel_requested_at"] is None
                        and current["remaining_seconds"] > 0 and not current["lease_live"])
            if eligible:
                active = await self._row(connection, "SELECT count(*) AS n FROM agent.runs "
                    "WHERE status IN ('running','cancelling') AND lease_until>clock_timestamp()")
                if local_busy or not active or active["n"] >= 1:
                    raise ExecutionError("CAPACITY_EXCEEDED")
            acquired = await self._row(connection, "SELECT * FROM agent.acquire_run(%s,%s,%s)",
                                       (run_id, owner, lease))
            row = await self._row(connection, _READ, (run_id,))
            if acquired is None or row is None:
                raise ExecutionError("DEPENDENCY_UNAVAILABLE")
            return row | {"outcome": acquired["outcome"]}

    async def heartbeat(self, token: ExecutionToken, lease: int) -> str | None:
        async with self._transaction() as connection:
            row = await self._row(connection, _READ, (token.run_id,))
            if row is None:
                return "RUN_NOT_FOUND"
            if row["status"] in TERMINAL:
                return "TERMINAL"
            if (row["execution_owner"], row["execution_epoch"]) != (token.owner, token.epoch):
                return "STALE_EXECUTION"
            if row["cancel_requested_at"] is not None:
                return "CANCEL_REQUESTED"
            await self._row(connection, "SELECT agent.heartbeat_run(%s,%s,%s,%s)", (*token.parameters, lease))
        return None

    async def finalize(self, token: ExecutionToken, code: str) -> None:
        # This deliberately has no guard_run_write wrapper: SQL must apply
        # cancellation/deadline precedence even when that guard would reject.
        async with self._transaction() as connection:
            row = await self._row(connection, _READ, (token.run_id,))
            if (row is None or row["status"] in TERMINAL
                    or (row["execution_owner"], row["execution_epoch"]) != (token.owner, token.epoch)):
                return
            outcome = "cancelled" if row["cancel_requested_at"] is not None else "failed"
            error = None if outcome == "cancelled" else code if code in _ERRORS else "INTERNAL_ERROR"
            operation = uuid5(token.run_id, f"manager-final:{token.epoch}:{outcome}:{error}")
            await self._row(connection, "SELECT * FROM agent.finalize_run(%s,%s,%s,%s,%s,NULL,NULL,NULL,NULL,%s)",
                            (*token.parameters, operation, outcome, error))

    async def recover(self) -> list[dict[str, Any]]:
        async with self._transaction() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                await cursor.execute("SELECT id,status FROM agent.recover_runs(100)")
                return await cursor.fetchall()


@dataclass
class _Active:
    token: ExecutionToken
    cancel: asyncio.Event
    task: asyncio.Task[None]


class RunManager:
    def __init__(self, settings: Settings, pool: AsyncConnectionPool, *,
                 execute: Callable[[ExecutionToken, asyncio.Event, float], Awaitable[None]],
                 configuration_fingerprint: str):
        if (settings.run_global_concurrency != 1 or not 1 <= settings.run_lease_seconds <= 300
                or not 0 < settings.run_heartbeat_seconds < settings.run_lease_seconds):
            raise ConfigurationError("RunManager requires the verified single-execution lease profile")
        if not configuration_fingerprint.strip() or len(configuration_fingerprint) > 1000 or not callable(execute):
            raise ConfigurationError("Invalid run execution configuration")
        self.settings, self.execute, self.configuration_fingerprint = settings, execute, configuration_fingerprint
        self._store = _RunStore(pool)
        self._owner = uuid4()
        self._lock = asyncio.Lock()
        self._stopping = asyncio.Event()
        self._active: dict[UUID, _Active] = {}
        self._reconciler: asyncio.Task[None] | None = None
        self._opened = False
        self.last_error_code: str | None = None

    @property
    def active_count(self) -> int:
        return len(self._active)

    @property
    def ready(self) -> bool:
        return (self._opened and not self._stopping.is_set() and self._reconciler is not None
                and not self._reconciler.done() and self.last_error_code is None)

    async def open(self) -> None:
        async with self._lock:
            if self._stopping.is_set():
                raise ExecutionError("DEPENDENCY_UNAVAILABLE")
            if not self._opened:
                self._opened = True
                self._reconciler = asyncio.create_task(self._reconcile_loop(), name="run-reconciler")

    async def aclose(self) -> None:
        async with self._lock:
            self._stopping.set()
            self._opened = False
            tasks = [active.task for active in self._active.values()]
            for active in self._active.values():
                active.cancel.set()
            reconciler = self._reconciler
        interrupted = False
        if reconciler is not None:
            try:
                await _drain(reconciler)
            except asyncio.CancelledError:
                interrupted = True
        # Supervisors observe _stopping themselves and cancel/drain callbacks.
        if tasks:
            joined = asyncio.gather(*tasks, return_exceptions=True)
            while not joined.done():
                try:
                    await asyncio.shield(joined)
                except asyncio.CancelledError:
                    interrupted = True
        if interrupted:
            raise asyncio.CancelledError

    async def start(self, run_id: UUID, request: StartRunRequest) -> StartRunResponse:
        # Request IDs are correlation only; run ownership/configuration and all
        # recovery identities come from PostgreSQL, never from this command.
        if not isinstance(run_id, UUID) or not isinstance(request, StartRunRequest):
            raise ExecutionError("INVALID_REQUEST")
        async with self._lock:
            if not self._opened or self._stopping.is_set():
                raise ExecutionError("DEPENDENCY_UNAVAILABLE")
            current = await self._store.state(run_id)
            if current["status"] in TERMINAL:
                return StartRunResponse(run_id=run_id, status="already_terminal")
            if current.get("lease_live"):
                return StartRunResponse(run_id=run_id, status="already_running")
            if self._active:
                raise ExecutionError("CAPACITY_EXCEEDED")
            permit = await self._store.permit()
            transferred = False
            before_query = asyncio.get_running_loop().time()
            try:
                row = await self._store.acquire(run_id, self._owner, self.settings.run_lease_seconds, local_busy=False)
                if row["status"] in TERMINAL:
                    return StartRunResponse(run_id=run_id, status="already_terminal")
                if row["outcome"] == "already_running":
                    return StartRunResponse(run_id=run_id, status="already_running")
                if row["outcome"] != "acquired" or row["execution_owner"] != self._owner:
                    raise ExecutionError("INTERNAL_ERROR")
                token = ExecutionToken(run_id, self._owner, row["execution_epoch"])
                cancel = asyncio.Event()
                # Query latency shortens admission; it never extends DB deadline.
                deadline = before_query + max(0.0, row["remaining_seconds"])
                task = asyncio.create_task(self._supervise(token, cancel, deadline,
                    row["configuration_fingerprint"] == self.configuration_fingerprint, permit), name=f"run-{run_id}")
                self._active[run_id] = _Active(token, cancel, task)
                transferred = True
                return StartRunResponse(run_id=run_id, status="accepted")
            finally:
                if not transferred:
                    await _release(permit)

    async def execution(self, run_id: UUID) -> ExecutionStatus:
        row = await self._store.state(run_id)
        return ExecutionStatus(run_id=row["id"], status=row["status"], current_stage=row["current_stage"],
            execution_epoch=row["execution_epoch"], lease_until=row["lease_until"], heartbeat_at=row["heartbeat_at"])

    async def _heartbeat(self, token: ExecutionToken, permit: _AdmissionPermit) -> str:
        while True:
            await asyncio.sleep(self.settings.run_heartbeat_seconds)
            try:
                await permit.check()
                reason = await self._store.heartbeat(token, self.settings.run_lease_seconds)
            except ExecutionError as error:
                return _safe_code(error)
            if reason is not None:
                return reason

    async def _supervise(self, token: ExecutionToken, cancel: asyncio.Event, deadline: float,
                         matching: bool, permit: _AdmissionPermit) -> None:
        children: list[asyncio.Future] = []
        reason = "INTERNAL_ERROR"
        try:
            if self._stopping.is_set():
                reason = "SHUTDOWN"
            elif matching and deadline > asyncio.get_running_loop().time():
                call = asyncio.ensure_future(self.execute(token, cancel, deadline))
                heartbeat = asyncio.create_task(self._heartbeat(token, permit), name=f"run-heartbeat-{token.run_id}")
                stopping = asyncio.create_task(self._stopping.wait())
                children = [call, heartbeat, stopping]
                done, _ = await asyncio.wait(children, timeout=max(0, deadline-asyncio.get_running_loop().time()),
                                             return_when=asyncio.FIRST_COMPLETED)
                if self._stopping.is_set():
                    reason = "SHUTDOWN"
                elif call in done:
                    if not call.cancelled():
                        try:
                            call.result()
                        except Exception as error:
                            reason = _safe_code(error)
                        # A normal return must already have finalized in the
                        # callback. finalize below is a no-op for terminal rows.
                elif heartbeat in done:
                    reason = heartbeat.result()
                else:
                    reason = "DEADLINE_EXCEEDED"
            elif matching:
                reason = "DEADLINE_EXCEEDED"
        except asyncio.CancelledError:
            reason = "SHUTDOWN"
        except Exception as error:
            reason = _safe_code(error)
        finally:
            cancel.set()
            try:
                await _drain(*children)
                if reason not in {"SHUTDOWN", "STALE_EXECUTION", "LEASE_EXPIRED", "RUN_NOT_FOUND", "TERMINAL"}:
                    try:
                        await self._store.finalize(token, reason)
                    except ExecutionError as error:
                        if error.code not in {"TERMINAL_CONFLICT", "STALE_EXECUTION", "LEASE_EXPIRED", "RUN_NOT_FOUND"}:
                            self.last_error_code = _safe_code(error)
                            logger.warning("run.finalization_deferred", extra={"safe_fields": {"error_code": self.last_error_code}})
            finally:
                try:
                    await _release(permit)
                except Exception as error:
                    self.last_error_code = _safe_code(error)
                    logger.warning("run.permit_release_failed", extra={"safe_fields": {"error_code": self.last_error_code}})
                finally:
                    self._active.pop(token.run_id, None)

    async def recover_once(self) -> int:
        rows = await self._store.recover()
        started = 0
        for row in rows:
            if row["status"] in TERMINAL:
                continue
            try:
                result = await self.start(row["id"], StartRunRequest(request_id=uuid4(),
                    execution_request_id=uuid5(row["id"], "recovery-start")))
            except ExecutionError as error:
                if error.code == "CAPACITY_EXCEEDED":
                    break
                if error.code == "RUN_NOT_FOUND":
                    continue
                raise
            if result.status == "accepted":
                started += 1
        return started

    async def _reconcile_loop(self) -> None:
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=self.settings.run_heartbeat_seconds)
                break
            except TimeoutError:
                pass
            try:
                await self.recover_once()
                self.last_error_code = None
            except Exception as error:
                self.last_error_code = _safe_code(error)
                logger.warning("run.reconciliation_deferred", extra={"safe_fields": {"error_code": self.last_error_code}})
