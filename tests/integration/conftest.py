"""Opt-in, isolated P02 databases on the project's fixed loopback PostgreSQL.

No migration or cleanup ever targets the application's ``expert`` database.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import sys
from typing import Mapping
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic import SecretStr
import pytest


ROOT = Path(__file__).resolve().parents[2]
SECRET_DIRECTORY = ROOT / ".secrets/local"
EVIDENCE_DIRECTORY = ROOT / "docs/implementation/evidence/p02-fixture"
HOST = "127.0.0.1"
PORT = 25432
OWNER = "expert_migrate"
ROLES = {
    "admin": "expert_admin", "migrate": OWNER, "backend": "expert_backend",
    "runtime": "expert_runtime", "ingest": "expert_ingest", "outbox": "expert_outbox",
}
DATABASE_PATTERN = re.compile(r"expert_test_p02_[0-9a-f]{12}\Z")
_SESSION_ID = uuid4().hex
_JOURNAL: list[dict] = []


def pytest_asyncio_loop_factories(config, item):
    """Psycopg async requires Selector on Windows; no global policy mutation."""
    if sys.platform == "win32":
        return {"selector": asyncio.SelectorEventLoop}
    return None


class DatabaseFixtureError(RuntimeError):
    """A safe diagnostic code; no driver exception, DSN, or credential content."""


def _journal(record: dict) -> None:
    EVIDENCE_DIRECTORY.mkdir(parents=True, exist_ok=True)
    _JOURNAL.append(record)
    path = EVIDENCE_DIRECTORY / f"lifecycle-{_SESSION_ID}.json"
    path.write_text(json.dumps({"scope": "isolated P02 test resources only", "events": _JOURNAL}, indent=2) + "\n", encoding="utf-8")


def _validate_drop_target(name: str, owner: str, *, created: bool, oid: int | None, expected_oid: int | None) -> None:
    if not created or DATABASE_PATTERN.fullmatch(name) is None or owner != OWNER:
        raise DatabaseFixtureError("ISOLATED_DATABASE_CLEANUP_REFUSED")
    if expected_oid is None or oid != expected_oid:
        raise DatabaseFixtureError("ISOLATED_DATABASE_IDENTITY_CHANGED")


def _read_conninfo(alias: str, database: str) -> SecretStr:
    __tracebackhide__ = True
    path = SECRET_DIRECTORY / f"{alias}_database_dsn"
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 16_384:
            raise DatabaseFixtureError("ISOLATED_DATABASE_SECRET_UNAVAILABLE")
        source = path.read_text(encoding="utf-8").strip()
        parsed = conninfo_to_dict(source)
        if set(parsed) != {"dbname", "user", "password", "host", "port"}:
            raise DatabaseFixtureError("ISOLATED_DATABASE_SOURCE_CONTRACT_INVALID")
        if (parsed["dbname"] != "expert" or parsed["host"] != "postgres" or parsed["port"] != "5432"
                or parsed["user"] != ROLES[alias] or not parsed["password"]):
            raise DatabaseFixtureError("ISOLATED_DATABASE_SOURCE_CONTRACT_INVALID")
        parsed.update(host=HOST, port=str(PORT), dbname=database, connect_timeout="5",
                      application_name="expert-p02-isolated-tests",
                      options="-c statement_timeout=15000 -c lock_timeout=5000")
        return SecretStr(make_conninfo(**parsed))
    except DatabaseFixtureError:
        raise
    except Exception:
        raise DatabaseFixtureError("ISOLATED_DATABASE_SECRET_OR_CONNINFO_INVALID") from None


def _connect(dsn: SecretStr, *, autocommit: bool = False):
    __tracebackhide__ = True
    try:
        return psycopg.connect(dsn.get_secret_value(), autocommit=autocommit)
    except Exception:
        raise DatabaseFixtureError("ISOLATED_DATABASE_CONNECTION_FAILED") from None


@dataclass(frozen=True)
class IsolatedDatabase:
    dbname: str
    role_dsns: Mapping[str, SecretStr] = field(repr=False)
    host: str = HOST
    port: int = PORT

    @property
    def name(self) -> str:
        return self.dbname

    @property
    def admin_dsn(self) -> SecretStr:
        return self.role_dsns["admin"]

    @property
    def migrate_dsn(self) -> SecretStr:
        return self.role_dsns["migrate"]

    def connect(self, role: str = "migrate", *, autocommit: bool = False):
        if role not in ROLES:
            raise DatabaseFixtureError("ISOLATED_DATABASE_ROLE_INVALID")
        return _connect(self.role_dsns[role], autocommit=autocommit)

    def write_dsn_file(self, role: str, path: Path) -> Path:
        """Create a caller-owned temporary file exclusively; never overwrite one."""
        __tracebackhide__ = True
        if role not in ROLES:
            raise DatabaseFixtureError("ISOLATED_DATABASE_ROLE_INVALID")
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(self.role_dsns[role].get_secret_value() + "\n")
        except Exception:
            raise DatabaseFixtureError("ISOLATED_DATABASE_DSN_FILE_FAILED") from None
        return path


def _verify_cluster(connection) -> None:
    current = connection.execute("SELECT current_user, current_database()").fetchone()
    if current != (ROLES["admin"], "postgres"):
        raise DatabaseFixtureError("ISOLATED_DATABASE_ADMIN_TARGET_INVALID")
    main = connection.execute(
        "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s", ("expert",),
    ).fetchone()
    if main != (ROLES["admin"],):
        raise DatabaseFixtureError("ISOLATED_DATABASE_PROJECT_IDENTITY_INVALID")
    actual = connection.execute(
        "SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication "
        "FROM pg_roles WHERE rolname = ANY(%s)", (list(ROLES.values()),),
    ).fetchall()
    by_name = {row[0]: row[1:] for row in actual}
    if set(by_name) != set(ROLES.values()):
        raise DatabaseFixtureError("ISOLATED_DATABASE_EXPECTED_ROLES_MISSING")
    for alias, name in ROLES.items():
        if alias == "admin":
            if not by_name[name][0] or not by_name[name][1]:
                raise DatabaseFixtureError("ISOLATED_DATABASE_ADMIN_ROLE_INVALID")
        elif by_name[name] != (True, False, False, False, False):
            raise DatabaseFixtureError("ISOLATED_DATABASE_SERVICE_ROLE_INVALID")


def _cleanup_database(admin_dsn: SecretStr, name: str, expected_oid: int | None, created: bool) -> None:
    __tracebackhide__ = True
    try:
        with _connect(admin_dsn, autocommit=True) as connection:
            _verify_cluster(connection)
            row = connection.execute(
                "SELECT oid, pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s", (name,),
            ).fetchone()
            if row is None:
                raise DatabaseFixtureError("ISOLATED_DATABASE_DISAPPEARED_BEFORE_CLEANUP")
            _validate_drop_target(name, row[1], created=created, oid=row[0], expected_oid=expected_oid)
            connection.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
            if connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is not None:
                raise DatabaseFixtureError("ISOLATED_DATABASE_CLEANUP_INCOMPLETE")
            _journal({"database": name, "event": "dropped", "owner_verified": True, "oid_verified": True})
    except DatabaseFixtureError:
        _journal({"database": name, "event": "cleanup_refused_or_failed"})
        raise
    except Exception:
        _journal({"database": name, "event": "cleanup_failed"})
        raise DatabaseFixtureError("ISOLATED_DATABASE_CLEANUP_FAILED") from None


@pytest.fixture
def isolated_database():
    """A fresh DB per test. Integration gating intentionally fails instead of skips."""
    __tracebackhide__ = True
    if os.environ.get("EXPERT_INTEGRATION_TESTS") != "1":
        pytest.fail("P02 integration tests require explicit EXPERT_INTEGRATION_TESTS=1", pytrace=False)
    name = "expert_test_p02_" + uuid4().hex[:12]
    created = False
    database_oid = None
    admin_dsn = _read_conninfo("admin", "postgres")
    database = IsolatedDatabase(name, {alias: _read_conninfo(alias, name) for alias in ROLES})
    try:
        with _connect(admin_dsn, autocommit=True) as connection:
            _verify_cluster(connection)
            if connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone() is not None:
                raise DatabaseFixtureError("ISOLATED_DATABASE_RANDOM_NAME_COLLISION")
            connection.execute(sql.SQL("CREATE DATABASE {} OWNER {} TEMPLATE template0").format(
                sql.Identifier(name), sql.Identifier(OWNER),
            ))
            created = True
            database_oid = connection.execute("SELECT oid FROM pg_database WHERE datname = %s", (name,)).fetchone()[0]
            connection.execute(sql.SQL("REVOKE CONNECT ON DATABASE {} FROM PUBLIC").format(sql.Identifier(name)))
            for alias, role in ROLES.items():
                if alias != "admin":
                    connection.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(name), sql.Identifier(role)))
        with database.connect("admin", autocommit=True) as connection:
            connection.execute("CREATE EXTENSION vector WITH SCHEMA public")
            connection.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            connection.execute("GRANT USAGE, CREATE ON SCHEMA public TO expert_migrate")
            connection.execute(sql.SQL("GRANT CREATE ON DATABASE {} TO expert_migrate").format(sql.Identifier(name)))
        _journal({"database": name, "event": "created", "owner": OWNER, "host": HOST, "port": PORT,
                  "vector_extension": True, "public_connect_revoked": True})
    except Exception:
        if created:
            _cleanup_database(admin_dsn, name, database_oid, created)
        raise DatabaseFixtureError("ISOLATED_DATABASE_SETUP_FAILED") from None
    try:
        yield database
    finally:
        _cleanup_database(admin_dsn, name, database_oid, created)
