"""Single migration coordinator. Credentials and raw DB errors are never printed."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from time import monotonic, sleep

import psycopg
from psycopg.conninfo import make_conninfo
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[1]


def run_migrations(dsn: str, *, host: str | None = None, port: int | None = None,
                   database: str | None = None, target_revision: str = "head") -> dict:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    config = Config()
    config.set_main_option("script_location", str(ROOT / "apps/backend/migrations"))
    config.set_main_option("path_separator", "os")
    config.set_main_option("version_locations", os.pathsep.join(str(ROOT / p) for p in (
        "apps/backend/migrations/versions", "apps/ingestion-worker/migrations/versions",
        "apps/agent-runtime/migrations/versions")))
    scripts = ScriptDirectory.from_config(config)
    head = scripts.get_current_head()
    revisions = {revision.revision for revision in scripts.walk_revisions()}
    # Accept only an exact repository revision or head, never Alembic's partial,
    # relative or branch selectors. Resolve before any database connection/DDL.
    if head is None or not isinstance(target_revision, str) or (
        target_revision != "head" and target_revision not in revisions
    ):
        raise ValueError("Unknown migration target revision")
    target = head if target_revision == "head" else target_revision
    ancestors = {revision.revision for revision in scripts.iterate_revisions(target, None)}
    overrides = {key: value for key, value in {"host": host, "port": port, "dbname": database}.items()
                 if value is not None}
    engine = create_engine("postgresql+psycopg://", poolclass=NullPool,
                           creator=lambda: psycopg.connect(make_conninfo(dsn, **overrides), connect_timeout=5,
                                                           prepare_threshold=0,
                                                           application_name="expert-migration-coordinator"))
    try:
        with engine.connect() as connection:
            identity = connection.execute(text("SELECT current_user, current_database()")).one()
            if identity[0] != "expert_migrate":
                raise RuntimeError("Migration must use the dedicated expert_migrate role")
            connection.execute(text("SET lock_timeout='15s'"))
            connection.commit()
            # A blocking SELECT pg_advisory_lock holds a statement snapshot.
            # Vendor CREATE INDEX CONCURRENTLY can wait for that snapshot while
            # its owner waits for our lock, forming a deadlock. Every failed
            # try-lock ends its transaction before waiting outside PostgreSQL.
            deadline = monotonic() + 30
            while True:
                acquired = connection.execute(text("SELECT pg_try_advisory_lock(92608, 2)")).scalar_one()
                connection.commit()
                if acquired:
                    break
                if monotonic() >= deadline:
                    raise TimeoutError("Migration coordinator lock timeout")
                sleep(0.05)
            try:
                exists = connection.execute(text("SELECT to_regclass('app.alembic_version') IS NOT NULL")).scalar_one()
                before = connection.execute(text("SELECT version_num FROM app.alembic_version")).scalar_one_or_none() if exists else None
                connection.commit()
                if before is not None and before not in ancestors:
                    raise ValueError("Migration target must include the current database revision; downgrade is unsupported")
                connection.execute(text("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION expert_migrate"))
                connection.commit()
                config.attributes["connection"] = connection
                command.upgrade(config, target)
                after = connection.execute(text("SELECT version_num FROM app.alembic_version")).scalar_one()
                connection.commit()
                if after != target:
                    raise RuntimeError("Database did not reach the requested migration revision")
                return {"database": identity[1], "before_revision": before,
                        "after_revision": after, "changed": before != after,
                        "requested_revision": target_revision, "target_revision": target,
                        "head_revision": head}
            finally:
                connection.rollback()
                connection.execute(text("SELECT pg_advisory_unlock(92608, 2)"))
                connection.commit()
    finally:
        engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn-file", type=Path, default=ROOT / ".secrets/local/migrate_database_dsn")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--database")
    parser.add_argument("--revision", default="head",
                        help="Exact revision ID to apply, or head (default); downgrade is unsupported")
    args = parser.parse_args()
    try:
        if args.dsn_file.is_symlink() or not args.dsn_file.is_file() or args.dsn_file.stat().st_size > 16384:
            raise ValueError("Invalid credential file")
        result = run_migrations(args.dsn_file.read_text(encoding="utf-8").strip(), host=args.host,
                                port=args.port, database=args.database, target_revision=args.revision)
        print(json.dumps(result))
        return 0
    except Exception as error:
        database_error = getattr(error, "orig", error)
        print(json.dumps({"status": "failed", "error_class": type(error).__name__,
                          "sqlstate": getattr(database_error, "sqlstate", None),
                          "credentials_disclosed": False}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
