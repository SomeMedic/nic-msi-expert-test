"""Local credentials and minimal database principals; never rotates existing secrets."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
ROLES = {"backend": "expert_backend", "runtime": "expert_runtime", "ingest": "expert_ingest",
         "outbox": "expert_outbox", "migrate": "expert_migrate"}
APP_ACCESS_KEYS = ("viewer_access_key", "operator_access_key", "admin_access_key")
REDIS_COMMANDS = {
    "backend": "+ping +client|setinfo +client|setname +xread +xrange +xrevrange +get +set +del +expire",
    "runtime": "+ping +client|setinfo +client|setname +xadd +xread +get +set +del +expire",
    "ingest": "+ping +client|setinfo +client|setname +xreadgroup +xack +xautoclaim +xpending +xgroup|create +xinfo|groups",
    "outbox": "+ping +client|setinfo +client|setname +xadd +xlen",
    "health": "+ping",
}


class BootstrapError(RuntimeError):
    pass


def read_secret(directory: Path, name: str) -> str:
    path = directory / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 16_384:
        raise BootstrapError(f"Missing or invalid secret: {name}")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise BootstrapError(f"Empty secret: {name}")
    return value


def write_once(directory: Path, name: str, value: str) -> str:
    path = directory / name
    if path.exists() or path.is_symlink():
        return read_secret(directory, name)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(value + "\n")
    if os.name != "nt":
        # Private host directory prevents traversal; mounted files must be readable
        # by each container's unprivileged service UID.
        path.chmod(0o444)
    return value


def password(directory: Path, name: str) -> str:
    return write_once(directory, name, secrets.token_urlsafe(32))


def generate_secrets(directory: Path) -> None:
    target = directory.resolve()
    if not target.is_relative_to(ROOT) or target in {ROOT, ROOT / ".secrets"}:
        raise BootstrapError("Secret directory must be a dedicated directory inside the project")
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    if directory.is_symlink():
        raise BootstrapError("Secret directory cannot be a symlink")
    if os.name == "nt":
        # Python 0700 under an elevated token can make Administrators the owner.
        # Docker Desktop runs as the interactive user, so explicitly grant that
        # token's user SID; never grant Everyone or the generic Users group.
        identity = subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"],
                                  capture_output=True, text=True, check=True)
        sid = next(csv.reader(identity.stdout.splitlines()))[1]
        if not sid.startswith("S-1-5-") or not all(part.isdigit() for part in sid[2:].split("-")):
            raise BootstrapError("Cannot determine Windows user SID")
        subprocess.run(["icacls", str(directory), "/grant:r", f"*{sid}:(OI)(CI)F"],
                       capture_output=True, check=True)
    # A missing password beside an existing DSN indicates damaged state, not permission to rotate it.
    for key in ["admin", *ROLES]:
        if (directory / f"{key}_database_dsn").exists() and not (directory / f"{key}_db_password").exists():
            raise BootstrapError(f"Inconsistent database credential pair: {key}")
    for key in REDIS_COMMANDS:
        if (directory / f"{key}_redis_url").exists() and not (directory / f"{key}_redis_password").exists():
            raise BootstrapError(f"Inconsistent Redis credential pair: {key}")
    for key, role in {"admin": "expert_admin", **ROLES}.items():
        value = password(directory, f"{key}_db_password")
        expected = f"postgresql://{role}:{quote(value, safe='')}@postgres:5432/expert"
        actual = write_once(directory, f"{key}_database_dsn", expected)
        if actual != expected:
            raise BootstrapError(f"Inconsistent database credential pair: {key}")
    for name in ("agent_runtime_token", "retrieval_ml_token", "llm_api_key", "minio_root_password"):
        password(directory, name)
    for name in APP_ACCESS_KEYS:
        password(directory, name)
    write_once(directory, "minio_root_user", "expert_storage_admin")
    # Runtime never receives S3 credentials. Object permissions come from the
    # service policies; ingestion has no access to the backend's debug namespace.
    for key in ("backend", "ingest"):
        write_once(directory, f"{key}_s3_access_key", f"expert_{key}")
        password(directory, f"{key}_s3_secret_key")
    acl = ["user default off"]
    for key, commands in REDIS_COMMANDS.items():
        value = password(directory, f"{key}_redis_password")
        digest = hashlib.sha256(value.encode()).hexdigest()
        patterns = "~ingestion.jobs.v1 ~run.wakeup.* ~expert:*" if key != "health" else ""
        acl.append(f"user expert_{key} on #{digest} -@all {patterns} {commands}".strip())
        if key != "health":
            expected = f"redis://expert_{key}:{quote(value, safe='')}@redis:6379/0"
            if write_once(directory, f"{key}_redis_url", expected) != expected:
                raise BootstrapError(f"Inconsistent Redis credential pair: {key}")
    expected_acl = "\n".join(acl)
    if write_once(directory, "redis_users.acl", expected_acl) != expected_acl:
        raise BootstrapError("Existing Redis ACL differs; explicit migration required")
    print(json.dumps({"status": "ready", "credentials": "created_or_preserved", "values_disclosed": False}))


def database_init(directory: Path) -> None:
    import psycopg
    from psycopg import sql

    dsn = read_secret(directory, "admin_database_dsn")
    # No HTTP/Redis/S3 operations occur in this short transaction.
    with psycopg.connect(dsn, connect_timeout=5) as connection:
        connection.execute("SET LOCAL statement_timeout = '15s'")
        connection.execute("REVOKE CONNECT ON DATABASE expert FROM PUBLIC")
        connection.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        connection.execute("CREATE EXTENSION IF NOT EXISTS vector")
        for key, role in ROLES.items():
            exists = connection.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,)).fetchone()
            if not exists:
                connection.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION").format(
                    sql.Identifier(role), sql.Literal(read_secret(directory, f"{key}_db_password"))))
            connection.execute(sql.SQL("GRANT CONNECT ON DATABASE expert TO {}").format(sql.Identifier(role)))
        connection.execute("GRANT USAGE, CREATE ON SCHEMA public TO expert_migrate")
        connection.execute("GRANT CREATE ON DATABASE expert TO expert_migrate")
    print(json.dumps({"status": "ready", "database_principals": list(ROLES.values()), "product_schema_migrated": False}))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["secrets", "database-init"])
    parser.add_argument("--secret-dir", type=Path)
    args = parser.parse_args()
    directory = args.secret_dir or (Path("/run/secrets") if args.command == "database-init" else ROOT / ".secrets" / "local")
    try:
        if args.command == "secrets":
            generate_secrets(directory)
        else:
            database_init(directory)
    except BootstrapError as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception:
        # DB/SDK exceptions may echo credentials or a full connection string.
        print("Bootstrap failed; verify local service availability and secret files", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
