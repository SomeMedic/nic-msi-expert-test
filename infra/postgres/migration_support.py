"""SQL revision loader; SQL bodies are fixed project resources, never caller text."""
from pathlib import Path
from typing import Any, cast

from psycopg import Connection
from alembic import op


def execute_sql(name: str) -> None:
    path = Path(__file__).parent / "revisions" / name
    # Static, repository-owned scripts use the simple query protocol so function
    # bodies and multiple statements remain one transaction without SQL splitting.
    raw = cast(Connection[Any], op.get_bind().connection.driver_connection)
    raw.execute(path.read_text(encoding="utf-8"), prepare=False)


def no_downgrade() -> None:
    raise RuntimeError("Destructive downgrade is unsupported; use an explicit reviewed forward revision")
