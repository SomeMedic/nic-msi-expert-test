"""Pinned vendor setup followed by guarded checkpoint privileges."""
from importlib.metadata import version
from typing import Any, cast

from alembic import op
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg import Connection
from psycopg.rows import dict_row

from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p02_005_vendor_acl"
down_revision = "p02_004_routines"
branch_labels = None
depends_on = None


def upgrade():
    if version("langgraph-checkpoint-postgres") != "3.1.2":
        raise RuntimeError("Checkpoint vendor revision must be reviewed before upgrading")
    # Vendor's concurrent indexes require autocommit. The coordinator keeps its
    # session advisory lock across this block. setup() is itself resumable.
    with op.get_context().autocommit_block():
        connection = op.get_bind()
        raw = cast(Connection[dict[str, Any]], connection.connection.driver_connection)
        factory = raw.row_factory
        connection.exec_driver_sql("SET search_path=agent,pg_catalog,public")
        try:
            raw.row_factory = dict_row
            PostgresSaver(raw).setup()
        finally:
            raw.row_factory = factory
            connection.exec_driver_sql("RESET search_path")
    execute_sql("005_vendor_acl.sql")


def downgrade():
    no_downgrade()
