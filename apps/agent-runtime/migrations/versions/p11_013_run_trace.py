"""Preserve one trace identity across fenced execution recovery."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p11_013_run_trace"
down_revision = "p09_012_library_sources"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("013_run_trace.sql")


def downgrade():
    no_downgrade()
