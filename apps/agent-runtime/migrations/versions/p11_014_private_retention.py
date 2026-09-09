"""Keep committed citations while expiring opt-in capture and recovery payloads."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p11_014_private_retention"
down_revision = "p11_013_run_trace"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("014_retention.sql")
    execute_sql("014_debug.sql")


def downgrade():
    no_downgrade()
