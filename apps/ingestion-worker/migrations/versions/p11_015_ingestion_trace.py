"""Preserve ingestion correlation through delivery retries and lease takeover."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p11_015_ingestion_trace"
down_revision = "p11_014_private_retention"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("015_ingestion_trace.sql")


def downgrade():
    no_downgrade()
