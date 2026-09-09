"""Bounded outbox claims and PostgreSQL-authoritative queue reconciliation."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p03_008_outbox"
down_revision = "p03_007_jobs"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("008_outbox.sql")


def downgrade():
    no_downgrade()
