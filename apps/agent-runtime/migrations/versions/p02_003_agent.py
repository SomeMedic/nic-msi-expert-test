"""Durable runs, immutable snapshots and private artifacts."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p02_003_agent"
down_revision = "p02_002_knowledge"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("003_agent.sql")


def downgrade():
    no_downgrade()
