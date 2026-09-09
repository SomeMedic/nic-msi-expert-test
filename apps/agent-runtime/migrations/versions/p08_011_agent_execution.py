"""Bind durable execution and publication to immutable verified artifacts."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p08_011_agent_execution"
down_revision = "p05_010_index_persistence"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("011_agent_execution.sql")


def downgrade():
    no_downgrade()
