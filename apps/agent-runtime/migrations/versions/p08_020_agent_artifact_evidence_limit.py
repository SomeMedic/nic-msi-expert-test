"""Align durable agent artifact evidence limits with canonical DTOs."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p08_020_artifact_evidence"
down_revision = "p04_019_parse_context_pages"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("020_agent_artifact_evidence_limit.sql")


def downgrade():
    no_downgrade()
