"""Require an immutable reference-checked plan before exact source deletion."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p14_018_document_purge"
down_revision = "p05_017_index_context_spans"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("018_document_purge.sql")


def downgrade():
    no_downgrade()
