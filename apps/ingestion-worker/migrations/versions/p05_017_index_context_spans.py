"""Preserve source intervals from immutable canonical context during indexing."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p05_017_index_context_spans"
down_revision = "p10_016_document_archive"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("017_index_context_spans.sql")


def downgrade():
    no_downgrade()
