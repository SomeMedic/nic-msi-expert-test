"""Exclude ordinary archived documents from new answers without losing history."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p10_016_document_archive"
down_revision = "p11_015_ingestion_trace"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("016_document_archive.sql")


def downgrade():
    no_downgrade()
