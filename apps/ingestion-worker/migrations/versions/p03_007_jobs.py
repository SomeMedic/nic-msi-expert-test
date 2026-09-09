"""Fenced durable ingestion commands and idempotent reindex."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p03_007_jobs"
down_revision = "p03_006_upload_auth"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("007_jobs.sql")


def downgrade():
    no_downgrade()
