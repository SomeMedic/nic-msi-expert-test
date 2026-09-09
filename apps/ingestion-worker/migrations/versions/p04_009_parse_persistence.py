"""Persist immutable parse generations under the ingestion execution fence."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p04_009_parse_persistence"
down_revision = "p03_008_outbox"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("009_parse_persistence.sql")


def downgrade():
    no_downgrade()
