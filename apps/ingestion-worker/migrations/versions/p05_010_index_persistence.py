"""Keep source-mapped index batches immutable under the ingestion fence."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p05_010_index_persistence"
down_revision = "p04_009_parse_persistence"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("010_index_persistence.sql")


def downgrade():
    no_downgrade()
