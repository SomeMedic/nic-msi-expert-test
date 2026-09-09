"""Immutable parse/index generations and publication provenance."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p02_002_knowledge"
down_revision = "p02_001_app"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("002_knowledge.sql")


def downgrade():
    no_downgrade()
