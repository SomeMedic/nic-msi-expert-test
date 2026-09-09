"""Keep cited source reads private and ordinary deactivation historically safe."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p09_012_library_sources"
down_revision = "p08_011_agent_execution"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("012_library_sources.sql")


def downgrade():
    no_downgrade()
