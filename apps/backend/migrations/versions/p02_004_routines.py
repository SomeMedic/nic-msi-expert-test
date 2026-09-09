"""Atomic publication, snapshot capture and narrow run commands."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p02_004_routines"
down_revision = "p02_003_agent"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("004_routines.sql")


def downgrade():
    no_downgrade()
