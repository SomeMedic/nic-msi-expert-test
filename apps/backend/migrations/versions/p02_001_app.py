"""Application identities, immutable source metadata and transport state."""
from infra.postgres.migration_support import execute_sql, no_downgrade
revision = "p02_001_app"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("001_app.sql")


def downgrade():
    no_downgrade()
