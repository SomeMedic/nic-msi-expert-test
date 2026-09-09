"""Allow durable precheck refusal for exact repeated source-bound claims."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p08_023_repeated_draft_guard"
down_revision = "p08_022_model_output_refusal"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("023_repeated_draft_guard.sql")


def downgrade():
    no_downgrade()
