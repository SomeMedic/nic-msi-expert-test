"""Permit proven terminal model failures to produce an honest verification refusal."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p08_022_model_output_refusal"
down_revision = "p08_021_table_variant_repair"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("022_model_output_refusal.sql")


def downgrade():
    no_downgrade()
