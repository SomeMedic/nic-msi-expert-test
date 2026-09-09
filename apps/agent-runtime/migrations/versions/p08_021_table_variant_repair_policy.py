"""Persist deterministic policy decisions for table-variant repair/refusal."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p08_021_table_variant_repair"
down_revision = "p08_020_artifact_evidence"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("021_table_variant_repair_policy.sql")


def downgrade():
    no_downgrade()

