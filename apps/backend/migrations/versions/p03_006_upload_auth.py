"""Durable upload reservations, attachment and application sessions."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p03_006_upload_auth"
down_revision = "p02_005_vendor_acl"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("006_upload_auth.sql")


def downgrade():
    no_downgrade()
