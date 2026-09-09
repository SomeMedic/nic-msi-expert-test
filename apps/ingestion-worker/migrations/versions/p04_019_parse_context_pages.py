"""Keep cross-page canonical context while bounding owned source spans."""
from infra.postgres.migration_support import execute_sql, no_downgrade

revision = "p04_019_parse_context_pages"
down_revision = "p14_018_document_purge"
branch_labels = None
depends_on = None


def upgrade():
    execute_sql("019_parse_context_pages.sql")


def downgrade():
    no_downgrade()
