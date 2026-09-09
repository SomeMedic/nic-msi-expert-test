"""Owner-scoped revisions, one supplied connection and one linear revision graph."""
from alembic import context

connection = context.config.attributes.get("connection")
if connection is None:
    raise RuntimeError("Use scripts/migrate.py; independent migration runners are unsupported")
context.configure(connection=connection, target_metadata=None, version_table="alembic_version",
                  version_table_schema="app", transaction_per_migration=True)
with context.begin_transaction():
    context.run_migrations()
