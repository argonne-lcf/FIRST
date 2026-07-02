"""add notify triggers

Revision ID: 7461993a66c6
Revises: a71ea9d19503
Create Date: 2026-07-01 21:10:56.185782

Shared LISTEN dispatcher: controllers subscribe to per-table asyncio.Events;
a single LISTEN connection receives all notifications keyed by table name.
See docs/architecture/controllers.md "Shared LISTEN dispatcher".
"""

from typing import Sequence, Union

from alembic import op

revision: str = "7461993a66c6"
down_revision: Union[str, None] = "a71ea9d19503"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "first"

NOTIFY_FUNCTION = f"""\
CREATE OR REPLACE FUNCTION {SCHEMA}.notify_resource_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'resource_changes',
    json_build_object('table', TG_TABLE_NAME)::text
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
"""

# Each entry: (table_name, optional WHEN clause for UPDATE filtering).
# Set when_clause to None to fire on every row change (INSERT/UPDATE/DELETE).
# Add column-specific IS DISTINCT FROM checks as controllers are built out.
# Example with column filtering:
#   ("pilot_job", """
#       NEW.phase IS DISTINCT FROM OLD.phase
#       OR NEW.scheduled_deletion IS DISTINCT FROM OLD.scheduled_deletion
#       OR (TG_OP != 'UPDATE')
#   """),
TRIGGERED_TABLES: list[tuple[str, str | None]] = [
    ("access_group", None),
    ("cluster", None),
    ("model", None),
    ("pilot_deployment", None),
    ("pilot_job", None),
    ("pilot_replica", None),
    ("static_deployment", None),
]


def _create_trigger(table: str, when_clause: str | None) -> str:
    when = f"\n  WHEN (\n    {when_clause.strip()}\n  )" if when_clause else ""
    return (
        f"CREATE TRIGGER {table}_notify\n"
        f"  AFTER INSERT OR UPDATE OR DELETE ON {SCHEMA}.{table}\n"
        f"  FOR EACH ROW{when}\n"
        f"  EXECUTE FUNCTION {SCHEMA}.notify_resource_change();\n"
    )


def upgrade() -> None:
    op.execute(NOTIFY_FUNCTION)
    for table, when_clause in TRIGGERED_TABLES:
        op.execute(_create_trigger(table, when_clause))


def downgrade() -> None:
    for table, _ in reversed(TRIGGERED_TABLES):
        op.execute(f"DROP TRIGGER IF EXISTS {table}_notify ON {SCHEMA}.{table};")
    op.execute(f"DROP FUNCTION IF EXISTS {SCHEMA}.notify_resource_change();")
