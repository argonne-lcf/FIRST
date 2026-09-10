"""add startup ETA columns

Revision ID: 3301fe3c3001
Revises: a71ea9d19503
Create Date: 2026-09-08 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3301fe3c3001"
down_revision: Union[str, None] = "a71ea9d19503"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pilot_replica",
        sa.Column("placed_at", sa.DateTime(timezone=True), nullable=True),
        schema="first",
    )
    op.add_column(
        "pilot_deployment",
        sa.Column("last_startup_sec", sa.Float(), nullable=True),
        schema="first",
    )


def downgrade() -> None:
    op.drop_column("pilot_deployment", "last_startup_sec", schema="first")
    op.drop_column("pilot_replica", "placed_at", schema="first")
