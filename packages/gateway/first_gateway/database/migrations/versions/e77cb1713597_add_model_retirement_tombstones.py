"""add model retirement tombstones

Revision ID: e77cb1713597
Revises: 6495707fef20
Create Date: 2026-11-06 12:23:20.983093
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e77cb1713597"
down_revision: Union[str, None] = "6495707fef20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "model",
        sa.Column(
            "retired", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        schema="first",
    )
    op.add_column(
        "model",
        sa.Column(
            "retired_deployments",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        schema="first",
    )


def downgrade() -> None:
    op.drop_column("model", "retired_deployments", schema="first")
    op.drop_column("model", "retired", schema="first")
