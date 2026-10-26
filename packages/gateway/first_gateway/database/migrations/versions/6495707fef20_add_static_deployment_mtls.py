"""add static_deployment mtls

Revision ID: 6495707fef20
Revises: a71ea9d19503
Create Date: 2026-10-26 14:43:40.850328
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "6495707fef20"
down_revision: Union[str, None] = "a71ea9d19503"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "static_deployment",
        sa.Column("mtls", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        schema="first",
    )


def downgrade() -> None:
    op.drop_column("static_deployment", "mtls", schema="first")
