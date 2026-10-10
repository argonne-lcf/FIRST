"""Durable node quarantine records and report receipts.

Revision ID: b21f0e4a7c10
Revises: a71ea9d19503
"""

from typing import Any, Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b21f0e4a7c10"
down_revision: str | None = "a71ea9d19503"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def common_columns() -> list[sa.Column[Any]]:
    return [
        sa.Column("uid", sa.BigInteger(), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False, unique=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("reconcile_failures", sa.Integer(), nullable=False),
        sa.Column("reconcile_last_error", sa.Text()),
        sa.Column("reconcile_retry_at", sa.DateTime(timezone=True)),
    ]


def upgrade() -> None:
    op.create_table(
        "node_quarantine",
        *common_columns(),
        sa.Column(
            "cluster_name",
            sa.Text(),
            sa.ForeignKey("first.cluster.name", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("host", sa.Text(), nullable=False),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("operation", sa.Text(), nullable=False),
        sa.Column("origin", postgresql.JSONB(), nullable=False),
        sa.Column("fault", postgresql.JSONB(), nullable=False),
        sa.Column("evidence", postgresql.JSONB(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("state_message", sa.Text(), nullable=False),
        sa.Column(
            "faulted_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "transitioned_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("submission_attempts", sa.Integer(), nullable=False),
        sa.Column("submission_started_at", sa.DateTime(timezone=True)),
        sa.Column("submission_uncertain", sa.Boolean(), nullable=False),
        sa.Column("job_identity", postgresql.JSONB()),
        sa.Column("runtime_policy", postgresql.JSONB()),
        sa.Column("activation", postgresql.JSONB()),
        sa.Column("expires_at", sa.DateTime(timezone=True)),
        sa.Column("repair_reference", sa.Text()),
        sa.Column("screen_nonce", sa.Text()),
        sa.Column("screen_requested_at", sa.DateTime(timezone=True)),
        sa.Column("screen_submission_started_at", sa.DateTime(timezone=True)),
        sa.Column("screen_result", postgresql.JSONB()),
        sa.Column("release_requested_at", sa.DateTime(timezone=True)),
        sa.Column("deletion_started_at", sa.DateTime(timezone=True)),
        sa.Column("deletion_acknowledged_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("cluster_name", "host"),
        sa.CheckConstraint("environment IN ('dev', 'prod')"),
        sa.CheckConstraint(
            "state IN ('pending', 'isolated', 'failed', 'lost', 'released')"
        ),
        sa.CheckConstraint("submission_attempts BETWEEN 0 AND 3"),
        schema="first",
    )
    op.create_index(
        "ix_first_node_quarantine_cluster_name",
        "node_quarantine",
        ["cluster_name"],
        schema="first",
    )
    op.create_table(
        "quarantine_report_inbox",
        *common_columns(),
        sa.Column(
            "cluster_name",
            sa.Text(),
            sa.ForeignKey("first.cluster.name", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("report_job_id", sa.Text(), nullable=False, unique=True),
        sa.Column("fingerprint", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("validation_error", sa.Text()),
        sa.Column("processed_at", sa.DateTime(timezone=True)),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True)),
        schema="first",
    )
    op.create_index(
        "ix_first_quarantine_report_inbox_cluster_name",
        "quarantine_report_inbox",
        ["cluster_name"],
        schema="first",
    )


def downgrade() -> None:
    op.drop_table("quarantine_report_inbox", schema="first")
    op.drop_table("node_quarantine", schema="first")
