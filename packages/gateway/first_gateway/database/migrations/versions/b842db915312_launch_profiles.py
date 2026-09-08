"""Add reusable launch profiles, retaining existing inline launch specs."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "b842db915312"
down_revision = "a71ea9d19503"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "launch_profile",
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
        sa.Column("parameters", postgresql.JSONB(), nullable=False),
        sa.Column("env", postgresql.JSONB(), nullable=False),
        sa.Column("serve_script_template", sa.String(), nullable=False),
        sa.Column("pre_stop_script_template", sa.String()),
        sa.Column("post_stop_script_template", sa.String()),
        sa.Column("max_startup_sec", sa.Integer(), nullable=False),
        sa.Column("pre_stop_timeout_sec", sa.Float(), nullable=False),
        sa.Column("post_stop_timeout_sec", sa.Float(), nullable=False),
        sa.Column("max_unhealthy_sec", sa.Integer()),
        sa.Column("health_check", postgresql.JSONB(), nullable=False),
        schema="first",
    )
    op.add_column(
        "pilot_deployment", sa.Column("launch_profile_name", sa.Text()), schema="first"
    )
    op.create_foreign_key(
        "fk_pilot_deployment_launch_profile",
        "pilot_deployment",
        "launch_profile",
        ["launch_profile_name"],
        ["name"],
        source_schema="first",
        referent_schema="first",
    )
    op.create_index(
        "ix_first_pilot_deployment_launch_profile_name",
        "pilot_deployment",
        ["launch_profile_name"],
        schema="first",
    )


def downgrade() -> None:
    # Never discard a referenced profile and leave an unlaunchable deployment.
    bind = op.get_bind()
    if bind.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM first.pilot_deployment WHERE launch_profile_name IS NOT NULL)"
        )
    ):
        raise RuntimeError(
            "Convert profile deployments to inline launch specs before downgrade"
        )
    op.drop_index(
        "ix_first_pilot_deployment_launch_profile_name",
        table_name="pilot_deployment",
        schema="first",
    )
    op.drop_constraint(
        "fk_pilot_deployment_launch_profile",
        "pilot_deployment",
        schema="first",
        type_="foreignkey",
    )
    op.drop_column("pilot_deployment", "launch_profile_name", schema="first")
    op.drop_table("launch_profile", schema="first")
