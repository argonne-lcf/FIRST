"""backfill cluster max_nodes_per_job

PilotConfig gains a required `max_nodes_per_job` and a stricter `ip_allowlist`.
Stored `cluster.pilot_system` rows are re-validated into PilotConfig by the API
server and the controllers, so:

- Rows without `max_nodes_per_job` get the row's own `max_num_nodes` (at least
  1). No job could exceed that total cap anyway, so behavior is unchanged until
  the updated Cluster manifests are applied with the real per-job limits.
- Rows whose `ip_allowlist` the new schema would reject make this migration
  fail (the transaction rolls back) instead of leaving rows the new code cannot
  read. Fix them by applying a corrected Cluster with the old version first.

Revision ID: a68035973f3a
Revises: e77cb1713597
Create Date: 2026-11-11 12:09:23.869140
"""

from ipaddress import IPv4Network, IPv6Network, ip_network
from typing import Any, Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a68035973f3a"
down_revision: Union[str, None] = "e77cb1713597"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Frozen copy of first_common's rule as of this revision.
_MIN_PREFIXLEN = {4: 24, 6: 64}


def _allowlist_problem(allowlist: Any) -> str | None:
    if not isinstance(allowlist, list) or not allowlist:
        return "ip_allowlist must be a non-empty list"
    for entry in allowlist:
        if not isinstance(entry, str):
            return f"{entry!r} is not a string"
        try:
            net: IPv4Network | IPv6Network = ip_network(entry, strict=True)
        except ValueError as exc:
            return f"{entry!r}: {exc}"
        version, prefixlen = net.version, net.prefixlen
        if isinstance(net, IPv6Network) and net.network_address.ipv4_mapped is not None:
            version, prefixlen = 4, prefixlen - 96
        if net.prefixlen == 0 or prefixlen < _MIN_PREFIXLEN[version]:
            return f"{entry!r} is too broad"
    return None


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        sa.text(
            "SELECT name, pilot_system -> 'ip_allowlist' FROM first.cluster "
            "WHERE jsonb_typeof(pilot_system) = 'object'"
        )
    ).all()
    problems = [
        f"Cluster {name}: {problem}"
        for name, allowlist in rows
        if (problem := _allowlist_problem(allowlist)) is not None
    ]
    if problems:
        raise RuntimeError(
            "Stored pilot_system ip_allowlist values would be unreadable after "
            "this upgrade; re-apply corrected Cluster manifests first:\n"
            + "\n".join(problems)
        )

    conn.execute(
        sa.text(
            "UPDATE first.cluster SET pilot_system = pilot_system || "
            "jsonb_build_object('max_nodes_per_job', "
            "GREATEST(COALESCE((pilot_system ->> 'max_num_nodes')::int, 1), 1)) "
            "WHERE jsonb_typeof(pilot_system) = 'object' "
            "AND NOT pilot_system ? 'max_nodes_per_job'"
        )
    )


def downgrade() -> None:
    # Older code ignores the key, but drop it so a re-upgrade backfills afresh.
    op.execute(
        "UPDATE first.cluster SET pilot_system = pilot_system - 'max_nodes_per_job' "
        "WHERE jsonb_typeof(pilot_system) = 'object'"
    )
