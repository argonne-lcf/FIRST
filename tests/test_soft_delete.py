"""Tests for the SoftDeletable mixin and sweep_expired."""

from datetime import datetime, timedelta, timezone

from sqlalchemy.ext.asyncio import AsyncSession

from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _seed_cluster(sess: AsyncSession) -> str:
    """Insert the minimum parent rows and return the cluster name."""
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(
        Cluster(
            name="cl",
            status_method="none",
            status_kwargs={},
        )
    )
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd",
            cluster_name="cl",
            model_name="mdl",
            router_params={},
            health_check_method="none",
            health_check_kwargs={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=1,
            launch_spec={},
        )
    )
    await sess.flush()
    return "cl"


async def _add_pilot_job(
    sess: AsyncSession,
    name: str,
    *,
    deleted_at: datetime | None = None,
    retention_days: int = 7,
) -> PilotJob:
    job = PilotJob(
        name=name,
        cluster_name="cl",
        walltime_min=60,
        num_nodes=1,
        gpus_per_node=4,
        deleted_at=deleted_at,
        retention_days=retention_days,
    )
    sess.add(job)
    await sess.flush()
    return job


async def _add_pilot_replica(
    sess: AsyncSession,
    name: str,
    *,
    deleted_at: datetime | None = None,
    retention_days: int = 7,
) -> PilotReplica:
    replica = PilotReplica(
        name=name,
        pilot_deployment_name="pd",
        deleted_at=deleted_at,
        retention_days=retention_days,
    )
    sess.add(replica)
    await sess.flush()
    return replica


# ── PilotJob sweep ──────────────────────────────────────────────────


async def test_sweep_deletes_expired_pilot_jobs(db_session: AsyncSession) -> None:
    await _seed_cluster(db_session)
    await _add_pilot_job(
        db_session,
        "expired",
        deleted_at=_now() - timedelta(days=10),
        retention_days=7,
    )
    await db_session.commit()

    count = await PilotJob.sweep_expired(db_session)
    await db_session.commit()

    assert count == 1
    assert await PilotJob.list(db_session) == []


async def test_sweep_keeps_pilot_jobs_within_retention(
    db_session: AsyncSession,
) -> None:
    await _seed_cluster(db_session)
    await _add_pilot_job(
        db_session,
        "recent",
        deleted_at=_now() - timedelta(days=2),
        retention_days=7,
    )
    await db_session.commit()

    count = await PilotJob.sweep_expired(db_session)
    await db_session.commit()

    assert count == 0
    assert len(await PilotJob.list(db_session)) == 1


async def test_sweep_ignores_pilot_jobs_without_deleted_at(
    db_session: AsyncSession,
) -> None:
    await _seed_cluster(db_session)
    await _add_pilot_job(db_session, "alive", deleted_at=None)
    await db_session.commit()

    count = await PilotJob.sweep_expired(db_session)
    await db_session.commit()

    assert count == 0
    assert len(await PilotJob.list(db_session)) == 1


async def test_sweep_respects_per_row_retention_days(
    db_session: AsyncSession,
) -> None:
    """A row deleted 3 days ago with retention_days=2 is swept;
    a row deleted 3 days ago with retention_days=5 is kept."""
    await _seed_cluster(db_session)
    three_days_ago = _now() - timedelta(days=3)
    await _add_pilot_job(
        db_session, "short-retention", deleted_at=three_days_ago, retention_days=2
    )
    await _add_pilot_job(
        db_session, "long-retention", deleted_at=three_days_ago, retention_days=5
    )
    await db_session.commit()

    count = await PilotJob.sweep_expired(db_session)
    await db_session.commit()

    assert count == 1
    remaining = await PilotJob.list(db_session)
    assert len(remaining) == 1
    assert remaining[0].name == "long-retention"


# ── PilotReplica sweep ──────────────────────────────────────────────


async def test_sweep_deletes_expired_pilot_replicas(
    db_session: AsyncSession,
) -> None:
    await _seed_cluster(db_session)
    await _add_pilot_replica(
        db_session,
        "expired-r",
        deleted_at=_now() - timedelta(days=10),
        retention_days=7,
    )
    await db_session.commit()

    count = await PilotReplica.sweep_expired(db_session)
    await db_session.commit()

    assert count == 1
    assert await PilotReplica.list(db_session) == []


async def test_sweep_keeps_pilot_replicas_within_retention(
    db_session: AsyncSession,
) -> None:
    await _seed_cluster(db_session)
    await _add_pilot_replica(
        db_session,
        "recent-r",
        deleted_at=_now() - timedelta(days=2),
        retention_days=7,
    )
    await db_session.commit()

    count = await PilotReplica.sweep_expired(db_session)
    await db_session.commit()

    assert count == 0
    assert len(await PilotReplica.list(db_session)) == 1
