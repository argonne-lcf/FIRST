"""Tests for the RetentionSweeper worker."""

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_gateway.controllers.retention import RetentionSweeper
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


def _make_client_state(
    db: async_sessionmaker[AsyncSession],
) -> MagicMock:
    cs = MagicMock()
    cs.db_sessionmaker = db
    return cs


async def _seed(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"health_url": ""}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()
    sess.add(
        PilotDeployment(
            name="pd",
            cluster_name="cl",
            model_name="mdl",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=1,
            launch_spec={},
        )
    )
    await sess.flush()


async def test_sweeper_starts_and_heartbeats(
    db: async_sessionmaker[AsyncSession],
) -> None:
    sweeper = RetentionSweeper(
        "retention-sweeper",
        _make_client_state(db),
        heartbeat_timeout=10,
    )
    sweeper.poll_interval = 0.05

    task = asyncio.create_task(sweeper.run())
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    status = sweeper.check_heartbeat()
    assert not status.timed_out, "heartbeat should not have timed out"
    assert len(sweeper._heartbeats) == 1
    assert sweeper._heartbeats[0].name == "retention-sweeper.sweep"


async def test_sweeper_deletes_expired_rows(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        sess.add(
            PilotJob(
                name="expired-job",
                cluster_name="cl",
                walltime_min=60,
                num_nodes=1,
                gpus_per_node=4,
                deleted_at=_now() - timedelta(days=10),
                retention_days=7,
            )
        )
        sess.add(
            PilotReplica(
                name="expired-replica",
                pilot_deployment_name="pd",
                deleted_at=_now() - timedelta(days=10),
                retention_days=7,
            )
        )

    sweeper = RetentionSweeper(
        "retention-sweeper",
        _make_client_state(db),
        heartbeat_timeout=10,
    )
    await sweeper._sweep_all()

    async with db() as sess:
        assert await PilotJob.list(sess) == []
        assert await PilotReplica.list(sess) == []


async def test_sweeper_keeps_rows_within_retention(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed(sess)
        sess.add(
            PilotJob(
                name="recent-job",
                cluster_name="cl",
                walltime_min=60,
                num_nodes=1,
                gpus_per_node=4,
                deleted_at=_now() - timedelta(days=2),
                retention_days=7,
            )
        )

    sweeper = RetentionSweeper(
        "retention-sweeper",
        _make_client_state(db),
        heartbeat_timeout=10,
    )
    await sweeper._sweep_all()

    async with db() as sess:
        remaining = await PilotJob.list(sess)
        assert len(remaining) == 1
        assert remaining[0].name == "recent-job"
