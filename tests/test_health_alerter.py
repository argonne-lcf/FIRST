"""Tests for the HealthAlerter worker.

The debounce/recovery logic lives in the pure `advance()` function and is
tested there with plain dicts and an injected clock — no DB or Redis. The
integration tests only cover poll() wiring (checks → advance → post → commit →
Redis) and the real SQL in each check function.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import Redis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.resources.runtime import (
    CommittedAlert,
    HealthAlertState,
    Severity,
    StagedTransition,
)
from first_common.schema.types import (
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
)
from first_gateway import Settings
from first_gateway.controllers.workers.health_alerter import (
    HealthAlerter,
    Observation,
    _build_alert_blocks,
    advance,
    check_cluster_health,
    check_db_liveness,
    check_pilot_deployment,
    check_pilot_job,
    check_pilot_replica,
    check_static_deployment,
)
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
    StaticDeployment,
)
from first_gateway.database.redis.repo import RedisRepo

DEBOUNCE = timedelta(seconds=45)


def _obs(
    key: str, status: str, severity: Severity = "crit", owner: str = "check_x"
) -> Observation:
    return Observation(
        key=key, status=status, summary=status, severity=severity, owner=owner
    )


# ---------------------------------------------------------------------------
# Pure state-machine tests (no DB / Redis / clock)
# ---------------------------------------------------------------------------


def test_advance_stages_then_matures() -> None:
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    state = HealthAlertState()
    obs = {"cluster/1/health": _obs("cluster/1/health", "unhealthy")}

    plan = advance(state, obs, {"check_x"}, t0, DEBOUNCE)
    assert plan.degradations == []  # staged, not matured
    assert "cluster/1/health" in state.staging

    plan = advance(state, obs, {"check_x"}, t0 + timedelta(seconds=46), DEBOUNCE)
    assert [k for k, _ in plan.degradations] == ["cluster/1/health"]


def test_advance_swallows_flap() -> None:
    """Degrade then clear inside the debounce window → nothing matures."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    state = HealthAlertState()
    key = "cluster/1/health"

    advance(state, {key: _obs(key, "unhealthy")}, {"check_x"}, t0, DEBOUNCE)
    plan = advance(state, {}, {"check_x"}, t0 + timedelta(seconds=30), DEBOUNCE)

    assert plan.degradations == []
    assert plan.recoveries == []
    assert key not in state.staging  # stale entry cleared


def test_advance_recovery_scoped_to_ran_checks() -> None:
    """A committed key whose owning check did NOT run is not recovered."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    state = HealthAlertState(
        committed={"postgres": CommittedAlert(status="down", owner="check_db_liveness")}
    )
    # check_db_liveness raised this tick → absent from ran_checks
    advance(state, {}, {"check_host"}, t0, DEBOUNCE)
    assert "postgres" not in state.staging


def test_advance_recovery_matures_and_commits() -> None:
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    key = "cluster/1/health"
    state = HealthAlertState(
        committed={key: CommittedAlert(status="unhealthy", owner="check_x")}
    )

    advance(state, {}, {"check_x"}, t0, DEBOUNCE)
    plan = advance(state, {}, {"check_x"}, t0 + timedelta(seconds=46), DEBOUNCE)

    assert plan.recoveries == [key]
    HealthAlerter._commit(state, plan)
    assert key not in state.committed
    assert key not in state.staging


def test_advance_info_recovery_clears_committed() -> None:
    """Regression: an info-level recovery must clear committed, not leak it."""
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    key = "pilotjob/1/idle"
    state = HealthAlertState(
        committed={
            key: CommittedAlert(status="idle", severity="info", owner="check_pilot_job")
        }
    )

    advance(state, {}, {"check_pilot_job"}, t0, DEBOUNCE)
    plan = advance(state, {}, {"check_pilot_job"}, t0 + timedelta(seconds=46), DEBOUNCE)

    assert plan.recoveries == [key]  # reported so the caller can clear committed
    HealthAlerter._commit(state, plan)
    assert key not in state.committed  # cleared despite info severity


# ---------------------------------------------------------------------------
# Slack block tests
# ---------------------------------------------------------------------------


def test_alert_blocks_degradation_header() -> None:
    now = datetime.now(timezone.utc)
    staged = StagedTransition(
        status="unhealthy",
        severity="crit",
        summary="bad",
        group="Clusters",
        first_seen=now,
    )
    blocks = _build_alert_blocks([("cluster/1/health", staged)], [], [])
    assert blocks[0]["text"]["text"] == "🚨 Health degradation"


def test_alert_blocks_recovery_header() -> None:
    now = datetime.now(timezone.utc)
    staged = StagedTransition(status="", group="Clusters", first_seen=now)
    blocks = _build_alert_blocks([], [("cluster/1/health", staged)], [])
    assert blocks[0]["text"]["text"] == "✅ Recovery"


def test_alert_blocks_failed_checks() -> None:
    blocks = _build_alert_blocks([], [], [("check_db_liveness", "connection refused")])
    assert "check_db_liveness" in blocks[1]["text"]["text"]


# ---------------------------------------------------------------------------
# Integration fixtures & helpers (DB + Redis required)
# ---------------------------------------------------------------------------


@pytest.fixture
async def redis():  # type: ignore[no-untyped-def]
    url = Settings().redis_url
    r = Redis.from_url(url, decode_responses=True)
    await r.flushdb()
    try:
        yield r
    finally:
        await r.aclose()


def _make_alerter(
    db: async_sessionmaker[AsyncSession],
    redis: Redis,
) -> HealthAlerter:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis = redis
    cs.redis_repo = RedisRepo(redis)
    cs.settings = MagicMock()
    cs.settings.health_slack_webhook_url = "https://hooks.slack.test/webhook"
    cs.settings.gateway_health_url = "http://127.0.0.1/health"
    return HealthAlerter("health-alerter", cs, MagicMock())


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(AccessGroup(name="ag", allowed_groups=[], allowed_domains=[]))
    sess.add(Cluster(name="cl", health_check={"url": "", "debounce": 2}))
    await sess.flush()
    sess.add(Model(name="mdl", access_group_name="ag", supported_endpoints=["chat"]))
    await sess.flush()


async def _redis_state(redis: Redis) -> HealthAlertState:
    return await RedisRepo(redis).get_health_alert_state()


def _sd(
    name: str = "sd1", health: str = HealthCheckResult.unhealthy.value
) -> StaticDeployment:
    return StaticDeployment(
        name=name,
        cluster_name="cl",
        model_name="mdl",
        api_url="http://x",
        upstream_model_name="m",
        router_params={},
        health_check={"url": "", "debounce": 2},
        health=health,
        prometheus_scrape_interval_sec=30,
    )


def _digest_posted(mock_post: AsyncMock) -> bool:
    """True if any posted message was the daily digest (by header)."""
    return any(
        call.args[0] and call.args[0][0]["text"]["text"].startswith("📊")
        for call in mock_post.call_args_list
    )


# ---------------------------------------------------------------------------
# poll() wiring tests
# ---------------------------------------------------------------------------


async def test_degradation_flush_and_commit(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    """Hold unhealthy past the debounce → one POST, committed after 2xx, round-tripped through Redis."""
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_sd())

    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    with patch.object(alerter, "_post_slack", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = True

        await alerter.poll(t0)
        mock_post.assert_not_called()

        await alerter.poll(t0 + timedelta(seconds=46))
        mock_post.assert_called_once()

        state = await _redis_state(redis)
        assert any(k.startswith("staticdeployment/") for k in state.committed)
        assert len(state.staging) == 0


async def test_recovery_after_committed(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    """A committed degradation clears → recovery posted and committed emptied."""
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            Cluster(
                name="cl2",
                health_check={"url": "", "debounce": 2},
                health=HealthCheckResult.unhealthy.value,
            )
        )

    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    with patch.object(alerter, "_post_slack", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = True
        await alerter.poll(t0)
        await alerter.poll(t0 + timedelta(seconds=46))
        assert any(
            k.startswith("cluster/") for k in (await _redis_state(redis)).committed
        )

        async with db.begin() as sess:
            await sess.execute(
                update(Cluster)
                .where(Cluster.name == "cl2")
                .values(health=HealthCheckResult.healthy.value)
            )

        t1 = t0 + timedelta(seconds=100)
        await alerter.poll(t1)
        await alerter.poll(t1 + timedelta(seconds=46))

        assert not any(
            k.startswith("cluster/") for k in (await _redis_state(redis)).committed
        )


async def test_slack_failure_preserves_state(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    """Non-2xx leaves committed/staging intact for retry (no double-send)."""
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_sd())

    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    with patch.object(alerter, "_post_slack", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = False

        await alerter.poll(t0)
        await alerter.poll(t0 + timedelta(seconds=46))

        state = await _redis_state(redis)
        assert len(state.committed) == 0
        assert len(state.staging) > 0


async def test_daily_digest(db: async_sessionmaker[AsyncSession], redis: Redis) -> None:
    """Digest fires once at/after 13:00 UTC and dedups within the day."""
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)

    t_noon = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t_1pm = datetime(2025, 1, 1, 13, 0, 0, tzinfo=timezone.utc)

    with patch.object(alerter, "_post_slack", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = True

        await alerter.poll(t_noon)
        assert not _digest_posted(mock_post)

        await alerter.poll(t_1pm)
        assert _digest_posted(mock_post)
        assert (await _redis_state(redis)).last_daily_report == "2025-01-01"

        mock_post.reset_mock()
        await alerter.poll(t_1pm + timedelta(minutes=1))
        assert not _digest_posted(mock_post)


# ---------------------------------------------------------------------------
# Individual check SQL tests
# ---------------------------------------------------------------------------


async def test_check_cluster_health(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            Cluster(
                name="cl-bad",
                health_check={"url": "", "debounce": 2},
                health=HealthCheckResult.unhealthy.value,
            )
        )

    obs = await check_cluster_health(alerter)
    assert [o for o in obs if o.status == "unhealthy"]


async def test_check_static_deployment(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(_sd("sd-bad"))

    obs = await check_static_deployment(alerter)
    assert len(obs) == 1
    assert obs[0].severity == "crit"


async def test_check_pilot_deployment_state(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            PilotDeployment(
                name="pd1",
                cluster_name="cl",
                model_name="mdl",
                router_params={},
                scaling_strategy=None,
                min_replicas=0,
                max_replicas=2,
                launch_spec={},
                state=PilotDeploymentState.failed.value,
                prometheus_scrape_interval_sec=30,
            )
        )

    obs = await check_pilot_deployment(alerter)
    state_obs = [o for o in obs if "/state" in o.key]
    assert len(state_obs) == 1
    assert state_obs[0].severity == "crit"


async def test_check_pilot_job_reconcile(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            PilotJob(
                name="cl/pilot-job/abc",
                cluster_name="cl",
                walltime_min=60,
                num_nodes=1,
                gpus_per_node=4,
                reconcile_failures=3,
                reconcile_last_error="timeout connecting to scheduler",
            )
        )

    obs = await check_pilot_job(alerter)
    rec_obs = [o for o in obs if "/reconcile" in o.key]
    assert len(rec_obs) == 1
    assert "timeout" in rec_obs[0].summary


async def test_check_pilot_job_idle(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            PilotJob(
                name="cl/pilot-job/idle1",
                cluster_name="cl",
                walltime_min=60,
                num_nodes=1,
                gpus_per_node=4,
                idle_since=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        )

    obs = await check_pilot_job(alerter)
    idle_obs = [o for o in obs if "/idle" in o.key]
    assert len(idle_obs) == 1
    assert idle_obs[0].severity == "info"


async def test_check_pilot_replica_bad_state(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    async with db.begin() as sess:
        await _seed_parents(sess)
        sess.add(
            PilotDeployment(
                name="pd1",
                cluster_name="cl",
                model_name="mdl",
                router_params={},
                scaling_strategy=None,
                min_replicas=0,
                max_replicas=2,
                launch_spec={},
                prometheus_scrape_interval_sec=30,
            )
        )
        await sess.flush()
        sess.add(
            PilotReplica(
                name="pd1/replica/abc",
                pilot_deployment_name="pd1",
                state=ReplicaState.error.value,
                state_message="CUDA OOM",
            )
        )

    obs = await check_pilot_replica(alerter)
    assert len(obs) == 1
    assert obs[0].severity == "crit"
    assert "CUDA OOM" in obs[0].summary


async def test_check_db_liveness_healthy(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    alerter = _make_alerter(db, redis)
    obs = await check_db_liveness(alerter)
    assert len(obs) == 0
