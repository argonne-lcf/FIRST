"""DB/Redis integration tests for the PilotAutoscaler controller.

These exercise the reconcile shell: reading Model + child deployments, sampling
the model runtime from Redis, and writing desired_replicas with a premised
update. The signal/ladder/sustain math itself is covered in
test_autoscaler_logic.py.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from first_common.schema.resources.runtime import (
    AutoscalerModelRuntime,
    RejectSample,
    ScaledownCandidate,
)
from first_common.schema.types import DemandSignalConfig
from first_gateway import Settings
from first_gateway.controllers.workers.autoscaler import (
    PilotAutoscaler,
    decide_scale,
    ladder_target,
    update_ewma,
    update_reject_window,
)
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
)
from first_gateway.database.redis.keys import Keys
from first_gateway.database.redis.pubsub import Channel, RedisPubSub
from first_gateway.database.redis.repo import RedisRepo

NOW = datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc)


def _at(offset_sec: float) -> datetime:
    return NOW + timedelta(seconds=offset_sec)


@pytest.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    r = Redis.from_url(Settings().redis_url, decode_responses=True)
    try:
        yield r
    finally:
        await r.aclose()


def _make_controller(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> PilotAutoscaler:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_repo = RedisRepo(redis)
    cs.redis_pubsub = RedisPubSub(redis)
    cs.redis_pubsub.publish = AsyncMock()
    return PilotAutoscaler("pilot-autoscaler", cs, MagicMock())


async def _seed_parents(
    sess: AsyncSession, demand_signal: dict[str, Any] | None = None
) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
            pilot_system=None,
        )
    )
    sess.add(AccessGroup(name="default-ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(
        Model(
            name="llama",
            access_group_name="default-ag",
            supported_endpoints=["chat"],
            demand_signal=demand_signal or {},
        )
    )
    await sess.flush()


async def _insert_deployment(
    sess: AsyncSession,
    name: str,
    *,
    model_name: str = "llama",
    desired_replicas: int = 0,
    min_replicas: int = 0,
    max_replicas: int = 10,
    scaling_strategy: dict[str, Any] | None = None,
    consecutive_launch_failures: int = 0,
    max_consecutive_launch_failures: int = 3,
) -> int:
    dep = PilotDeployment(
        name=name,
        cluster_name="polaris",
        model_name=model_name,
        router_params={},
        prometheus_scrape_interval_sec=30,
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        scaling_strategy=scaling_strategy,
        launch_spec={"num_nodes": 1, "gpus_per_node": 4},
        desired_replicas=desired_replicas,
        consecutive_launch_failures=consecutive_launch_failures,
        max_consecutive_launch_failures=max_consecutive_launch_failures,
    )
    sess.add(dep)
    await sess.flush()
    return dep.uid


async def _get_desired(db: async_sessionmaker[AsyncSession], uid: int) -> int:
    async with db() as sess:
        dep = await sess.get(PilotDeployment, uid)
    assert dep is not None
    return dep.desired_replicas


def _strategy(
    thresholds: list[tuple[float, int]],
    *,
    immediate_cold_start: bool = True,
    scale_down_sustain_sec: int = 7200,
) -> dict[str, Any]:
    return {
        "strategy": "DemandThresholdStrategy",
        "immediate_cold_start": immediate_cold_start,
        "scale_down_sustain_sec": scale_down_sustain_sec,
        "scaling_thresholds": [list(t) for t in thresholds],
    }


# ---------------------------------------------------------------------------
# list_actionable
# ---------------------------------------------------------------------------


async def test_list_actionable_only_models_with_deployments(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        # Second model with no deployments.
        sess.add(
            Model(
                name="empty-model",
                access_group_name="default-ag",
                supported_endpoints=["chat"],
            )
        )
        await sess.flush()
        model = await Model.get_by_name(sess, "llama")
        await _insert_deployment(sess, "dep-a")

    ctrl = _make_controller(db, redis)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == [model.uid]


# ---------------------------------------------------------------------------
# scale up via cold start (reject-driven)
# ---------------------------------------------------------------------------


async def test_cold_start_on_recent_reject(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess, "dep-cold", desired_replicas=0, scaling_strategy=_strategy([(0.0, 1)])
        )

    # A recent capacity reject drives cold start even with zero inflight.
    await redis.hset(
        Keys.model_rejects("llama"),
        mapping={
            "capacity_rejects_total": "5",
            "last_reject_ts": str(datetime.now(timezone.utc).timestamp()),
        },
    )

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    assert await _get_desired(db, uid) == 1
    ctrl.client_state.redis_pubsub.publish.assert_awaited_once_with(  # type: ignore[attr-defined]
        Channel.desired_replicas_changed, "dep-cold"
    )


async def test_lazy_and_eager_siblings_diverge(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    """immediate_cold_start True/False siblings react differently to the same
    reject-driven signal at desired=0."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        model = await Model.get_by_name(sess, "llama")
        eager = await _insert_deployment(
            sess,
            "dep-eager",
            desired_replicas=0,
            scaling_strategy=_strategy([(0.0, 1)], immediate_cold_start=True),
        )
        lazy = await _insert_deployment(
            sess,
            "dep-lazy",
            desired_replicas=0,
            scaling_strategy=_strategy([(0.0, 1)], immediate_cold_start=False),
        )

    await redis.hset(
        Keys.model_rejects("llama"),
        mapping={
            "capacity_rejects_total": "5",
            "last_reject_ts": str(datetime.now(timezone.utc).timestamp()),
        },
    )

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    # Eager jumps to 1; lazy stays at 0 (ladder gate only, no inflight yet).
    assert await _get_desired(db, eager) == 1
    assert await _get_desired(db, lazy) == 0


# ---------------------------------------------------------------------------
# scale up via inflight-driven EWMA
# ---------------------------------------------------------------------------


async def test_scale_up_from_inflight(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        # alpha=1.0 so ewma == instantaneous in one tick.
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess,
            "dep-up",
            desired_replicas=1,
            scaling_strategy=_strategy([(0.0, 1), (10.0, 2), (25.0, 3)]),
        )

    # 30 inflight -> ewma 30 -> ladder target 3.
    for i in range(30):
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    assert await _get_desired(db, uid) == 3


# ---------------------------------------------------------------------------
# manual scaling + latch
# ---------------------------------------------------------------------------


async def test_manual_scaling_left_alone(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess, "dep-manual", desired_replicas=5, scaling_strategy=None
        )

    for i in range(50):
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    assert await _get_desired(db, uid) == 5
    ctrl.client_state.redis_pubsub.publish.assert_not_awaited()  # type: ignore[attr-defined]


async def test_latch_pins_zero_when_launch_failures_exceed_max(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess,
            "dep-latch",
            desired_replicas=3,
            scaling_strategy=_strategy([(0.0, 1), (10.0, 2)]),
            consecutive_launch_failures=4,
            max_consecutive_launch_failures=3,
        )

    # Plenty of inflight demand — but the latch pins desired to 0 regardless.
    for i in range(50):
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    assert await _get_desired(db, uid) == 0


# ---------------------------------------------------------------------------
# per-model fan-out: one sample drives staggered-ladder spillover
# ---------------------------------------------------------------------------


async def test_two_tier_spillover(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    """Deployment B's rungs sit above A's, so at moderate demand A scales up
    while B stays at its minimum. One sample drives both."""
    async with db.begin() as sess:
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        a = await _insert_deployment(
            sess,
            "dep-a",
            desired_replicas=0,
            min_replicas=0,
            scaling_strategy=_strategy(
                [(0.0, 1), (10.0, 2)], immediate_cold_start=False
            ),
        )
        b = await _insert_deployment(
            sess,
            "dep-b",
            desired_replicas=0,
            min_replicas=0,
            # B only adds capacity above demand 100.
            scaling_strategy=_strategy(
                [(100.0, 1), (200.0, 2)], immediate_cold_start=False
            ),
        )

    for i in range(15):
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    # ewma 15 -> A at rung 2; B still below its bottom rung -> min_replicas (0).
    assert await _get_desired(db, a) == 2
    assert await _get_desired(db, b) == 0


# ---------------------------------------------------------------------------
# scale-down sustain enactment through Redis-persisted candidates
# ---------------------------------------------------------------------------


async def test_scale_down_enacts_after_sustain(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess,
            "dep-sd",
            desired_replicas=3,
            scaling_strategy=_strategy(
                [(0.0, 1), (10.0, 2), (25.0, 3)], scale_down_sustain_sec=120
            ),
        )

    # Pre-seed a candidate whose sustain window has already elapsed.
    rt = AutoscalerModelRuntime(
        scale_down_candidates={
            "dep-sd": [
                ScaledownCandidate(
                    num_replicas=2,
                    starting_from=datetime.now(timezone.utc) - timedelta(seconds=300),
                )
            ]
        }
    )
    await RedisRepo(redis).set_autoscaler_model_runtime("llama", rt)

    # Low current demand keeps the ladder target at 2 (5 inflight -> rung 1? no,
    # 5 -> rung 1 == 1). Use 15 inflight so target=2 matches the candidate rung.
    for i in range(15):
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    assert await _get_desired(db, uid) == 2


async def test_scale_down_held_below_sustain_no_change(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess,
            "dep-hold",
            desired_replicas=3,
            scaling_strategy=_strategy(
                [(0.0, 1), (10.0, 2), (25.0, 3)], scale_down_sustain_sec=120
            ),
        )

    for i in range(15):  # ewma 15 -> target 2, below current 3
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)
    await ctrl.reconcile(model.uid)

    # First tick just records the candidate; desired unchanged.
    assert await _get_desired(db, uid) == 3
    rt = await RedisRepo(redis).get_autoscaler_model_runtime("llama")
    assert rt.scale_down_candidates["dep-hold"][0].num_replicas == 2


# ---------------------------------------------------------------------------
# premised write: stale premise skips this deployment, siblings still written
# ---------------------------------------------------------------------------


async def test_stale_premise_skips_without_publishing(
    db: async_sessionmaker[AsyncSession], redis: Redis
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess, demand_signal={"ewma_alpha": 1.0})
        model = await Model.get_by_name(sess, "llama")
        uid = await _insert_deployment(
            sess,
            "dep-race",
            desired_replicas=1,
            scaling_strategy=_strategy([(0.0, 1), (10.0, 2), (25.0, 3)]),
        )

    for i in range(30):  # would scale to 3
        await redis.zadd(Keys.model_inflight("llama"), {f"req-{i}": 1.0})

    ctrl = _make_controller(db, redis)

    # Simulate a concurrent operator edit landing between read and write by
    # bumping desired_replicas after the controller loads the deployment.
    orig_write = ctrl._write_desired

    async def racing_write(dep: PilotDeployment, new_desired: int) -> None:
        async with db.begin() as sess:
            await sess.execute(
                sa.update(PilotDeployment)
                .where(PilotDeployment.uid == dep.uid)
                .values(desired_replicas=7)
            )
        await orig_write(dep, new_desired)

    ctrl._write_desired = racing_write  # type: ignore[method-assign]
    await ctrl.reconcile(model.uid)

    # The premise (desired_replicas == 1) no longer holds; write is a no-op.
    assert await _get_desired(db, uid) == 7
    ctrl.client_state.redis_pubsub.publish.assert_not_awaited()  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# reject-rate windowing
# ---------------------------------------------------------------------------


def test_reject_rate_over_window() -> None:
    window = [RejectSample(ts=_at(0), rejects_total=10)]
    new_window, rate = update_reject_window(
        window, _at(60), rejects_total=70, reject_window_sec=60
    )
    # 60 rejects over 60s = 1.0/sec
    assert rate == 1.0
    assert new_window[0].rejects_total == 10  # oldest retained
    assert new_window[-1] == RejectSample(ts=_at(60), rejects_total=70)


def test_reject_rate_drops_stale_samples() -> None:
    window = [
        RejectSample(ts=_at(0), rejects_total=0),  # stale, > 60s ago
        RejectSample(ts=_at(50), rejects_total=20),
    ]
    new_window, rate = update_reject_window(
        window, _at(100), rejects_total=40, reject_window_sec=60
    )
    # The t=0 sample is dropped (cutoff = t=40); reference is t=50.
    assert new_window[0].rejects_total == 20
    # 20 rejects over 50s = 0.4/sec
    assert rate == 20 / 50


def test_reject_rate_clamps_negative_delta_on_counter_reset() -> None:
    """A Redis flush resets the monotonic counter; delta must clamp to 0."""
    window = [RejectSample(ts=_at(0), rejects_total=1000)]
    _, rate = update_reject_window(
        window, _at(30), rejects_total=5, reject_window_sec=60
    )
    assert rate == 0.0


def test_reject_rate_zero_dt_is_safe() -> None:
    _, rate = update_reject_window([], NOW, rejects_total=100, reject_window_sec=60)
    assert rate == 0.0


# ---------------------------------------------------------------------------
# calculate_demand + EWMA
# ---------------------------------------------------------------------------


def test_calculate_demand() -> None:
    cfg = DemandSignalConfig(avg_request_duration_sec=30)
    # inflight 5 + reject_rate 2/sec * 30s = 5 + 60 = 65
    assert cfg.calculate_demand(inflight=5, reject_rate=2.0) == 65.0


def test_ewma_update() -> None:
    assert update_ewma(prev=10.0, instantaneous=20.0, alpha=0.5) == 15.0
    assert update_ewma(prev=0.0, instantaneous=100.0, alpha=1.0) == 100.0


# ---------------------------------------------------------------------------
# ladder
# ---------------------------------------------------------------------------

_LADDER = [(0.0, 1), (10.0, 2), (25.0, 3)]


def test_ladder_below_bottom_is_min_replicas() -> None:
    assert ladder_target(0.0, _LADDER, min_replicas=0, max_replicas=5) == 0
    assert ladder_target(-5.0, _LADDER, min_replicas=2, max_replicas=5) == 2


def test_ladder_interior_rungs() -> None:
    assert ladder_target(5.0, _LADDER, min_replicas=0, max_replicas=5) == 1
    assert ladder_target(15.0, _LADDER, min_replicas=0, max_replicas=5) == 2
    assert ladder_target(30.0, _LADDER, min_replicas=0, max_replicas=5) == 3


def test_ladder_boundaries_are_exclusive_lower_inclusive_upper() -> None:
    # exactly on a rung's lower bound stays on the rung below (`>` lower bound)
    assert ladder_target(10.0, _LADDER, min_replicas=0, max_replicas=5) == 1
    # just above jumps up (`<=` upper bound)
    assert ladder_target(10.001, _LADDER, min_replicas=0, max_replicas=5) == 2


def test_ladder_above_top_capped_by_max_replicas() -> None:
    assert ladder_target(1000.0, _LADDER, min_replicas=0, max_replicas=2) == 2
    assert ladder_target(1000.0, _LADDER, min_replicas=0, max_replicas=5) == 3


# ---------------------------------------------------------------------------
# scale up
# ---------------------------------------------------------------------------


def test_scale_up_is_immediate() -> None:
    desired, candidates = decide_scale(
        now=NOW,
        ewma=30.0,
        thresholds=_LADDER,
        min_replicas=0,
        max_replicas=5,
        current_desired=1,
        candidates=[],
        sustain_sec=120,
    )
    assert desired == 3
    assert candidates == []


def test_scale_up_clears_pending_candidates() -> None:
    desired, candidates = decide_scale(
        now=NOW,
        ewma=30.0,
        thresholds=_LADDER,
        min_replicas=0,
        max_replicas=5,
        current_desired=1,
        candidates=[ScaledownCandidate(num_replicas=1, starting_from=_at(-10))],
        sustain_sec=120,
    )
    assert desired == 3
    assert candidates == []


# ---------------------------------------------------------------------------
# scale-down sustain — the worked example from controllers.md
# ---------------------------------------------------------------------------


def test_worked_example_scale_down_sustain() -> None:
    """Reproduce the docs table: thresholds [(0,1),(10,2),(25,3)], sustain 120s,
    starting from desired=3."""
    thresholds = _LADDER
    sustain = 120
    desired = 3
    candidates: list[ScaledownCandidate] = []

    def step(t: float, ewma: float) -> None:
        nonlocal desired, candidates
        desired, candidates = decide_scale(
            now=_at(t),
            ewma=ewma,
            thresholds=thresholds,
            min_replicas=0,
            max_replicas=5,
            current_desired=desired,
            candidates=candidates,
            sustain_sec=sustain,
        )

    # t=0, ewma=30 -> target 3 == current, no candidate
    step(0, 30)
    assert desired == 3
    assert candidates == []

    # t=10, ewma=8 -> target 1, append (1, t=10)
    step(10, 8)
    assert desired == 3
    assert candidates == [ScaledownCandidate(1, _at(10))]

    # t=20, ewma=12 -> target 2; ewma rose above rung-1, drop (1,·), append (2, t=20)
    step(20, 12)
    assert desired == 3
    assert candidates == [ScaledownCandidate(2, _at(20))]

    # t=120, ewma=12 -> (2, t=20) held 100s, not yet eligible
    step(120, 12)
    assert desired == 3
    assert candidates == [ScaledownCandidate(2, _at(20))]

    # t=140, ewma=12 -> (2, t=20) held 120s -> eligible; desired=2; clear >= 2
    step(140, 12)
    assert desired == 2
    assert candidates == []

    # t=150, ewma=3 -> target 1, append (1, t=150)
    step(150, 3)
    assert desired == 2
    assert candidates == [ScaledownCandidate(1, _at(150))]

    # t=270, ewma=3 -> (1, t=150) held 120s -> eligible; desired=1; clear
    step(270, 3)
    assert desired == 1
    assert candidates == []


def test_scale_down_candidate_survives_deeper_dip_same_rung() -> None:
    """Docs note: had ewma fallen 8->4 (still rung 1) at t=20, the (1, t=10)
    candidate survives and becomes eligible at t=130."""
    thresholds = _LADDER
    desired = 3
    candidates: list[ScaledownCandidate] = []

    desired, candidates = decide_scale(
        now=_at(10),
        ewma=8,
        thresholds=thresholds,
        min_replicas=0,
        max_replicas=5,
        current_desired=desired,
        candidates=candidates,
        sustain_sec=120,
    )
    assert candidates == [ScaledownCandidate(1, _at(10))]

    # deeper into the same rung — candidate must survive, clock not reset
    desired, candidates = decide_scale(
        now=_at(20),
        ewma=4,
        thresholds=thresholds,
        min_replicas=0,
        max_replicas=5,
        current_desired=desired,
        candidates=candidates,
        sustain_sec=120,
    )
    assert desired == 3
    assert candidates == [ScaledownCandidate(1, _at(10))]

    # eligible at t=130
    desired, candidates = decide_scale(
        now=_at(130),
        ewma=4,
        thresholds=thresholds,
        min_replicas=0,
        max_replicas=5,
        current_desired=desired,
        candidates=candidates,
        sustain_sec=120,
    )
    assert desired == 1
    assert candidates == []


def test_scale_down_multi_rung_lowest_eligible_wins() -> None:
    """Two candidates accumulate; when both are eligible the lowest wins and all
    at-or-below candidates are cleared."""
    candidates = [
        ScaledownCandidate(num_replicas=2, starting_from=_at(0)),
        ScaledownCandidate(num_replicas=1, starting_from=_at(10)),
    ]
    desired, new_candidates = decide_scale(
        now=_at(200),
        ewma=3.0,  # rung 1
        thresholds=_LADDER,
        min_replicas=0,
        max_replicas=5,
        current_desired=3,
        candidates=candidates,
        sustain_sec=120,
    )
    assert desired == 1
    assert new_candidates == []
