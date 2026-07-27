"""Tests for ReplicaLauncher: launching placed replicas onto running pilot jobs."""

import itertools
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_replica_counter = itertools.count()

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import (
    PilotJobStatus,
    PilotResources,
    ReplicaInfo,
)
from first_common.schema.types import ReplicaState
from first_gateway.controllers.worker import Worker
from first_gateway.controllers.workers.replica_launcher import ReplicaLauncher
from first_gateway.database.models import (
    AccessGroup,
    Cluster,
    Model,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)
from first_gateway.services.pilot_control import PilotControlClient

NOW = datetime(2026, 7, 12, 12, 0, 0, tzinfo=timezone.utc)

MANAGER_URL = "https://10.0.0.1:8443/control"

# A complete, valid PilotLaunchSpec (validated by the launcher when building the
# start request).
LAUNCH_SPEC: dict[str, Any] = {
    "served_model_name": "llama-3",
    "gpus_per_node": 4,
    "num_nodes": 1,
    "venv_path": "/unused",
    "weights_path": "/unused",
    "weights_cache_path": "/unused",
    "env": {},
    "serve_script_template": "echo {{ port }}",
    "max_startup_sec": 60,
    "health_check": {"url": "http://localhost/health"},
}


# ── Transport / controller construction ─────────────────────────────────


def _reject(request: httpx.Request) -> httpx.Response:
    """Handler for tests that never expect an HTTP call to be made."""
    return httpx.Response(404)


def _make_transport(
    handler: Any,
) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


def _make_controller(
    db: async_sessionmaker[AsyncSession],
    handler: Any,
) -> ReplicaLauncher:
    cs = MagicMock()
    cs.db_sessionmaker = db
    cs.redis_pubsub.publish = AsyncMock()
    ctrl = ReplicaLauncher.__new__(ReplicaLauncher)
    Worker.__init__(ctrl, "replica-launcher", cs, MagicMock())
    client = PilotControlClient.__new__(PilotControlClient)
    client._client = httpx.AsyncClient(transport=_make_transport(handler))
    ctrl.client = client
    return ctrl


# ── Seed helpers ────────────────────────────────────────────────────────


async def _seed_parents(sess: AsyncSession) -> None:
    sess.add(
        Cluster(
            name="polaris",
            health_check={"url": "http://x/health", "debounce": 2},
        )
    )
    sess.add(AccessGroup(name="default-ag", allowed_groups=[], allowed_domains=[]))
    await sess.flush()
    sess.add(
        Model(
            name="llama", access_group_name="default-ag", supported_endpoints=["chat"]
        )
    )
    await sess.flush()


async def _insert_deployment(
    sess: AsyncSession,
    name: str = "deploy-1",
) -> None:
    sess.add(
        PilotDeployment(
            name=name,
            cluster_name="polaris",
            model_name="llama",
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=10,
            launch_spec=LAUNCH_SPEC,
        )
    )
    await sess.flush()


async def _insert_job(
    sess: AsyncSession,
    name: str = "job-1",
    *,
    scheduler_state: SchedulerJobState = SchedulerJobState.running,
    manager_url: str | None = MANAGER_URL,
) -> str:
    sess.add(
        PilotJob(
            name=name,
            cluster_name="polaris",
            scheduler_state=scheduler_state.value,
            manager_url=manager_url,
            walltime_min=60,
            num_nodes=1,
            gpus_per_node=4,
        )
    )
    await sess.flush()
    return name


async def _insert_replica(
    sess: AsyncSession,
    deployment_name: str = "deploy-1",
    *,
    state: ReplicaState = ReplicaState.placed,
    pilot_job_name: str | None = "job-1",
    claimed_gpu_ids: list[tuple[int, int]] | None = None,
    scheduled_deletion_at: datetime | None = None,
    deleted_at: datetime | None = None,
    reconcile_retry_at: datetime | None = None,
) -> int:
    r = PilotReplica(
        name=f"{deployment_name}/replica/{next(_replica_counter)}",
        pilot_deployment_name=deployment_name,
        state=state.value,
        pilot_job_name=pilot_job_name,
        claimed_gpu_ids=claimed_gpu_ids or [(0, 0), (0, 1)],
        scheduled_deletion_at=scheduled_deletion_at,
        deleted_at=deleted_at,
        reconcile_retry_at=reconcile_retry_at,
    )
    sess.add(r)
    await sess.flush()
    return r.uid


async def _get_replica(db: async_sessionmaker[AsyncSession], uid: int) -> PilotReplica:
    async with db() as sess:
        r = await sess.get(PilotReplica, uid)
    assert r is not None
    return r


async def _get_deployment(
    db: async_sessionmaker[AsyncSession], name: str
) -> PilotDeployment:
    async with db() as sess:
        dep = await PilotDeployment.get_by_name(sess, name)
    return dep


def _status_json(replica_names: list[str]) -> dict[str, Any]:
    replicas = [
        ReplicaInfo(
            name=n,
            url="http://10.0.0.1:8000",
            state=ReplicaState.launching,
            started_at=NOW,
            state_message="starting",
            served_model_name="llama-3",
            resources=[],
            log_path=Path("/path/to/replica.log"),
        )
        for n in replica_names
    ]
    return PilotJobStatus(
        resources=PilotResources(hosts=[]), replicas=replicas
    ).model_dump(mode="json")


# ── list_actionable ──────────────────────────────────────────────────────


async def test_list_actionable_includes_placed_on_running_job(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == [uid]


async def test_list_actionable_excludes_when_job_not_running(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess, scheduler_state=SchedulerJobState.starting)
        await _insert_replica(sess)

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


async def test_list_actionable_excludes_when_manager_url_missing(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess, manager_url=None)
        await _insert_replica(sess)

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


async def test_list_actionable_excludes_non_placed_and_draining(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_replica(sess, state=ReplicaState.launching)
        await _insert_replica(sess, scheduled_deletion_at=NOW)
        await _insert_replica(sess, deleted_at=NOW)

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


async def test_list_actionable_excludes_backoff(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        await _insert_replica(
            sess, reconcile_retry_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )

    ctrl = _make_controller(db, _reject)
    async with db() as sess:
        actionable = await ctrl.list_actionable(sess)

    assert actionable == []


# ── reconcile: happy path ────────────────────────────────────────────────


async def test_reconcile_launches_placed_replica(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = request.read()
        return httpx.Response(200)

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.launching.value
    assert seen["path"] == "/control/start-replica"
    assert b'"gpu_indices"' in seen["body"]


async def test_reconcile_noop_when_job_not_ready(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """Guarded against a stale wake: replica placed but job's manager_url gone."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess, manager_url=None)
        uid = await _insert_replica(sess)

    called = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200)

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    assert not called
    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value


async def test_reconcile_missing_replica_is_noop(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)

    ctrl = _make_controller(db, _reject)
    await ctrl.reconcile(999_999)


# ── reconcile: 400 ReplicaStartError ─────────────────────────────────────


async def test_reconcile_start_error_marks_error_and_counts_failure(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={
                "error": {
                    "code": "replica_start_error",
                    "message": "boom",
                    "info": {},
                }
            },
        )

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.error.value
    assert "boom" in replica.state_message

    dep = await _get_deployment(db, "deploy-1")
    assert dep.consecutive_launch_failures == 1


# ── reconcile: 409 ReplicaAlreadyPlaced ──────────────────────────────────


async def test_reconcile_conflict_confirmed_marks_launching(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """409 + replica present in /status → treat as a successful launch."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    replica_name = (await _get_replica(db, uid)).name

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/start-replica"):
            return httpx.Response(
                409,
                json={
                    "error": {
                        "code": "replica_already_placed",
                        "message": "dup",
                        "info": {},
                    }
                },
            )
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json=_status_json([replica_name]))
        return httpx.Response(404)

    ctrl = _make_controller(db, handler)
    await ctrl.reconcile(uid)

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.launching.value

    dep = await _get_deployment(db, "deploy-1")
    assert dep.consecutive_launch_failures == 0


async def test_reconcile_conflict_absent_raises(
    db: async_sessionmaker[AsyncSession],
) -> None:
    """409 but the replica isn't actually registered → raise for cooldown/retry."""
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/start-replica"):
            return httpx.Response(
                409,
                json={"error": {"code": "x", "message": "dup", "info": {}}},
            )
        if request.url.path.endswith("/status"):
            return httpx.Response(200, json=_status_json([]))
        return httpx.Response(404)

    ctrl = _make_controller(db, handler)
    try:
        await ctrl.reconcile(uid)
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected RuntimeError when replica absent after 409")

    # State is unchanged; the base class would record a failure and cool down.
    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value


# ── reconcile: 5xx bubbles up ────────────────────────────────────────────


async def test_reconcile_server_error_raises_without_penalty(
    db: async_sessionmaker[AsyncSession],
) -> None:
    async with db.begin() as sess:
        await _seed_parents(sess)
        await _insert_deployment(sess)
        await _insert_job(sess)
        uid = await _insert_replica(sess)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    ctrl = _make_controller(db, handler)
    try:
        await ctrl.reconcile(uid)
    except httpx.HTTPStatusError:
        pass
    else:
        raise AssertionError("expected HTTPStatusError on 500")

    replica = await _get_replica(db, uid)
    assert replica.state == ReplicaState.placed.value
    dep = await _get_deployment(db, "deploy-1")
    assert dep.consecutive_launch_failures == 0
