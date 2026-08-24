import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.orm import load_only, selectinload

from first_common.schema.resources.runtime import AlertGroup, Severity
from first_common.schema.types import (
    HealthCheckResult,
    PilotConfig,
    PilotDeploymentState,
    ReplicaState,
)
from first_gateway.controllers.workers.health_alerter.types import Observation
from first_gateway.controllers.workers.replica_placement import AT_CAPACITY
from first_gateway.database.models import (
    Cluster,
    PilotDeployment,
    PilotJob,
    PilotReplica,
    StaticDeployment,
)
from first_gateway.platforms.schedulers import build_scheduler
from first_gateway.settings import ClientState

logger = logging.getLogger(__name__)

_SCHEDULER_CHECK_TIMEOUT_S = 10.0
_DEBOUNCE_S = 150.0


def _error_tail(err: str | None, limit: int = 300) -> str:
    if not err:
        return ""
    lines = [ln.strip() for ln in err.strip().splitlines() if ln.strip()]
    if not lines:
        return ""
    tail = lines[-1]
    return tail[:limit]


_BAD_REPLICA_STATES = {
    ReplicaState.unhealthy.value,
    ReplicaState.error.value,
    ReplicaState.start_timeout.value,
}

# PilotDeployment states worth alerting on, and their severity. States absent
# from this map (healthy, starting) are omitted; recovery is by absence.
_PILOT_DEPLOYMENT_SEVERITY: dict[str, Severity] = {
    PilotDeploymentState.failed.value: "crit",
    PilotDeploymentState.degraded.value: "warn",
    PilotDeploymentState.stopping.value: "info",
    PilotDeploymentState.awaiting_capacity.value: "info",
    PilotDeploymentState.offline.value: "info",
}


@dataclass
class Check:
    func: Callable[[ClientState], Awaitable[list[Observation]]]
    group: AlertGroup


def _disk_severity(use: int) -> Severity | None:
    if use > 90:
        return "crit"
    if use > 80:
        return "warn"
    if use > 70:
        return "info"
    return None


async def check_cluster_health(client_state: ClientState) -> list[Observation]:
    async with client_state.db_sessionmaker() as sess:
        q = sa.select(Cluster.uid, Cluster.name).where(
            Cluster.health == HealthCheckResult.unhealthy.value
        )
        clusters = (await sess.execute(q)).all()

    return [
        Observation(
            key=f"cluster/{c.uid}/health",
            status="unhealthy",
            summary=f"Cluster {c.name}: health check failing",
            display_name=f"Cluster {c.name}",
            severity="crit",
            debounce_s=_DEBOUNCE_S,
        )
        for c in clusters
    ]


async def _check_scheduler(
    pilot_config: PilotConfig, client_state: ClientState, key: str, name: str
) -> Observation | None:
    try:
        adapter = await build_scheduler(pilot_config, client_state)
    except Exception:
        return Observation(
            key=key,
            status="error",
            summary=f"Cluster {name}: failed to build scheduler adapter",
            display_name=f"Cluster {name} scheduler",
            severity="crit",
            debounce_s=_DEBOUNCE_S,
        )

    try:
        async with asyncio.timeout(_SCHEDULER_CHECK_TIMEOUT_S):
            await adapter.get_job_statuses()
    except Exception as e:
        detail = _error_tail(str(e)) or "scheduler check failed"
        return Observation(
            key=key,
            status="error",
            summary=f"Cluster {name}: scheduler check failed: {detail}",
            display_name=f"Cluster {name} scheduler",
            severity="crit",
            debounce_s=_DEBOUNCE_S,
        )
    else:
        return None


async def check_schedulers(client_state: ClientState) -> list[Observation]:
    observations: list[Observation] = []
    async with client_state.db_sessionmaker() as sess:
        q = sa.select(Cluster.uid, Cluster.name, Cluster.pilot_system).where(
            Cluster.pilot_system.is_not(None),
            Cluster.pilot_system != sa.JSON.NULL,
        )
        clusters = (await sess.execute(q)).all()

    for c in clusters:
        assert c.pilot_system is not None
        key = f"cluster/{c.uid}/scheduler"
        pilot_config = PilotConfig.model_validate(c.pilot_system)
        if obs := await _check_scheduler(pilot_config, client_state, key, c.name):
            observations.append(obs)

    return observations


async def check_static_deployment(client_state: ClientState) -> list[Observation]:
    async with client_state.db_sessionmaker() as sess:
        q = sa.select(StaticDeployment.uid, StaticDeployment.name).where(
            StaticDeployment.health == HealthCheckResult.unhealthy.value
        )
        deps = (await sess.execute(q)).all()

    return [
        Observation(
            key=f"staticdeployment/{d.uid}/health",
            status="unhealthy",
            summary=f"StaticDeployment {d.name}: health unhealthy",
            display_name=f"StaticDeployment {d.name}",
            severity="crit",
            debounce_s=_DEBOUNCE_S,
        )
        for d in deps
    ]


async def check_pilot_deployment(client_state: ClientState) -> list[Observation]:
    obs: list[Observation] = []
    async with client_state.db_sessionmaker() as sess:
        deps = (
            await sess.scalars(
                sa.select(PilotDeployment).options(
                    load_only(
                        PilotDeployment.uid, PilotDeployment.name, PilotDeployment.state
                    ),
                    selectinload(PilotDeployment.replicas).load_only(
                        PilotReplica.deleted_at,
                        PilotReplica.state,
                        PilotReplica.state_message,
                    ),
                )
            )
        ).all()

    for d in deps:
        sev = _PILOT_DEPLOYMENT_SEVERITY.get(d.state)
        if sev is not None:
            obs.append(
                Observation(
                    key=f"pilotdeployment/{d.uid}/state",
                    status=d.state,
                    summary=f"PilotDeployment {d.name}: state={d.state}",
                    display_name=f"PilotDeployment {d.name}",
                    severity=sev,
                )
            )

        active_replicas = [r for r in d.replicas if r.deleted_at is None]
        all_pending = bool(active_replicas) and all(
            r.state == ReplicaState.pending.value for r in active_replicas
        )
        any_at_capacity = any(r.state_message == AT_CAPACITY for r in active_replicas)
        if all_pending and any_at_capacity:
            obs.append(
                Observation(
                    key=f"pilotdeployment/{d.uid}/capacity",
                    status="replicas_awaiting_capacity",
                    summary=f"PilotDeployment {d.name}: all replicas awaiting cluster capacity",
                    display_name=f"PilotDeployment {d.name}",
                    severity="info",
                )
            )

    return obs


async def check_pilot_job(client_state: ClientState) -> list[Observation]:
    obs: list[Observation] = []
    async with client_state.db_sessionmaker() as sess:
        jobs = (
            await sess.scalars(sa.select(PilotJob).where(PilotJob.deleted_at.is_(None)))
        ).all()

    for j in jobs:
        if j.reconcile_failures > 0:
            err = _error_tail(j.reconcile_last_error)
            n = j.reconcile_failures
            summary = f"PilotJob {j.name}: {n} reconcile failures"
            if err:
                summary = f"{summary} — {err}"
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/reconcile",
                    status="reconcile_failing",
                    summary=summary,
                    display_name=f"PilotJob {j.name}",
                    recovery_hint=f"{n} reconcile failures",
                    severity="crit",
                )
            )
        if j.manager_health == HealthCheckResult.unhealthy.value:
            since = (
                f" (since {j.manager_unhealthy_since})"
                if j.manager_unhealthy_since
                else ""
            )
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/health",
                    status="manager_unhealthy",
                    summary=f"PilotJob {j.name}: manager unhealthy{since}",
                    display_name=f"PilotJob {j.name}",
                    recovery_hint="manager unhealthy",
                    severity="crit",
                )
            )
        if j.idle_since is not None:
            obs.append(
                Observation(
                    key=f"pilotjob/{j.uid}/idle",
                    status="idle",
                    summary=f"PilotJob {j.name}: idle since {j.idle_since}",
                    display_name=f"PilotJob {j.name}",
                    severity="info",
                )
            )

    return obs


async def check_pilot_replica(client_state: ClientState) -> list[Observation]:
    obs: list[Observation] = []
    async with client_state.db_sessionmaker() as sess:
        replicas = (
            await sess.scalars(
                sa.select(PilotReplica).where(PilotReplica.deleted_at.is_(None))
            )
        ).all()

    for r in replicas:
        if r.state in _BAD_REPLICA_STATES:
            msg = r.state_message or ""
            summary = f"PilotReplica {r.name}: {r.state}"
            if msg:
                summary = f"{summary} — {msg}"
            obs.append(
                Observation(
                    key=f"pilotreplica/{r.uid}/state",
                    status=r.state,
                    summary=summary,
                    display_name=f"PilotReplica {r.name}",
                    recovery_hint=r.state,
                    severity="crit",
                )
            )
        if r.reconcile_failures > 0:
            err = _error_tail(r.reconcile_last_error)
            n = r.reconcile_failures
            summary = f"PilotReplica {r.name}: {n} reconcile failures"
            if err:
                summary = f"{summary} — {err}"
            obs.append(
                Observation(
                    key=f"pilotreplica/{r.uid}/reconcile",
                    status="reconcile_failing",
                    summary=summary,
                    display_name=f"PilotReplica {r.name}",
                    recovery_hint=f"{n} reconcile failures",
                    severity="crit",
                )
            )

    return obs


async def check_db_liveness(client_state: ClientState) -> list[Observation]:
    obs: list[Observation] = []
    try:
        async with client_state.db_sessionmaker() as sess:
            await sess.execute(sa.text("SELECT 1"))
    except Exception as e:
        obs.append(
            Observation(
                key="postgres",
                status="down",
                summary=f"Postgres unreachable: {_error_tail(str(e))}",
                display_name="Postgres",
                severity="crit",
            )
        )
    try:
        await client_state.redis.ping()
    except Exception as e:
        obs.append(
            Observation(
                key="redis",
                status="down",
                summary=f"Redis unreachable: {_error_tail(str(e))}",
                display_name="Redis",
                severity="crit",
            )
        )
    return obs


async def check_host(client_state: ClientState) -> list[Observation]:
    obs: list[Observation] = []
    http = AsyncClient(timeout=10.0)

    gateway_url = client_state.settings.gateway_health_url
    try:
        resp = await http.get(gateway_url)
        if resp.status_code >= 300:
            obs.append(
                Observation(
                    key="gateway_health",
                    status="unreachable",
                    summary=f"Gateway /health returned {resp.status_code}",
                    display_name="Gateway /health",
                    severity="crit",
                )
            )
    except Exception as e:
        obs.append(
            Observation(
                key="gateway_health",
                status="unreachable",
                summary=f"Gateway /health unreachable: {_error_tail(str(e))}",
                display_name="Gateway /health",
                severity="crit",
            )
        )

    try:
        resp = await http.get("http://127.0.0.1:9100/healthz")
        if resp.status_code == 503:
            obs.append(
                Observation(
                    key="controller_healthz",
                    status="stale",
                    summary=f"Controller /healthz: {_error_tail(resp.text)}",
                    display_name="Controller /healthz",
                    severity="crit",
                )
            )
    except Exception as e:
        obs.append(
            Observation(
                key="controller_healthz",
                status="stale",
                summary=f"Controller /healthz unreachable: {_error_tail(str(e))}",
                display_name="Controller /healthz",
                severity="crit",
            )
        )

    proc = await asyncio.create_subprocess_exec(
        "df",
        "-P",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
    except Exception:
        logger.exception("df check failed")
        proc.kill()
        await asyncio.wait_for(proc.wait(), timeout=10)
    else:
        for line in out.decode(errors="replace").splitlines()[1:]:
            fields = line.split()
            if len(fields) < 6:
                continue
            source = fields[0]
            if source in ("tmpfs", "devfs") or not fields[5].startswith("/"):
                continue
            try:
                use = int(fields[4].rstrip("%"))
            except ValueError:
                continue
            mount = " ".join(fields[5:])
            sev = _disk_severity(use)
            if sev is not None:
                obs.append(
                    Observation(
                        key=f"disk:{mount}",
                        status=sev,
                        summary=f"{mount} {use}% full",
                        display_name=f"Disk {mount}",
                        severity=sev,
                    )
                )

    return obs


CHECK_REGISTRY = [
    Check(check_cluster_health, "Clusters"),
    Check(check_schedulers, "Clusters"),
    Check(check_static_deployment, "Deployments"),
    Check(check_pilot_deployment, "Deployments"),
    Check(check_pilot_job, "Pilot Jobs"),
    Check(check_pilot_replica, "Pilot Replicas"),
    Check(check_db_liveness, "Infrastructure"),
    Check(check_host, "Infrastructure"),
]
