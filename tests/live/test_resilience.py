"""
Resilience of the fleet model: a replica's model server dies, and a cluster
goes into maintenance with its pilot jobs killed underneath it.

Both run a low background demand pump so the user-visible impact (status mix,
messages) is recorded alongside the control-plane timeline.  The failures are
injected from inside the controller container, behind the controllers' backs:
the replica is stopped through its pilot manager, the pilot jobs are
terminated at the scheduler.
"""

import asyncio
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from .backend import Admin, Compose, Docs, find, log_message
from .conftest import LIVE, require_model
from .harness import (
    STARTUP_TIMEOUT,
    DemandPump,
    Gateway,
    LiveConfig,
    Run,
    Timeline,
    wait_for,
)

pytestmark = LIVE

UNEXPECTED_STATUSES = {500, 502, 504}


def _replica_state(detail: dict[str, Any], name: str) -> str | None:
    for r in detail["replicas"]:
        if r["name"] == name:
            return str(r["state"])
    return None


def _about(rows: list[dict[str, Any]], needle: str) -> list[dict[str, Any]]:
    return [r for r in rows if needle in log_message(r)]


async def test_replica_process_dies_and_is_replaced(
    gw: Gateway, cfg: LiveConfig, run: Run, compose: Compose
) -> None:
    """When a replica's model server dies under load, the control plane
    notices, takes it out of service and starts a replacement.  Meanwhile
    users only see answers or clean 503s (never 500/502/504), no reservations
    leak, and nothing is logged as CRITICAL."""
    model = cfg.fleet_model
    await require_model(gw, model)

    async def serving_victim() -> tuple[str, dict[str, Any], int] | None:
        """A ready replica that is actually in service: routable, on a
        deployment that still wants it (not one left draining by an earlier
        test, whose death would need no replacement)."""
        routable = await gw.backend_ids(model)
        for dep, rep in await gw.ready_replicas(model):
            want = int((await gw.pilot_deployment(dep))["desired_replicas"])
            if want >= 1 and routable.get(dep):
                return dep, rep, want
        return None

    dep_name: str
    victim: dict[str, Any]
    want_ready: int
    try:
        dep_name, victim, want_ready = await wait_for(
            f"a ready, routable replica of {model}", serving_victim, timeout=300
        )
    except AssertionError:
        pytest.skip(f"{model} has no ready, routable replica to stop")
    since = datetime.now(timezone.utc) - timedelta(seconds=2)

    pump = DemandPump(gw, model, max_tokens=3000, max_backoff=15.0)
    tl = Timeline(gw, model, pump, run)
    tl.phase = "pre"
    await pump.set_level(2)
    tl.start()
    run.step(f"Started background load on {model} (2 concurrent streams)")
    try:
        await asyncio.sleep(20)  # some healthy traffic first
        before = Counter(pump.statuses)
        tl.phase = "kill"
        out = await compose.stop_replica_process(victim["name"])
        run.step(
            f"Stopped the model server of replica {victim['name']} ({dep_name}) "
            f"through its pilot manager, without telling the controllers "
            f"(pilot replied: {out.strip()[:80]})"
        )
        t_kill = time.time()

        async def victim_gone() -> bool:
            st = _replica_state(await gw.pilot_deployment(dep_name), victim["name"])
            return st != "ready"

        await wait_for(
            "the replica to leave ready", victim_gone, timeout=180, interval=3
        )
        st = _replica_state(await gw.pilot_deployment(dep_name), victim["name"])
        run.check(
            True,
            "The control plane noticed and took the replica out of service",
            f"state {st} {time.time() - t_kill:.0f}s after the stop",
        )

        tl.phase = "heal"

        async def healed() -> bool:
            detail = await gw.pilot_deployment(dep_name)
            fresh = [
                r
                for r in detail["replicas"]
                if r["state"] == "ready" and r["name"] != victim["name"]
            ]
            return len(fresh) >= want_ready

        await wait_for(
            f"{want_ready} replacement replica(s) to be ready",
            healed,
            timeout=STARTUP_TIMEOUT,
            interval=10,
        )
        healed_after = time.time() - t_kill
        run.check(
            True,
            f"A replacement replica became ready ({want_ready} wanted)",
            f"{healed_after:.0f}s after the stop",
        )
        await asyncio.sleep(20)  # healthy traffic again
    finally:
        await pump.stop()
        await tl.stop()

    during = Counter(pump.statuses) - before
    bad = {s: n for s, n in pump.statuses.items() if s in UNEXPECTED_STATUSES}
    run.check(
        not bad,
        "Users only saw 200/429/503, never 500/502/504",
        f"statuses during the incident {dict(during)}; messages {dict(pump.messages)}",
    )

    async def drained() -> bool:
        return await gw.total_inflight(model) == 0

    await wait_for("in-flight to return to 0", drained, timeout=90)
    run.check(
        True, "No reservations leaked", "in-flight back to 0 after the load stopped"
    )

    rows = _about(await compose.logs(cfg.controller_service, since), victim["name"])
    levels = Counter(r.get("level", "") for r in rows)
    for r in rows:
        run.note(f"controller {r.get('level', '')}: {log_message(r)[:160]}")
    run.check(
        bool(rows) and not levels.get("CRITICAL"),
        "The controller logged the incident, with nothing CRITICAL",
        f"{len(rows)} lines about the replica, by level {dict(levels)}",
    )
    run.evidence(
        f"{victim['name']} -> {st}; replaced in {healed_after:.0f}s; users saw "
        f"{dict(during)}"
    )


def _notice(cluster: str, notice: str | None) -> Any:
    def edit(docs: Docs) -> None:
        find(docs, "Cluster", cluster)["spec"]["maintenance_notice"] = notice

    return edit


async def test_maintenance_notice_and_pilot_loss(
    gw: Gateway, cfg: LiveConfig, run: Run, admin: Admin, compose: Compose
) -> None:
    """Putting a cluster into maintenance is visible to users and the router,
    and while it lasts the control plane treats lost pilot jobs as expected:
    it does not place replicas there, attributes the loss to maintenance,
    and logs no errors.  Lifting the notice restores normal service."""
    cluster = cfg.maint_cluster
    clusters = {c["name"]: c for c in await gw.clusters()}
    if cluster not in clusters:
        pytest.skip(f"cluster {cluster} not configured")
    if clusters[cluster].get("maintenance_notice"):
        pytest.skip(f"{cluster} is already under maintenance")
    notice = f"LIVETEST {run.run_id}: planned maintenance"
    since = datetime.now(timezone.utc) - timedelta(seconds=2)

    offline = cfg.maint_offline_model
    offline_ok = False
    try:
        om = await gw.model(offline)
        dep_clusters = {d["cluster_name"] for d in om["pilot_deployments"]} | {
            d["cluster_name"] for d in om["static_deployments"]
        }
        offline_ok = dep_clusters == {cluster} and not await gw.ready_replicas(offline)
    except LookupError:
        pass
    if not offline_ok:
        run.note(
            f"{offline} is not an offline {cluster}-only model: skipped that check"
        )

    pump = DemandPump(gw, cfg.fleet_model, max_tokens=3000, max_backoff=15.0)
    tl = Timeline(gw, cfg.fleet_model, pump, run)
    jobs: list[dict[str, Any]] = []
    async with admin.patched(_notice(cluster, notice)):
        run.step(
            f'Applied a temporary manifest with maintenance_notice "{notice}" on {cluster}'
        )

        async def notice_live() -> bool:
            cs = {c["name"]: c for c in await gw.clusters()}
            fleet = await gw.model(cfg.fleet_model)
            return (
                cs[cluster].get("maintenance_notice") == notice
                and fleet.get("maintenance_notices", {}).get(cluster) == notice
            )

        await wait_for("the notice in the catalog", notice_live, timeout=60)

        async def routed() -> bool:
            m = await gw.model_config(cfg.fleet_model)
            return any(d.get("maintenance_notice") == notice for d in m["deployments"])

        await wait_for("the notice in the router config", routed, timeout=60)
        run.check(
            True,
            "The notice appears in the cluster list, the model, and the router config",
        )

        if offline_ok:
            r = await gw.chat(offline, max_tokens=4, tag="maint-offline")
            run.check(
                r.status == 503
                and f"unavailable due to cluster maintenance: {cluster}: {notice}"
                in r.message
                and r.info.get("maintenance_notices") == {cluster: notice}
                and r.retry_after is not None,
                f"A request to {offline} (only on {cluster}) gets a 503 quoting the notice",
                r.summary(),
            )

        tl.phase = "maint"
        await pump.set_level(2)
        tl.start()
        run.step(f"Started background load on {cfg.fleet_model}")
        try:
            detail = await gw.get_json(f"/catalog/v1/clusters/{cluster}")
            jobs = [
                j
                for j in detail["pilot_jobs"]
                if j["scheduler_state"] in ("queued", "running")
            ]
            if not jobs:
                run.note(f"{cluster} has no live pilot jobs to kill")
            for j in jobs:
                out = await compose.kill_pilot_job(j["name"])
                run.step(
                    f"Terminated pilot job {j['name']} at the scheduler, behind the "
                    f"controllers' backs ({out.strip()[:60]})"
                )
            if jobs:
                t_kill = time.time()
                names = {j["name"] for j in jobs}

                async def jobs_gone() -> bool:
                    d = await gw.get_json(f"/catalog/v1/clusters/{cluster}")
                    return not [
                        j
                        for j in d["pilot_jobs"]
                        if j["name"] in names
                        and j["scheduler_state"] in ("queued", "running")
                    ]

                await wait_for(
                    "the killed pilot jobs to be seen as gone",
                    jobs_gone,
                    timeout=600,
                    interval=10,
                )
                run.check(
                    True,
                    "The control plane observed the pilot loss",
                    f"{time.time() - t_kill:.0f}s after the kill",
                )
                tl.phase = "lost"
                run.step("Watched the fleet for 60s")
                await asyncio.sleep(60)
                placed = {
                    name: dict(dep["replicas"])
                    for name, dep in tl.samples[-1]["deployments"].items()
                    if name.startswith(f"{cluster}/")
                }
                run.check(
                    all(
                        sum(r.get(s, 0) for s in ("placed", "launching", "ready")) == 0
                        for r in placed.values()
                    ),
                    f"Nothing was placed on {cluster} during maintenance",
                    f"replica states {placed}",
                )
        finally:
            await pump.stop()
            await tl.stop()
    run.step("Lifted the notice (re-applied the profile)")

    async def notice_gone() -> bool:
        cs = {c["name"]: c for c in await gw.clusters()}
        return not cs[cluster].get("maintenance_notice")

    await wait_for("the notice to be lifted", notice_gone, timeout=60)

    # Requests are admitted against the router config, which the controller
    # republishes on its own poll (up to ~10s after the catalog changes).
    async def unrouted() -> bool:
        cfg_ = await gw.router_config()
        return not any(
            d.get("maintenance_notice")
            for m in cfg_["models"]
            for d in m["deployments"]
            if d["name"].startswith(f"{cluster}/")
        )

    await wait_for("the notice to leave the router config", unrouted, timeout=60)

    bad = {s: n for s, n in pump.statuses.items() if s in UNEXPECTED_STATUSES}
    run.check(
        not bad,
        "Users of the fleet model only saw 200/429/503",
        f"statuses {dict(pump.statuses)}; messages {dict(pump.messages)}",
    )

    if offline_ok:
        r = await gw.chat(offline, max_tokens=4, tag="maint-lifted")
        run.check(
            "maintenance" not in r.message,
            f"{offline} no longer reports maintenance",
            r.summary(),
        )

    rows = [
        r
        for r in await compose.logs(cfg.controller_service, since)
        if cluster in log_message(r).lower()
    ]
    levels = Counter(r.get("level", "") for r in rows)
    expected = _about(rows, "cluster under maintenance")
    loud = [r for r in rows if r.get("level") in ("ERROR", "CRITICAL")]
    for row in loud:
        run.note(f"controller {row['level']}: {log_message(row)[:200]}")
    if jobs:
        run.check(
            bool(expected),
            "The controller attributed the pilot loss to maintenance",
            f"{len(expected)} lines say 'cluster under maintenance'",
        )
    run.check(
        not loud,
        f"No ERROR or CRITICAL lines about {cluster}",
        f"{len(rows)} lines about {cluster}, by level {dict(levels)}",
    )
    run.evidence(
        f"notice visible everywhere; {len(jobs)} pilot jobs killed, nothing "
        f"placed, no loud logs; users saw {dict(pump.statuses)}"
    )
