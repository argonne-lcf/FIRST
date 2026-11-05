"""
Scaling down while requests are still running (fleet model; real replica).

One PilotDeployment of the fleet model is switched to manual scaling for the
test (temporary manifest edit), brought to exactly one ready replica, and
loaded with finite streams pinned to it.  Then desired_replicas drops to 0
mid-inference: the router must stop sending it new work at once, while the
ReplicaDrainer waits for in-flight requests to finish (at least 20s, at most
300s) before it stops the replica.

LIVE_DRAIN_DEPLOYMENT picks the deployment (default: one with a ready replica).
LIVE_DRAIN_MAX_TOKENS sets the stream length; it must outlast the drainer's
20s minimum wait by a clear margin but stay well under its 300s cap.
"""

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from .backend import Admin, Compose, Docs, find, log_message
from .conftest import LIVE, require_model
from .harness import STARTUP_TIMEOUT, Gateway, Hold, LiveConfig, Run, wait_for

pytestmark = LIVE

MAX_TOKENS = int(os.environ.get("LIVE_DRAIN_MAX_TOKENS", "8000"))
N_STREAMS = 3
MIN_DRAIN_WAIT_SEC = 20  # replica_drainer._MIN_DRAIN_WAIT_SEC
MAX_DRAIN_WAIT_SEC = 300  # replica_drainer._MAX_DRAIN_WAIT_SEC


async def _pick_deployment(gw: Gateway, model: str) -> str:
    if name := os.environ.get("LIVE_DRAIN_DEPLOYMENT"):
        return name
    ready = await gw.ready_replicas(model)
    if ready:
        return ready[0][0]
    deps = (await gw.model(model))["pilot_deployments"]
    if not deps:
        pytest.skip(f"{model} has no pilot deployments")
    return str(deps[0]["name"])


def _manual(dep: str) -> Any:
    def edit(docs: Docs) -> None:
        spec = find(docs, "PilotDeployment", dep)["spec"]
        spec["scaling_strategy"] = None
        spec["min_replicas"] = 0

    return edit


def _replica(detail: dict[str, Any], name: str) -> dict[str, Any]:
    return next(r for r in detail["replicas"] if r["name"] == name)


async def test_scale_down_drains_inflight_requests(
    gw: Gateway, cfg: LiveConfig, run: Run, admin: Admin, compose: Compose
) -> None:
    """Scaling a deployment down must not cut off requests that are still
    running on the replica being removed.  The replica stops receiving new
    work immediately, but it is only shut down after its in-flight streams
    have finished normally."""
    model = cfg.fleet_model
    await require_model(gw, model, probe=False)
    dep = await _pick_deployment(gw, model)
    since = datetime.now(timezone.utc) - timedelta(seconds=2)

    async with admin.patched(_manual(dep)):
        run.step(
            f"Applied a temporary manifest switching {dep} to manual scaling, "
            "and set desired replicas to 1"
        )
        await admin.set_desired_replicas(dep, 1)

        async def one_ready() -> list[dict[str, Any]] | None:
            d = await gw.pilot_deployment(dep)
            live = [r for r in d["replicas"] if r.get("deleted_at") is None]
            ready = [r for r in live if r["state"] == "ready"]
            others = [r for r in live if r["state"] != "ready"]
            routable = (await gw.backend_ids(model)).get(dep, [])
            return ready if len(ready) == 1 and not others and routable else None

        (victim,) = await wait_for(
            f"{dep} to have exactly one ready, routable replica",
            one_ready,
            timeout=STARTUP_TIMEOUT,
            interval=10,
        )
        name = victim["name"]

        hold = Hold(gw, model, MAX_TOKENS, dep, "drain")
        try:
            await hold.open(N_STREAMS)
            run.step(
                f"Opened {N_STREAMS} streams of {MAX_TOKENS} tokens each, pinned "
                f"to {dep} (so all run on replica {name})"
            )
            landed = {s.deployment for s in hold.admitted}
            run.check(
                len(hold.admitted) == N_STREAMS and landed == {dep},
                f"All {N_STREAMS} streams are running on the replica",
                f"{len(hold.admitted)} admitted on {landed}",
            )

            t_scale = time.time()
            await admin.set_desired_replicas(dep, 0)
            run.step(f"Scaled {dep} down to 0 replicas while the streams were running")

            async def unroutable() -> bool:
                return not (await gw.backend_ids(model)).get(dep)

            await wait_for(
                "the replica to leave the router config",
                unroutable,
                timeout=30,
                interval=1,
            )
            late = await gw.chat(model, max_tokens=4, deployment=dep, tag="after-drain")
            run.check(
                late.status == 503,
                "The replica stopped receiving new work at once",
                f"left the router after {time.time() - t_scale:.0f}s; a new pinned "
                f"request got {late.summary()}",
            )
            if len(hold.live) < N_STREAMS:
                pytest.skip("streams ended before the scale-down; raise MAX_TOKENS")

            states: list[str] = []
            torn_down = None
            while hold.live:
                r = _replica(await gw.pilot_deployment(dep), name)
                states.append(r["state"])
                if r.get("deleted_at") is not None:
                    torn_down = len(hold.live)
                    break
                await asyncio.sleep(3)
            run.check(
                torn_down is None,
                "The replica stayed up while requests were in flight",
                f"polled every 3s: states {sorted(set(states))}, never deleted"
                if torn_down is None
                else f"deleted with {torn_down} streams still running",
            )
            await hold.finish(timeout=60)
        finally:
            await hold.release()

        last_end = max(s.ended_at or 0 for s in hold.streams)
        ran_for = last_end - t_scale
        endings = [
            f"{'[DONE]' if s.done else s.stream_error or 'no [DONE]'}, "
            f"{(s.usage or {}).get('completion_tokens')} tokens"
            for s in hold.streams
        ]
        run.check(
            all(
                s.done and s.usage and s.usage["completion_tokens"] == MAX_TOKENS
                for s in hold.streams
            ),
            f"Every stream finished normally with all {MAX_TOKENS} tokens",
            "; ".join(endings),
        )
        if ran_for < MIN_DRAIN_WAIT_SEC + 10:
            pytest.skip(
                f"streams ended {ran_for:.0f}s after scale-down, inside the "
                f"drainer's {MIN_DRAIN_WAIT_SEC}s minimum wait: raise "
                "LIVE_DRAIN_MAX_TOKENS"
            )
        run.check(
            ran_for < MAX_DRAIN_WAIT_SEC,
            "The streams outlived the drainer's minimum wait but not its hard cap",
            f"last stream ended {ran_for:.0f}s after the scale-down (minimum "
            f"{MIN_DRAIN_WAIT_SEC}s, cap {MAX_DRAIN_WAIT_SEC}s)",
        )

        async def gone() -> dict[str, Any] | None:
            r = _replica(await gw.pilot_deployment(dep), name)
            return r if r.get("deleted_at") else None

        final = await wait_for(
            f"replica {name} to be removed", gone, timeout=120, interval=3
        )
        deleted = datetime.fromisoformat(final["deleted_at"]).timestamp()
        run.check(
            deleted >= last_end - 2 and final["state"] == "terminated",
            "Only then was the replica shut down",
            f"state {final['state']}, deleted {deleted - last_end:.0f}s after the "
            "last stream finished",
        )

    rows = await compose.logs(cfg.controller_service, since)
    drained = [
        log_message(r) for r in rows if f"drained replica {name}" in log_message(r)
    ]
    run.check(bool(drained), "The controller logged the drain", f"{drained}")
    run.evidence(
        f"{N_STREAMS} streams ran {ran_for:.0f}s past the scale-down and completed; "
        f"replica removed {deleted - last_end:.0f}s after the last one"
    )
