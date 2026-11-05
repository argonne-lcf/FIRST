"""
Federated routing across deployments (e.g. Tara + Sophia) for the fleet model.

The router races an Exp(weight) draw per *backend*, so with free capacity each
request lands on deployment d with probability
    weight_d * n_backends_d / sum_d'(weight_d' * n_backends_d').

Requests are sent one at a time (so none can spill over because of
saturation) and attributed by the X-First-Deployment response header.
"""

import asyncio
import os
import time
from collections import Counter

import pytest

from .conftest import LIVE, require_model
from .harness import STARTUP_TIMEOUT, Gateway, LiveConfig, Run, binomial_tolerance

pytestmark = LIVE

ROUNDS = int(os.environ.get("LIVE_FED_ROUNDS", "200"))


async def _routable(gw: Gateway, model: str) -> dict[str, float]:
    """Expected share per deployment, from the live router config."""
    m = await gw.model_config(model)
    raw = {
        d["name"]: d["router_params"]["weight"] * len(d["backends"])
        for d in m["deployments"]
        if d["router_params"]["weight"] > 0 and d["backends"]
    }
    total = sum(raw.values())
    return {name: w / total for name, w in raw.items()}


async def _wait_routable(gw: Gateway, model: str, n: int) -> dict[str, float]:
    """Wait (up to STARTUP_TIMEOUT) for ``n`` routable deployments, sending a
    probe every 10s: the probes are demand, so a deployment that is still
    starting is not scaled back to zero in the meantime."""
    t0 = time.monotonic()
    while True:
        expected = await _routable(gw, model)
        if len(expected) >= n or time.monotonic() - t0 > STARTUP_TIMEOUT:
            if time.monotonic() - t0 > 5:
                gw.run.note(
                    f"Waited {time.monotonic() - t0:.0f}s for {n} routable deployments"
                )
            return expected
        await gw.chat(model, max_tokens=1, tag="fed-warm")
        await asyncio.sleep(10)


async def test_weighted_balance(gw: Gateway, cfg: LiveConfig, run: Run) -> None:
    """Federated requests are spread across deployments in proportion to
    weight x number of backends."""
    model = cfg.fleet_model
    await require_model(gw, model)
    expected = await _wait_routable(gw, model, 2)
    if len(expected) < 2:
        pytest.skip(f"need >=2 deployments with ready backends, have {expected}")

    run.step(
        f"Sent {ROUNDS} requests to {model} one at a time (so none spills over) "
        "and read which deployment served each from X-First-Deployment"
    )
    counts: Counter[str] = Counter()
    failed = []
    for _ in range(ROUNDS):
        r = await gw.chat(model, max_tokens=1, tag="fed")
        if not r.ok or not r.deployment:
            failed.append(r.summary())
        else:
            counts[r.deployment] += 1
    run.check(
        not failed,
        "Every request succeeded and named its deployment",
        f"{len(failed)} did not: {failed[:3]}" if failed else f"{ROUNDS} ok",
    )
    run.check(
        await _routable(gw, model) == expected,
        "The fleet did not change during the test",
    )
    n = sum(counts.values())
    run.check(
        set(counts) <= set(expected),
        "Requests only went to routable deployments",
        f"{dict(counts)}",
    )
    for name, p in sorted(expected.items()):
        observed = counts[name] / n
        tol = binomial_tolerance(n, p)
        run.check(
            abs(observed - p) <= tol,
            f"{name} got its share",
            f"expected {p:.0%}, observed {observed:.0%} ({counts[name]}/{n}); "
            f"3-sigma tolerance ±{tol:.0%}",
        )
    run.evidence(
        ", ".join(
            f"{k}: {counts[k] / n:.0%} (expected {p:.0%})"
            for k, p in sorted(expected.items())
        )
    )


async def test_pinned_deployment_route(gw: Gateway, cfg: LiveConfig, run: Run) -> None:
    """A request to /deployments/{name}/v1 is only ever served by that
    deployment."""
    model = cfg.fleet_model
    await require_model(gw, model)
    deps = list(await _routable(gw, model))
    run.step(f"Sent 5 requests pinned to each of {deps}")
    for name in deps:
        landed = {
            (await gw.chat(model, max_tokens=1, deployment=name, tag="pin")).deployment
            for _ in range(5)
        }
        run.check(
            landed == {name},
            f"Pinned to {name}: served only there",
            f"served by {landed}",
        )
    run.evidence(f"{len(deps)} deployments x 5 pinned requests, all served in place")
