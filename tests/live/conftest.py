"""
Live functional tests against the dev stack, run on the gateway VM (opt-in).

    FIRST_LIVE=1 uv run pytest tests/live -s -v [-k ...]

Without FIRST_LIVE these modules are not even collected, so `make test` is
unaffected.  See tests/live/README.md.

Every fixture is session-scoped and runs on one session event loop: each test
module sets ``pytestmark = LIVE``.
"""

import os
import time
from pathlib import Path
from typing import AsyncIterator, Iterator

import pytest
import pytest_asyncio

if not os.environ.get("FIRST_LIVE"):
    collect_ignore_glob = ["test_*.py"]

from first_common.schema.auth import UserAuthEvent  # noqa: E402

from .backend import (  # noqa: E402
    CHAOS_MODEL,
    Admin,
    ChaosUpstream,
    Compose,
    RedisState,
    describe_plan,
)
from .harness import Gateway, LiveConfig, Run, wait_for  # noqa: E402

LIVE = pytest.mark.asyncio(loop_scope="session")

session_fixture = pytest_asyncio.fixture(scope="session", loop_scope="session")


@pytest.fixture(scope="session")
def cfg() -> LiveConfig:
    return LiveConfig()


_RUN: Run | None = None


def _get_run() -> Run:
    global _RUN
    if _RUN is None:
        _RUN = Run(LiveConfig().base_url)
        _RUN.log(f"Live run {_RUN.run_id}: report in {_RUN.dir / 'report.md'}")
    return _RUN


@pytest.fixture(scope="session")
def run() -> Run:
    return _get_run()


# ---------------------------------------------------------------------------
# Report: one section per test, opened before its fixtures run, closed with
# the verdict of its setup/call phases.
# ---------------------------------------------------------------------------


def _goal(item: pytest.Item) -> str:
    doc = getattr(getattr(item, "function", None), "__doc__", None) or ""
    para = doc.strip().split("\n\n")[0]
    return " ".join(para.split()) or "(no description)"


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    _get_run().begin(item.nodeid, _goal(item))


def _reason(call: pytest.CallInfo[None]) -> str:
    exc = call.excinfo
    if exc is None:
        return ""
    if exc.errisinstance(pytest.skip.Exception):
        return str(exc.value.msg)  # type: ignore[attr-defined]
    lines = [ln for ln in str(exc.value).splitlines() if ln.strip()]
    text = " ".join(lines[:3]) if lines else exc.typename
    return text if isinstance(exc.value, AssertionError) else f"{exc.typename}: {text}"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Iterator[None]:
    outcome = yield
    rep = outcome.get_result()  # type: ignore[attr-defined]
    run = _get_run()
    if rep.when == "setup" and not rep.passed:
        if rep.skipped:
            run.finish("SKIPPED", _reason(call))
        else:
            run.finish("ERROR", f"Setup failed: {_reason(call)}")
    elif rep.when == "call":
        verdict = "PASSED" if rep.passed else "SKIPPED" if rep.skipped else "FAILED"
        run.finish(verdict, _reason(call) or None)
    elif rep.when == "teardown" and rep.failed:
        run.note(f"Teardown failed: {_reason(call)}")
        run.write_report()


def pytest_sessionfinish(session: pytest.Session) -> None:
    if _RUN is not None:
        _RUN.write_report()
        print(f"\nLive test report: {_RUN.dir / 'report.md'}", flush=True)


@pytest.fixture(scope="session")
def compose(cfg: LiveConfig) -> Compose:
    return Compose(cfg.apiserver_service, cfg.controller_service)


@session_fixture
async def gw(cfg: LiveConfig, run: Run) -> AsyncIterator[Gateway]:
    gateway = Gateway(cfg, run)
    if not await gateway.healthy():
        pytest.skip(f"Gateway at {cfg.base_url} is not healthy")
    try:
        yield gateway
    finally:
        await gateway.aclose()


@session_fixture
async def me(gw: Gateway) -> UserAuthEvent:
    return await gw.whoami()


@session_fixture
async def redis(cfg: LiveConfig) -> AsyncIterator[RedisState]:
    state = RedisState(cfg.redis_url)
    try:
        await state.r.ping()
    except Exception as exc:
        pytest.skip(f"Redis at {cfg.redis_url} unreachable: {exc!r}")
    try:
        yield state
    finally:
        await state.aclose()


@session_fixture
async def admin(cfg: LiveConfig, run: Run) -> AsyncIterator[Admin]:
    """Applies the live-test profile (tests/live/manifests) for the session.

    LIVE_RESTORE_MANIFESTS=<dir> applies that directory (e.g. the dev
    manifests) when the session ends.
    """
    a = Admin(cfg.admin_base_url)
    plan = await a.apply_profile()
    run.step(f"Applied the live-test manifest profile ({describe_plan(plan)})")
    try:
        yield a
    finally:
        if restore := os.environ.get("LIVE_RESTORE_MANIFESTS"):
            plan = await a.apply_dir(Path(restore))
            run.step(f"Restored {restore} ({describe_plan(plan)})")
        a.close()


@pytest_asyncio.fixture(scope="session", loop_scope="session", autouse=True)
async def _profile(admin: Admin) -> None:
    """Every live test runs against the live-test profile."""


async def require_model(gw: Gateway, name: str, *, probe: bool = True) -> None:
    """Skip unless the model is visible and (optionally) currently serving."""
    try:
        await gw.model(name)
    except LookupError as exc:
        pytest.skip(str(exc))
    if probe:
        reply = await gw.chat(name, max_tokens=4, tag="probe")
        if reply.status == 429:
            return  # serving, just rate limited
        if not reply.ok:
            pytest.skip(f"{name} is not serving right now: {reply.summary()}")


# ---------------------------------------------------------------------------
# Chaos upstream
# ---------------------------------------------------------------------------

# The chaos model: RPM and TPM charged at slow rates, so one request's charge
# is easy to read off the bucket, with bursts deep enough that they never bind
# (tests empty the buckets in Redis between steps).
CHAOS_LIMITS = {
    "tpm": 600,  # 10 tokens/s
    "burst_tokens": 32768,  # = max_model_len
    "rpm": 6,  # one request = 10s of bucket
    "burst_requests": 1000,
    "max_user_concurrency": 64,
}
CHAOS_ROUTER = {
    "max_backend_concurrency": 8,
    "cooldown_threshold": 3,
    "cooldown_window_sec": 30,
    "cooldown_bench_sec": 20,
}


class Chaos(ChaosUpstream):
    """The chaos upstream once its model is routable: ``ids`` maps each
    virtual backend to its gateway backend id."""

    ids: dict[str, str]

    async def reset_all(self, redis: RedisState, user_id: str) -> None:
        """Healthy backends, no benches, empty buckets."""
        await self.reset()
        await redis.unbench(*self.ids.values())
        await redis.reset_quota(CHAOS_MODEL, user_id)


@session_fixture
async def chaos(
    cfg: LiveConfig,
    run: Run,
    gw: Gateway,
    admin: Admin,
    compose: Compose,
    redis: RedisState,
    me: UserAuthEvent,
) -> AsyncIterator[Chaos]:
    """Start the chaos sidecar and add the chaos model to the profile."""
    c = Chaos(compose, cfg.chaos_cluster, cfg.chaos_port)
    await c.start()
    t0 = time.monotonic()
    try:
        async with admin.patched(c.edit(CHAOS_LIMITS, CHAOS_ROUTER)):

            async def routable() -> dict[str, list[str]] | None:
                try:
                    ids = await gw.backend_ids(CHAOS_MODEL)
                except LookupError:
                    return None
                want = {c.deployment(b) for b in c.backends}
                ok = want <= set(ids) and all(ids[d] for d in want)
                return ids if ok else None

            # Static deployments become routable on their first healthy check.
            ids = await wait_for(
                "chaos deployments routable", routable, timeout=300, interval=5
            )
            c.ids = {b: ids[c.deployment(b)][0] for b in c.backends}
            run.step(
                f"Started the chaos upstream sidecar and added {CHAOS_MODEL} with "
                f"backends {c.ids}; routable after {time.monotonic() - t0:.0f}s"
            )
            await c.reset_all(redis, me.id)
            yield c
    finally:
        await c.stop()
