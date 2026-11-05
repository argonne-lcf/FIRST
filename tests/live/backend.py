"""
Direct control of the dev stack, for live tests running on the gateway VM.

Alongside the authenticated client in harness.py, tests act on the backend:

- Compose: `docker compose` logs, kill/start/restart, and Python run inside a
  service container (with that service's settings, certificates and proxy).
- RedisState: read and write admission-controller state (benches, in-flight
  sets, reservations, quota buckets, token-estimate EWMAs).
- Admin: plan/apply the live-test manifest profile, plus temporary edits on
  top of it, and manual replica scaling (what `first-admin` does).
- ChaosUpstream: a fake upstream (chaos_upstream.py) run as a sidecar on the
  compose network, registered as StaticDeployments of a throwaway model.

Every Redis key comes from `Keys`, the gateway's single source of truth.
"""

import asyncio
import copy
import json
import os
import shlex
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import httpx
from redis.asyncio import Redis
from yaml import safe_load_all

from first_common.schema.resources import ResourceChangePlan, ResourceManifest
from first_gateway.admin.client import AdminClient
from first_gateway.database.redis.keys import Keys

LIVE_DIR = Path(__file__).parent
REPO_DIR = LIVE_DIR.parent.parent
PROFILE_DIR = LIVE_DIR / "manifests"


def _compose_redis_url() -> str:
    """FIRST_REDIS_URL from .env.compose, reached through the host port."""
    url = "redis://redis:6380/1"
    env = REPO_DIR / ".env.compose"
    if env.exists():
        for line in env.read_text().splitlines():
            if line.startswith("FIRST_REDIS_URL="):
                url = line.split("=", 1)[1].strip().strip("\"'")
    return url.replace("//redis:", "//127.0.0.1:")


# ---------------------------------------------------------------------------
# docker compose
# ---------------------------------------------------------------------------


async def _docker(*args: str, timeout: float = 60) -> str:
    proc = await asyncio.create_subprocess_exec(
        "docker", *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )  # fmt: skip
    out, err = await asyncio.wait_for(proc.communicate(), timeout)
    if proc.returncode:
        raise RuntimeError(
            f"docker {shlex.join(args)} exited {proc.returncode}:\n"
            f"{err.decode(errors='replace')[-2000:]}"
        )
    return out.decode(errors="replace")


class Compose:
    """`docker compose` in the repo root, so `.env` picks COMPOSE_FILE and
    COMPOSE_PROJECT_NAME exactly as `make compose-up` does."""

    def __init__(self, apiserver: str, controller: str) -> None:
        self.apiserver = apiserver
        self.controller = controller

    async def run(
        self, *args: str, stdin: str | None = None, timeout: float = 120
    ) -> str:
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "compose",
            *args,
            cwd=REPO_DIR,
            stdin=asyncio.subprocess.PIPE if stdin is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await asyncio.wait_for(
            proc.communicate(stdin.encode() if stdin is not None else None), timeout
        )
        if proc.returncode:
            raise RuntimeError(
                f"docker compose {shlex.join(args)} exited {proc.returncode}:\n"
                f"{err.decode(errors='replace')[-2000:]}"
            )
        return out.decode(errors="replace")

    async def logs(self, service: str, since: datetime) -> list[dict[str, Any]]:
        """A service's JSON log lines since ``since`` (non-JSON lines dropped)."""
        text = await self.run(
            "logs", service, "--no-log-prefix", "--since", since.isoformat()
        )
        return parse_log_lines(text)

    async def kill(self, service: str, signal: str = "KILL") -> None:
        """Signal the service's own container(s) only.

        `docker compose kill SERVICE` also signals one-off `compose run`
        containers of that service, which would take down the chaos upstream
        sidecar (started with `compose run ... <apiserver image>`) and break
        every later chaos test with ConnectError.
        """
        ids = (await self.run("ps", "-q", service)).split()
        ours = []
        for cid in ids:
            oneoff = await _docker(
                "inspect", "-f",
                '{{index .Config.Labels "com.docker.compose.oneoff"}}', cid,
            )  # fmt: skip
            if oneoff.strip() != "True":
                ours.append(cid)
        if not ours:
            raise RuntimeError(f"no running container for service {service}")
        await _docker("kill", "-s", signal, *ours)

    async def start(self, service: str) -> None:
        await self.run("start", service)

    async def python(self, service: str, script: str, timeout: float = 120) -> str:
        """Run a Python script inside a running service container."""
        return await self.run(
            "exec",
            "-T",
            service,
            ".venv/bin/python",
            "-",
            stdin=script,
            timeout=timeout,
        )

    async def stop_replica_process(self, replica_name: str) -> str:
        """Stop a replica's model server through its pilot manager, behind the
        controllers' backs (what a SIGTERM on the host looks like to them)."""
        return await self.python(
            self.controller,
            _IN_CONTROLLER.format(action=_STOP_REPLICA, arg=replica_name),
        )

    async def kill_pilot_job(self, job_name: str) -> str:
        """Terminate a pilot job at the scheduler (qdel) without flagging it for
        deletion, so the controllers observe an unexplained loss."""
        return await self.python(
            self.controller, _IN_CONTROLLER.format(action=_KILL_JOB, arg=job_name)
        )


_IN_CONTROLLER = """
import asyncio
from first_common.schema.types import PilotConfig
from first_gateway.database import models as db
from first_gateway.platforms.schedulers import build_scheduler, scheduler_rpc
from first_gateway.services.pilot_control import PilotControlClient
from first_gateway.settings import Settings

async def main(arg):
    async with Settings().build_clients() as cs:
{action}

asyncio.run(main({arg!r}))
"""

_STOP_REPLICA = """\
        async with cs.db_sessionmaker() as sess:
            rep = await db.PilotReplica.get_by_name(sess, arg)
            job = await db.PilotJob.get_by_name(sess, rep.pilot_job_name)
        client = PilotControlClient(cs)
        resp = await client.stop_replica(job.manager_url, arg, cluster_name=job.cluster_name)
        await client.aclose()
        print(resp.status_code, resp.text[:200])"""

_KILL_JOB = """\
        async with cs.db_sessionmaker() as sess:
            job = await db.PilotJob.get_by_name(sess, arg)
            cluster = await db.Cluster.get_by_name(sess, job.cluster_name)
        adapter = await build_scheduler(PilotConfig.model_validate(cluster.pilot_system), cs)
        await scheduler_rpc(adapter.terminate_job(job.scheduler_job_id))
        print("terminated", job.scheduler_job_id)"""


def parse_log_lines(text: str) -> list[dict[str, Any]]:
    """JSON objects from log output, tolerating prefixes and non-JSON noise."""
    out = []
    for line in text.splitlines():
        i = line.find("{")
        if i < 0:
            continue
        try:
            obj = json.loads(line[i:])
        except ValueError:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def log_message(row: dict[str, Any]) -> str:
    return str(row.get("message") or row.get("msg") or "")


@dataclass
class Trace:
    """One request's structured logs: RequestLog, ResponseLog, InferenceLogs."""

    request: dict[str, Any]
    response: dict[str, Any] | None
    inference: list[dict[str, Any]]  # by attempt

    @property
    def request_id(self) -> str:
        return str(self.request["request_id"])

    @property
    def outcomes(self) -> list[str]:
        return [str(i["outcome"]) for i in self.inference]


def build_traces(rows: list[dict[str, Any]], markers: list[str]) -> dict[str, Trace]:
    """Find each marker's RequestLog by body, then its other logs by id."""
    requests = [r for r in rows if r.get("stream") == "request"]
    out: dict[str, Trace] = {}
    for marker in markers:
        hits = [r for r in requests if marker in (r.get("body") or "")]
        if len(hits) != 1:
            continue
        rid = hits[0]["request_id"]
        out[marker] = Trace(
            request=hits[0],
            response=next(
                (
                    r
                    for r in rows
                    if r.get("stream") == "response" and r["request_id"] == rid
                ),
                None,
            ),
            inference=sorted(
                (
                    r
                    for r in rows
                    if r.get("stream") == "inference" and r["request_id"] == rid
                ),
                key=lambda r: (r["attempt"], r["timestamp"]),
            ),
        )
    return out


async def traces(
    compose: "Compose",
    since: datetime,
    markers: list[str],
    *,
    min_inference: int = 1,
    timeout: float = 20,
) -> dict[str, Trace]:
    """Fetch the apiserver logs until every marker has a ResponseLog and at
    least ``min_inference`` InferenceLogs (or the timeout passes)."""
    deadline = time.monotonic() + timeout
    while True:
        rows = await compose.logs(compose.apiserver, since)
        found = build_traces(rows, markers)
        complete = len(found) == len(markers) and all(
            t.response is not None and len(t.inference) >= min_inference
            for t in found.values()
        )
        if complete or time.monotonic() > deadline:
            return found
        await asyncio.sleep(2)


# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------


class RedisState:
    """The admission controller's Redis state, read and written directly."""

    def __init__(self, url: str) -> None:
        self.r = Redis.from_url(url, decode_responses=True)

    async def aclose(self) -> None:
        await self.r.aclose()

    async def now(self) -> float:
        sec, usec = await self.r.time()
        return float(sec) + usec / 1e6

    # ---- cooldown ----------------------------------------------------------

    async def bench(self, backend_id: str, threshold: int, sec: int) -> None:
        """Bench a backend exactly as record_error.lua does on its
        `threshold`-th error: count >= threshold, TTL = bench duration."""
        await self.r.set(Keys.backend_errors(backend_id), threshold, ex=sec)

    async def unbench(self, *backend_ids: str) -> None:
        if backend_ids:
            await self.r.delete(*(Keys.backend_errors(b) for b in backend_ids))

    async def errors(self, backend_id: str) -> tuple[int, int]:
        """(error count, TTL in seconds; -2 if absent)."""
        key = Keys.backend_errors(backend_id)
        count, ttl = await asyncio.gather(self.r.get(key), self.r.ttl(key))
        return int(count or 0), int(ttl)

    # ---- in-flight ledger --------------------------------------------------

    async def model_inflight(self, model: str) -> set[str]:
        return {str(m) for m in await self.r.zrange(Keys.model_inflight(model), 0, -1)}

    async def backend_inflight(self, model: str, backend_id: str) -> set[str]:
        key = Keys.backend_inflight(model, backend_id)
        return {str(m) for m in await self.r.zrange(key, 0, -1)}

    async def reservation(self, request_id: str) -> dict[str, Any] | None:
        raw = await self.r.get(Keys.reservation(request_id))
        return json.loads(raw) if raw else None

    async def deadline(self, request_id: str) -> float | None:
        score = await self.r.zscore(Keys.deadlines(), request_id)
        return float(score) if score is not None else None

    # ---- quotas and estimates ----------------------------------------------

    async def quota_tat(self, model: str, user: str, resource: str) -> float | None:
        raw = await self.r.get(Keys.user_rate_limit(model, user, resource))  # type: ignore[arg-type]
        return float(raw) if raw else None

    async def reset_quota(self, model: str, user: str) -> None:
        """Empty both GCRA buckets: no waiting for a refill."""
        await self.r.delete(
            Keys.user_rate_limit(model, user, "rpm"),
            Keys.user_rate_limit(model, user, "tokens"),
        )

    async def set_token_stats(
        self,
        model: str,
        user: str,
        *,
        chars_per_token: float | None,
        output_tokens: float | None,
    ) -> None:
        """Pin (or with None, clear) the learned estimation EWMAs, so the next
        request's estimate is known exactly."""
        for metric, value in (
            ("input_token_chars", chars_per_token),
            ("output_token", output_tokens),
        ):
            key = Keys.usage_ewma(model, user, metric)  # type: ignore[arg-type]
            if value is None:
                await self.r.delete(key)
            else:
                await self.r.set(key, value, ex=3600)


# ---------------------------------------------------------------------------
# Manifests
# ---------------------------------------------------------------------------

Docs = list[dict[str, Any]]
Edit = Callable[[Docs], None]


def load_docs(spec_dir: Path) -> Docs:
    docs: Docs = []
    for ext in ("yml", "yaml"):
        for f in sorted(spec_dir.rglob(f"*.{ext}")):
            with f.open() as fp:
                docs += [d for d in safe_load_all(fp) if d]
    return docs


def find(docs: Docs, kind: str, name: str) -> dict[str, Any]:
    for d in docs:
        if d["kind"] == kind and d["name"] == name:
            return d
    raise LookupError(f"{kind} {name} not in the manifest profile")


class Admin:
    """Plan/apply against the control API, the way `first-admin apply` does
    (without the prompt).  The live-test profile is the baseline; `patched`
    layers temporary edits over it and restores the previous state on exit.
    """

    def __init__(self, base_url: str | None) -> None:
        self.client = AdminClient(base_url)
        self.client.timeout = httpx.Timeout(60.0)
        self._edits: list[Edit] = []

    def close(self) -> None:
        self.client.close()

    def _resources(self, extra: list[Edit]) -> list[ResourceManifest]:
        docs = load_docs(PROFILE_DIR)
        for edit in extra:
            edit(docs)
        return [ResourceManifest.model_validate(d) for d in docs]

    async def apply_dir(self, spec_dir: Path) -> ResourceChangePlan:
        docs = load_docs(spec_dir)
        return await self._apply([ResourceManifest.model_validate(d) for d in docs])

    async def _apply(self, resources: list[ResourceManifest]) -> ResourceChangePlan:
        plan = await asyncio.to_thread(self.client.plan, resources)
        if plan.to_add or plan.to_update or plan.to_delete:
            await asyncio.to_thread(self.client.apply, resources, plan)
        return plan

    async def apply_profile(self) -> ResourceChangePlan:
        """Bring the gateway to the profile plus the active edits."""
        return await self._apply(self._resources(self._edits))

    @asynccontextmanager
    async def patched(self, edit: Edit) -> AsyncIterator[None]:
        self._edits.append(edit)
        try:
            await self.apply_profile()
            yield
        finally:
            self._edits.remove(edit)
            await self.apply_profile()

    async def set_desired_replicas(self, deployment: str, n: int) -> None:
        await asyncio.to_thread(
            self.client.set_desired_pilot_deployment_replicas, deployment, n
        )


def describe_plan(plan: ResourceChangePlan) -> str:
    changes = [
        *(f"+{r.kind}.{r.name}" for r in plan.to_add),
        *(f"~{p.kind}.{p.name}" for p in plan.to_update),
        *(f"-{r.kind}.{r.name}" for r in plan.to_delete),
    ]
    return ", ".join(changes) or "no changes"


# ---------------------------------------------------------------------------
# Chaos upstream
# ---------------------------------------------------------------------------

CHAOS_MODEL = "livetest/chaos"


class ChaosUpstream:
    """The fake upstream sidecar plus a throwaway model served only by it.

    Each virtual backend becomes one StaticDeployment of ``CHAOS_MODEL`` named
    ``<cluster>/livetest/chaos-<backend>``.  Its mode (healthy, 5xx, disconnect,
    slow stream, broken stream) is switched at runtime with ``set``.
    """

    container = "livetest-chaos"
    backends = ("a", "b", "c", "d")
    # "b" is the healthy fallback: drawn after the others almost always.
    weights = {"a": 1000, "b": 1, "c": 1000, "d": 1000}

    def __init__(self, compose: Compose, cluster: str, port: int) -> None:
        self.compose = compose
        self.cluster = cluster
        self.control = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}/_control", timeout=10
        )
        self.port = port

    def deployment(self, backend: str) -> str:
        return f"{self.cluster}/livetest/chaos-{backend}"

    async def start(self) -> None:
        await self.stop()
        script = LIVE_DIR / "chaos_upstream.py"
        await self.compose.run(
            "run", "-d", "--rm", "--no-deps", "--name", self.container,
            "-p", f"127.0.0.1:{self.port}:8000",
            "-v", f"{script}:/chaos.py:ro",
            "--entrypoint", ".venv/bin/python",
            self.compose.apiserver, "/chaos.py", "8000",
        )  # fmt: skip
        deadline = time.monotonic() + 30
        while True:
            try:
                await self.state()
                return
            except httpx.HTTPError:
                if time.monotonic() > deadline:
                    raise
                await asyncio.sleep(0.5)

    async def stop(self) -> None:
        proc = await asyncio.create_subprocess_exec(
            "docker", "rm", "-f", self.container,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )  # fmt: skip
        await proc.wait()

    async def set(self, backend: str, mode: str = "ok", **options: Any) -> None:
        resp = await self.control.post(f"/{backend}", json={"mode": mode, **options})
        resp.raise_for_status()

    async def reset(self) -> None:
        for b in self.backends:
            await self.set(b, "ok")

    async def state(self) -> dict[str, dict[str, Any]]:
        resp = await self.control.get("")
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    async def hits(self) -> dict[str, int]:
        st = await self.state()
        return {b: int(st.get(b, {}).get("hits", 0)) for b in self.backends}

    def edit(self, usage_limits: dict[str, Any], router_params: dict[str, Any]) -> Edit:
        """Manifest edit adding the chaos model and one deployment per backend."""

        def apply(docs: Docs) -> None:
            docs.append(
                {
                    "kind": "Model",
                    "name": CHAOS_MODEL,
                    "spec": {
                        "access_group_name": "public",
                        "max_model_len": 32768,
                        "supported_endpoints": ["chat/completions"],
                        "usage_limits": {"default": copy.deepcopy(usage_limits)},
                        "overload": {"short_retry_sec": 5, "retry_jitter_percent": 0},
                    },
                }
            )
            for b in self.backends:
                docs.append(
                    {
                        "kind": "StaticDeployment",
                        "name": self.deployment(b),
                        "spec": {
                            "cluster_name": self.cluster,
                            "model_name": CHAOS_MODEL,
                            "api_url": f"http://{self.container}:8000/{b}/v1",
                            "upstream_model_name": f"chaos-{b}",
                            "router_params": {
                                **router_params,
                                "weight": self.weights[b],
                            },
                            "health_check": {
                                "url": f"http://{self.container}:8000/{b}/health"
                            },
                            "prometheus_metrics_path": None,
                        },
                    }
                )

        return apply


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))
