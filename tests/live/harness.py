"""
Client side of the live functional tests.

Talks to the gateway with the developer's real alcf-tokens login, the same way
the ``alcf-ai`` CLI does.  The backend side (docker compose, Redis, manifests,
the chaos upstream) lives in backend.py.
"""

import asyncio
import json
import math
import os
import random
import string
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Callable

import httpx
from alcf_tokens.auth import get_access_token

from alcf_ai.client import INFERENCE_SERVICE
from first_common.schema.auth import UserAuthEvent
from first_common.schema.types import UsageLimits, UsagePolicy
from first_gateway.apiserver.inference import DEPLOYMENT_HEADER

from .backend import _compose_redis_url

LIVE_DIR = Path(__file__).parent
RUNS_DIR = LIVE_DIR / ".runs"

# ---------------------------------------------------------------------------
# Configuration.  See tests/live/README.md for the manifest profile each
# model role expects.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveConfig:
    base_url: str = os.environ.get("inference_base_url", "http://localhost:8000")
    admin_base_url: str = os.environ.get("admin_base_url", "http://localhost:9100")
    redis_url: str = os.environ.get("LIVE_REDIS_URL") or _compose_redis_url()
    # RPM GCRA + max_user_concurrency (small, request-metered limits).
    rpm_model: str = os.environ.get("LIVE_RPM_MODEL", "google/gemma-4-31B-it")
    # TPM GCRA (token-metered limits, rpm unlimited).
    tpm_model: str = os.environ.get(
        "LIVE_TPM_MODEL", "mistralai/Mistral-Large-3-675B-Instruct-2512"
    )
    # Pilot-backed vLLM model on Tara+Sophia: inflight, saturation, federation,
    # autoscaling, drain, resilience.  Quotas are unlimited so only capacity binds.
    fleet_model: str = os.environ.get("LIVE_FLEET_MODEL", "openai/gpt-oss-20b")
    # A model hosted only on the maintenance cluster, with no running replica.
    maint_offline_model: str = os.environ.get(
        "LIVE_MAINT_OFFLINE_MODEL", "google/gemma-3-27b-it"
    )
    maint_cluster: str = os.environ.get("LIVE_MAINT_CLUSTER", "sophia")
    # Cluster that hosts the chaos upstream's StaticDeployments (any healthy
    # cluster that is not the maintenance cluster).
    chaos_cluster: str = os.environ.get("LIVE_CHAOS_CLUSTER", "tara")
    chaos_port: int = int(os.environ.get("LIVE_CHAOS_PORT", "18765"))
    # controller-manager Prometheus endpoint (autoscaler demand EWMA).
    control_metrics_url: str = os.environ.get(
        "LIVE_CONTROL_METRICS_URL", "http://127.0.0.1:9100/metrics"
    )
    apiserver_service: str = os.environ.get(
        "LIVE_APISERVER_SERVICE", "inference-gateway"
    )
    controller_service: str = os.environ.get(
        "LIVE_CONTROLLER_SERVICE", "controller-manager"
    )
    # vLLM-only knob that makes a generation run for exactly max_tokens, so a
    # "hold" lasts long enough.  Only sent to the fleet model.
    fleet_hold_extra: tuple[tuple[str, Any], ...] = (("ignore_eos", True),)

    def hold_extra(self, model: str) -> dict[str, Any]:
        return dict(self.fleet_hold_extra) if model == self.fleet_model else {}


STARTUP_TIMEOUT = float(os.environ.get("LIVE_STARTUP_TIMEOUT", "1800"))
LIVE_STATES = {"pending", "placed", "launching", "ready", "unhealthy"}

# ---------------------------------------------------------------------------
# GCRA reference model (mirrors lua/admit.lua) used to predict outcomes.
# ---------------------------------------------------------------------------


@dataclass
class Gcra:
    """Client-side twin of the admit.lua GCRA, driven by client timestamps."""

    rate_per_sec: float
    burst: float
    tat: float = 0.0

    @property
    def tau(self) -> float:
        return self.burst / self.rate_per_sec

    def evaluate(self, now: float, cost: float = 1.0) -> tuple[bool, float]:
        """Return (allowed, margin_sec).  ``margin`` is how far from the
        decision boundary this arrival was: predictions with a tiny margin are
        at the mercy of network latency and should not be asserted on."""
        tat = max(self.tat, now)
        new_tat = tat + cost / self.rate_per_sec
        allow_at = new_tat - self.tau
        if allow_at > now:
            return False, allow_at - now
        self.tat = new_tat
        return True, now - allow_at


# ---------------------------------------------------------------------------
# Replies
# ---------------------------------------------------------------------------


@dataclass
class Reply:
    status: int
    body: Any
    text: str
    headers: dict[str, str]
    marker: str
    model: str
    sent_at: float  # time.time() when the request was sent
    elapsed: float
    usage: dict[str, Any] | None = None
    stream: bool = False
    endpoint: str = "chat/completions"
    request_bytes: int = 0
    # Streaming only: the transport error that cut the stream, if any.
    stream_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == 200

    @property
    def retry_after(self) -> int | None:
        raw = self.headers.get("retry-after")
        return int(raw) if raw is not None else None

    @property
    def deployment(self) -> str | None:
        return self.headers.get(DEPLOYMENT_HEADER.lower())

    @property
    def error(self) -> dict[str, Any]:
        if isinstance(self.body, dict) and isinstance(self.body.get("error"), dict):
            err: dict[str, Any] = self.body["error"]
            return err
        return {}

    @property
    def info(self) -> dict[str, Any]:
        return self.error.get("info") or {}

    @property
    def message(self) -> str:
        return str(self.error.get("message", self.text[:300]))

    def summary(self) -> str:
        s = f"{self.status}"
        if not self.ok:
            s += f" {self.message!r}"
            if self.retry_after is not None:
                s += f" (Retry-After {self.retry_after})"
        elif self.stream_error:
            s += f" cut short by {self.stream_error}"
        elif self.usage:
            s += f" usage={self.usage}"
        return s

    def record(self) -> dict[str, Any]:
        return {
            "marker": self.marker,
            "model": self.model,
            "status": self.status,
            "sent_at": self.sent_at,
            "elapsed": round(self.elapsed, 3),
            "stream": self.stream,
            "endpoint": self.endpoint,
            "deployment": self.deployment,
            "request_bytes": self.request_bytes,
            "usage": self.usage,
            "retry_after": self.retry_after,
            "error": self.error or None,
            "stream_error": self.stream_error,
        }


def filler_text(n_chars: int, seed: int = 0) -> str:
    """Deterministic English-ish filler of exactly ``n_chars`` characters, so
    the chars-per-token ratio is stable across runs."""
    rng = random.Random(seed)
    words = (
        "the gateway admits each request against a leaky bucket while replicas "
        "on the cluster serve tokens to users who wait patiently for answers"
    ).split()
    out: list[str] = []
    size = 0
    while size < n_chars:
        w = rng.choice(words)
        out.append(w)
        size += len(w) + 1
    return " ".join(out)[:n_chars]


def sse_usage(text: str) -> dict[str, Any] | None:
    """The last non-null ``usage`` object in an SSE stream."""
    usage = None
    for line in text.splitlines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            continue
        try:
            obj = json.loads(data)
        except ValueError:
            continue
        if isinstance(obj, dict):
            u = obj.get("usage") or (obj.get("response") or {}).get("usage")
            if u:
                usage = u
    return usage


# ---------------------------------------------------------------------------
# Gateway client
# ---------------------------------------------------------------------------


class _CachedTokenAuth(httpx.Auth):
    """Bearer auth from alcf-tokens, cached so concurrent bursts do not each
    hit the token store."""

    ttl_sec = 300.0

    def __init__(self) -> None:
        self._token: str | None = None
        self._at = 0.0

    def token(self) -> str:
        if self._token is None or time.monotonic() - self._at > self.ttl_sec:
            self._token = get_access_token(INFERENCE_SERVICE)
            self._at = time.monotonic()
        return self._token

    def auth_flow(self, request: httpx.Request) -> Any:
        request.headers["Authorization"] = f"Bearer {self.token()}"
        yield request


class Gateway:
    def __init__(self, cfg: LiveConfig, run: "Run") -> None:
        self.cfg = cfg
        self.run = run
        self.http = httpx.AsyncClient(
            base_url=cfg.base_url,
            auth=_CachedTokenAuth(),
            timeout=httpx.Timeout(30.0, read=900.0),
            limits=httpx.Limits(max_connections=500, max_keepalive_connections=100),
        )
        self._seq = 0

    async def aclose(self) -> None:
        await self.http.aclose()

    def marker(self, tag: str = "") -> str:
        self._seq += 1
        return f"[livetest {self.run.run_id} #{self._seq}{' ' + tag if tag else ''}]"

    # ---- inference -------------------------------------------------------

    def chat_body(
        self,
        model: str,
        marker: str,
        prompt: str,
        max_tokens: int,
        stream: bool,
        extra: dict[str, Any] | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": model,
            "messages": [{"role": "user", "content": f"{marker} {prompt}"}],
            "max_tokens": max_tokens,
        }
        if stream:
            body["stream"] = True
        body.update(extra or {})
        return body

    @staticmethod
    def path(endpoint: str, deployment: str | None) -> str:
        if deployment:
            return f"/deployments/{deployment.replace('/', '~')}/v1/{endpoint}"
        return f"/federated/v1/{endpoint}"

    async def chat(
        self,
        model: str,
        prompt: str = "Reply with the single word OK.",
        *,
        max_tokens: int = 16,
        stream: bool = False,
        deployment: str | None = None,
        extra: dict[str, Any] | None = None,
        tag: str = "",
    ) -> Reply:
        marker = self.marker(tag)
        body = self.chat_body(model, marker, prompt, max_tokens, stream, extra)
        return await self.post(
            "chat/completions", body, marker=marker, deployment=deployment
        )

    async def post(
        self,
        endpoint: str,
        body: dict[str, Any],
        *,
        marker: str,
        deployment: str | None = None,
    ) -> Reply:
        path = self.path(endpoint, deployment)
        # Serialize ourselves so the exact byte count is known: it is the
        # RequestLog.content_length the gateway will report.
        content = json.dumps(body, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json"}
        sent_at = time.time()
        t0 = time.perf_counter()
        stream_error = None
        if body.get("stream"):
            async with self.http.stream(
                "POST", path, content=content, headers=headers
            ) as resp:
                raw = bytearray()
                try:
                    async for chunk in resp.aiter_raw():
                        raw += chunk
                except httpx.TransportError as exc:
                    stream_error = type(exc).__name__
            reply = self._reply(resp, bytes(raw), body, marker, sent_at, t0, True)
        else:
            resp = await self.http.post(path, content=content, headers=headers)
            reply = self._reply(resp, resp.content, body, marker, sent_at, t0)
        reply.endpoint = endpoint
        reply.request_bytes = len(content)
        reply.stream_error = stream_error
        self.run.log_reply(reply)
        return reply

    def _reply(
        self,
        resp: httpx.Response,
        raw: bytes,
        body: dict[str, Any],
        marker: str,
        sent_at: float,
        t0: float,
        stream: bool = False,
    ) -> Reply:
        text = raw.decode(errors="replace")
        parsed: Any = None
        usage = None
        if stream and resp.status_code == 200:
            usage = sse_usage(text)
        else:
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict):
                usage = parsed.get("usage")
        return Reply(
            status=resp.status_code,
            body=parsed,
            text=text,
            headers={k.lower(): v for k, v in resp.headers.items()},
            marker=marker,
            model=str(body.get("model")),
            sent_at=sent_at,
            elapsed=time.perf_counter() - t0,
            usage=usage,
            stream=stream,
        )

    # ---- holds: keep N requests in flight --------------------------------

    @asynccontextmanager
    async def hold(
        self,
        model: str,
        n: int,
        *,
        max_tokens: int = 12000,
        deployment: str | None = None,
        tag: str = "hold",
    ) -> AsyncIterator["Hold"]:
        """Open ``n`` concurrent streaming requests and keep them open until the
        block exits, then disconnect (exercising settle-on-disconnect)."""
        hold = Hold(self, model, max_tokens, deployment, tag)
        await hold.open(n)
        try:
            yield hold
        finally:
            await hold.release()

    # ---- health ------------------------------------------------------------

    async def healthy(self) -> bool:
        try:
            return (await self.http.get("/health", timeout=5)).status_code == 200
        except httpx.HTTPError:
            return False

    # ---- catalog -----------------------------------------------------------

    async def get_json(self, path: str) -> Any:
        resp = await self.http.get(path)
        resp.raise_for_status()
        return resp.json()

    async def whoami(self) -> UserAuthEvent:
        return UserAuthEvent.model_validate(await self.get_json("/whoami"))

    async def model(self, name: str) -> dict[str, Any]:
        for m in await self.get_json("/catalog/v1/models"):
            if m["name"] == name:
                return m  # type: ignore[no-any-return]
        raise LookupError(f"Model {name!r} is not visible in /catalog/v1/models")

    async def model_runtime(self, name: str) -> dict[str, Any]:
        return (await self.model(name))["runtime"]  # type: ignore[no-any-return]

    async def total_inflight(self, name: str) -> int:
        return int((await self.model_runtime(name))["total_inflight"])

    async def pilot_deployment(self, name: str) -> dict[str, Any]:
        return await self.get_json(f"/catalog/v1/deployments/pilot/{name}")  # type: ignore[no-any-return]

    async def router_config(self) -> dict[str, Any]:
        return await self.get_json("/catalog/v1/router-config")  # type: ignore[no-any-return]

    async def clusters(self) -> list[dict[str, Any]]:
        return await self.get_json("/catalog/v1/clusters")  # type: ignore[no-any-return]

    async def effective_limits(self, model_name: str) -> UsageLimits:
        """The caller's limits for a model, resolved exactly as the gateway
        does (default + overrides matched against /whoami)."""
        model = await self.model(model_name)
        policy = UsagePolicy.model_validate(model.get("usage_limits") or {})
        return policy.get_limits(await self.whoami())

    async def model_config(self, model_name: str) -> dict[str, Any]:
        cfg = await self.router_config()
        for m in cfg["models"]:
            if m["name"] == model_name:
                return m  # type: ignore[no-any-return]
        raise LookupError(f"{model_name} not in router config")

    async def backend_ids(self, model_name: str) -> dict[str, list[str]]:
        """Routable backend ids per deployment."""
        m = await self.model_config(model_name)
        return {d["name"]: [b["id"] for b in d["backends"]] for d in m["deployments"]}

    async def routable_capacity(self, model_name: str) -> tuple[int, dict[str, int]]:
        """(total slots, per-deployment slots) across routable backends."""
        m = await self.model_config(model_name)
        per: dict[str, int] = {}
        for d in m["deployments"]:
            if d["router_params"]["weight"] > 0:
                per[d["name"]] = (
                    len(d["backends"]) * d["router_params"]["max_backend_concurrency"]
                )
        return sum(per.values()), per

    async def deployment_inflight(self, model_name: str) -> dict[str, int]:
        """In-flight count per deployment as reported by the catalog APIs."""
        model = await self.model(model_name)
        out: dict[str, int] = {}
        for d in model["pilot_deployments"]:
            detail = await self.pilot_deployment(d["name"])
            out[d["name"]] = sum(
                r["runtime"]["inflight"]
                for r in detail["replicas"]
                if r.get("deleted_at") is None
            )
        if model["static_deployments"]:
            for sd in await self.get_json("/catalog/v1/deployments/static"):
                if sd["model_name"] == model_name:
                    out[sd["name"]] = sd["runtime"]["inflight"]
        return out

    async def ready_replicas(self, model_name: str) -> list[tuple[str, dict[str, Any]]]:
        """(deployment, replica) for every ready replica of a model."""
        out = []
        for d in (await self.model(model_name))["pilot_deployments"]:
            detail = await self.pilot_deployment(d["name"])
            out += [
                (d["name"], r)
                for r in detail["replicas"]
                if r["state"] == "ready" and r.get("deleted_at") is None
            ]
        return out

    async def fleet_snapshot(self, model_name: str) -> dict[str, Any]:
        """Compact autoscaling view: desired + replica states per deployment."""
        model = await self.model(model_name)
        deps = {}
        for d in model["pilot_deployments"]:
            detail = await self.pilot_deployment(d["name"])
            live = [r for r in detail["replicas"] if r.get("deleted_at") is None]
            deps[d["name"]] = {
                "desired": detail["desired_replicas"],
                "state": detail["state"],
                "replicas": Counter(r["state"] for r in live),
                "inflight": sum(r["runtime"]["inflight"] for r in live),
            }
        return {
            "t": time.time(),
            "total_inflight": model["runtime"]["total_inflight"],
            "capacity_rejects_total": model["runtime"]["capacity_rejects_total"],
            "maintenance_notices": model.get("maintenance_notices", {}),
            "deployments": deps,
        }

    async def demand_ewma(self, model_name: str) -> float | None:
        """Scrape autoscaler_model_demand_ewma from the controller."""
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                text = (await c.get(self.cfg.control_metrics_url)).text
        except httpx.HTTPError:
            return None
        needle = f'autoscaler_model_demand_ewma{{model="{model_name}"}}'
        for line in text.splitlines():
            if line.startswith(needle):
                return float(line.rsplit(" ", 1)[1])
        return None


async def wait_for(
    what: str,
    probe: Callable[[], Any],
    *,
    timeout: float,
    interval: float = 2.0,
) -> Any:
    """Poll an async ``probe`` until it returns a truthy value."""
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        last = await probe()
        if last:
            if ACTIVE_RUN:
                ACTIVE_RUN.note(f"Waited {time.monotonic() - t0:.0f}s for {what}")
            return last
        await asyncio.sleep(interval)
    raise AssertionError(f"Timed out after {timeout:.0f}s waiting for {what}")


# ---------------------------------------------------------------------------
# Holds
# ---------------------------------------------------------------------------


@dataclass
class HeldStream:
    marker: str
    status: int | None = None
    headers: dict[str, str] = field(default_factory=dict)
    error_body: str = ""
    chunks: int = 0
    tail: bytes = b""  # last bytes received (for [DONE] / usage)
    stream_error: str | None = None  # transport error that cut the stream
    released: bool = False  # we disconnected on purpose
    ended_at: float | None = None
    task: asyncio.Task[None] | None = None

    @property
    def message(self) -> str:
        try:
            return str(json.loads(self.error_body)["error"]["message"])
        except (ValueError, KeyError, TypeError):
            return self.error_body[:300]

    @property
    def deployment(self) -> str | None:
        return self.headers.get(DEPLOYMENT_HEADER.lower())

    @property
    def done(self) -> bool:
        """The stream ended cleanly with [DONE]."""
        return self.stream_error is None and b"data: [DONE]" in self.tail

    @property
    def usage(self) -> dict[str, Any] | None:
        return sse_usage(self.tail.decode(errors="replace"))

    @property
    def finished_early(self) -> bool:
        return self.ended_at is not None and not self.released


class Hold:
    """N concurrent long-running streams.  ``admitted`` are generating;
    ``rejected`` got a non-200 status back immediately."""

    def __init__(
        self,
        gw: Gateway,
        model: str,
        max_tokens: int,
        deployment: str | None,
        tag: str,
    ) -> None:
        self.gw = gw
        self.model = model
        self.max_tokens = max_tokens
        self.deployment = deployment
        self.tag = tag
        self.streams: list[HeldStream] = []
        self._release = asyncio.Event()

    @property
    def admitted(self) -> list[HeldStream]:
        return [s for s in self.streams if s.status == 200]

    @property
    def rejected(self) -> list[HeldStream]:
        return [s for s in self.streams if s.status not in (None, 200)]

    @property
    def live(self) -> list[HeldStream]:
        return [s for s in self.admitted if s.ended_at is None]

    async def open(self, n: int, *, stagger_sec: float = 0.0) -> list[HeldStream]:
        """Open ``n`` more streams; return once each has a status."""
        new: list[HeldStream] = []
        for _ in range(n):
            s = HeldStream(marker=self.gw.marker(self.tag))
            s.task = asyncio.create_task(self._run(s))
            new.append(s)
            self.streams.append(s)
            if stagger_sec:
                await asyncio.sleep(stagger_sec)
        await asyncio.wait_for(
            asyncio.gather(*(self._status(s) for s in new)), timeout=120
        )
        return new

    async def _status(self, s: HeldStream) -> None:
        while s.status is None:
            if s.task is not None and s.task.done():
                s.task.result()  # surface the exception
                return
            await asyncio.sleep(0.05)

    async def _run(self, s: HeldStream) -> None:
        body = self.gw.chat_body(
            self.model,
            s.marker,
            "Count upward from 1, one number per line, and never stop.",
            self.max_tokens,
            True,
            self.gw.cfg.hold_extra(self.model),
        )
        path = self.gw.path("chat/completions", self.deployment)
        sent_at = time.time()
        try:
            async with self.gw.http.stream("POST", path, json=body) as resp:
                s.headers = {k.lower(): v for k, v in resp.headers.items()}
                if resp.status_code != 200:
                    s.error_body = (await resp.aread()).decode(errors="replace")
                    s.status = resp.status_code
                    return
                s.status = 200
                reader = asyncio.create_task(self._drain(resp, s))
                release = asyncio.create_task(self._release.wait())
                await asyncio.wait(
                    {reader, release}, return_when=asyncio.FIRST_COMPLETED
                )
                if reader.done():
                    release.cancel()
                    reader.result()
                else:
                    s.released = True
                    reader.cancel()
                # Leaving the context closes the connection: a client disconnect.
        except httpx.TransportError as exc:
            s.stream_error = type(exc).__name__
            if s.status is None:
                s.status = -1
        finally:
            s.ended_at = time.time()
            self.gw.run.log_event(
                "hold",
                marker=s.marker,
                status=s.status,
                sent_at=sent_at,
                ended_at=s.ended_at,
                deployment=s.deployment,
                done=s.done,
                stream_error=s.stream_error,
                released=s.released,
            )

    async def _drain(self, resp: httpx.Response, s: HeldStream) -> None:
        async for chunk in resp.aiter_raw():
            s.chunks += 1
            s.tail = (s.tail + chunk)[-8192:]

    async def finish(self, timeout: float) -> None:
        """Wait for every stream to end on its own."""
        tasks = [s.task for s in self.streams if s.task is not None]
        if tasks:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout
            )

    async def release(self) -> None:
        self._release.set()
        tasks = [s.task for s in self.streams if s.task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Demand pump: hold a target level of concurrent demand over time
# ---------------------------------------------------------------------------


class DemandPump:
    """Keep ``level`` concurrent attempts against a model.  Each worker opens
    one long stream, re-opening it when it finishes or is rejected (after the
    server's Retry-After, capped).  ``set_level`` changes demand live."""

    def __init__(
        self,
        gw: Gateway,
        model: str,
        *,
        max_tokens: int = 4000,
        max_backoff: float = 5.0,
    ) -> None:
        self.gw = gw
        self.model = model
        self.max_tokens = max_tokens
        self.max_backoff = max_backoff
        self.statuses: Counter[int] = Counter()
        self.messages: Counter[str] = Counter()
        self.active = 0
        self._workers: list[asyncio.Task[None]] = []

    @property
    def level(self) -> int:
        return len(self._workers)

    async def set_level(self, level: int) -> None:
        while len(self._workers) < level:
            self._workers.append(asyncio.create_task(self._worker()))
        while len(self._workers) > level:
            task = self._workers.pop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def stop(self) -> None:
        await self.set_level(0)

    async def _worker(self) -> None:
        body_extra = self.gw.cfg.hold_extra(self.model)
        while True:
            marker = self.gw.marker("pump")
            body = self.gw.chat_body(
                self.model,
                marker,
                "Count upward from 1, one number per line, and never stop.",
                self.max_tokens,
                True,
                body_extra,
            )
            backoff = 0.0
            try:
                async with self.gw.http.stream(
                    "POST", "/federated/v1/chat/completions", json=body
                ) as resp:
                    self.statuses[resp.status_code] += 1
                    if resp.status_code == 200:
                        self.active += 1
                        try:
                            async for _ in resp.aiter_raw():
                                pass
                        finally:
                            self.active -= 1
                    else:
                        text = (await resp.aread()).decode(errors="replace")
                        try:
                            msg = json.loads(text)["error"]["message"]
                        except (ValueError, KeyError, TypeError):
                            msg = text[:120]
                        self.messages[_squash_numbers(msg)] += 1
                        ra = resp.headers.get("retry-after")
                        backoff = min(float(ra) if ra else 2.0, self.max_backoff)
            except httpx.HTTPError as exc:
                self.statuses[-1] += 1
                self.messages[type(exc).__name__] += 1
                backoff = 2.0
            await asyncio.sleep(backoff + random.uniform(0, 0.5))


def _squash_numbers(msg: str) -> str:
    return "".join("N" if c.isdigit() else c for c in msg).replace("NN", "N")


# ---------------------------------------------------------------------------
# Fleet timeline (autoscaling / resilience)
# ---------------------------------------------------------------------------


class Timeline:
    """5s samples of the fleet, printed and written to timeline.jsonl."""

    def __init__(self, gw: Gateway, model: str, pump: DemandPump, run: "Run") -> None:
        self.gw, self.model, self.pump, self.run = gw, model, pump, run
        self.samples: list[dict[str, Any]] = []
        self.phase = "init"
        self._task: asyncio.Task[None] | None = None
        self._file = (run.dir / "timeline.jsonl").open("a")

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def sample(self) -> dict[str, Any]:
        snap = await self.gw.fleet_snapshot(self.model)
        snap["ewma"] = await self.gw.demand_ewma(self.model)
        snap["phase"] = self.phase
        snap["pump_level"] = self.pump.level
        snap["pump_active"] = self.pump.active
        snap["pump_statuses"] = dict(self.pump.statuses)
        self.samples.append(snap)
        self._file.write(json.dumps(snap, default=str) + "\n")
        self._file.flush()
        deps = "  ".join(
            f"{name.split('/')[0]}: want {d['desired']} "
            f"{dict(d['replicas']) or '{}'} in {d['inflight']}"
            for name, d in snap["deployments"].items()
        )
        ewma = f" ewma={snap['ewma']:.2f}" if snap["ewma"] is not None else ""
        self.run.log(
            f"[{time.strftime('%H:%M:%S')}] {self.phase:4s} level={self.pump.level} "
            f"active={self.pump.active} inflight={snap['total_inflight']} "
            f"rejects={snap['capacity_rejects_total']}{ewma} | {deps}"
        )
        return snap

    async def _loop(self) -> None:
        while True:
            try:
                await self.sample()
            except Exception as exc:  # keep sampling through blips
                self.run.log(f"timeline sample failed: {exc!r}")
            await asyncio.sleep(5)

    def desired(self, dep: str) -> int:
        return int(self.samples[-1]["deployments"][dep]["desired"])

    def first_time(self, dep: str, value: int, since: float) -> float | None:
        for s in self.samples:
            if s["t"] >= since and s["deployments"][dep]["desired"] == value:
                return float(s["t"])
        return None

    async def until(self, what: str, cond: Any, timeout: float) -> float:
        t0 = time.monotonic()
        while time.monotonic() - t0 < timeout:
            if cond(await self.sample()):
                self.run.note(
                    f"[{self.phase}] waited {time.monotonic() - t0:.0f}s: {what}"
                )
                return time.time()
            await asyncio.sleep(5)
        raise AssertionError(f"[{self.phase}] timed out after {timeout:.0f}s: {what}")


def all_deps(deps: list[str], pred: Any) -> Any:
    return lambda snap: all(pred(snap["deployments"][d]) for d in deps)


# ---------------------------------------------------------------------------
# Run bookkeeping
# ---------------------------------------------------------------------------


@dataclass
class Section:
    """One test's part of the report."""

    nodeid: str
    goal: str
    started: float = field(default_factory=time.monotonic)
    lines: list[str] = field(default_factory=list)
    checks: int = 0
    failed: list[str] = field(default_factory=list)
    headline: str | None = None
    verdict: str | None = None  # PASSED / FAILED / SKIPPED / ERROR
    reason: str | None = None
    duration: float = 0.0

    @property
    def name(self) -> str:
        return self.nodeid.split("::", 1)[-1]


ACTIVE_RUN: "Run | None" = None

VERDICT_MARK = {"PASSED": "✔", "FAILED": "✘", "ERROR": "✘", "SKIPPED": "–"}


class Run:
    """One live test session: a run id and a directory of artifacts.

    - ``report.md``: per test, its goal, what was done (``step``), every check
      with its evidence (``check``), and the verdict; a summary table on top.
      Rewritten after every test, so an interrupted run still has one.
    - ``notes.md``: everything printed, in order (including fleet timelines).
    - ``requests.jsonl``: every request sent, with its outcome.
    """

    def __init__(self, base_url: str = "") -> None:
        self.started = datetime.now(timezone.utc)
        suffix = "".join(random.choices(string.ascii_lowercase, k=4))
        self.run_id = f"{self.started:%m%d-%H%M%S}-{suffix}"
        self.dir = RUNS_DIR / self.run_id
        self.dir.mkdir(parents=True, exist_ok=True)
        self.base_url = base_url
        self._records = (self.dir / "requests.jsonl").open("a")
        self.preface: list[str] = []  # session-level lines before any test
        self.sections: list[Section] = []
        self.current: Section | None = None
        global ACTIVE_RUN
        ACTIVE_RUN = self

    # ---- machine-readable records -----------------------------------------

    def log_reply(self, reply: Reply) -> None:
        self._write({"kind": "reply", **reply.record()})

    def log_event(self, kind: str, **fields: Any) -> None:
        self._write({"kind": kind, **fields})

    def _write(self, row: dict[str, Any]) -> None:
        self._records.write(json.dumps(row, default=str) + "\n")
        self._records.flush()

    # ---- narrative --------------------------------------------------------

    def log(self, text: str) -> None:
        """Print and append to notes.md only (bulky detail, e.g. timelines)."""
        print(text, flush=True)
        with (self.dir / "notes.md").open("a") as f:
            f.write(text + "\n")

    def _add(self, line: str) -> None:
        self.log(line)
        (self.current.lines if self.current else self.preface).append(line)

    def note(self, text: str) -> None:
        """Context worth reading in the report (measurements, observations)."""
        self._add(f"- {text.strip()}")

    def step(self, text: str) -> None:
        """Something the test did to the system."""
        self._add(f"- **Did:** {text}")

    def check(
        self, ok: bool, claim: str, evidence: str = "", *, soft: bool = False
    ) -> bool:
        """Record one checked claim and the evidence for it.  Fails the test
        unless ``soft`` (soft failures still fail the report section)."""
        __tracebackhide__ = True
        mark = "✔" if ok else "✘"
        line = f"- {mark} {claim}" + (f" — {evidence}" if evidence else "")
        self._add(line)
        if self.current:
            self.current.checks += 1
            if not ok:
                self.current.failed.append(
                    f"{claim}" + (f" ({evidence})" if evidence else "")
                )
        if not ok and not soft:
            raise AssertionError(f"{claim}" + (f" — {evidence}" if evidence else ""))
        return ok

    def evidence(self, text: str) -> None:
        """The one-line result shown in the summary table."""
        if self.current:
            self.current.headline = text
        self._add(f"- **Result:** {text}")

    # ---- sections and the report ------------------------------------------

    def begin(self, nodeid: str, goal: str) -> None:
        self.current = Section(nodeid, goal)
        self.sections.append(self.current)
        self.log(f"\n=== {nodeid}\n{goal}")

    def finish(self, verdict: str, reason: str | None = None) -> None:
        sec = self.current
        if sec is None or sec.verdict is not None:
            return
        if verdict == "PASSED" and sec.failed:
            verdict, reason = "FAILED", sec.failed[0]
        sec.verdict, sec.reason = verdict, reason
        sec.duration = time.monotonic() - sec.started
        self.log(f"=== {verdict} {sec.name}" + (f": {reason}" if reason else ""))
        self.write_report()

    def write_report(self) -> None:
        counts = Counter(s.verdict or "RUNNING" for s in self.sections)
        out = [
            f"# Live test report: run {self.run_id}",
            "",
            f"Gateway {self.base_url}, started {self.started:%Y-%m-%d %H:%M:%S} UTC, "
            f"updated {datetime.now(timezone.utc):%H:%M:%S} UTC.  "
            + ", ".join(f"{n} {v.lower()}" for v, n in sorted(counts.items())),
            "",
            "| | Test | Checks | Result |",
            "|---|---|---|---|",
        ]
        for s in self.sections:
            mark = VERDICT_MARK.get(s.verdict or "", "…")
            detail = (s.reason if s.verdict != "PASSED" else s.headline) or ""
            out.append(
                f"| {mark} {s.verdict or 'RUNNING'} | `{s.name}` | {s.checks} | "
                f"{_cell(detail)} |"
            )
        if self.preface:
            out += ["", "## Session setup", "", *self.preface]
        for s in self.sections:
            mark = VERDICT_MARK.get(s.verdict or "", "…")
            out += [
                "",
                f"## {mark} {s.verdict or 'RUNNING'}: `{s.name}` ({s.duration:.0f}s)",
                "",
                f"**Goal:** {s.goal}",
                "",
                *s.lines,
            ]
            if s.verdict and s.verdict != "PASSED":
                out += ["", f"**{s.verdict}:** {s.reason or ''}"]
        (self.dir / "report.md").write_text("\n".join(out) + "\n")


def _cell(text: str) -> str:
    text = " ".join(text.split())
    return (text[:200] + "…" if len(text) > 200 else text).replace("|", "\\|")


def binomial_tolerance(n: int, p: float, sigmas: float = 3.0) -> float:
    """Allowed |observed - expected| fraction for n Bernoulli(p) samples."""
    return sigmas * math.sqrt(p * (1 - p) / max(n, 1)) + 1.0 / max(n, 1)
