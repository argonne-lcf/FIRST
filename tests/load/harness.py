"""Closed-loop load generator: N users, each sending requests back to back.

Every user owns one connection and sends its next request as soon as the last
one finished, so the offered load is set by the number of users alone.  A run
sweeps a list of user counts; each count is one *step* of `warmup + duration`
seconds, spread over several worker processes (one event loop each) so the
generator is not the bottleneck.

The harness needs nothing from the gateway but HTTP: the same code drives a
`/health` route, V1's and V2's `/chat/completions`, unary or streaming.  The
server-side split of each request comes back in its `Server-Timing` header.

Depends only on the stdlib and httpx, so it can run from any host.
"""

import asyncio
import json
import multiprocessing
import random
import re
import signal
import ssl
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit, urlunsplit

import httpx

from .stats import Record, Summary, parse_server_timing, summarize

Mode = Literal["health", "unary", "stream"]

DEPLOYMENT_HEADER = "x-first-deployment"

# A chunk carrying generated text (vLLM's first chunk has only the role).
# Reasoning text arrives as `reasoning` (vLLM >= 0.2x) or `reasoning_content`.
_CONTENT = re.compile(rb'"(?:reasoning_content|reasoning|content)"\s*:\s*"[^"]')
_ERROR = re.compile(rb'"error"\s*:\s*[{"]')

# Roughly one token each for common tokenizers.
_WORDS = (
    "the of and to in is that for it as was with be by on not he this are or his "
    "from at which but have an had they you were their one all we can her has "
    "there been if more when will would who so no out up into than them only its "
    "time some could these two may then do first any my now such like our over man "
    "me even most made after also did many before must through years where much "
    "your way well down should because each just those people how too little state "
    "good very make world still own see men work long get here between both life"
).split()


@dataclass(frozen=True)
class StepConfig:
    """Everything one worker process needs for one step (picklable)."""

    url: str
    mode: Mode
    model: str | None
    users: int
    start_at: float  # wall clock; shared by every worker of the step
    warmup: float
    duration: float
    ramp: float  # users start spread over this many seconds
    input_tokens: int = 32
    output_tokens: int = 8
    pin_output: bool = True
    unique_prompts: bool = True
    extra_body: dict[str, Any] = field(default_factory=dict)
    token: str | None = None
    timeout: float = 300.0
    keepalive: bool = True
    verify: bool | str = True
    think_sec: float = 0.0
    seed: int = 0
    # Spread users over these ports (same URL otherwise), round-robin by user:
    # one tunnel per port, so no single ssh process carries every connection.
    ports: tuple[int, ...] = ()

    @property
    def end_at(self) -> float:
        return self.start_at + self.warmup + self.duration

    def url_for(self, index: int) -> str:
        """The URL user `index` of this worker sends to."""
        if not self.ports:
            return self.url
        parts = urlsplit(self.url)
        port = self.ports[index % len(self.ports)]
        host = parts.hostname or ""
        netloc = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
        return urlunsplit(parts._replace(netloc=netloc))


def build_body(cfg: StepConfig, rng: random.Random) -> bytes:
    """A chat completion request of about `input_tokens` in, `output_tokens` out.

    A random prefix makes every prompt distinct, so the model server's prefix
    cache cannot skip the prefill.  `pin_output` adds vLLM's `min_tokens` /
    `ignore_eos` so every response has exactly `output_tokens` tokens.
    """
    words = [rng.choice(_WORDS) for _ in range(max(1, cfg.input_tokens - 12))]
    if cfg.unique_prompts:
        words[0] = f"{rng.getrandbits(48):012x}"
    body: dict[str, Any] = {
        "model": cfg.model,
        "messages": [
            {
                "role": "user",
                "content": " ".join(words) + "\n\nContinue this text at length.",
            }
        ],
        "max_tokens": cfg.output_tokens,
        "temperature": 0,
    }
    if cfg.pin_output:
        body["min_tokens"] = cfg.output_tokens
        body["ignore_eos"] = True
    if cfg.mode == "stream":
        body["stream"] = True
        body["stream_options"] = {"include_usage": True}
    body.update(cfg.extra_body)
    return json.dumps(body).encode()


def _completion_tokens(payload: bytes | None) -> int | None:
    try:
        usage = json.loads(payload or b"null")["usage"]
        return int(usage["completion_tokens"])
    except (ValueError, TypeError, KeyError):
        return None


async def _consume_stream(resp: httpx.Response, rec: Record, sent: float) -> None:
    """Read an SSE stream to its end, timing the first token and the pauses."""
    buf = b""
    prev: float | None = None
    last: bytes | None = None
    events = 0
    gap = 0.0
    done = failed = False
    async for chunk in resp.aiter_bytes():
        now = time.perf_counter()
        buf += chunk.replace(b"\r\n", b"\n")
        while (i := buf.find(b"\n\n")) >= 0:
            event, buf = buf[:i], buf[i + 2 :]
            if not event.startswith(b"data:"):
                continue  # comment / keep-alive
            payload = event[5:].strip()
            if payload == b"[DONE]":
                done = True
                continue
            events += 1
            last = payload
            if rec["ttft"] is None:
                if _CONTENT.search(payload):
                    rec["ttft"] = (now - sent) * 1000
                    prev = now
            else:  # pauses are measured between tokens, after the first one
                assert prev is not None
                gap = max(gap, now - prev)
                prev = now
            if _ERROR.search(payload):
                failed = True
    rec["n"] = events
    rec["gap"] = gap * 1000
    rec["out"] = _completion_tokens(last)
    if failed:
        rec["err"] = "stream_error_event"
    elif not done:
        rec["err"] = "stream_incomplete"
    elif rec["ttft"] is None:
        rec["err"] = "stream_empty"
    else:
        rec["ok"] = True


async def _request(
    client: httpx.AsyncClient, cfg: StepConfig, url: str, body: bytes
) -> Record:
    rec: Record = dict.fromkeys(
        ("t1", "err", "e2e", "ttfb", "ttft", "gap", "n", "out", "gw", "up", "q")
    )
    rec.update(t0=time.time() - cfg.start_at, st=0, ok=False, att=None, dep=None)
    sent = time.perf_counter()
    try:
        if cfg.mode == "health":
            request = client.build_request("GET", url)
        else:
            request = client.build_request(
                "POST",
                url,
                content=body,
                headers={"Content-Type": "application/json"},
            )
        resp = await client.send(request, stream=True)
        try:
            rec["ttfb"] = (time.perf_counter() - sent) * 1000
            rec["st"] = resp.status_code
            timing = parse_server_timing(resp.headers.get("server-timing"))
            rec["gw"], rec["up"] = timing.get("gateway"), timing.get("upstream")
            rec["q"] = timing.get("queue")
            phases = {k[2:]: v for k, v in timing.items() if k.startswith("p_")}
            if phases:
                rec["ph"] = phases
            if "attempts" in timing:
                rec["att"] = int(timing["attempts"])
            rec["dep"] = resp.headers.get(DEPLOYMENT_HEADER)
            if resp.status_code != 200:
                await resp.aread()
            elif cfg.mode == "stream":
                await _consume_stream(resp, rec, sent)
            else:
                content = await resp.aread()
                if cfg.mode == "unary":
                    rec["out"] = _completion_tokens(content)
                    rec["ok"] = b'"choices"' in content
                    if not rec["ok"]:
                        rec["err"] = "no_choices"
                else:
                    rec["ok"] = True
        finally:
            await resp.aclose()
    except httpx.HTTPError as exc:
        rec["err"] = type(exc).__name__
    rec["e2e"] = (time.perf_counter() - sent) * 1000
    rec["t1"] = rec["t0"] + rec["e2e"] / 1000
    return rec


async def _user(
    cfg: StepConfig, index: int, verify: ssl.SSLContext | bool, records: list[Record]
) -> None:
    rng = random.Random(cfg.seed * 100_003 + index)
    delay = cfg.start_at + cfg.ramp * (index + rng.random()) / cfg.users - time.time()
    if delay > 0:
        await asyncio.sleep(delay)
    headers = {"Authorization": f"Bearer {cfg.token}"} if cfg.token else {}
    if not cfg.keepalive:
        headers["Connection"] = "close"
    url = cfg.url_for(index)
    async with httpx.AsyncClient(
        headers=headers,
        verify=verify,
        timeout=httpx.Timeout(cfg.timeout, connect=30.0),
        limits=httpx.Limits(max_connections=1),
    ) as client:
        while time.time() < cfg.end_at:
            body = b"" if cfg.mode == "health" else build_body(cfg, rng)
            record = await _request(client, cfg, url, body)
            records.append(record)
            if record["st"] == 0:  # could not connect or lost the connection:
                await asyncio.sleep(0.2)  # do not spin against a dead server
            if cfg.think_sec:
                await asyncio.sleep(cfg.think_sec)


async def run_users(cfg: StepConfig) -> list[Record]:
    """Run `cfg.users` users in this event loop until the step ends."""
    records: list[Record] = []
    verify: ssl.SSLContext | bool
    if cfg.verify is False or not cfg.url.startswith("https"):
        verify = False
    else:  # one TLS context for every user: building it per client is slow
        verify = ssl.create_default_context(
            cafile=cfg.verify if isinstance(cfg.verify, str) else None
        )
    await asyncio.gather(*(_user(cfg, i, verify, records) for i in range(cfg.users)))
    return records


def _worker_main(cfg: StepConfig, path: str) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # the parent decides when to stop
    try:
        import uvloop

        loop_factory: Any = uvloop.new_event_loop
    except ImportError:
        loop_factory = None
    cpu0 = time.process_time()
    with asyncio.Runner(loop_factory=loop_factory) as runner:
        records = runner.run(run_users(cfg))
    cpu = (time.process_time() - cpu0) / (cfg.warmup + cfg.duration)
    with open(path, "w") as f:
        f.write(json.dumps({"_cpu": cpu}) + "\n")
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")


# --- replica count, read from the V2 catalog -----------------------------------


class ReplicaPoller:
    """Counts the backends serving a model, from the V2 public catalog.

    `serving` = ready replicas of the model's PilotDeployments + its healthy
    StaticDeployments.  The harness never changes the count; it records it so
    throughput can be read against the replicas that were actually up.
    """

    def __init__(
        self, base_url: str, model: str, token: str | None, verify: bool | str
    ):
        self.model = model
        self.client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"} if token else {},
            verify=verify,
            timeout=15.0,
        )

    def sample(self) -> dict[str, int] | None:
        try:
            resp = self.client.get("/catalog/v1/models")
            resp.raise_for_status()
            entry = next(
                m
                for m in resp.json()
                if m["name"] == self.model or self.model in m.get("aliases", [])
            )
            ready = desired = 0
            for dep in entry["pilot_deployments"]:
                detail = self.client.get(f"/catalog/v1/deployments/pilot/{dep['name']}")
                detail.raise_for_status()
                ready += detail.json()["replica_counts"].get("ready", 0)
                desired += dep["desired_replicas"]
            static = sum(d["health"] == "healthy" for d in entry["static_deployments"])
        except (httpx.HTTPError, StopIteration, KeyError, ValueError) as exc:
            print(f"  replica poll failed: {type(exc).__name__}: {exc}", flush=True)
            return None
        return {"serving": ready + static, "ready": ready, "desired": desired}


# --- one step ------------------------------------------------------------------


def run_step(
    cfg: StepConfig,
    *,
    processes: int,
    out_dir: Path,
    poller: ReplicaPoller | None = None,
    poll_interval: float = 5.0,
) -> Summary:
    """Run one user count across worker processes and summarize it.

    `cfg.users` is the step's total; `cfg.start_at` is ignored and set here.
    Writes `requests.jsonl` (every request) and `replicas.jsonl` to `out_dir`.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    procs = max(1, min(processes, cfg.users))
    start_at = time.time() + 3.0 + 0.25 * procs  # time for the workers to import
    ctx = multiprocessing.get_context("spawn")
    workers = []
    for i in range(procs):
        share = cfg.users // procs + (1 if i < cfg.users % procs else 0)
        worker_cfg = replace(
            cfg, users=share, start_at=start_at, seed=cfg.seed * 1000 + i
        )
        path = out_dir / f"worker-{i}.jsonl"
        proc = ctx.Process(target=_worker_main, args=(worker_cfg, str(path)))
        proc.start()
        workers.append((proc, path))

    samples: list[dict[str, Any]] = []
    try:
        while any(proc.is_alive() for proc, _ in workers):
            offset = time.time() - start_at
            if poller and cfg.warmup <= offset <= cfg.warmup + cfg.duration:
                if (sample := poller.sample()) is not None:
                    samples.append({"t": round(offset, 1), **sample})
            workers[0][0].join(timeout=poll_interval if poller else 1.0)
    except KeyboardInterrupt:
        for proc, _ in workers:
            proc.terminate()
        raise

    records: list[Record] = []
    cpu = 0.0
    for proc, path in workers:
        proc.join()
        if not path.exists():
            raise RuntimeError(f"load worker exited with code {proc.exitcode}")
        with open(path) as f:
            cpu = max(cpu, json.loads(f.readline())["_cpu"])
            records.extend(json.loads(line) for line in f)
        path.unlink()
    records.sort(key=lambda r: r["t0"])
    with open(out_dir / "requests.jsonl", "w") as f:
        for rec in records:
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    with open(out_dir / "replicas.jsonl", "w") as f:
        for sample in samples:
            f.write(json.dumps(sample) + "\n")

    summary = summarize(
        records,
        users=cfg.users,
        warmup=cfg.warmup,
        duration=cfg.duration,
        stream=cfg.mode == "stream",
    )
    summary["client_cpu"] = cpu
    # Epoch bounds of the measured window, to line up server-side samples.
    summary["window"] = [start_at + cfg.warmup, start_at + cfg.warmup + cfg.duration]
    summary["processes"] = procs
    if samples:
        serving = [s["serving"] for s in samples]
        summary["replicas"] = {
            "min": min(serving),
            "max": max(serving),
            "desired": samples[-1]["desired"],
            "samples": len(samples),
        }
    return summary
