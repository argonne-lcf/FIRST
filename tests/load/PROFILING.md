# Profiling the V2 hot path under load

Branch `loadtest-profiling` (off `firstv2`). **Not for production**: it exists to
answer one question and is meant to be thrown away once the fixes are chosen.

## The question

Run B0-v2 (mock upstream, no errors) showed V2's four workers pegged at 82–88%
of a core each from about 32 users, and `upstream` growing from 1.8 ms to 1.3 s
although the mock was not slowing down. The gateway is CPU-bound, and a
saturated event loop inflates every wall-clock timer that spans an `await`:
the reply has arrived, but the worker has not come back to the request yet.

So "where does the time go" is two questions, and they need different tools:

1. **Where does the CPU go?** That sets the ceiling (req/s per worker).
2. **How many times does a request wait for the loop, and for how long?** That
   sets the latency once the CPU is busy.

## What is in the bundle

| Layer | Answers | Where | Output |
| --- | --- | --- | --- |
| 1. py-spy sampling | CPU by library, by gateway function, by leaf function, including framework code no timer of ours can see | `profile.sh pyspy`, `deploy/compose.profile.yaml` | `pyspy-*.txt` (collapsed stacks) |
| 2. Phase laps | the gateway time of every request cut into 16 consecutive phases, as p50/p95/p99 per step | `lap()` in `apiserver/context.py`; sent as `p_<phase>` in `Server-Timing` | `ph` in `requests.jsonl`, phase table in `summary.md` |
| 3. Worker gauges | per worker, per second: event-loop lag, loop busy fraction, CPU per request, CPU on other threads | `apiserver/profiling.py` (on when `FIRST_PROFILE_DIR` is set) | `worker-<pid>.jsonl` |

`python -m tests.load profile-report` joins the three into one `profile.md`.

### Phases

Laps are consecutive checkpoints, so the phases of a request add up to its
`gateway` time with nothing left over. Each phase is one of two kinds:

- **`cpu`**: no `await` inside. Its time is CPU spent on this request, and it
  does not inflate under load. These are exact.
- **`wait`**: awaits I/O. Its time is *I/O + event-loop lag + the client
  library's own CPU*. At 1 user it is close to the true I/O time; whatever it
  gains at 32 users is the loop, not Redis or the mock.

| Phase | Kind | Covers |
| --- | --- | --- |
| `pre_auth` | cpu* | ASGI stack, routing, body read, FastAPI's JSON decode, dependency setup |
| `auth` | wait | token hash, Redis token-cache GET, `model_validate_json` of the cached result |
| `request_log` | cpu | `RequestLog` build + emit |
| `validate` | cpu | pydantic validation of the body into the payload model |
| `route` | cpu | resolve model, permission check, shuffle backends |
| `token_stats` | wait | `get_token_stats` (Redis MGET) |
| `estimate` | cpu | `payload.estimate_tokens` |
| `admit` | wait | `admit_request` (Redis Lua) |
| `dump` | cpu | `payload.model_dump` + httpx request build (JSON encode) |
| `upstream` | wait | backend round trip; for a stream, until the backend's headers |
| `parse` | cpu | `response.json()` + usage parse |
| `settle` | wait | `admission.settle` (Redis GET + Lua) |
| `record_stats` | wait | `record_token_stats` (Redis MGET + pipeline) |
| `inference_log` | cpu | `InferenceLog` build + emit |
| `render` | cpu | `JSONResponse` re-serialization |
| `respond` | cpu | back out through FastAPI to the first response byte |

\* `pre_auth` awaits the body read, which for a small body is already buffered.

Not covered by phases, only by py-spy: sending the body, `ResponseLog`, the
stream relay (a stream's phases stop where its headers go out), and the log
listener thread.

## Hypotheses

Written from reading the code, before measuring. Each names what would confirm
it and the change it suggests. Fill in the verdict column after the run.

| # | Hypothesis | Confirmed if | Candidate change | Verdict |
| --- | --- | --- | --- | --- |
| H1 | **Too many loop round trips.** A unary request makes 7 sequential Redis round trips (token cache GET; `get_token_stats` MGET; admit Lua; settle GET + Lua; `record_token_stats` MGET + pipeline) plus the upstream call: at least 8 waits, each paying the loop lag. | The five Redis `wait` phases are ≪ 1 ms each at 1 user and together dominate `gateway` at 32; their growth ≈ lag p50 × round trips. | Fewer round trips: see H2–H4. | **Confirmed** (P1, 2026-11-17): 5.4 of 8.1 ms overhead at 1 user; redis-py 15.5% of CPU at B0 saturation. |
| H2 | **`settle` pays an avoidable GET.** `InferenceService` calls `settle(request_id, ...)` without `model_name`/`user_id`/`backend_id`, so it takes the pre-read path that the docstring says the hot path should skip. | `settle` ≈ 2× `admit` at 1 user. | Pass the three ids (all in scope). One line per call site. | **Confirmed**: settle 1.80 vs admit 0.76 ms at 1 user (5.4 vs 2.3 at 32). |
| H3 | **Token-stats EWMA costs 3 round trips per request** (1 read before admission, 2 to update after). | `token_stats` + `record_stats` are a large share of the Redis wait. | Read the EWMAs inside the admit script and update them inside the settle script; or update off the response path; or cache the read per worker for a few seconds. | **Confirmed**: token_stats + record_stats 2.05 ms at 1 user, 8.0 ms at 32 users. |
| H4 | **Auth re-validates the cached token on every request**: SHA-256, Redis GET, `model_validate_json`. | `auth` is among the top `wait` phases at 1 user; py-spy shows `introspect_token` → pydantic. | Per-worker cache of the validated `UserAuthEvent` with a short TTL (seconds), in front of Redis. | Minor: auth 0.75 ms at 1 user; `introspect_token` 3.7% of CPU. |
| H5 | **JSON is handled five times.** FastAPI `json.loads` → pydantic validate → `model_dump(mode="json")` → httpx `json.dumps`; then `response.json()` → `JSONResponse` `json.dumps`, although `response.content` already holds the bytes to send. | `validate` + `dump` + `parse` + `render` dominate the `cpu` phases and grow with `--input-tokens`; py-spy leaf time in `stdlib:json` and `pydantic_core`. | Relay `response.content` as the response body and parse it only for usage; forward the request body bytes when no rewrite is needed (or patch `model` only); a faster JSON codec. | Minor: ~3% of CPU at 8 tokens; +1.3 ms CPU/req at 4096 input tokens. |
| H6 | **Three structured-log events per request** (`RequestLog`, `InferenceLog`, `ResponseLog`): each builds a pydantic model, a UUID, `model_dump(mode="json")`, and a log record through the `QueueHandler`; the listener thread then formats and writes it while competing for the GIL. | `request_log` + `inference_log` are a visible share of `cpu`; `other threads` CPU in the worker table is not small; py-spy under `emit`. | Cheaper event serialization (one `model_dump_json`), or build the line on the listener thread. | **Confirmed (moderate)**: ~9% of CPU in log emits, plus `uuid4` 4.5%. |
| H7 | **Framework overhead per request**: Starlette/FastAPI routing and dependency resolution, `HTTPBearer`, contextvars, the exit stack. | `pre_auth` + `respond` are a large share of `cpu`; py-spy charges it to `__call__ (log_middleware.py) → fastapi/starlette`. | Fewer dependencies on the inference routes; a plain Starlette route for the hot path. | **Confirmed (moderate)**: `solve_dependencies` 12.7% of CPU, including H13 (sync `InferenceService` dependency run in the thread pool). |
| H8 | **Body files written on threads** for bodies ≥ 2000 bytes (`asyncio.create_task` + `to_thread`) add loop turns and GIL contention. | Only at larger `--input-tokens`: `other threads` CPU rises, lag rises with it. | Batch the writes, or hand them to the log listener thread. | Plausible: at 4096 tokens other-thread CPU 37% and req/s −43% while loop threads are ~54% busy. |

What the numbers say about the ceiling, independent of any hypothesis: one
worker tops out at `1000 / (CPU ms per request)` req/s. The `cpu` phases plus
the py-spy tables say which milliseconds to remove.

## Running it

Two hosts, as in the campaign: the **gateway host** (Dev VM, V2 compose stack)
and the **client host** (tunnel to V2 nginx, `V2=http://localhost:8001`). The
mock model `loadtest/mock` must be registered and its limits lifted as in
[README.md](README.md) ("The mock upstream").

### 1. Gateway host: deploy the profiling build

```bash
git fetch && git checkout loadtest-profiling
# in .env, append the overlay:
#   COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml:...:deploy/compose.profile.yaml
make profile-up        # rebuilds the image with py-spy, restarts the gateway
```

Check from the client that phases come back:

```bash
curl -s -o /dev/null -D - $V2/health | grep -i server-timing   # ... p_respond;dur=...
```

### 2. Client host: unsaturated baseline (1 user)

```bash
run="uv run python -m tests.load run"
chat=$V2/federated/v1/chat/completions
$run --name prof-01u --url $chat --model loadtest/mock --users 1 \
     --warmup 10 --duration 60 --catalog-url $V2
```

This gives every phase's true cost: no loop lag yet.

### 3. Client host: the saturated step, clean (32 users)

```bash
make profile-reset     # on the gateway host: drop gauges from earlier runs
$run --name prof-32u --url $chat --model loadtest/mock --users 32 \
     --warmup 15 --duration 120 --catalog-url $V2
```

Phases and gauges from this run are the clean numbers (no profiler attached).

### 4. Same step again, with py-spy attached

Start the run on the client:

```bash
$run --name prof-32u-pyspy --url $chat --model loadtest/mock --users 32 \
     --warmup 15 --duration 120 --catalog-url $V2
```

and about 20 s later, on the gateway host:

```bash
make profile-pyspy DURATION=60                 # all four workers, 100 samples/s each
NATIVE=1 LABEL=native make profile-pyspy DURATION=30   # optional: inside C extensions
```

py-spy pauses a worker briefly for each sample. Compare this run's req/s with
step 3: if it dropped by more than a few percent, lower `RATE` or set
`NONBLOCKING=1`.

### 5. Collect and report

```bash
# gateway host
make profile-collect DIR=loadtest-runs/profile-32u
# client host: fetch it, then join it with each run
scp -r inferapi-dev:<repo>/loadtest-runs/profile-32u loadtest-runs/
for r in prof-01u prof-32u prof-32u-pyspy; do
  uv run python -m tests.load profile-report loadtest-runs/$r-* \
      --profile loadtest-runs/profile-32u
done
```

Each run directory gets a `profile.md`. Worker gauges are matched to a step by
wall clock (the step's `window` in `summary.json`), so both hosts need NTP; if
the report says there are no samples in the window, pass `--clock-offset`.

To look at the stacks as a flame graph, drop a `pyspy-*.txt` file onto
<https://www.speedscope.app> (it reads collapsed stacks), or run
`tests/load/profile.sh top` for a live view of one worker.

### Variations worth one run each

- `--users 8,16,32,64` on the clean build: lag and the `wait` phases versus load.
- `--input-tokens 4096`: scales the JSON and body-file costs (H5, H8).
- `--stream`: phases stop at the stream's headers, so use py-spy for the relay.

## Reading the report

For each step, `profile.md` has:

1. **Gateway time by phase.** Compare 1 user with 32. `cpu` phases should
   barely move: their means, summed, are CPU you can remove by making that code
   cheaper. `wait` phases that balloon are waiting for the loop: the fix is
   fewer round trips (or less CPU elsewhere), not a faster Redis.
2. **Gateway workers.** `loop busy` near 100% means saturated. `CPU ms/req` is
   the service demand: the number every optimization should lower, and the one
   to compare before and after a change. `other threads` is CPU outside the
   event loop (log listener, body writes), which competes for the GIL.
   `lag` is what each loop round trip costs at this load.
3. **Reading the two together.** How much of the measured CPU per request the
   `cpu` phases explain. The remainder is inside the `wait` phases (redis-py,
   httpx), the ASGI server, and post-header work: the py-spy tables split it.
4. **CPU samples** (once per capture, for the whole capture):
   - *By library*: self time. Is it JSON, pydantic, redis-py, httpx, logging?
   - *By gateway function*: each sample charged to the innermost line of our
     code on its stack, with the library the CPU was actually spent in. This is
     the table to rank changes with: "`settle` → redis is 9% of all CPU".
   - *By leaf function*: the hottest individual functions.

A change is worth making if it removes CPU ms per request (raises the ceiling)
or removes loop round trips (cuts latency at a given load). Re-run steps 2–3
after each change and compare `CPU ms/req` and the phase table.

## High concurrency: the backend connection pool (B1s)

Everything above profiles the **CPU-bound, low-concurrency** case (B0: a 0 ms
mock, ~32 in flight). Block B1s found a second, different limit: with a 1 s mock
(`ttft_ms 200`, `token_ms 5`, 160 tokens) and ~1000 requests in flight, V2's
four workers are CPU-pegged at only **62–69 req/s**, where B0's saturated workers
did **756 req/s**. CPU per request grows with the number of requests **in flight**,
not with the request rate. Details: campaign RUNLOG, "Root cause of the B1s
collapse" (2026-11-17).

### What is already known

- **Reproduced without the gateway** (`pool_bench.py`, plain HTTP, local mock,
  httpx 0.28.1 / httpcore 1.0.9). One client like V2's backend pool
  (`max_connections=4096`, keepalive 30 s): 64 in flight 60 req/s at 2.8 CPU
  ms/req; **256 in flight 42 req/s at 21.5 ms/req** (expected ~256 req/s).
- **cProfile at 256:** 71% of CPU in
  `httpcore/_async/connection_pool.py:_assign_requests_to_connections`, with
  ~5500 `is_idle()` calls per invocation. On every request start and finish the
  pool walks every connection × every queued request. Also visible:
  `has_expired()` → `is_socket_readable()` → `getpeername` / `poll` per idle
  connection.
- **Sharding helps but does not finish the job:** 16 clients × 256 connections,
  round-robin: 256 in flight **238 req/s, 1.48 ms/req**; but 1024 in flight still
  collapses (67 req/s, 97% CPU, 14.6 ms/req). A second cost that grows with
  concurrency remains.

### Hypotheses

| # | Hypothesis | Confirmed if | Candidate change | Verdict |
| --- | --- | --- | --- | --- |
| H9 | **httpcore pool scan is O(connections × waiting requests)** per acquire/release. | py-spy at 256/1024 in flight (1 s mock): `_assign_requests_to_connections` + `is_idle` dominate; `upstream` (a `wait` phase) grows while the mock is flat. | Smaller pools per client (shard by backend × N, round-robin); or a different client for pilot calls (aiohttp, or httpcore with a patched pool); upstream fix to httpcore. | **Confirmed in the gateway**: 83% of CPU at 512 in flight (1 s mock); CPU/req 6 → 54 ms from 128 to 512 users. |
| H10 | **Per-connection idle checks** (`has_expired` → `is_socket_readable`: a `poll()` + `getpeername` on each idle connection at each scan) add a second O(connections) cost. | With sharding, py-spy at 1024 still shows `has_expired` / `is_socket_readable` / anyio `extra_attributes` high. | Fewer idle connections (lower `max_keepalive_connections`), or a client that does not probe sockets on acquire. | **Confirmed**: `has_expired` → `is_socket_readable` / `getpeername` / `extra_attributes` ≈ 1/3 of CPU at 512 in flight. |
| H11 | **The remaining 1024 cost is outside the pool**: per-request anyio/asyncio overhead (cancel scopes, timeouts, task switches) that scales with live coroutines; or the 1024-connection warm-up storm (TLS to the pilot in the gateway). | Sharded `pool_bench` at 1024 profiled with py-spy: top frames in anyio / asyncio / `ssl`, not httpcore's pool. | Depends on frames: a uvloop + aiohttp comparison would separate "httpx stack" from "asyncio at 1000 tasks". | **Rejected**: 16-shard `pool_bench` at 1024 is still 76% the pool scan; cost ≈ connections × waiters per pool, so sharding only divides it. |
| H12 | **Event-loop lag from one hot worker** amplifies the tail: in one B1s run one worker was at 94% while the other three were at 12–18%. Connections from nginx are long-lived (upstream keepalive), so a worker that accepted many early keeps them. | Worker gauges at 256/1024 in flight: busy fraction uneven across workers; per-worker in-flight counts uneven. | Spread connections (`SO_REUSEPORT` in uvicorn workers, or nginx `keepalive_requests` lower so connections rebalance). | **Confirmed**: 64 streams 94/15/23/64% per worker; 128 unary users 7/8/19/41%. |

### Runs to add

1. **`pool_bench.py` first** (minutes, no Dev VM): `--users 64,256,1024`, with
   `--shards 1` and `--shards 16`, then the same under py-spy
   (`py-spy record -o pool.txt -f raw -- python tests/load/pool_bench.py ...`).
   `--client aiohttp` drives the gateway's `BackendClient` instead, and `--stream`
   reads SSE streams (mock `MOCK_TTFT_MS=200 MOCK_TOKEN_MS=5`). Local mock, 1 s
   unary, CPU ms/req at 64 / 256 / 512 in flight: httpx 1.0 / 26.5 / 24.7 (25
   req/s at 512), aiohttp 0.13 / 0.10 / 0.09 (507 req/s). 64 streams of 160
   chunks: httpx 6.6, aiohttp 4.2 ms per stream.
2. **Gateway at high concurrency**, profiling build, mock `ttft_ms 200`,
   `token_ms 5`: `--output-tokens 160 --users 64,256,1024 --warmup 30 --ramp 25`,
   py-spy attached at 256 and at 1024. Compare `CPU ms/req` and the phase table
   with B0's 32-user step: `upstream` growing while the mock is flat, with
   httpcore on top of the CPU table, confirms H9 in the gateway.
3. **Don't drive more than ~1000 users through the tunnels.** One `ssh -L` listener has a
   hard-coded backlog of 128 and drops connection bursts; the campaign uses 4 separate ssh
   processes per gateway with the harness's `--ports` (EXPERIMENT_PLAN 0.2), which kept
   `ListenOverflows` at +0 up to 512 users. The public Dev hostname is not used (shared proxy).

## Additional findings (P1, 2026-11-17)

- **H13, sync dependency in the thread pool:** `InferenceServiceDep = Annotated[InferenceService,
  Depends(InferenceService)]` (`apiserver/inference.py`). `InferenceService.__init__` is sync, so
  FastAPI calls it via `run_in_threadpool` on every inference request (anyio's default limiter is
  40 threads). It inflates the `validate` lap from 0.8 to 5.8 ms at 32 users. Fix: an `async def`
  provider.
- **Stream relay:** 0.17–0.27 ms of gateway CPU per relayed chunk, linear in chunk rate (64 streams
  at 5 / 20 / 50 ms per token). About 64% of busy CPU sits in httpx `aiter_raw` → httpcore → h11 →
  anyio TLS `read` (each read inside a `fail_after` cancel scope, ~10% of samples); the gateway's own
  `UsageTap.feed` is ~2%.
- Results and verdict table: campaign RUNLOG, "P1 profiling".

## Caveats

- **Observer effect.** Laps cost two clock reads and a dict update each (about
  2 µs per request in total), the probe wakes each worker 100 times a second,
  and the header grows by about 350 bytes. Check it: req/s at 32 users on this
  branch (step 3) against B0-v2 on `firstv2`.
- **Lag resolution.** uvloop timers tick in whole milliseconds, so an idle
  worker reads up to ~1 ms of lag. Lag samples are taken every 10 ms *plus* the
  lag, so long stalls are slightly under-weighted in the percentiles.
- **py-spy sees on-CPU Python.** Idle time is excluded. uvloop's own work
  (socket I/O, HTTP parsing) appears as the leaf `run (asyncio/runners.py)`
  unless captured with `NATIVE=1`.
- **Closed loop**, as always: at saturation the client is throttled by the
  gateway, so compare steps by users, not by offered rate.

## Later: Python 3.15's `profiling.sampling`

Not needed for this round: py-spy answers the CPU question on 3.12 without
changing the runtime under test. Moving to 3.15 is its own experiment (does the
ceiling move?), to be measured with this same bundle; its sampling profiler
would then replace py-spy in `profile.sh`, after checking that every compiled
dependency (uvloop, httptools, pydantic-core, psycopg, pyzstd, cryptography)
ships 3.15 wheels.
