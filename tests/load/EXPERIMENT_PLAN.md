# V1 vs V2 load-test campaign: plan and runbook

This file is written for an agent starting in a fresh session. Your job is to
guide the user through the experiments below one run at a time, check each
result before moving on, keep the run log, and at the end build the report.
The user deploys things and scales replicas; you give the exact command for
each run, read the output, and say what to do next.

Read `tests/load/README.md` first: it defines the harness, the columns, and the
caveats. This file adds the *what, in which order, and why*.

## 0. Current state (read this first; keep it current)

Last updated 2026-11-17 ~10:50, end of session 3. **A, B0, B50 complete; B1s
not accepted.** Deployed: fd limits (both gateways + compose nginx), V2 Redis
pool fix (`firstv2` @ `5ced577`). Mock is at **`ttft_ms 200`, `token_ms 5`**
(ConfigVersion 316, 1 replica). **Decisions (user, 2026-11-17):**
- **V1 stays as it is**: no more V1 tuning. V1 is the baseline as it exists
  today; the report shows what V2 changes.
- **Tunnels, several per gateway (not the public name)**: the shared CELS proxy in
  front of `dev-inference-api` must not be stressed. Users are spread over 4
  separate ssh processes per gateway with the harness's new `--ports` (0.8).
- **V2 high-concurrency profiling is deferred** to a follow-up session on branch
  `loadtest-profiling` (`tests/load/PROFILING.md`, H9–H12, `pool_bench.py`,
  commit `bc886d8`, local only, not pushed).
- **Finish only the runs that make sense** under the known limits: section 0.8.

**Fixed and deployed (verified 2026-11-16 ~20:00):**
1. nginx → gateway connection reuse (first A attempt: V2 502 / port exhaustion
   `(99: Cannot assign requested address)` at ~850 req/s). V2: `firstv2` @
   `e4c7933` ("Keepalive nginx->gunicorn"): `deploy/nginx.conf` upstream block
   with `resolve`, `keepalive 128`, `keepalive_timeout 60s`;
   `nginx.prod.conf.example` same pool; `Dockerfile` gunicorn `--keep-alive 75`.
   V1: user applied the equivalent on the Dev host nginx (upstream pool to
   127.0.0.1:7000, `Connection $connection_upgrade` in `proxy_params`,
   gunicorn `keepalive = 75`). After A-v2-health, confirm no 502 and no
   `(99: …)` in `docker logs first-nginx-1`.
2. V1 `/health` throttled (anonymous route, Django `AnonRateThrottle("10/s")`):
   `main` @ `b9341c6` ("Do not throttle health endpoint", `throttle=[]`).
   Verified: 40 rapid requests through the tunnel, 40 × 200.

**Client vantage (decided 2026-11-16):** the campaign client moves from the
laptop to another VM with lower latency to the Dev VM (still through SSH
tunnels). Session 1's laptop measurements (shakedown, Cloudflare runs) were
made on the lab network at ~5 ms tunnel RTT; from home it was ~55 ms, and A
was aborted for that reason. **Before A, measure and record the new host's
tunnel RTT** (snippet in 0.1, step 2); every campaign run must use the same
client host. Re-run the Cloudflare edge pair (A-v1prod-*/A-v1dev-*) from the
new host so the report's edge numbers share the vantage (light load only).

Campaign directory: `loadtest-runs/campaign-20261116/` (gitignored):
`RUNLOG.md` (environment, every run, accepted/rejected), `manifests/`
(`campaign.yaml`, `mock.yaml`, `restore-original.yaml`). Shakedown runs:
`loadtest-runs/shakedown-20261116/` (not campaign data).

### 0.8 Session 4 plan (multiple tunnels; known limits)

**Path.** As in sessions 2–3 (client VM → `ssh -L` → Dev nginx), but **4 separate
ssh processes per gateway**, opened by the user (each is its own TCP connection
and process; forwards added to the ControlMaster share one process and only
halve the drops, measured: `diag-20261117/tunnel-burst/`):

```bash
# V2 (compose nginx :8000, HTTP)     V1 (host nginx :443, TLS)
ssh -fN -o Compression=no -L 8001:localhost:8000 inferapi-dev
ssh -fN -o Compression=no -L 8011:localhost:8000 inferapi-dev
ssh -fN -o Compression=no -L 8012:localhost:8000 inferapi-dev
ssh -fN -o Compression=no -L 8013:localhost:8000 inferapi-dev
ssh -fN -o Compression=no -L 8002:localhost:443  inferapi-dev
ssh -fN -o Compression=no -L 8021:localhost:443  inferapi-dev
ssh -fN -o Compression=no -L 8022:localhost:443  inferapi-dev
ssh -fN -o Compression=no -L 8023:localhost:443  inferapi-dev
ssh -fN -L 9100:localhost:9100 inferapi-dev                      # first-admin
ssh -fN -o ControlMaster=yes -o ControlPath=~/.ssh/cm-inferapi-dev -o ControlPersist=12h inferapi-dev
```
```bash
V2=http://localhost:8001;  V2P=--ports=8001,8011,8012,8013
V1=https://localhost:8002; V1P=--ports=8002,8021,8022,8023      # V1 runs: --insecure
```
Every run passes its `--ports` (harness change, RUNLOG 2026-11-17 ~11:20): user *i*
of each worker process uses port *i* mod 4; the catalog poll stays on `--catalog-url
$V2`. Check each port before run 1 (`curl -si http://localhost:801{1,2,3}/health`,
`curl -sik https://localhost:802{1,2,3}/resource_server/health`), and record
client `nstat -az TcpExtListenOverflows` before and after each run (it should stay
near 0 below ~1000 users).

The public name (`dev-inference-api.cels.anl.gov` → shared CELS proxy
`nginx-internal`) is **not used** (user decision: shared, would throttle).

**Known limits (don't re-measure them, stay below):**
- V2: CPU per request grows with requests in flight from ~256 (httpcore pool
  scan; profiling deferred). Keep V2 at ≤ ~256 concurrent long requests, except
  the capped B1s run, which shows the onset.
- V1: bridge httpx defaults (100 connections/worker → ~400 in flight, 5 s pool
  timeout → 504s), `max_requests 3000` recycling. Measured as is.
- One `ssh -L` process drops connection bursts (backlog 128, and one process carries every channel): spread over 4 tunnels per gateway; still stay ≤ ~1000 users.

**Runs, in order** (all with `$V2P` / `$V1P`):

| # | Run IDs | Setup | Arguments | Why |
| --- | --- | --- | --- | --- |
| 1 | `A-v2-health-4t`, `A-v1-health-4t` | none | `--url $V2/health $V2P` / `$V1/resource_server/health $V1P --insecure`, `--users 1,4,16,64,256,1024` | does spreading over 4 tunnels raise the client ceiling (A-v2-health: 7.2k req/s, client CPU 86%)? |
| 2 | (dropped) | | | same path as B0/B50: no recalibration needed |
| 3 | `B1s-v2`, `B1s-v1` (capped) | mock `ttft_ms 200`, `token_ms 5` (current) | `--output-tokens 160 --users 32,64,128,256,512 --warmup 30 --ramp 25` | connection handling as it exists; V2 onset at ~256, V1's pool cap |
| 4 | `B1s-v2-stream`, `B1s-v1-stream` | same | as 3 with `--stream` | streaming relay at the same concurrency |
| 5 | `C-v2-1r`, `C-v1-1r` | gemma-4-E4B 1 replica; mock scaled to 0 | `--input-tokens 16 --output-tokens 2 --users 1,4,16,64,128,256` | real pilot path, model nearly idle |
| 6 | `D-v2-1r`, `-2r`, `-4r`, `D-v2-1r-repeat` | gpt-oss-20b 1 / 2 / 4 replicas | as section 4 D | replica scaling up to 128 users |
| 7 | `E-v1-1r`, `E-v1-4r` | right after the matching D run | as section 4 E | same replicas through V1 |
| 8 | `G-v2-stream-4r`, `G-v1-bridge-stream-4r` | 4 replicas (with D-4r / E-v1-4r) | as section 4 G | streaming under concurrency |
| 9 (optional) | F pair + G Sophia pair | user brings up V1 Globus endpoint + 1 V2 Sophia replica | as section 4 F / G | only if the user wants it |

**Dropped:** B0-direct (mock on a UDS), B1s ≥ 1024 users, C-2r/4r (the gateway
limits sit below what 4 tiny replicas can do; 16 GPUs), D-8r and D-8r-over (256+
in flight hits V2's pool limit), B50 rerun.

Before run 1: every port answers with `Server-Timing` (`queue`, `gateway`); replicas
as the run needs; only this agent session runs the harness (`ps`).

### 0.1 Session start checklist (sessions 1–3; tunnel commands superseded by 0.8)

1. The user opens two dedicated SSH tunnels on the client host (the VM the
   agent runs on; `inferapi-dev` must be an SSH alias there):
   ```bash
   ssh -N -o Compression=no -L 8001:localhost:8000 inferapi-dev   # V2: compose nginx, HTTP
   ssh -N -o Compression=no -L 8002:localhost:443  inferapi-dev   # V1: host nginx, TLS
   ```
   ```bash
   V2=http://localhost:8001
   V1=https://localhost:8002          # V1 routes keep their /resource_server prefix
   ```
   Every V1 run needs `--insecure` (cert is for `data-portal-dev`). Never use
   port 7000 for V1: that is uvicorn directly, bypassing nginx (no `queue`).
   The user's interactive tunnel (`ssh ... -L 8000 -L 9100 ... inferapi-dev`)
   stays separate; `first-admin` uses its 9100 forward.
2. Check headers: `curl -si $V2/health` and `curl -sik $V1/resource_server/health`
   both show `Server-Timing: queue;…, gateway;…`. Measure the tunnel RTT
   (record it in RUNLOG's environment table; it should be a few ms):
   ```bash
   .venv/bin/python -c 'import httpx,time,statistics as s
   for u,v in [("http://localhost:8001/health",True),("https://localhost:8002/resource_server/health",False)]:
       c=httpx.Client(verify=v); c.get(u); t=[]
       for _ in range(30): a=time.perf_counter(); c.get(u); t.append((time.perf_counter()-a)*1e3)
       print(u, "p50 %.1f ms" % s.median(t))'
   ```
3. Check replicas: `COLUMNS=250 uv run first-admin deployments ls | grep -E "Name|loadtest|gpt-oss-20b|gemma-4-E4B"`.
   Wait for a count with `first-admin deployments wait <name> --ready N`.
4. Token: `uv run alcf-tokens get-token inference` (`alcf-tokens` is not on PATH;
   the harness finds it itself, or set `FIRST_TOKEN`).

### 0.2 Environment as it stands

| Item | State |
| --- | --- |
| Client | `aurora-gpt-proto-vmw-02-dev.cels.anl.gov` (lab VM), 8 cores, 31 GB, `ulimit -n` 1048576; tunnel RTT p50 V2 1.8 / V1 4.2 ms; health ceiling 7.2k req/s (client+tunnel). Session 1 used the laptop. Tunnels: 3 separate ssh processes (8001, 8002, 9100). Read-only Dev VM SSH via the user's ControlMaster: `ssh -o ControlPath=~/.ssh/cm-inferapi-dev inferapi-dev '…'` |
| Dev VM (`inferapi-dev`) | 6 cores, 47 GB; hosts both gateways; read-only `ssh -o BatchMode=yes inferapi-dev '…'` works for inspection (no sudo) |
| V2 | compose stack `first-*`; nginx (compose, port 8000) `limit_req` commented out, upstream keepalive; 4 workers, `--keep-alive 75`; `firstv2` @ `e4c7933` |
| V1 | systemd `gateway_async` (gunicorn, 4 workers, `max_requests` 3000, keepalive 75), host nginx :443 → upstream keepalive pool → 127.0.0.1:7000; `api_limit` 15000 r/s per IP (effectively off); `RATE_LIMIT_PER_SEC_PER_USER=100000`; `/health` unthrottled; `main` @ `b9341c6` |
| Gateway → Tara | **both direct** over the conduit. V1 bridge: `FIRST_V2_PROXY_URL` unset. V2: `NO_PROXY` includes `10.124.128.0/21,10.124.160.0/21`. Direct TCP connect ≈ 1.3 ms |
| Gateway → Sophia | V2 via SOCKS tunnel container `first-tunnel` (`ssh -N -D` to the bastion); V1 via Globus Compute. Relevant only to F |
| V2 catalog | `manifests/campaign.yaml` + `manifests/mock.yaml` applied (plan shows no diff) |
| Replicas now | `tara/openai/gpt-oss-20b` 1, `tara/google/gemma-4-E4B-it` 1, `tara/loadtest/mock` 1 (ttft 0, token 0); Sophia deployments 0 |
| vLLM | Tara 0.29.0 (`system_fingerprint`); GPU type still TBD |

### 0.3 Catalog operations (V2)

- **Always `first-admin apply --no-prune <file>`**; the default prunes every
  resource missing from the file. `apply` prompts: `echo y | first-admin apply --no-prune …`.
  Check with `first-admin plan --no-prune <file>` ("No changes" after apply).
- Scale: `first-admin set-desired-replicas <deployment> <n>`. Autoscaling is
  off on all test deployments (`scaling_strategy` removed).
- Rate limits for the test: Model `usage_limits.default` `tpm: 0, rpm: 0`
  (0 = unlimited) and `max_user_concurrency: 4096` (default was 8).
- V1 needs no catalog work: its `tara/api/<model>` endpoints are written by the
  V1→V2 bridge from the V2 catalog (`tpm_user` = Model default `tpm` = 0), and
  appear only while the deployment has a healthy replica (404 otherwise; check
  `GET $V1/resource_server/list-endpoints`).
- After the campaign: `echo y | first-admin apply --no-prune manifests/restore-original.yaml`,
  scale `tara/loadtest/mock` to 0, and restore Dev nginx `limit_req` on V2.

### 0.4 Code state

- Dev V2 runs `firstv2` @ `5ced577` (blocking Redis pool, `redis_max_connections`
  512, `redis_pool_timeout` 5 s), on top of `75a4ccc` (fd limits:
  `compose.yaml` gateway `ulimits nofile 65536`, `deploy/nginx.main.conf`), `e4c7933`
  (nginx→gunicorn keepalive), `db83edf` (CIDR NO_PROXY, harness fix).
- Dev V1 runs `main` @ `b9341c6` plus host changes: nginx keepalive pool,
  systemd `LimitNOFILE=65536`. No further V1 changes (user decision).
- This file is untracked on `firstv2` in `/home/msalim/FIRST` (the working copy).
  An older committed copy is on `loadtest-profiling`; don't edit that one.
- Branch `loadtest-profiling` (worktree `/home/msalim/FIRST-loadtest-profiling`):
  profiling bundle + `PROFILING.md` H9–H12 + `tests/load/pool_bench.py`
  (commit `bc886d8`, not pushed).
- Harness changes must be noted in RUNLOG (section 8).

### 0.5 The mock on Tara

PilotDeployment `tara/loadtest/mock` (Model `loadtest/mock`, LaunchTemplate
`tara/loadtest-mock`, `manifests/mock.yaml`): `tests/load/mock_upstream.py`
copied to `/vast/draco/tara/projects/inference_service/deployment/artifacts/loadtest-mock/`,
run by `/vast/draco/tara/projects/inference_service/deployment/envs/first-pilot-9dd645311082-py312/bin/python -I -m uvicorn … --uds <runtime.uds> --workers 8`.
1 GPU slot (the pilot requires ≥ 1). It sits behind the pilot nginx (mTLS)
exactly like a model, so B measures gateway + pilot hop with zero model time,
and C − B is vLLM.
- Routes: V2 `$V2/federated/v1/chat/completions`, V1
  `$V1/resource_server/tara/api/v1/chat/completions`, both `--model loadtest/mock`.
- Switch blocks B0 / B50 / B1s: edit `ttft_ms` / `token_ms` under the
  PilotDeployment's `launch_spec.parameters` in `mock.yaml`, apply, then scale
  0 → 1 (`deployments wait … --drained`, then `--ready 1`) so the replica
  restarts with the new values. Record the values in RUNLOG for each block.
- If the mock file changes, re-copy it to Tara (the user does this).

### 0.6 Changes to the plan (sections 3–6)

- **Client paths**: the laptop through the two tunnels above, for every
  experiment. Each tunnel is one SSH session (one TCP stream), so A measures
  each tunnel's ceiling; any later peak near it is flagged as client/tunnel
  limited. Residual asymmetry: V1's nginx terminates TLS, V2's does not
  (inside `network`, not `gateway`).
- **A**: run `A-v2-health`, `A-v1-health` (add `--insecure`),
  `A-v2-health-noka`. `A-v2-health-edge` is replaced by the Prod V1 Cloudflare
  runs already accepted (RUNLOG). Also run `A-v1-health-noka` for symmetry.
- **B gate "upstream stays flat" replaced (2026-11-17)**: `upstream` is wall
  clock inside the gateway's event loop, so once workers are CPU-pegged it
  grows on both gateways even though the mock is idle. Judge saturation from
  req/s plus a Dev VM CPU sample (`top` / `docker stats` over the
  ControlMaster during a short diag step), and the mock from low-load
  `upstream` (~1.8 ms).
- **V1 logs**: gunicorn log `/var/log/inference-service/out.log` (JSON, UTC) is
  readable by the agent; host nginx `error.log` needs the user (sudo).
- **B0-direct is dropped**: the mock listens on a Unix socket on a Tara node
  and cannot be driven directly. Use instead: the mock's `upstream` must stay
  flat as users grow (gate), and 8 uvicorn workers.
- **C at 4 replicas costs 16 GPUs** (gemma-4-E4B uses `gpus_per_node: 4`).
  Confirm with the user before `C-*-4r`.
- **Streams**: V1 returns its streaming response before calling the backend,
  so V1 stream rows have `gateway` = time-to-open and no `upstream`. Compare
  streams on `ttft`, `e2e`, `max gap`, errors; never `overhead`.
- **`--catalog-url $V2`** works for V1 runs too (same replicas); confirmed.

### 0.7 Findings so far (for the report)

- **Cloudflare edge** (Prod V1 vs Dev V1, laptop, 1–2 users): +15–35 ms p50,
  +20–100 ms p95 per request. Goes in Setup / Limits.
- **SOCKS tunnel latency**: when V2 reached Tara through `ssh -N -D`, a 0 ms
  mock showed `upstream` p50 44 / 15 / 7 ms at 1 / 2 / 4 users; direct, it is
  1.9 ms at every load (V1: 2 ms). ~40 ms at low load is the Nagle /
  delayed-ACK signature (OpenSSH sets TCP_NODELAY only for interactive
  sessions). Relevant to Prod and to Sophia (F), which still use the tunnel.
- **Shakedown numbers** (not campaign data; 10 s steps): mock, 1 user, unary,
  direct path: V2 `overhead` p50 3.5 ms, V1 8.0 ms; at 64 users V2 540 req/s,
  V1 263 req/s, no errors on either.
- **A (2026-11-17)**: client+tunnel ceiling 7.2k req/s (V2 health, client
  CPU 86%; V2 never hit its own limit). V1 health levels off at ~950 req/s,
  server-side. At 1 user, `gateway` is V2 0.1 ms vs V1 1.1 ms. With no keepalive,
  V2 adds ~3 ms per new TCP connection and caps at ~1.6k req/s. V1 adds a
  ~15–20 ms TLS handshake (measured on the Dev VM).
- **Tunnel stall on new TLS connections**: through the ssh `-L` forward, a
  fresh V1 connection waits ~40 ms after the TLS handshake (none on the Dev VM
  itself). Same Nagle/delayed-ACK cause as the SOCKS finding. It affects only
  no-keepalive runs; B–F all reuse connections.
- **Cloudflare edge, rerun from the lab VM**: +8–35 ms p50, +13–38 ms p95 per
  request (supersedes the laptop numbers for the report).
- **B0 (2026-11-17)**: zero-delay mock, 1 replica. V2 peak 756 req/s, V1 290
  (2.6×), both at 32 users, both CPU-bound (4 workers each). 1-user overhead p50
  V2 3.1 ms, V1 7.0 ms. V2 had no errors to 1024 users. V1 errors from 64 users
  (10.5% at 1024) come from `max_requests 3000` recycling: each worker exit resets
  the connections it holds; one worker also hung (WORKER TIMEOUT, SIGABRT).
- **B50 (2026-11-17)**: 50 ms mock. V2 peak 556 req/s, V1 246; 1-user
  overhead p50 V2 5.1 ms, V1 9.1 ms. V2 had no errors to 2048 users; V1 had
  recycling 502s from 32 users, and 32.9% errors at 2048 from fd exhaustion
  (soft NOFILE 1024 on both gateways' workers).
- **B1s (2026-11-17, not accepted)**: at ~1000 in flight, V2's 4 workers are
  CPU-pegged at ~65 req/s (B0: 756). Cause: httpcore's
  `_assign_requests_to_connections` walks every connection × every queued request
  (71% of CPU in a local profile). V1's bridge client uses httpx defaults (100
  connections/worker, 5 s pool timeout); its 504s are pool timeouts. Also, the
  client `ssh -L` backlog of 128 drops connection bursts at ≥ 2048 users.
- **Measurement fixes**: harness counted only `content`/`reasoning_content`
  as a stream's first token; vLLM 0.29 sends `reasoning` for gpt-oss. Fixed
  before any campaign stream run.

## 1. What the campaign must show

| # | Claim to test | Experiment |
| --- | --- | --- |
| C1 | The client and network path are not the bottleneck | A (health baseline) |
| C2 | V2 adds less gateway overhead per request than V1 | B (mock), C (tiny model) |
| C3 | V2 sustains a higher request rate before saturating | B, C |
| C4 | V2 throughput grows in proportion to replicas | D (realistic, 1/2/4/8 replicas) |
| C5 | V1 and V2 differ on the *same* replicas only by gateway cost | E |
| C6 | V1 through Globus Compute is slower in kind, not degree | F |
| C7 | Many concurrent streams all finish, with steady token flow | G |

Do not presume the outcome. If a number contradicts a claim, record it and say
so; the report states what was measured.

## 2. State of the code (verify before anything else)

At the time of writing, nothing below was committed or deployed:

- `firstv2` working tree: `Server-Timing` in
  `packages/gateway/first_gateway/apiserver/{log_middleware,context,inference}.py`,
  nginx stamp in `deploy/nginx.conf` and `deploy/nginx.prod.conf.example`,
  the harness in `tests/load/`, tests in `tests/test_load_harness.py`.
- V1: worktree `../inference-gateway-v1-loadtest`, branch
  `loadtest-server-timing` off `main`: `resource_server_async/server_timing.py`,
  one line in `inference_gateway/settings.py` (MIDDLEWARE), a timer in
  `resource_server_async/services.py`, nginx stamp in `deploy/nginx/proxy_params`
  and `deploy/docker/nginx.conf`.

*(Historical. Both are now merged/deployed and the shakedown is done; see
section 0.)*

## 3. Fixed choices

These are decided; change one only if the user asks or a shakedown run shows it
cannot work, and record the change in the run log.

### Environment

- **Gateways**: V1 and V2 both on the Dev VM. Only one is driven at a time; the
  other stays up but idle.
- **Worker processes**: V2 runs 4 gunicorn/uvicorn workers (`deploy/Dockerfile`),
  V1 runs 5 (`deploy/gateway_asgi.config.py`). Ask the user to run both at **4**
  for the campaign. If that is not done, the report must state the counts next
  to every comparison. V1's `max_requests = 3000` (workers recycle) stays as
  deployed: it is part of what V1 is.
- **Client host**: a machine on the lab network that is *not* the Dev VM, with
  at least 8 cores, hitting the Dev VM's nginx directly (no Cloudflare). This is
  the primary vantage point for every experiment. Experiment A is repeated once
  through the public Cloudflare hostname to size the edge.
- **Mock upstream host**: not the Dev VM (it would compete with the gateway for
  CPU). A login/service node the Dev VM can reach, 4 workers.
- If either "not the Dev VM" constraint cannot be met, say so in the run log
  and the report; the gateway ceiling in B is then a lower bound.

### Models

| Role | Model | Why |
| --- | --- | --- |
| No-work | `loadtest/mock` (mock upstream) | Isolates the gateway completely. Manifest and V1 fixture are in the README. |
| Tiny | `google/gemma-4-E4B-it` on Tara, ~16 tokens in, 2 out | Smallest chat model already in the Dev catalog with a Tara recipe (`tara/google/gemma-4-E4B-it`, `max_backend_concurrency: 64`). With 2 output tokens the model does almost nothing, but the request crosses the real pilot path (gateway → pilot nginx, mTLS → vLLM), which the mock does not. |
| Realistic | `openai/gpt-oss-20b` on Tara, ~1024 tokens in, 256 out | 1 GPU per replica (`tara/openai/gpt-oss-20b`), so 8 replicas cost 8 GPUs. Reachable from V1 over the bridge and, on Sophia, over Globus Compute. |

A sub-1B model would be an even emptier "tiny" model, but it needs a new Tara
recipe and bundle; the mock already covers the zero-work case, so it is not
worth it unless the user wants it.

### Routes

| Target | URL |
| --- | --- |
| V2 chat | `$V2/federated/v1/chat/completions` |
| V1 chat, same Tara replicas (bridge maps `tara/` → cluster `tara`, framework `api`) | `$V1/resource_server/tara/api/v1/chat/completions` |
| V1 chat, Globus Compute on Sophia | `$V1/resource_server/sophia/vllm/v1/chat/completions` |
| V1 mock | `$V1/resource_server/<cluster>/<framework>/v1/chat/completions` of the `DirectAPIEndpoint` the user registers |
| Health | `$V2/health`, `$V1/resource_server/health` |

Confirm the V1 paths with `GET $V1/resource_server/list-endpoints` before the
first V1 run; the bridge mapping is configurable.

### Manifest changes the user applies for the campaign (V2 Dev)

- `loadtest/mock` Model + StaticDeployment (README).
- `tara/google/gemma-4-E4B-it`: `max_replicas: 4`, `scaling_strategy: null`,
  `max_backend_concurrency: 256`.
- `tara/openai/gpt-oss-20b`: `max_replicas: 8`, `scaling_strategy: null`
  (manual scaling), `max_backend_concurrency: 32`.
- `sophia/openai/gpt-oss-20b` and `sophia/google/gemma-4-E4B-it`: 0 replicas
  except during experiment F, so federated routing stays on Tara and the
  replica count the harness reads is Tara's.
- On all three test models, for the test identity: `tpm: 0`, `rpm: 0`,
  `max_user_concurrency: 4096`.
- Scale with `first-admin set-desired-replicas`; wait until the catalog shows
  the replicas `ready` before starting a run.

V1: raise `RATE_LIMIT_PER_SEC_PER_USER` and the test endpoints' `tpm_user` /
`tpm_model`; exempt the test identity in both nginx `limit_req` zones.

### Run parameters

| Kind | `--warmup` | `--duration` | Notes |
| --- | --- | --- | --- |
| Health, mock, tiny | 10 | 30 | fast requests; 30s gives >1000 samples per step even at 1 user |
| Realistic | 30 | 120 | a request takes seconds; fewer samples per second |
| Streaming | 30 | 120 | |

- `--processes 8` everywhere.
- Always pass `--catalog-url $V2` for chat runs (for V1 too: same replicas),
  with `--catalog-model` when the V1 model name differs.
- All runs of the campaign go under one directory:
  `--out loadtest-runs/campaign-<YYYYMMDD>`.
- `--name` is exactly the run ID from the tables below.

## 4. Experiments

Run IDs are `<exp>-<target>[-<variant>]`. Run them in the order listed. Where a
V1 and a V2 run are paired, run them back to back.

### A. Health baseline (C1) — about 10 min

No inference, no database. Establishes the ceiling of the client and the path,
and the `network` floor.

| Run ID | Command arguments |
| --- | --- |
| `A-v2-health` | `--url $V2/health --users 1,4,16,64,256,1024` |
| `A-v1-health` | `--url $V1/resource_server/health --users 1,4,16,64,256,1024` |
| `A-v2-health-edge` | as `A-v2-health`, from outside through Cloudflare, `--users 1,4,16,64` |
| `A-v2-health-noka` | as `A-v2-health` with `--no-keepalive --users 1,4,16,64` (prices the TLS handshake) |

Gate: client CPU below 85% at the peak step. If not, raise `--processes` or use
a bigger client host, and rerun A before going on. The peak req/s here is the
most the client can offer; any later experiment whose peak approaches it is
client-limited and must be flagged.

### B. Mock upstream: gateway overhead and ceiling (C2, C3) — about 40 min

The mock is started three ways. Restart it between blocks.

| Block | Mock settings | Purpose |
| --- | --- | --- |
| B0 | `--ttft-ms 0 --token-ms 0` | Pure overhead and the req/s ceiling |
| B50 | `--ttft-ms 50 --token-ms 0` | Overhead when requests overlap (50 ms upstream) |
| B1s | `--ttft-ms 200 --token-ms 5`, 160 output tokens (1 s) | High concurrency at low req/s: connection handling |

| Run ID | Command arguments |
| --- | --- |
| `B0-direct` | `--url http://<mock>:9000/v1/chat/completions --model mock --no-auth --users 1,16,256,1024` (the mock's own ceiling; no `--catalog-url`) |
| `B0-v2`, `B0-v1` | `--model loadtest/mock --output-tokens 8 --users 1,2,4,8,16,32,64,128,256,512,1024` |
| `B50-v2`, `B50-v1` | same, `--users 1,8,32,128,512,1024,2048` |
| `B1s-v2`, `B1s-v1` | `--output-tokens 160 --users 64,256,1024,2048,4096` |
| `B1s-v2-stream`, `B1s-v1-stream` | as `B1s-*` with `--stream` |

Gates: `B0-direct` peak must be several times `B0-v2` peak, or the mock is the
limit (add workers). In `B0-*`, `upstream` must stay flat as users grow while
`overhead` and `queue` rise: that is what "the gateway saturated" looks like.
Expect V1 to stop at some user count (errors or timeouts); the sweep ends
itself above 50% errors. That end point is a result, not a failure of the run.

Raise the client's file-descriptor limit (`ulimit -n 16384`) before the 2048+
steps.

### C. Tiny model through the real pilot path (C2, C3) — about 40 min

`google/gemma-4-E4B-it` on Tara, `--input-tokens 16 --output-tokens 2`.

| Run ID | Replicas | Command arguments |
| --- | --- | --- |
| `C-v2-1r`, `C-v1-1r` | 1 | `--users 1,4,16,64,128,256` |
| `C-v2-2r` | 2 | same |
| `C-v2-4r`, `C-v1-4r` | 4 | `--users 1,4,16,64,128,256,512` |

Reading: if peak req/s still doubles from 2 to 4 replicas, vLLM is the limit
and the gateway ceiling is "at least" the 4-replica peak; B0 gives the actual
ceiling. Compare `overhead` here with B: the difference is the cost of the
pilot path's mTLS hop.

### D. Realistic model: replica scaling on V2 (C4) — about 2 h plus scaling waits

`openai/gpt-oss-20b` on Tara, `--input-tokens 1024 --output-tokens 256
--warmup 30 --duration 120`. The user sets the replica count by hand before
each run. Users are multiples of the replica count, so each run covers the same
per-replica concurrency: 4, 8, 16, 24, 32.

| Run ID | Replicas | `--users` |
| --- | --- | --- |
| `D-v2-1r` | 1 | `4,8,16,24,32` |
| `D-v2-2r` | 2 | `8,16,32,48,64` |
| `D-v2-4r` | 4 | `16,32,64,96,128` |
| `D-v2-8r` | 8 | `32,64,128,192,256` |
| `D-v2-8r-over` | 8 | `320,384` (above `replicas × max_backend_concurrency`) |
| `D-v2-1r-repeat` | 1 | as `D-v2-1r`, run last, to bound run-to-run variation |

Gates per run: replica column shows one number (no ⚠); `out_tokens` in
`summary.json` is 256 at every percentile (if not, output pinning did not reach
vLLM: stop and investigate); `retried` is 0 or small.

`D-v2-8r-over` shows what admission does past capacity: expect rejections
(429/503 listed under errors) with the served requests' latency intact. Report
it as admission behaviour, separately from the scaling figure.

Scaling is judged two ways: peak req/s per replica, and req/s at equal
per-replica concurrency (same column position across the four runs).

### E. Same replicas through V1 (C5) — about 40 min

Same model, tokens and timing as D, through V1's bridge to the same Tara
replicas. Run each right after the D run with the same replica count if
practical; otherwise rescale.

| Run ID | Replicas | `--users` |
| --- | --- | --- |
| `E-v1-1r` | 1 | `4,8,16,24,32` |
| `E-v1-4r` | 4 | `16,32,64,96,128` |

Add `--catalog-url $V2 --catalog-model openai/gpt-oss-20b`. Note for the
report: V1 picks a replica at random and has no per-backend concurrency limit,
so differences here include load balancing as well as overhead.

### F. V1 through Globus Compute (C6) — about 30 min

Sophia, `openai/gpt-oss-20b`, V1's `GlobusComputeEndpoint` versus V2's pilot
deployment on the same cluster. The user brings up the V1 Globus endpoint
(model hot) and, separately, one V2 replica of `sophia/openai/gpt-oss-20b`.
Keep Tara's replicas at 0 for the V2 run or use the deployment-pinned route.

| Run ID | Command arguments |
| --- | --- |
| `F-v1-globus-tiny` | `--input-tokens 16 --output-tokens 2 --users 1,4,16` (transport cost with the model nearly idle) |
| `F-v2-sophia-tiny` | same against V2 |
| `F-v1-globus-real` | `--input-tokens 1024 --output-tokens 256 --users 1,4,16 --warmup 30 --duration 120` |
| `F-v2-sophia-real` | same against V2 |

This is deliberately brief. For V1 Globus, `upstream` is the whole Globus round
trip, so compare `e2e` and `upstream`, not `overhead`.

### G. Streaming under concurrency (C7) — about 1 h

`openai/gpt-oss-20b`, `--stream --input-tokens 1024 --output-tokens 256
--warmup 30 --duration 120`.

| Run ID | Setup | `--users` |
| --- | --- | --- |
| `G-v2-stream-4r` | Tara, 4 replicas | `16,64,128` |
| `G-v1-bridge-stream-4r` | same replicas via V1 | `16,64,128` |
| `G-v2-sophia-stream` | Sophia, 1 V2 replica | `4,16,32` |
| `G-v1-globus-stream` | Sophia via Globus Compute | `4,16,32,64` |

Plus the mock streaming runs `B1s-*-stream` (thousands of streams, no GPU).

What to read: error rate and causes (`stream_incomplete`, `stream_error_event`,
`ReadTimeout`), `ttft`, `max gap`, and whether `e2e` stays near the unary value
from D. V1 over Globus is expected to degrade; record at which user count and
how (errors vs stalls).

## 5. Running a step with the user

For each run:

1. State the preconditions (which gateway, model, replica count, mock settings)
   and ask the user to confirm they hold.
2. Give the full command. Template:

   ```bash
   uv run python -m tests.load run --name <RUN_ID> --out loadtest-runs/campaign-<DATE> \
       --processes 8 --url <URL> --model <MODEL> --catalog-url $V2 <arguments>
   ```

3. When the user reports it finished, read `summary.md` and `summary.json` in
   the run directory and check the gates:
   - `gateway` and `queue` columns are populated (else `Server-Timing` or the
     nginx stamp is missing on that gateway);
   - client CPU below 85%;
   - replica count constant and as intended;
   - errors: none below the saturation point, and their causes make sense;
   - `out_tokens` equals the requested output length.
4. Append to `loadtest-runs/campaign-<DATE>/RUNLOG.md`: run ID, directory,
   start time, preconditions as confirmed, gate results, anything odd, and
   whether the run is **accepted** or must be repeated. A rerun keeps the same
   `--name`; the log says which directory is the accepted one.
5. Give a two-line reading of the result and name the next run.

Once at the start, record in `RUNLOG.md`: Dev VM cores and memory, V1 and V2
worker counts, the deployed commit of each, client host and its cores, mock
host, nginx/Cloudflare path used, vLLM version and GPU type per cluster.

## 6. Shakedown (before A)

Ten minutes that save hours:

1. `curl -si $V2/health` and `curl -si $V1/resource_server/health`: both must
   show `Server-Timing` with `queue` and `gateway`.
2. One chat request to each gateway by hand: `upstream` present with
   `attempts=1`.
3. A two-step run on each target (`--users 1,4 --duration 10 --warmup 3`) for
   mock, tiny and realistic, unary and `--stream`, on V1 and V2. Check that
   `out_tokens` matches. If V1 or vLLM rejects `min_tokens` / `ignore_eos`,
   decide with the user between fixing the pass-through and `--no-pin-output`
   (then output length varies and D/E need longer steps).
4. Confirm the rate-limit exemptions: a 64-user, 10-second mock run must show
   no 429s on either gateway.

## 7. The report

Build it only from accepted runs. Deliverable: one self-contained HTML page
(publish it as an artifact; load the `artifact-design` and `dataviz` skills
first), plus the data it was built from.

Data sources: `steps.csv` and `summary.json` for per-step numbers,
`requests.jsonl` for distributions and time series,
`python -m tests.load report <dirs>` for the peak/scaling tables.

Structure:

1. **Summary**: the seven claims, each with its verdict and the one number that
   decides it.
2. **Setup**: environment table from `RUNLOG.md`; the latency decomposition
   (client → Cloudflare → nginx → `queue` → gateway `overhead` → `upstream`) as
   a diagram; method and caveats (closed loop, streams measured to first byte,
   per-request subtraction).
3. **Results**, one subsection per experiment.
4. **Limits of the study**: single VM, one client vantage point, anything from
   `RUNLOG.md` that weakens a conclusion.
5. **Appendix**: every run with its command and full step table.

Figures (V1 and V2 keep the same two colours throughout):

| # | Figure | Data |
| --- | --- | --- |
| 1 | Health: req/s vs users, V1 and V2, with the client ceiling marked | A |
| 2 | Mock: req/s vs users (log x), V1 and V2, saturation points annotated | B0 |
| 3 | Mock: where the time goes — stacked `network` / `queue` / `overhead` / `upstream` p50 vs users, one panel per gateway | B0, B50 |
| 4 | Gateway `overhead` p50 / p95 / p99 vs users, V1 and V2 | B0, B50, C |
| 5 | Overhead distribution at a matched load (ECDF, V1 vs V2) | B50 `requests.jsonl` |
| 6 | Tiny model: req/s vs users at 1, 2, 4 replicas | C |
| 7 | Realistic: req/s vs users, one curve per replica count | D |
| 8 | Scaling: peak req/s vs replicas against the ideal line, with efficiency labels | D |
| 9 | Realistic: e2e p50 / p95 vs per-replica concurrency, one curve per replica count | D |
| 10 | Same replicas: V1 vs V2 req/s and e2e p95 vs users | D, E |
| 11 | Globus: e2e p50 / p95 at 1, 4, 16 users, V1-Globus vs V2, tiny and realistic | F |
| 12 | Streams: completion rate and `ttft` p50 / p95 / p99 vs concurrent streams, four setups | G, B1s |
| 13 | Streams: `max gap` distribution at the highest shared load | G `requests.jsonl` |
| 14 | Stability: req/s and p95 in 5 s buckets across one long step (shows V1 worker recycling if present) | D/E `requests.jsonl` |

Tables: environment; peak throughput per run (`report` output); overhead
percentiles V1 vs V2 at matched loads; replica scaling efficiency; errors by
cause for every run that had any; streaming completion.

State every comparison with its conditions (users, replicas, worker counts) and
give absolute numbers next to any ratio.

## 8. If the harness needs changing

Expect small fixes during shakedown. Keep them in `tests/load/`, keep
`tests/test_load_harness.py` passing (`make test` needs `make db-up`), and note
in `RUNLOG.md` which runs were made before and after a change that affects
measurement. A change to how a number is measured invalidates the earlier runs
of that experiment.
