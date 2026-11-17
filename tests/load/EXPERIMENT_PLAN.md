# V1 vs V2 load-test campaign: plan and runbook

Written for an agent starting in a fresh session. The agent drives the harness,
checks each run, keeps the run log, and builds the reports; the user deploys,
patches code and changes Tara/Dev VM state. Read `tests/load/README.md` for the
harness, its columns and caveats; this file says *what to run next, how, and why*.

**Resume here:** read section 0, then the tail of
`loadtest-runs/campaign-20261116/RUNLOG.md`. Section 0 is the only "current
state"; sections 1–6 are findings, playbook and reference.

## 0. Status (keep current)

Last updated 2026-11-17 ~13:30 (end of session 4).

| Phase | What | State |
| --- | --- | --- |
| P0 | Pre-optimization suite: A, B0, B50, B1s, B1s-stream (mock); C/D/E/G at 1 replica | **done** (RUNLOG, sessions 2–4) |
| P1 | Profile V2 hot spots on the mock (section 2.1) | **next**; needs the profiling build on Dev |
| P2 | Pre-optimization report: P0 results + P1 hot spots (section 2.2) | after P1 |
| P3 | V1-only runs while the user fixes V2 (section 2.3) | after P2 |
| P4 | Post-optimization V2 suite, full (section 2.4) | after the user deploys the fixes |
| P5 | Final report, V1 vs V2-pre vs V2-post (section 2.5) | last |

**Decisions (user) that hold for the whole campaign**
- **V1 stays exactly as deployed** (`main` @ `b9341c6` + host nginx keepalive pool,
  `LimitNOFILE=65536`). It is the baseline; never propose V1 tuning or fixes.
  V1 numbers from P0/P3 are reused in the final report.
- **Path: SSH tunnels, 4 separate ssh processes per gateway, `--ports` on every
  run.** Never send load to the public `dev-inference-api.cels.anl.gov` (shared
  CELS proxy `nginx-internal`). There is no `/v2-test/` route.
- **Same client host for every run**: lab VM `aurora-gpt-proto-vmw-02-dev` (8 cores).
- Stay ≤ ~1000 concurrent users through the tunnels (section 0.4).
- **One harness session at a time.** Before each run check `ps` for other harness
  processes / Claude sessions; an unknown run directory in `loadtest-runs/` means
  stop and ask the user.
- Always `first-admin apply --no-prune`.
- Replica changes cost GPUs (C at 4 replicas = 16 GPUs): confirm with the user.

### 0.1 Where things are

| Item | Location |
| --- | --- |
| This plan | `tests/load/EXPERIMENT_PLAN.md`, untracked on `firstv2` in `/home/msalim/FIRST`. Previous version: `loadtest-runs/campaign-20261116/EXPERIMENT_PLAN-through-session4.md` |
| Run log (every run, accepted or not) | `loadtest-runs/campaign-20261116/RUNLOG.md` (gitignored) |
| Campaign runs | `loadtest-runs/campaign-20261116/<RUN>-<timestamp>/` (`summary.md`, `summary.json`, `steps.csv`, `users-*/requests.jsonl`) |
| Catalog manifests | `loadtest-runs/campaign-20261116/manifests/` (`campaign.yaml`, `mock.yaml`, `restore-original.yaml`) |
| Diagnostics (CPU samples, benches) | `loadtest-runs/diag-20261117/` |
| Harness | `tests/load/` on `firstv2` (`--ports` committed in `c428cd5`) |
| Profiling bundle | branch `loadtest-profiling`, pushed, rebased on `c428cd5` (`f36f1d2` bundle, `c4a29ef` pool hypotheses); worktree `/home/msalim/FIRST-loadtest-profiling`: `tests/load/PROFILING.md` (H1–H12), `pool_bench.py`, `profile.sh`, `deploy/compose.profile.yaml` |
| V2 deploy on Dev | `/home/webportal/FIRSTv2/deploy`; compose files `compose.yaml`, `compose.dev.yaml`, `compose.tunnel.yaml`, `compose.dev-analytics.yaml`; containers `first-inference-gateway-1`, `first-nginx-1` |
| V1 on Dev | systemd `gateway_async`; log `/var/log/inference-service/out.log` (JSON, UTC, agent-readable); host nginx `error.log` needs sudo (user) |

### 0.2 Session start checklist

1. The user opens the tunnels on the client host (each line is its own process):
   ```bash
   for p in 8001 8011 8012 8013; do ssh -fN -o Compression=no -L $p:localhost:8000 inferapi-dev; done  # V2 compose nginx, HTTP
   for p in 8002 8021 8022 8023; do ssh -fN -o Compression=no -L $p:localhost:443  inferapi-dev; done  # V1 host nginx, TLS
   ssh -fN -L 9100:localhost:9100 inferapi-dev                                                          # first-admin
   ssh -fN -o ControlMaster=yes -o ControlPath=~/.ssh/cm-inferapi-dev -o ControlPersist=12h inferapi-dev # agent's SSH
   ```
   ```bash
   V2=http://localhost:8001;  V2P=--ports=8001,8011,8012,8013
   V1=https://localhost:8002; V1P=--ports=8002,8021,8022,8023      # V1 runs add --insecure
   ```
2. Every port answers with `Server-Timing: queue;…, gateway;…`:
   `for p in 8001 8011 8012 8013; do curl -si http://localhost:$p/health | grep -i server-timing; done`;
   `for p in 8002 8021 8022 8023; do curl -sik https://localhost:$p/resource_server/health | grep -i server-timing; done`.
3. `ssh -o ControlPath=~/.ssh/cm-inferapi-dev -O check inferapi-dev`.
4. Replicas: `COLUMNS=250 uv run first-admin deployments ls | grep -E "Name|loadtest|gpt-oss-20b|gemma-4-E4B"`.
5. Token: the harness gets one itself (`uv run alcf-tokens get-token inference`); if
   that fails, ask the user to refresh their login.
6. `ps -eo pid,etime,args | grep -E 'tests\.load|claude'`: nothing but this session.
7. Record client `nstat -az TcpExtListenOverflows` before and after each run (it
   stayed at +0 for every session-4 run).

Run template (`--processes 8` always; one output directory):
```bash
uv run python -m tests.load run --name <RUN_ID> --out loadtest-runs/campaign-20261116 \
    --processes 8 --url <URL> $V2P|$V1P [--insecure] --model <MODEL> --catalog-url $V2 <args>
```
For CPU evidence, sample the gateway every ~20 s over the ControlMaster during a run:
`docker stats --no-stream` (V2 container) and `ps -C gunicorn -o pid,pcpu,etime,args`
(V1 workers; a short `etime` is a recycled worker).

### 0.3 Environment

| Item | State |
| --- | --- |
| Client | lab VM `aurora-gpt-proto-vmw-02-dev`, 8 cores, 31 GB, `ulimit -n` 1048576. Tunnel RTT p50 V2 1.8 / V1 4.2 ms. Client+tunnel health ceiling ~6.7–7.2k req/s (8 harness processes at ~80% CPU) |
| Dev VM `inferapi-dev` | 6 cores, 47 GB; hosts both gateways (drive one at a time) |
| V2 | compose stack `first-*`; compose nginx :8000 (`limit_req` off, upstream keepalive 128); gunicorn 4 uvicorn workers, `--keep-alive 75`, fd 65536; `firstv2` @ `5ced577` (Redis `BlockingConnectionPool`, 512/worker, 5 s wait) |
| V1 | gunicorn 4 workers, `max_requests` 3000, keepalive 75; host nginx :443 → keepalive pool → 127.0.0.1:7000; `/health` unthrottled; `RATE_LIMIT_PER_SEC_PER_USER=100000`; bridge httpx client with default limits (100 connections/worker, 5 s pool timeout) |
| Gateway → Tara | both direct over the conduit (V2 `NO_PROXY` has the Tara CIDRs; V1 `FIRST_V2_PROXY_URL` unset), TCP connect ~1.3 ms |
| Gateway → Sophia | V2 through SOCKS container `first-tunnel`; V1 through Globus Compute (only F) |
| Catalog | `manifests/campaign.yaml` + `manifests/mock.yaml` applied; test models `tpm 0, rpm 0, max_user_concurrency 4096`; autoscaling off |
| Replicas (end of session 4) | Tara: `loadtest/mock` 1 (**ttft 200, token 5**), `gemma-4-E4B-it` 1, `gpt-oss-20b` 1; Sophia 0 |
| vLLM | Tara 0.29.0 |

### 0.4 Rules for reading numbers

- **Tunnels**: one `ssh -L` listener has a hard-coded backlog of 128 and one process
  carries every channel, so bursts of new connections are dropped (`ListenOverflows`),
  giving client `ConnectTimeout`s at ≥ 2048 users. 4 processes per gateway gave +0
  overflows up to 512 users; stay ≤ ~1000 users.
- **`upstream` is wall clock inside the gateway's event loop**: when workers are
  CPU-bound it grows although the backend is idle. Judge saturation by req/s plus a
  gateway CPU sample, never by `upstream` alone. The mock itself is ~1.8 ms at low load.
- **Streams**: V1 returns its response before calling the backend, so V1 stream rows
  have `gateway` = time to open and no `upstream`; V2 stream `gateway`/`upstream` are
  time to first byte. Compare streams on `ttft`, `e2e`, `max gap`, errors.
- **V1 502s under load are fast and bursty** (5–15 ms): requests caught on a worker
  recycled by `max_requests 3000`. The count is timing-dependent (±1% between runs).
  V1 504s at ~6 s are its bridge pool timeout (≥ ~400 in flight).
- Both gateways run 4 workers: comparisons are like for like.
- A harness change to how a number is measured invalidates earlier runs of that
  experiment; note harness changes in RUNLOG.

### 0.5 Catalog and mock operations

- `echo y | uv run first-admin apply --no-prune <file>`; check with
  `uv run first-admin plan --no-prune <file>` ("No changes").
- Scale: `uv run first-admin set-desired-replicas <deployment> <n>`; wait with
  `uv run first-admin deployments wait <deployment> --ready N` (or `--drained`).
- **Mock settings** (`tara/loadtest/mock`, `manifests/mock.yaml`,
  `launch_spec.parameters.ttft_ms` / `token_ms`): edit, apply, then scale 0
  (`--drained`) → 1 (`--ready 1`) so the replica restarts with the new values; record
  them in RUNLOG. Blocks: B0 = 0/0, B50 = 50/0, B1s = 200/5. The mock runs 8 uvicorn
  workers on a Tara node behind the pilot nginx (mTLS), exactly like a model; if
  `tests/load/mock_upstream.py` changes, the user re-copies it to Tara.
- V1 needs no catalog work: the bridge writes its `tara/api/<model>` endpoints from
  the V2 catalog while the deployment has a healthy replica.
- After the campaign: `echo y | uv run first-admin apply --no-prune manifests/restore-original.yaml`,
  scale `tara/loadtest/mock` to 0, restore V2 nginx `limit_req` (user).

## 1. Findings so far (for the reports)

From accepted runs in RUNLOG. "Ideal" for B1s = 1 req/s per user (1 s mock).

### Path and client (A)
- Client+tunnel ceiling ~7k req/s (V2 health: 7.2k on 1 tunnel, 6.7k on 4; client
  CPU ~80% on 8 cores). V2 never reached its own health limit. **V1 health levels off
  at ~1.0–1.1k req/s, server-side.** At 1 user `gateway` is V2 0.1 ms vs V1 1.1 ms.
- Without keepalive: V2 +3–5 ms per new TCP connection (caps at ~1.6–2k req/s); V1
  +15–20 ms TLS handshake. Through `ssh -L`, a fresh TLS connection also stalls ~40 ms
  (Nagle / delayed ACK); this affects only no-keepalive runs.
- Cloudflare edge (Prod V1 vs Dev V1, lab VM): +8–35 ms p50, +13–38 ms p95 per request.
- SOCKS tunnel (V2 → Sophia, formerly Tara): ~40 ms `upstream` at low load from
  Nagle / delayed ACK (OpenSSH sets TCP_NODELAY only for interactive sessions); direct
  ~1.9 ms. Relevant to Prod and to Sophia.

### Mock (B): gateway cost with zero model time
- **B0** (0 ms mock): V2 peak **756 req/s**, V1 **290** (2.6×), both at 32 users,
  both CPU-bound on 4 workers. 1-user overhead p50 V2 3.1 ms, V1 7.0 ms. V2 0 errors
  to 1024 users; V1 errors from 64 users (10.5% at 1024) from worker recycling.
- **B50** (50 ms mock): V2 peak 556, V1 246; 1-user overhead V2 5.1, V1 9.1 ms. V2 0
  errors to 2048 users; V1 recycling 502s from 32 users and fd exhaustion at 2048 (fd
  limits since raised on both).
- **B1s** (1 s mock, 4 tunnels, capped at 512): to 128 users both near ideal; V2
  overhead p50 5–7 ms vs V1 12–38 ms. **V2 onset at 256**: 156 req/s, p95 5.8 s; at 512
  77 req/s, 0 errors, 4 workers saturated (330–415% CPU). V1: 156 req/s at 256 (p95
  2.0 s, 1.5% 502s), 214 req/s at 512 (4.2% 502s).
- **B1s-stream** (160 chunks, 5 ms apart): V2 0 errors throughout, peak 122 req/s at
  256, max gap p95 ≤ 72 ms to 256; **V2 uses ~1.1 cores at only 32 streams and
  saturates 4 workers from 128 streams (~0.2 ms CPU per relayed chunk)**. V1 plateaus
  at ~82 req/s from 128 streams with stalls (max gap p95 0.26 s at 128, 1.7 s at 512),
  6.9% 502s at 256.
- Session 3 (not accepted as data, single tunnel): at ~1000 in flight V2's 4 workers
  were pegged at ~65 req/s; V1 returned 504 bridge pool timeouts.

### Real models at 1 replica (pre-optimization)
- **C** (gemma-4-E4B, 16 in / 2 out): vLLM is the limit for both at ~175–187 req/s from
  16 users. V2 overhead p50 3.9 / 4.0 / 5.4 / 10.4 ms at 1 / 4 / 16 / 64 users; V1
  8.2 / 8.9 / 20 / 39 ms. Pilot-path overhead ≈ the mock's. **V2 gateway ~1 core at
  187 req/s ≈ 5–6 ms CPU per request.**
- **D / E / G** (gpt-oss-20b, 1024 in / 256 out): results in RUNLOG "Pre-optimization suite" (being filled at the end of session 4).

### V2 hot spots to profile (P1)
1. **Per-request CPU ~5–6 ms** (B0: 756 req/s on 4 cores; C: 187 req/s on 1 core). Sets
   V2's ceiling at low concurrency. Candidates H1–H8 in `PROFILING.md`: 7 Redis round
   trips, `settle`'s extra GET, auth re-validation, JSON handled 5×, 3 log events per
   request, framework overhead.
2. **CPU per request grows with requests in flight** (onset ~256): httpcore
   `_assign_requests_to_connections` walks connections × waiters (71% of CPU in
   `pool_bench` at 256). 16 sharded clients help at 256 (238 req/s) but 1024 still
   collapses: a second cost remains (H9–H12).
3. **Streaming relay ~0.2 ms CPU per chunk**: saturates 4 workers at ~17–20k chunks/s.
   Real models emit tens of tokens/s per stream, so this bounds concurrent streams
   per worker in production.
4. Fixed and deployed along the way (for the report): nginx→gunicorn keepalive (502s
   from port exhaustion), fd limits, Redis pool that raised `MaxConnectionsError` at
   100 per worker.

### V1 baseline behaviour (as deployed; facts for the report, not proposals)
- ~1k req/s health ceiling; 290 req/s on the 0 ms mock; overhead ~2× V2's at 1 user.
- `max_requests 3000` recycling → bursty fast 502s under sustained load; a `WORKER
  TIMEOUT` (SIGABRT) seen in B0 and B1s.
- Bridge httpx defaults: ≤ 100 backend connections/worker (~400 total), 5 s pool
  timeout → 504s; 20-connection keepalive cap churns mTLS connections.
- Streaming plateau ~82 streams/s with multi-100-ms stalls (B1s-stream).

## 2. Playbook

### 2.1 P1: profile V2 on the mock

Goal: name the functions behind hot spots 1–3 with numbers and give each hypothesis
in `PROFILING.md` (H1–H12) a verdict. V2 only, mock only.

**User, on the Dev VM, once:**
```bash
cd /home/webportal/FIRSTv2 && git fetch && git checkout loadtest-profiling   # c4a29ef
# wherever COMPOSE_FILE is set (deploy/.env): append the overlay, e.g.
#   COMPOSE_FILE=compose.yaml:compose.dev.yaml:compose.tunnel.yaml:compose.dev-analytics.yaml:compose.profile.yaml
make profile-up       # rebuilds the gateway image with py-spy, adds SYS_PTRACE and FIRST_PROFILE_DIR
```
**Agent checks** after the deploy: `curl -s -D - -o /dev/null $V2/health | grep -i
server-timing` shows `p_respond`; `docker exec first-inference-gateway-1 ls
/tmp/first-profile` lists `worker-*.jsonl`. The agent runs the gateway-side steps
itself over the ControlMaster with `docker exec [-u root] first-inference-gateway-1 …`
(what `profile.sh reset|pyspy|collect` does), so py-spy starts on cue mid-step.

Run the harness from the profiling worktree (it parses the phase timings):
`cd /home/msalim/FIRST-loadtest-profiling && uv run --frozen python -m tests.load run …
--out loadtest-runs/profile-20261117 $V2P --catalog-url $V2`.

Per test: a **clean run** (phases + gauges; reset gauges first), then the **same run
with py-spy** (`py-spy record --pid 1 --subprocesses --rate 100 --format raw --function
--duration 40`, started ~20 s into the measured window). If the py-spy run's req/s is
more than a few % below the clean run, retry with `--nonblocking`. Order minimises
mock restarts:

| # | Mock | Run IDs | Arguments | Answers |
| --- | --- | --- | --- | --- |
| 1 | 200/5 (current) | `prof-01u` | `--model loadtest/mock --users 1 --warmup 10 --duration 60` | true cost of each phase, no loop lag |
| 2 | 200/5 | `prof-b1s`, `prof-b1s-pyspy` | `--output-tokens 160 --users 128,512 --warmup 30 --ramp 25 --duration 60`; py-spy in each step | H9–H12 inside the gateway: pool scan vs. in-flight; worker balance |
| 3a | 200/5 | `prof-stream-t5`, `prof-stream-t5-pyspy` | `--stream --output-tokens 160 --users 64,128 --warmup 30 --ramp 25 --duration 60` | relay cost per chunk, and which code |
| 3b | 200/**20** | `prof-stream-t20` (py-spy in the step) | `--stream --output-tokens 160 --users 64 --warmup 30 --duration 60` | does CPU follow chunk rate or stream count? (64 streams: ~10k → ~3.2k chunks/s) |
| 3c | 200/**50** | `prof-stream-t50` (py-spy in the step) | same | third point (~1.3k chunks/s); drop if short of time |
| 4 | **0/0** | `prof-b0`, `prof-b0-pyspy` | `--output-tokens 8 --users 1,32 --warmup 15 --duration 60`; py-spy at 32 | per-request CPU at saturation (H1–H8) |
| 4b | 0/0 | `prof-b0-4k` | `--input-tokens 4096 --output-tokens 8 --users 32 --warmup 15 --duration 60` | JSON and body-file cost scaling (H5, H8) |
| 5 | none | `pool_bench` (client VM only; during mock restarts, never during a run) | `--users 64,256,1024 --shards 1`, then `--shards 16`, then under `py-spy record -f raw` | H9–H11 without the gateway; try candidate fixes |

Afterwards restore the mock to **200/5** at 1 replica. Collect
(`docker cp first-inference-gateway-1:/tmp/first-profile/. <dir>` on Dev, copy to the
client) and run `uv run python -m tests.load profile-report <run-dir> --profile <dir>`
per run (the two hosts' clocks agree within ms; no `--clock-offset`). Record every run
and the verdict table in RUNLOG; fill the Verdict column of `PROFILING.md` on
`loadtest-profiling` (ask before committing there; its "Runs to add" item 3 still names
the abandoned `/v2-test/` path: use the 4 tunnels instead).

The user can then revert Dev to `firstv2` (drop the overlay). P3 doesn't depend on
the V2 build, but V2 must stay idle during V1 runs.

### 2.2 P2: pre-optimization report

One self-contained HTML page (load the `artifact-design` and `dataviz` skills first;
publish as an artifact), built only from accepted runs, plus its data. Sections:

1. **Summary**: claims C1–C7 (section 3) with the evidence so far; ranked V2 hot spots
   with expected gain.
2. **Setup and method**: client, 4-tunnel path, both gateways, a latency decomposition
   diagram (client → tunnel → nginx `queue` → gateway `overhead` → `upstream`), the
   rules in 0.4.
3. **Results**: A; B0 / B50 / B1s / B1s-stream; C / D / E / G at 1 replica.
4. **Hot spots in the V2 inference path** (P1): CPU by library and function, phase
   table at 1 and 32 users, worker gauges, H1–H12 verdicts, candidate changes.
5. **V1 baseline behaviour** (section 1).
6. **Environment findings**: tunnel backlog, Nagle stalls, Cloudflare edge, fd limits,
   nginx keepalive, Redis pool.
7. **Limits of the study** and the plan for P3–P5.
8. **Appendix**: every accepted run, its command and step table.

### 2.3 P3: V1-only runs (while the user changes V2)

V1 is frozen, so these numbers go into the final report as is. V2 stays idle (same
Dev VM). Each needs the stated Tara state; confirm replica changes with the user.

| Run IDs | Setup | Arguments |
| --- | --- | --- |
| `E-v1-2r`, `E-v1-4r` | gpt-oss-20b 2 / 4 replicas | section 4 E, `--users 8,16,32,48,64` / `16,32,64,96,128` |
| `G-v1-bridge-stream-4r` | gpt-oss-20b 4 replicas | section 4 G, `--users 16,64,128` |
| `C-v1-4r` (optional, 16 GPUs) | gemma-4-E4B 4 replicas | `--users 1,4,16,64,128,256,512` |
| `B0-v1-4t`, `B50-v1-4t` (optional) | mock 0/0, 50/0 | section 4 B on the 4-tunnel path, for uniformity with P4 |
| `A-v1-health-noka` | none | `--no-keepalive --users 1,4,16,64` (symmetry with V2) |
| `F-v1-globus-tiny`, `F-v1-globus-real` (optional) | user brings up the V1 Globus endpoint on Sophia, model hot | section 4 F |

### 2.4 P4: post-optimization V2 suite (full)

After the user deploys the optimized V2 (record its commit in RUNLOG), redo the
session-start checks, then run V2 only, with the same path and arguments as P0/P3 so
every row has a V1 counterpart:

| Order | Run IDs (V2) | Notes |
| --- | --- | --- |
| 1 | `A-v2-health-post` | client ceiling unchanged? |
| 2 | `B0-v2-post`, `B50-v2-post` | mock 0/0, 50/0; users as section 4 |
| 3 | `B1s-v2-post`, `B1s-v2-stream-post` | mock 200/5, `--users 32,64,128,256,512` (+1024 if overflows stay ~0) |
| 4 | `C-v2-1r-post` (+ `C-v2-4r` if confirmed) | |
| 5 | `D-v2-1r-post`, `D-v2-2r`, `D-v2-4r`, `D-v2-1r-repeat` | replica scaling (C4) |
| 6 | `G-v2-stream-1r-post`, `G-v2-stream-4r` | |
| 7 (optional) | `F-v2-sophia-*` | with the F V1 runs |

Re-profile (P1 tests 2 and 4) on the optimized build only if a hot spot remains.

### 2.5 P5: final report

As 2.2, with V1, V2-pre and V2-post in every comparison figure (V1 and V2 keep two fixed
colours; V2-pre a lighter shade of V2), replica scaling from D/E, and a section on what
each fix bought (hot spot → change → measured gain).

## 3. Claims the campaign tests

| # | Claim | Experiment |
| --- | --- | --- |
| C1 | The client and network path are not the bottleneck | A |
| C2 | V2 adds less gateway overhead per request than V1 | B, C |
| C3 | V2 sustains a higher request rate before saturating | B, C |
| C4 | V2 throughput grows in proportion to replicas | D |
| C5 | V1 and V2 differ on the same replicas only by gateway cost | E |
| C6 | V1 through Globus Compute is slower in kind, not degree | F |
| C7 | Many concurrent streams all finish, with steady token flow | G, B1s-stream |

Do not presume the outcome; a number that contradicts a claim is reported as such.

## 4. Experiments (reference)

Run IDs are `<exp>-<target>[-<variant>]`; V1/V2 pairs run back to back. Timing:
health / mock / tiny `--warmup 10 --duration 30` (B1s: `--warmup 30 --ramp 25`);
realistic and streaming `--warmup 30 --duration 120`.

**Models.** `loadtest/mock` (mock upstream on Tara, zero model work);
`google/gemma-4-E4B-it` on Tara, 16 in / 2 out (real pilot path, model nearly idle; 4
GPUs per replica, `max_backend_concurrency` 64); `openai/gpt-oss-20b` on Tara, 1024 in
/ 256 out (1 GPU per replica, `max_backend_concurrency` 32). V1 runs add
`--catalog-model <same name>`.

**Routes.** V2 chat `$V2/federated/v1/chat/completions`; V1 chat on the same Tara
replicas `$V1/resource_server/tara/api/v1/chat/completions`; V1 Globus on Sophia
`$V1/resource_server/sophia/vllm/v1/chat/completions`; health `$V2/health`,
`$V1/resource_server/health`.

| Exp | Runs | Arguments | Status |
| --- | --- | --- | --- |
| A | `A-v2-health`, `A-v1-health` (+ `-4t`) | `--users 1,4,16,64,256,1024` | done |
| A-noka | `A-v2-health-noka`, `A-v1-health-noka` | `--no-keepalive --users 1,4,16,64` | V2 done; V1 in P3 |
| B0 | `B0-v2`, `B0-v1` | mock 0/0, `--output-tokens 8 --users 1,2,4,8,16,32,64,128,256,512,1024` | done (1 tunnel) |
| B50 | `B50-v2`, `B50-v1` | mock 50/0, `--output-tokens 8 --users 1,8,32,128,512,1024,2048` (cap at 1024 on tunnels) | done (1 tunnel) |
| B1s | `B1s-v2`, `B1s-v1` (+ `-stream`) | mock 200/5, `--output-tokens 160 --users 32,64,128,256,512 --warmup 30 --ramp 25` | done (4 tunnels) |
| C | `C-v2-1r`, `C-v1-1r` | gemma, `--users 1,4,16,64` | done; `-4r` optional |
| D | `D-v2-1r` / `-2r` / `-4r` / `-1r-repeat` | gpt-oss, 4, 8, 16, 24, 32 users per replica | 1r done; rest in P4 |
| E | `E-v1-1r` / `-2r` / `-4r` | as D via V1 | 1r done; rest in P3 |
| F | `F-v1-globus-{tiny,real}`, `F-v2-sophia-{tiny,real}` | tiny `--input-tokens 16 --output-tokens 2 --users 1,4,16`; real 1024/256 `--users 1,4,16` | optional |
| G | `G-v2-stream-{1r,4r}`, `G-v1-bridge-stream-{1r,4r}` | gpt-oss `--stream`; 1r `--users 4,16,32`, 4r `--users 16,64,128` | 1r done; 4r in P3/P4 |

Gates per run: `gateway`/`queue` columns populated; client CPU < 85% at the peak;
replica column constant (no ⚠); `out_tokens` equals the requested length; errors none
below saturation and explained above it (V1 errors at saturation are a result, not a
failed run); ListenOverflows ~+0; D/E: `retried` 0 or small. Dropped for good:
B0-direct (the mock listens on a UDS), C-2r, D-8r / D-8r-over (256+ in flight).

## 5. Recording a run

Append to RUNLOG: run ID, directory, start/end, arguments, preconditions (mock values,
replicas), the step table, gate results, CPU samples, anything odd, and **accepted**
or not. A rerun keeps the same `--name`; RUNLOG names the accepted directory. Give the
user a two-line reading and name the next run.

## 6. Harness changes

Keep them in `tests/load/`, keep `tests/test_load_harness.py` passing (`make test`
needs `make db-up`; the client VM has no Docker, so the user runs it), and note in
RUNLOG which runs predate a change that affects measurement.
