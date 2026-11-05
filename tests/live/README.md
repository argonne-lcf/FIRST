# Live functional tests

Opt-in, fully automated tests that drive the **dev stack** and run **on the
gateway VM**. They act as an authenticated client (your `alcf-tokens` login,
which must be in the admin group) and also act on the backend directly:

- `docker compose logs` for structured logs, and `docker compose kill/start`
  to crash the apiserver
- direct Redis reads/writes of admission state: benches, in-flight sets,
  reservations, quota buckets, token-estimate EWMAs
- plan/apply of the live-test manifest profile, with temporary edits layered
  on top and reverted afterwards, plus manual replica scaling (`first-admin`)
- Python run inside the controller container to stop a replica through its
  pilot manager, or terminate a pilot job at the scheduler, without the
  controllers knowing
- a **chaos upstream**: a fake OpenAI-compatible server (`chaos_upstream.py`),
  run as a sidecar on the compose network. It is registered as StaticDeployments
  of a throwaway model `livetest/chaos`, and its behaviour can be switched per
  backend at runtime (healthy, 5xx, disconnect, slow stream, stream broken
  after 200)

They never run as part of `make test`.

```bash
# On the VM, from the repo root, with the stack up (make compose-up):
alcf-tokens get-token inference >/dev/null      # make sure the login is fresh
FIRST_LIVE=1 uv run pytest tests/live -s -v     # everything (~1h+; GPU replicas)
FIRST_LIVE=1 uv run pytest tests/live -s -v \
  tests/live/test_failover.py tests/live/test_cooldown.py \
  tests/live/test_crash_recovery.py              # chaos-only: no GPUs needed
```

Every run writes `tests/live/.runs/<run_id>/` (gitignored):

- `report.md`: the readable report. A summary table (verdict, number of checks,
  key result or failure reason per test) is followed by one section per test:
  its goal in plain language, then in order every action it took
  (**Did:**), every wait and measurement, and every check with its evidence
  (✔/✘ claim — what was observed, against what was expected), and finally its
  verdict. It is rewritten after each test, so an interrupted run still has one.
- `notes.md`: everything printed, in order, including the 5s fleet timelines.
- `requests.jsonl`: every request sent and its outcome.
- `timeline.jsonl`: fleet samples (autoscaling/resilience).

Tests record evidence with `run.step(...)`, `run.check(ok, claim, evidence)`
(which fails the test when `ok` is false) and `run.evidence(...)` (the summary
line). New tests should use these instead of bare `assert`, so a passing check
shows up in the report too.

**Side effects on the shared dev stack.** The crash test SIGKILLs
`inference-gateway`, which drops every in-flight dev request. The cooldown
test benches every fleet-model backend for about 15s, and the
autoscaling/drain/resilience tests scale, stop and kill real replicas and
pilot jobs.

## Manifest profile (`tests/live/manifests`)

`tests/live/manifests` is a complete copy of the dev manifests
(`gateway-api-fixtures/firstv2-manifests/dev`) with the live-test edits applied.
Every edit is tagged `# livetest`. The session applies it automatically before
the first test, printing what changed. Plan/apply is declarative over the whole
bundle, so this replaces the dev configuration wholesale.

To put the dev profile back when the session ends:

```bash
LIVE_RESTORE_MANIFESTS=../gateway-api-fixtures/firstv2-manifests/dev \
  FIRST_LIVE=1 uv run pytest tests/live ...
```

Tests that need a different configuration (the chaos model, manual scaling for
the drain test, a maintenance notice) apply profile + edit and re-apply the
plain profile on exit. `make test` checks that the bundle passes plan validation
(`tests/test_plan_validation.py`). When the dev manifests change, re-copy them
and re-apply the `# livetest` edits.

| Role (env override)                  | Model                                   | Limits under test |
|--------------------------------------|-----------------------------------------|-------------------|
| RPM + concurrency (`LIVE_RPM_MODEL`) | `google/gemma-4-31B-it` (Metis)         | rpm 12, burst_requests 3, max_user_concurrency 2; max_model_len 8192 |
| TPM (`LIVE_TPM_MODEL`)               | `mistralai/Mistral-Large-3-675B-Instruct-2512` (Metis) | tpm 6000, burst_tokens 4096 = max_model_len 4096, rpm 0 |
| Fleet (`LIVE_FLEET_MODEL`)           | `openai/gpt-oss-20b` (Tara w=3 + Sophia w=1) | quota unlimited (concurrency 64); max_model_len 32768; max_backend_concurrency 4; ladder `[0→1, 6→2]`, sustain 120s |
| Chaos (added by the `chaos` fixture) | `livetest/chaos` on `LIVE_CHAOS_CLUSTER` (tara) | 4 static backends a–d, weights 1000/1/1000/1000; rpm 6 / tpm 600 (one request is easy to read off the buckets); cooldown threshold 3, window 30s, bench 20s |

Expectations are derived from **your effective limits** as reported by
`/catalog/v1/models`. If a limit is unlimited for you, the test skips.

## Configuration

| Variable | Default | |
|---|---|---|
| `inference_base_url` | `http://localhost:8000` | gateway (nginx) |
| `admin_base_url` | `http://localhost:9100` | control API |
| `LIVE_REDIS_URL` | `FIRST_REDIS_URL` from `.env.compose`, host `127.0.0.1` | |
| `LIVE_CHAOS_CLUSTER` / `LIVE_CHAOS_PORT` | `tara` / `18765` | chaos deployments' cluster; sidecar control port on 127.0.0.1 |
| `LIVE_APISERVER_SERVICE` / `LIVE_CONTROLLER_SERVICE` | `inference-gateway` / `controller-manager` | compose services |
| `LIVE_STARTUP_TIMEOUT` | 1800 | bound on each replica startup wait |
| `LIVE_DRAIN_DEPLOYMENT` / `LIVE_DRAIN_MAX_TOKENS` | a deployment with a ready replica / 8000 | drain test |
| `LIVE_FED_ROUNDS` | 200 | federation sample size |

With the SOCKS tunnel overlay, `livetest-chaos` is in the services' `NO_PROXY`
(`deploy/compose.tunnel.yaml`).

## What runs

**`test_admission.py`**: buckets are emptied in Redis rather than waiting for refills
- RPM: burst/pause schedule checked decision by decision against a client-side
  GCRA twin of `admit.lua`, including `Retry-After`. Waiting exactly
  `Retry-After` is enough.
- `max_user_concurrency`: the (c+1)th request gets a 429 that is not a capacity
  reject. Closing the streams frees the slots.
- TPM: with the estimation EWMAs pinned in Redis, a concurrent burst admits
  exactly `floor(burst_tokens / estimate)`. Sustained throughput tracks `tpm` in
  actual tokens.
- **TPM refund on settle**: requests reserving 60% of the bucket but using a
  few dozen tokens are admitted back to back. After each one the bucket's debt
  (read from Redis) is at most the tokens actually used.
- Over-`burst_tokens` request: 400 `request_too_large`, no `Retry-After`, bucket
  untouched.
- In-flight: held streams appear in the catalog and as reservations in Redis.
  A disconnect settles them at once.
- Lease renewal: a chaos stream held 45s keeps its reservation, and its
  deadline in `rt:deadlines` keeps moving forward.
- Upstream 4xx: relayed, settled, and never counted toward cooldown.
- Saturation: `503 Backend saturated` with the jittered `short_retry_sec`.

**`test_failover.py`** (chaos)
- **Failover** on 502, 503, 504 and a dropped connection: the request succeeds
  on the fallback backend. The RPM bucket shows exactly one unit charged; the
  TPM bucket keeps only the successful attempt's actual usage (the failed
  attempt's reservation is refunded). The failed backend's error counter is 1,
  and the logs show attempt 1 `upstream_error` (`HTTP 503` /
  `RemoteProtocolError`) then attempt 2 `success`.
- Three failing candidates out of four: 503 `All 3 backend(s) failed …`, and
  the fourth backend is never tried.
- **Stream broken after 200**: the client's stream is cut short (no `[DONE]`),
  there is no retry, the reservation is released at once, the failure counts
  toward cooldown, and the InferenceLog says `upstream_error` (not `success`).
  With no usage reported, the estimate stays charged.

**`test_cooldown.py`**
- **Benching** (chaos): `cooldown_threshold` 503s bench the backend (TTL
  re-armed to the bench period). A pinned request then gets 503 `Backend in
  cooldown phase.` (`capacity:all_benched`, logged as `admission_rejected`)
  without contacting the backend, and federated traffic avoids it. It serves
  again only after `cooldown_bench_sec`.
- Fleet, benches written to Redis: with all backends benched → 503 `Backend in
  cooldown phase.`. With one deployment spared, all traffic lands there. When
  the TTL lapses the model serves again by itself.

**`test_crash_recovery.py`** (chaos)
- **Sweeper after an apiserver crash**: hold 4 streams and SIGKILL
  `inference-gateway`. The reservations are orphaned (nothing settled them),
  the apiserver is restarted, and the InflightObserver sweeps them within
  lease + sweep interval (≤ 75s). The controller logs the sweep, and the user's
  slots work again.

**`test_drain.py`** (fleet; a real replica)
- **Scale-down mid-inference**: one deployment is switched to manual scaling
  and brought to one ready replica, with 3 finite streams pinned to it; then
  desired goes 1 → 0. The replica leaves the router at once (new pinned
  requests get 503), but it is not torn down while streams are live. Every
  stream finishes with `[DONE]` and full `completion_tokens`, and only then is
  the replica `terminated` (`drained replica …` in the controller logs).

**`test_federation.py`**: weighted balance over 200 sequential requests, attributed
by `X-First-Deployment`, within 3σ. Pinned routes only land on their deployment.

**`test_logs.py`**: a tagged request mix, with RequestLog/ResponseLog/InferenceLog
correlated and checked field by field. The EWMA recurrence is checked from a
cleared state. Large request/response bodies are checked on disk inside the
apiserver container.

**`test_autoscaling.py`** (long): forced idle, cold start, rung 2 up, sustained
scale-down 2 → 1 → 0, then the `desired_replicas a -> b (reason)` log lines.

**`test_resilience.py`**
- A ready replica's model server is stopped through its pilot manager under
  background load. It must leave `ready` and be replaced; users see only
  200/429/503; there are no leaked reservations and no CRITICAL logs.
- Maintenance notice applied on `LIVE_MAINT_CLUSTER` (catalog, router config,
  503 for an offline model on that cluster). Its pilot jobs are killed at the
  scheduler: the loss is observed, nothing is placed during maintenance, and it
  is logged as "cluster under maintenance" with no ERROR/CRITICAL. The notice
  is then lifted.
