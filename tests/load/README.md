# Load tests: V1 vs V2

A closed-loop load generator for comparing the V1 (`main`) and V2 (`firstv2`)
gateways. It never runs as part of `make test`.

The planned V1-vs-V2 campaign (which runs, in what order, and the report they
feed) is in [EXPERIMENT_PLAN.md](EXPERIMENT_PLAN.md).

```bash
uv run python -m tests.load run --url <URL> [--model <MODEL>] [--stream] --users 1,2,4,8
uv run python -m tests.load report <run-dir> [<run-dir> ...]
uv run python -m tests.load mock-upstream --port 9000
```

- **Load** is the number of users. Each user holds one connection and sends its
  next request as soon as the last one finished.
- **Target** is just a URL, so V1, V2 and the health route are driven the same way:

  | Target | URL |
  | --- | --- |
  | V2 chat | `https://<v2>/federated/v1/chat/completions` |
  | V1 chat | `https://<v1>/resource_server/<cluster>/<framework>/v1/chat/completions` |
  | V2 health | `https://<v2>/health` |
  | V1 health | `https://<v1>/resource_server/health` |

- **Mode**: no `--model` sends `GET` (health); `--model` sends a chat completion;
  `--stream` asks for an SSE stream.
- **Replicas** are read, never set: with `--catalog-url https://<v2>` the harness
  polls the V2 catalog every 5s and records how many backends were serving the
  model during each step (ready pilot replicas + healthy static deployments).
  Pass it for V1 runs too, since V1 reaches the same V2-managed replicas.
- The token comes from `FIRST_TOKEN`, else `alcf-tokens get-token inference`.

## What is measured

Both gateways answer every request with a `Server-Timing` header, so no log
access is needed:

```
Server-Timing: queue;dur=0.80, gateway;dur=41.30, upstream;dur=38.90;desc="attempts=1"
```

| Column | Meaning |
| --- | --- |
| `req/s` | successful requests completed per second in the measured window |
| `e2e` | client round trip, request sent to last byte |
| `gateway` | request time measured inside the gateway |
| `upstream` | time the gateway spent waiting on the model server, summed over retries |
| `overhead` | `gateway − upstream`: the gateway's own processing |
| `queue` | time between nginx forwarding the request and the gateway starting on it |
| `network` | `e2e − gateway − queue`: TLS, Cloudflare, nginx, the wire |
| `ttft`, `max gap` | streams: time to first token; longest pause between tokens |
| `client CPU` | busiest client process; above 85% the generator is the bottleneck |

Differences are taken per request and then aggregated (percentiles do not
subtract). Cells show p50 / p95 / p99 in ms.

Things to know when reading the numbers:

- **`queue` needs nginx to stamp requests** (`X-Request-Start`, added to the
  nginx configs on both branches). Without it, time spent waiting for a gateway
  worker is folded into `network`, which makes a saturated gateway look like a
  slow network.
- **Streams report the time to open the stream.** Response headers leave before
  the body, so for `--stream` runs `gateway`, `upstream` and `overhead` stop at
  the first byte, and `network` is measured to the response headers. V1 opens
  the stream before contacting the backend, so it reports no `upstream` there.
  Relay health is what `ttft`, `max gap`, `e2e` and the error rate show.
- **A stream counts as failed** unless it delivered content and ended with
  `[DONE]`; an error event inside a `200` stream (V1's way of reporting a
  backend failure) is a failure too. Failures are listed by cause per step.
- **V1 Globus Compute endpoints**: `upstream` is the whole Globus round trip.
- **Closed loop**: slow responses throttle the load, so latency at saturation
  is understated. Read the curves as throughput and latency *versus users*.
- Errors are excluded from latency and counted separately; a sweep stops once
  more than half of a step's requests fail (`--stop-error-rate`).

Each run writes `loadtest-runs/<name>-<timestamp>/` (gitignored): `summary.md`,
`summary.json`, `steps.csv` (for plotting), and per step `requests.jsonl` (one
record per request, fields documented in `stats.py`) and `replicas.jsonl`.

## Before a run

1. **Deploy `Server-Timing`** on both gateways (V2: `log_middleware.py`; V1:
   `server_timing.py`) and add `proxy_set_header X-Request-Start "t=$msec";`
   to each production nginx.
2. **Lift the limits for the test identity.** One identity plays every user, so
   it needs, per test model:
   - V2 `usage_limits`: `tpm: 0`, `rpm: 0` and a `max_user_concurrency` above
     the largest user count (default 8).
   - V2 `router_params.max_backend_concurrency` (default 16) caps in-flight
     requests per backend. For the mock and tiny models set it above the largest
     user count; for the realistic model leave it as in production and keep the
     sweep within `replicas × max_backend_concurrency`, or the excess is
     rejected by admission rather than served.
   - V1: `RATE_LIMIT_PER_SEC_PER_USER` (default 10/s) and the endpoint's
     `tpm_user` / `tpm_model`.
   - nginx `limit_req` on both, and any Cloudflare rate limiting.
3. **Pin the replica count** (autoscaling off) for the model under test.
4. **Check the generator first** with the health baseline below.

## The mock upstream

A model that does no work, so the gateway is the only thing measured:

```bash
uv run python -m tests.load mock-upstream --port 9000 --workers 4 --ttft-ms 20 --token-ms 2
```

`mock_upstream.py` is standalone (stdlib + uvicorn) and can be copied to another
host. Register it in V2:

```yaml
kind: Model
name: loadtest/mock
spec:
  access_group_name: public
  max_model_len: 32768
  supported_endpoints: [chat/completions]
  usage_limits:
    default: {tpm: 0, rpm: 0, max_user_concurrency: 4096}
---
kind: StaticDeployment
name: <cluster>/loadtest/mock
spec:
  cluster_name: <cluster>
  model_name: loadtest/mock
  api_url: "http://<mock-host>:9000/v1"
  upstream_model_name: mock
  router_params: {max_backend_concurrency: 4096}
  health_check: {url: "http://<mock-host>:9000/health"}
  prometheus_metrics_path: null
```

and in V1 as a `DirectAPIEndpoint` with
`"config": {"api_url": "http://<mock-host>:9000/v1", "api_key_env_name": "<ANY>"}`.

## Measurement plan

Set `V1`, `V2` to the base URLs. Run each V1/V2 pair back to back from the same
client host, one at a time.

```bash
run="uv run python -m tests.load run"

# 6. Baseline: no inference, no database. The ceiling of the client and the path.
$run --name v2-health --url $V2/health --users 1,8,32,128,512
$run --name v1-health --url $V1/resource_server/health --users 1,8,32,128,512

# 7a. Gateway ceiling against the mock: raise users until req/s stops growing.
$run --name v2-mock --url $V2/federated/v1/chat/completions --model loadtest/mock \
     --users 1,8,32,128,512,1024 --catalog-url $V2
$run --name v1-mock --url $V1/resource_server/<cluster>/<fw>/v1/chat/completions \
     --model loadtest/mock --users 1,8,32,128,512,1024

# 7b. The same through a real vLLM serving a tiny model with tiny input/output.
$run --name v2-tiny --url $V2/federated/v1/chat/completions --model <tiny> \
     --input-tokens 16 --output-tokens 4 --users 1,8,32,128,512 --catalog-url $V2

# 8. Realistic model: repeat the sweep at each replica count you set by hand.
for n in 1 2 4 8; do
  read -p "Scale <model> to $n replicas, wait until ready, press enter "
  $run --name v2-real-${n}r --url $V2/federated/v1/chat/completions --model <model> \
       --input-tokens 1024 --output-tokens 256 --users 4,8,16,32,64,128 \
       --duration 120 --warmup 30 --catalog-url $V2
done

# V1 against the same replicas (its first_v2 adapter proxies to them directly).
$run --name v1-real-1r --url $V1/resource_server/<cluster>/<fw>/v1/chat/completions \
     --model <model> --input-tokens 1024 --output-tokens 256 --users 4,8,16 \
     --duration 120 --warmup 30 --catalog-url $V2 --catalog-model <v2 model name>

# V1 through Globus Compute: a short run to show the difference in kind.
$run --name v1-globus --url $V1/resource_server/<cluster>/<fw>/v1/chat/completions \
     --model <globus model> --input-tokens 1024 --output-tokens 256 --users 1,4,16

# Streaming: many concurrent streams must all finish, with a steady token flow.
$run --name v2-stream --stream --url $V2/federated/v1/chat/completions \
     --model <model> --input-tokens 1024 --output-tokens 256 --users 16,64,256 \
     --catalog-url $V2
$run --name v1-globus-stream --stream --url $V1/resource_server/... --model <globus model> \
     --input-tokens 1024 --output-tokens 256 --users 16,64,256
```

How to tell which side saturated in 7: if `upstream` stays flat while `overhead`
and `queue` climb, the gateway is the limit. If `upstream` climbs, the model
server is; add mock workers or tiny-model replicas and repeat.

Compare runs (the first is the baseline for `vs first`; `scaling` is req/s per
replica relative to the first run that has a replica count, so 100% is ideal):

```bash
uv run python -m tests.load report loadtest-runs/v2-real-1r-* loadtest-runs/v2-real-2r-* \
    loadtest-runs/v2-real-4r-* loadtest-runs/v2-real-8r-* --out scaling.md
```

Useful options: `--processes` (client processes), `--no-keepalive` (a new
connection per request, to price the TLS handshake), `--cacheable-prompts`
(drop the random prompt prefix that defeats vLLM's prefix cache),
`--no-pin-output` (omit vLLM's `min_tokens` / `ignore_eos`), `--extra-body`,
`--think-ms`, `--insecure` / `--ca`.

Running the sweep twice, once from outside (through Cloudflare) and once from a
host next to the gateway, splits `network` into its edge and local parts.
