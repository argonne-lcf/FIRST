# Data Plane Design

The FIRST gateway combines:

- A [control plane](controllers.md) that brings up, scales, and tears down model deployments across multiple disjoint HPC clusters and provides a federated view of models across these heterogeneous systems. Capacity is dynamic: replicas appear and disappear due to autoscaling and cold starts.
- A **data plane** (this document) that routes user requests to live model replicas. It runs as ~4 FastAPI/asyncio worker processes sharing a Redis instance.

This page describes the data plane and *per-request* path through the gateway: how an
inference call is validated, mapped to a model deployment, and proxied to
a backend replica. For the durable resource model behind these views, see
[Data Model](data-model.md); for how routing config is published, see
[Declarative Configuration](declarative-config.md) and the
[Controller Framework](controllers.md).

## API Surface

Route path suffixes must be positioned so official SDKs work with only `base_url`.
The OpenAI SDK appends `/chat/completions` to a base ending in `/v1`;
the Anthropic SDK appends `/v1/messages` to a base without it.

The `/api/federated/` routes are the preferred routes for users seeking
available models without backend preferences:

```
/api/federated/v1/chat/completions    # replica chosen via "model" in body
/api/federated/v1/responses
/api/federated/v1/messages            # Anthropic dialect
/api/federated/v1/embeddings
/api/federated/v1/models              # OpenAI-format list (SDK compat)
```

Users seeking to target specific deployments can provide the deployment slug in
the URL:

```
/api/deployments/{slug}/v1/chat/completions
/api/deployments/{slug}/v1/responses
/api/deployments/{slug}/v1/messages
/api/deployments/{slug}/v1/models         # this deployment only
```


Non-LLM modalities are exposed via explicit task namespaces:

```
# Same scoping rules:
/api/federated/v1/tasks/{task}    # e.g. tasks/sam3-segmentation
/api/deployments/{slug}/v1/tasks/{task}
```

Users can discover available resources via `/api/catalog/`:

```
/api/catalog/v1/models
/api/catalog/v1/models/{name}
/api/catalog/v1/deployments
/api/catalog/v1/deployments/{slug}
```

The Control Plane APIs and operational APIs are restricted to admins:

```
# ── Control plane (admin; separate auth/middleware/audit) ──────
/api/control/v1/deployments               # CRUD, scale, drain
/api/control/v1/deployments/{slug}
/api/control/v1/clusters
/api/control/v1/usage                     # token metric rollups
/api/control/v1/demand                    # model-level demand signals (autoscaler view)

# ── Operational ────────────────────────────────────────────────
/healthz  /readyz  /metrics
```


## Architecture Overview

```
                   ┌────────────────────────────────────┐
                   │           Control Plane            │
                   │ lifecycle · autoscaler · cold start│
                   └───────┬──────────────────▲─────────┘
        writes cfg:* (3-   │                  │ reads live
        level hierarchy),  │                  │ usage metrics
        bumps version, pub │                  │
                           ▼                  │
     ┌────────────────────────────────────────┴───────────┐
     │                        Redis                        │
     │ cfg:*     control plane-written (models, replicas)  │
     │ rt:*      router state (inflight, cooldowns)        │
     │ quota:*   per-user GCRA buckets, concurrency        │
     │ pubsub    cfg-changed notifications                 │
     └──────────────┬──────────────────▲───────────────────┘
       snapshot pull│      Lua admit/settle · demand incr
       on notify    │                  │
  ┌─────────────────┴──────────────────┴───────────────────┐
  │        Data Plane — 4 × FastAPI/asyncio workers         │
  │                                                         │
  │  Auth → Classifier →   Router   → Admission ──┐        │
  │                          │                    ▼        │
  │                          │                 passthrough │
  │                          │                    │        │
  │                          ▼                 SSE tap     │
  │               429/503 + Retry-After           │        │
  │                                        metrics queue   │
  └───────────────────────────────────────────────┬────────┘
                                                   ▼
                                       vLLM / Model Servers
```

## Redis Contracts

The data plane runs as multiple independent worker processes sharing a single
Redis instance.  Admission decisions — "is this replica saturated?", "has this
user exceeded their quota?" — require reading a counter, comparing it to a limit,
and conditionally updating state.  If two workers execute these steps as
separate Redis commands, both can pass the same check and both increment — a
classic check-then-act race that admits more traffic than limits allow.

Redis Lua scripts solve this: a script runs atomically on the server with no
interleaving from other clients.  The gateway uses four Lua scripts (`admit`,
`settle`, `renew`, `record_error`) as the **only writers** of admission state.
Every check-and-update is a single indivisible operation.

All Redis keys are constructed by the `Keys` class in Python
(`first_gateway.database.redis.keys`) and passed into scripts via `KEYS[]`.
Lua scripts never assemble key strings — the key schema lives in one place.

### Master config (control-plane-owned)

`first_gateway.database.redis.router_config`: this defines the contract between
the control plane, which publishes the configuration blob, and the data plane,
where apiservers continuously reload an in-memory snapshot of the
`RouterConfig`.

- Control plane advertises a replica as `healthy` **only when actually warm**
(this requires passing health check)
- Replicas are identified by `uid`, which is a postgres autoincrementing primary
key that is never recycled.  This ensures replicas identifiers are unique/stable
across reconciles, and redis state remains valid across config rewrites.
- Models are identified from the HTTP request body `model` parameter, which is
matched to the unique preferred `name` or any number of optional legacy `aliases`.
- Deployments are identified  by `name`.  Since users may target deployments in the URL
path, and deployment names may contain `/`, the name is
mapped to a slug (1:1 mapping by swapping `/` for `~`).
- Config rewrite is atomic from the router's view: write keys → bump `cfg:version` → publish.

Even though FIRST Gateway does not explicitly manage Replicas for static
deployments, all deployment types converge to the uniform `RouterConfig` so that
routing is decoupled from the mechanics of model deployment.  Pilot deployments
will happen to have a truly dynamic list of replicas; Static deployments will
always have just one hard-coded replica.

### Router-managed state

The data plane tracks three categories of live state in Redis:

- **Replica utilization** — how busy is each backend right now?
- **Demand signals** — how much unmet demand exists, for the autoscaler?
- **Reservation ledger** — what has each in-flight request reserved, so settlement can undo it exactly?

#### Replica and model counters

| Key | Type | Description |
|---|---|---|
| `rt:model:{model}:inflight` | HASH `replica_id → int` | Per-replica concurrent request count, grouped into one hash per model so a single `HGETALL` retrieves all replica loads at once. Incremented atomically by `admit`, decremented by `settle`. |
| `rt:model:{model}:demand` | HASH `{inflight, capacity_rejects_total, last_reject_ts}` | Autoscaler-facing signals. `inflight` is a gauge of total model load across all replicas. `capacity_rejects_total` is a monotonic counter the autoscaler diffs over a window to compute reject rate. `last_reject_ts` drives scale-from-zero (recent reject with zero replicas → cold start). |
| `rt:replica:{id}:errors` | INT with TTL | Upstream error counter that doubles as the cooldown mechanism. The first error arms a TTL of `cooldown_window_sec`; if errors accumulate to `cooldown_threshold` within that window, the TTL is extended to `cooldown_bench_sec` and `admit` treats the replica as benched until the key expires. Incarnation-unique replica IDs (Postgres autoincrement, never recycled) guarantee a counter never haunts a relaunched replica. |

#### Reservation ledger

Each admitted request writes a **reservation** — a record of what resources it holds — so that `settle` can reverse its effects exactly, even if the worker crashes mid-stream.  The inflight counters and GCRA state are derived consequences; the reservation is the source of truth.

| Key | Type | Description |
|---|---|---|
| `rt:reserve:{request_id}` | JSON string | The reservation blob: `{request_id, model_name, user_id, replica_id, est_tokens, admit_ts, tokens_per_sec, burst_tokens}`.  Written by `admit`, read and deleted by `settle`.  Has **no TTL** — lifecycle is managed by the deadline index below. |
| `rt:deadlines` | ZSET `request_id → deadline_ts` | Lease index for all live reservations. Workers renew their requests' deadlines every ~10s (bumping to `now + lease_s`, capped at `admit_ts + max_stream_s`).  The sweeper runs `ZRANGEBYSCORE` to find past-due entries — reservations whose worker crashed or whose stream exceeded the cap — and settles them. |

#### Quota state

Per-user, per-model rate limiting uses the [GCRA algorithm](#gcra-quota-mechanics).  Each quota dimension gets its own key so they can be checked and advanced independently within the `admit` script.

| Key | Type | Description |
|---|---|---|
| `quota:{model}:{user}:tokens` | STRING (float) | GCRA theoretical arrival time (TAT) for token usage.  Represents the earliest moment the next token could be admitted at the configured `tokens_per_sec` rate.  Missing key = no outstanding token debt.  TTL is set to the burst window + 1s so idle users' keys expire naturally. |
| `quota:{model}:{user}:rpm` | STRING (float) | GCRA TAT for request rate.  Same mechanics as the token key but metered per-request at `requests_per_sec`. |
| `quota:{model}:{user}:inflight` | INT | Per-user concurrent request count for this model.  Incremented by `admit`, decremented by `settle`, deleted when it reaches zero (absence = 0). |

#### Feedback to Control Plane Autoscaler

The Control Plane observes and aggregates replica counters to determine
aggregate demand for the autoscaler.  Quota rejections never count as demand — a
user over fair share is not a scaling reason.

- Model capacity rejects are sampled and diffed over a window of time to obtain an _average rate_ of
  rejections per minute.
- In flight load plus capacity rejects are combined and normalized using `demand = inflight + reject_rate × avg_request_duration`: inflight gives currently occupied slots and second term gives slots that _would be occupied_ if the rejected traffic had been admitted.  Refer to `DemandThresholdStrategy` for details on the autoscaling methodology.


## Detailed Component Design

### Config Snapshot Manager

- Each worker holds an **immutable in-memory snapshot** of the full config (models, deployments, replicas, ACL maps, quota tables, compiled route tables).
- Subscribe to pub/sub; on notify (or poll fallback), compare `cfg:version`; if newer, load `cfg:*` and **atomically swap** the snapshot reference. In-flight requests keep the snapshot they started with.

### Cooldown

Cooldown is **error-driven** (threshold errors within window → bench replica)
and lives in `rt:*`; parameters come from the deployment config.  Relies on
Redis TTL to naturally expire cooldowns.

### AuthZ

User group/domain membership is intersected with model's AccessGroup to
authorize requests. Comparison is against in-memory config snapshot; does not
require DB lookup.

### Admission Controller

Four Lua scripts manage the admission lifecycle
(`first_gateway.database.redis.admission`):

| Script | Purpose |
|---|---|
| `admit` | Check quotas and capacity, assign a replica, write the reservation. |
| `settle` | Reverse a reservation's effects: decrement counters, correct GCRA, delete the reservation. |
| `renew` | Extend lease deadlines for in-flight requests (batched). |
| `record_error` | Track upstream failures and trigger cooldown (see Cooldown above). |

#### admit()

Takes the request identity (`request_id`, `model_name`, `user_id`), estimated
token usage, quota limits, and an ordered list of candidate replicas chosen by
the router.  Checks are evaluated in a fixed order — **quota first, then
capacity** — so that per-user rejections never inflate demand signals:

```
quota checks (affect only the requesting user):
  user concurrency  ≥ max_user_concurrency        → REJECT_QUOTA(user_concurrency)
  GCRA request rate  would exceed limit            → REJECT_QUOTA(user_rpm, retry_after_s)
  GCRA token rate    would exceed limit            → REJECT_QUOTA(user_tpm, retry_after_s)

capacity check (walks candidate replicas in router-chosen order):
  for each candidate:
    error count ≥ cooldown_threshold               → skip (benched)
    replica inflight ≥ max_replica_concurrency      → skip (saturated)
    first replica with headroom                     → chosen

  if no replica chosen → REJECT_CAPACITY(reason)
    reason: "no_candidates" | "all_benched" | "saturated"

commit:
  advance GCRA states (reserve est_tokens)
  increment user and replica inflight counters
  increment demand inflight gauge
  write reservation to rt:reserve:{request_id}
  add lease deadline to rt:deadlines               → ADMIT(replica_id)
```

Return values:

```lua
{1, replica_id}                  -- ADMIT
{2, reason, retry_after_s}       -- REJECT_QUOTA   (retry_after_s = -1 for concurrency)
{3, reason}                      -- REJECT_CAPACITY
```

Quota rejections return 429 with `Retry-After` computed from the GCRA refill
timestamp.  Capacity rejections also return 429; the `Retry-After` comes from
the configured short-retry interval ± jitter.  Only capacity rejects increment
the demand counter — a user over fair share is not a scaling reason.

#### settle()

Takes `request_id` and optionally `actual_tokens`.  Reads the reservation to
discover what was reserved, then reverses it:

1. Decrement the replica's count in `rt:model:{model}:inflight`
2. Decrement the user's count in `quota:{model}:{user}:inflight`
3. If actual token usage is known, apply a GCRA correction: adjust the TAT by
   `(actual − estimated) / tokens_per_sec`
4. Decrement the demand `inflight` gauge
5. Remove the request from `rt:deadlines` and delete the reservation

The script is **idempotent**: if the reservation is already gone, it no-ops.
This makes concurrent settlement safe — the request `finally` block, the
sweeper, and retries can all call `settle()` without coordination, and exactly
one caller applies.

For 99.9% of requests, settlement happens naturally in the request handler's
`finally` block.  The hot path passes `model_name` and `user_id` from the
request context to skip a pre-read round trip.  The sweeper, which only has
`request_id`, reads the reservation first to discover the identity.

Crashed workers and leaked reservations are caught by the sweeper, which
periodically runs `ZRANGEBYSCORE rt:deadlines -inf {now}` to surface
reservations whose lease has lapsed.

#### Reserve-then-settle pattern

`est_tokens` is a prompt-size heuristic plus `max_tokens` — an upper bound on
what the request might consume.  `settle()` applies the correction
`(actual − estimated)` to the GCRA state, so over-estimation doesn't permanently
penalize the user.

Without this reservation pattern, N concurrent agentic requests could all pass
the token quota check before any of them reports usage, blowing through the limit.

- The token bucket depth (`burst_tokens`) is matched to the model's max context
  window: a single burst request can fill the entire context.
- The refill rate (`tokens_per_sec`, derived from `tpm / 60`) dictates
  sustainable throughput (e.g. 128k tokens per minute).
- **Non-LLM tasks** use `est_tokens = 0`, giving them equal-footing admission
  (concurrency + RPM) while skipping token accounting.

### GCRA quota mechanics

[GCRA](https://en.wikipedia.org/wiki/Generic_cell_rate_algorithm) (Generic Cell
Rate Algorithm) is a leaky-bucket variant that needs only one value per key — the
theoretical arrival time (TAT) — instead of a counter and a timestamp.  The
implementation is a few lines inside `admit.lua`'s `gcra_eval()` helper.

- One Redis key per (model, user, resource) storing the TAT.
- Parameters from model config: `tpm` → refill rate (`tpm / 60` = `tokens_per_sec`);
  `burst_tokens` → bucket depth.
- Agentic pattern: idle "thinking" phases accrue credit as the TAT drifts toward
  `now`; a large tool-result turn spends it; sustained rate stays capped at tpm.
- On rejection the script returns seconds-until-conformant → `Retry-After`.
  Stock OpenAI/Anthropic SDKs back off automatically on 429 + Retry-After, so
  well-behaved agents self-pace with zero custom client code.

### Router

- For federated APIs, all replicas across the model's deployments form one candidate set.
- For deployment-pinned APIs, only consider the replicas within the chosen deployment.
- Selection weight _per-replica_ is `deployment.weight`, so effective deployment share = `weight × current replica count`
- Weighted-random sampling among available replicas with headroom.  On capacity REJECT from admission, drop that replica and retry.
- If all replicas exhausted → Overload Responder

### Overload / Cold-Start Handler

We will set `per_replica_concurrency` to slightly oversubscribe each backend,
allowing engines to naturally queue and serve inference requests over their max
headroom.  For example, a vLLM instance that averages ~10 concurrent requests
may be configured with a concurrency level of 20.  This provides a built-in,
bounded in-process hold queue.

The queue depth is conservative, so excess demand results in an immediate 429
with Retry-After and increments the deployment's capacity-reject counter.  When
replicas are available but saturated, the `Retry-After` is obtained from a
configured `short_retry_s` +/- `retry_jitter_pct` to break herds.

When cold-starting, the first request arrival is immediately rejected; the
capacity-reject increments demand, to which the autoscaler responds.  The
`Retry-After` ETA is obtained from the deployment, which advertises whether any
replica is starting (and its launch timestamp), the estimated replica cold start
time, and whether or not there is capacity to cold-start the replica.


### Request Classifier

- Extract scope (federated | deployment-pinned) and task from API path.
- Parse the JSON body **once**, extracting only `model`, `stream`, `max_tokens`/`max_output_tokens`; retain the **original raw bytes** for forwarding.
- No schema validation beyond routing needs — dialect enforcement is the backend's job, and every validated field is a maintenance liability against three moving specs.

### Proxy Engine (hot path)

- **Connections:** per-replica pooled `httpx.AsyncClient` (tuned pool, generous keepalive); lazily created and reaped.
- **Passthrough mode:** forward original bytes; strip scope prefix; map to the backend's matching dialect path; swap auth header for the replica's key; stream response bytes chunk-for-chunk via `StreamingResponse`. **No re-serialization anywhere.**
    - One exception: OpenAI chat streaming without `stream_options.include_usage` → inject it (request-side re-serialization only) and **drop the trailing usage chunk** before the client if the client didn't ask for it.
- **SSE hygiene:** iterate raw bytes (not lines); no compression middleware on streaming routes; `X-Accel-Buffering: no`; flush per chunk; propagate Content-Type.
- **Cancellation:** on client disconnect, close the upstream connection promptly (vLLM aborts generation); `finally` guarantees settle/decrements fire with tokens-so-far flagged estimated.
- **Retries:** safe only before the first response byte reaches the client. Connect errors and immediate 5xx errors are recorded for cooldown; router attempts next replica.
- **Mid-stream failure**: → SSE error event to the client, no retry.

### Usage Tap & Metrics Pipeline

- Tee on the response stream; byte-level pre-filter (`b'"usage"'`, `event: message_start`, `event: message_delta`, `response.completed`) JSON-parses only matching frames — 1–3 parses per request.

  | Dialect | Streaming usage location | Non-streaming |
  |---|---|---|
  | Anthropic Messages | input in `message_start`; cumulative output in final `message_delta` | `usage` in body |
  | OpenAI Chat | final chunk iff `include_usage` (gateway-injected) | `usage` in body |
  | OpenAI Responses | `response.completed` event | `usage` in body |

- Stream end → record `{request_id, user, model, deployment, replica, API path, tokens_in/out, ttfb, duration, status, estimated?}` → asyncio queue → batching task → sinks
    - Structured logs: JSONL to stdout
    - Redis rollups for `/api/control/v1/usage`; exposed by control plane observer as Prometheus metrics
    - Emission never blocks the request path; the settle script consumes the same record.
- Missing usage (disconnects, legacy backends): chunk-count proxy or tokens-so-far flagged `estimated: true`


### Discovery, Catalog & Observability

- `/api/{scope}/v1/models`: OpenAI-shaped, ACL-filtered, snapshot-served.
- `/api/catalog/v1/*`: full metadata: deployments with replica counts/states; snapshot + selected `rt:*`/`demand:*` reads.
- Prometheus per worker: request counts/latency by model/deployment/task, TTFB, inter-chunk latency, token counters, **admission rejects by reason (capacity vs each quota type)**,  demand gauges, translation-path counter (should trend → 0), snapshot version/age.
- `/readyz` = snapshot loaded ∧ Redis reachable.

## Request Lifecycles (reference walkthroughs)

**A. Streaming (common case):** auth → classify (raw bytes kept) → router picks replica → Lua admit (quota then capacity; reserves tokens, bumps demand) → byte passthrough with SSE tap → usage frame parsed → settle Lua (correct GCRA, decrement inflight/demand) → metrics enqueued.

**B. Capacity exhausted:** admit rejects capacity on all replicas (or zero healthy replicas) → overload handler: 429 + Retry-After ≈ ETA; capacity-reject counted in demand → autoscaler sees sustained demand → wakes deployment → later retries admit normally.

**C. User over token quota:** GCRA reject → 429 + Retry-After from refill math; demand untouched; SDK backs off; no scaling triggered.

**D. Replica failure:** replica starts erroring → router cooldown benches the replica; traffic flows to siblings → control plane's health check withdraws the replica seconds later → its rt: keys expire via TTL; relaunch arrives as a fresh replica ID.

