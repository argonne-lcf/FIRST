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

Users seeking to target specific deployments can provide the slug in the URL:

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
  │  Auth → Classifier → Tier Router → Admission ──┐        │
  │                          │                     ▼        │
  │                          │                  passthrough │
  │                          │                     │        │
  │                          ▼                  SSE tap     │
  │               429/503 + Retry-After            │        │
  │                                        metrics queue    │
  └────────────────────────────────────────────────┬────────┘
                                                   ▼
                                       vLLM / Model Servers
```

##  Redis Contracts

### Master config (control-plane-owned)

A single blob of configuration written by the control plane; read-only to the data plane:

```
cfg:version                      → monotonically increasing int
cfg:models                       → set of model names

cfg:model:{name}                 → JSON {
    name,
    allowed_groups: [...],
    supported_endpoints: [...],
    deployments: [ {slug, tier} ],
    quotas: {
        default:   {tpm, burst_tokens, rpm, max_user_concurrency},
        overrides: {group_or_user: {...}}
    },
    overload_policy: {
        short_retry_s: 15,               # micro-contention Retry-After base
        retry_jitter_pct: 30,            # server-side jitter to break retry herds
    }
}

cfg:deployment:{slug}            → JSON {
    slug, model, cluster,
    weight,                              # weighted random selection of replicas
    per_replica_concurrency,             # max replica internal queue/running depth
    cold_start_duration_s,               # Estimated replica cold start duration (from created to running)
    replica_launch_ts,                   # Timestamp of last launched replica (for calculating ETA)
    cold_capacity_available,             # Flags whether a cold replica could be started
    cooldown: {threshold, window_s, duration_s} | null,
    replicas: [replica_id, ...]
}

cfg:replica:{id}                 → JSON {
    id, deployment, url, api_key_ref,
}
```

Rules:
- Control plane advertises a replica as `healthy` **only when actually warm** (this requires passing /health check and perhaps a periodic small-prompt test)
- `replica_id` and slug are unique/stable across reconciles so churn state survives config rewrites.
- Config rewrite is atomic from the router's view: write keys → bump `cfg:version` → publish.

### Router-managed state

Replica utilization and cooldown state is tracked by the data plane.  A
reservation ledger is maintained for each request ID to maintain distributed
consistency:

```
rt:replica:{id}:inflight            → int (Lua-managed; vs per_replica_concurrency)
rt:replica:{id}:cooldown            → JSON {until_ts, reason, epoch}, TTL = duration
rt:reserve:{request_id}             → JSON {replica_id, model, user, est_tokens, admit_ts}

rt:demand:{model} → HASH {
    inflight,               # gauge
    capacity_rejects_total, # MONOTONIC counter
    last_reject_ts          # timestamp of most recent capacity reject
}
```

Quota state is maintained per-user, per-model:

```
quota:{model}:{user}:tokens       → GCRA state (theoretical arrival time), single key
quota:{model}:{user}:rpm          → GCRA state for request rate
quota:{model}:{user}:inflight     → int (per-user concurrency, Lua-managed)
```

#### Control Plane Feedback

The Control Plane observes and aggregates replica counters to determine
aggregate demand for the autoscaler.  Quota rejections never count as demand — a
user over fair share is not a scaling reason.

- Replica in-flights are summed to obtain model in-flights and track model-wide 1m/5m load averages.
- Model capacity rejects are sampled and diffed over a window of time to obtain an _average rate_ of
  rejections per minute.
- In flight load plus capacity rejects are combined and normalized using `demand = inflight + α × reject_rate × avg_request_duration`: inflight gives currently occupied slots and second term gives slots that _would be occupied_ if the rejected traffic had been admitted.  Leave the windowing and scaling factors to the autoscaler; the data plane just publishes the raw facts.

Each deployment has a configured **demand threshold ladder** that maps units of
demand to desired number of replicas.  This allows all deployments to scale
based on the combined model-level demand metric.  The autoscaling strategy
provides:

- **Hysteresis bands**: scale-up rung at N, scale-down at ~0.7×N
- **Sustain**: demand must exceed N for a minimum `scale_up_sustain_s` and must fall below 0.7N
for a minimum `scale_down_sustain_s` to come back down. The scale down sustain should
be significantly longer to prevent flapping and prematurely tearing down warm capacity.
- **Cold start signal:** do not require sustain to scale up from zero


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

A pair of atomic Redis Lua scripts is used to manage high-churn Redis state with
distributed consistency. The `admit()` function checks quotas/capacity and
assigns each request to a replica. The `settle(request_id)` frees resources and
performs idempotent cleanup.

The `admit()` function:

- Takes in `{model, user, replica_id, request_id, est_tokens, admit_ts}`
- Records a reservation in `rt:reserve:{request_id}` containing the input data above.
    - The reservation **must not have a TTL in Redis**!  Instead, the deadline is managed as a sorted set (`rt:reserve:deadlines`, key on `request_id`, score is `deadline_ts`).
    - Deadline renewal is handled by workers in a small batch Lua script (for each id, if EXISTS rt:reserve:{id} then ZADD). Deadline is renewed every 10s: bump deadlines out to 30s in the future, up to a max limit of ~15minutes after `admit_ts` for one inference task.
- The admit function performs the checks, updates counters, and returns the admit/reject state and metadata:

```lua
-- ADMIT:            {1, replica_id}
-- REJECT_QUOTA:     {2, reason, retry_after_ms}   -- reason: "user_tpm"|"user_rpm"|"user_concurrency"
-- REJECT_CAPACITY:  {3, reason}                   -- reason: "cooldown"|"concurrency"
```

Quota rejections return 429 with the reason and a `Retry-After` calculated based on the user's bucket refill timestamp.
Capacity rejections also 429, but the `Retry-After` comes from the expected ETA or short-retry-jitter (see below) and
the capacity rejection counter is incremented.

Pseudo-code:

```
admit():
  # ---- quota
  if quota:{model}:{user}:inflight >= max_user_concurrency   → REJECT(user_concurrency)
  if GCRA(quota:{model}:{user}:rpm) would exceed              → REJECT(user_rpm, retry_after)
  if GCRA(quota:{model}:{user}:tokens, est_tokens) exceeds    → REJECT(user_tpm, retry_after)

  # ---- capacity
  if rt:replica:{id}:cooldown active (epoch current)          → REJECT(cooldown)      [capacity]
  if rt:replica:{id}:inflight >= per_replica_concurrency      → REJECT(concurrency)   [capacity]

  # ---- commit ----
  INCR replica inflight
  INCR user inflight
  advance GCRA states (reserve est_tokens)
  SET rt:reserve:{request_id}
  → ADMIT
```

The `settle()` function performs the inverse compensating transaction:

- Takes `request_id` and `actual_tokens` as input
- Read the reservation; if missing: no-op & return
- Decrement replica inflight
- Decrement `quota:{model}:{user}` inflight
- Apply correction (`actual_tokens - est_tokens`) to GCRA
- Delete `rt:reserve{request_id}`
- Remove `request_id` from `rt:reserve:deadlines` index

This cleanup is atomic/idempotent and occurs naturally, for 99.9% of requests,
using `try/finally` semantics in the request path. Crashed workers and leaked
reservations are caught using a sweeper that periodically checks `ZRANGEBYSCORE
rt:reserve:deadlines {PAST-DUE}`: this surfaces reservations past the deadline
that must have expired.

Capacity rejects increment `demand:{model}:capacity_rejects_1m` and let the Tier Router try the next replica/tier; quota rejects return **429 + Retry-After computed from GCRA refill** and never touch demand.
- **Reserve-then-settle:** `est_tokens` = prompt-size heuristic + `max_tokens`.  `settle()` applies (actual − reserved) to the GCRA state, decrements both inflight counters, decrements demand, deletes the reservation. Without reservation, N concurrent agentic requests admit before any reports usage and blow through quota.
    - The token bucket depth is matched to the max context window for the model: in a single burst request, a user can fill the model's context.
    - The refill rate dictates how frequently a user can perform such a request (e.g. 128k context per minute)
- **Non-LLM tasks:** same script with `est_tokens = 0` (concurrency + RPM only) — equal-footing admission, LLM-only accounting skipped.

### GCRA quota mechanics

- One Redis key per (model, user, resource) storing the theoretical-arrival-time; standard GCRA in a few Lua lines.
- Parameters from model config: `tpm` → refill rate (tpm/60 per second); `burst_tokens` → bucket depth. Agentic pattern: idle "thinking" phases accrue credit; a large tool-result turn spends it; sustained rate stays capped at tpm.
- On rejection the script returns seconds-until-conformant → `Retry-After`. Stock OpenAI/Anthropic SDKs back off automatically on 429 + Retry-After, so well-behaved agents self-pace with zero custom client code.

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
- `/api/catalog/v1/*`: full metadata: deployments with replica counts/phases; snapshot + selected `rt:*`/`demand:*` reads.
- Prometheus per worker: request counts/latency by model/deployment/task, TTFB, inter-chunk latency, token counters, **admission rejects by reason (capacity vs each quota type)**,  demand gauges, translation-path counter (should trend → 0), snapshot version/age.
- `/readyz` = snapshot loaded ∧ Redis reachable.

## Request Lifecycles (reference walkthroughs)

**A. Streaming (common case):** auth → classify (raw bytes kept) → router picks replica → Lua admit (quota then capacity; reserves tokens, bumps demand) → byte passthrough with SSE tap → usage frame parsed → settle Lua (correct GCRA, decrement inflight/demand) → metrics enqueued.

**B. Capacity exhausted:** admit rejects capacity on all replicas (or zero healthy replicas) → overload handler: 429 + Retry-After ≈ ETA; capacity-reject counted in demand → autoscaler sees sustained demand → wakes deployment → later retries admit normally.

**C. User over token quota:** GCRA reject → 429 + Retry-After from refill math; demand untouched; SDK backs off; no scaling triggered.

**D. Replica failure:** replica starts erroring → router cooldown benches the replica; traffic flows to siblings → control plane's health check withdraws the replica seconds later → its rt: keys expire via TTL; relaunch arrives as a fresh replica ID.

