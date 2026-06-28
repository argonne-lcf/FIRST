# Declarative Configuration

FIRST is configured declaratively. Admins write YAML manifests describing
the **desired state** of the system; control loops are responsible for
making reality match.

## Spec and Status

Every resource is split into two halves:

| Half | Owner | Lives in |
|---|---|---|
| **Spec** | Admin-authored | YAML manifests checked into git and applied to Postgres |
| **Status** | Control-loop-authored | Postgres and Redis |

The split gives us a clean separation of concerns:

- **Admins only write Spec.** A `kubectl apply`-style flow takes a
  directory of manifests, diffs them against current Spec, and applies
  the delta.
- **Controllers only write Status.** Each controller owns a specific
  set of status fields on the resources it reconciles. See
  [Controller Framework](controllers.md) for the locking discipline.
- **Reads return both.** Admin views join Spec and Status so you can see
  what was requested vs. what's actually running.

A field lives in exactly one half, so it is either admin-writable or
system-managed — never ambiguously both. A `StaticDeployment`'s `api_url`
is Spec; its `health` is Status. The apply path can only touch Spec; the
controllers can only touch Status.

In practice this looks like:

```bash
# Diff and apply YAML manifests against the running gateway:
alcf-ai admin apply tests/resource_specs/baseline/

# Inspect what's currently configured (Spec) vs. what's live (Status):
alcf-ai admin audit
alcf-ai clusters get sophia
```

## Apply mechanics

A resource is matched across applies by its `kind` and `name`. Given the
incoming manifest set and the stored state, the apply algorithm does one
of three things per resource:

- **Create** — present in the manifest but absent from the DB. The
  incoming Spec is materialized with defaults and inserted; Status is
  initialized empty/unknown.
- **Update** — present in both. The incoming Spec is materialized with
  defaults and diffed against the stored Spec. A real diff triggers an
  in-place update; Status is untouched.
- **Delete** — present in the DB but absent from the manifest. Marked
  for deletion and torn down (see [Controller Framework](controllers.md)
  for soft-delete and finalizer mechanics).

Editing a `name` is **not** a rename: it destroys the old resource and
creates a new, unrelated one.

Apply is fully declarative — **there is no PATCH model and no partial
apply**. The manifest is authoritative; anything not in it does not
exist.

### Plan / Apply protocol

The CLI runs two HTTP calls so the user can review changes before
committing them, and so that concurrent admins cannot stomp each other:

1. `POST /resources/plan` with the manifests → returns a
   `ResourceChangePlan` (`previous_version`, `to_add`, `to_update`,
   `to_delete`, `no_change`).
2. `POST /resources/apply` with the manifests **and** the approved plan.

`apply_plan` (in `first_gateway.services.plan_apply`) then:

- Records a new `ConfigVersion` row keyed at `previous_version + 1`. The
  PK uniqueness gives optimistic concurrency for free — if another admin
  committed in between, `IntegrityError` becomes a `SpecApplyError`
  with HTTP 409.
- Re-runs `create_plan` against the now-locked transaction and aborts
  with `SpecApplyError` if the recomputed plan diverges from the
  approved one.
- Dispatches creates/updates/deletes through `models.resource_registry`
  (the ORM auto-registers each `ResourceRow` subclass by name, so adding
  a new resource kind is just defining a new `Spec` and a new
  `ResourceRow`).

Every applied `ConfigVersion` keeps a JSONB snapshot of `changes` for
audit; `GET /resources/config-versions/{uid}` returns one.

### Vignette: zero-downtime vLLM upgrade via canary

To upgrade vLLM on Sophia with no downtime and no SSH:

1. Add a second `PilotDeployment` on Sophia pinned to the new vLLM (a new
   `PilotLaunchSpec`), with `weight: 1` as a canary alongside the
   existing `weight: 100`. **Apply.**
2. Watch metrics. Shift weight toward the new deployment across a
   sequence of applies.
3. Remove the old deployment from the YAML. **Apply** — it is torn down.

The entire migration is a series of reviewed git commits and applies. No
login-node edits, no endpoint restarts, no opaque per-cluster state. This
is the headline demonstration of what v2's declarative configuration buys
over v1.


## Why declarative

- **Reproducibility.** The whole production config lives in version
  control. A fresh gateway plus the manifests reproduces production.
- **Single writer per field.** Spec has exactly one writer (the admin
  apply flow); each Status field has exactly one controller writing it.
  No two actors race on the same column.
- **Crash-safe.** Controllers are level-triggered — they re-read current
  state and recompute the next step on every reconcile, so a crash
  mid-operation just retries cleanly.

See the [Controller Framework](controllers.md) for the reconcile-loop
mechanics that make this work, and the [Data Model](data-model.md) for
the resource types that get Spec/Status pairs.

## Sample Resource

```yaml
kind: PilotDeployment
name: sophia/pilot/google/gemma-4-31B-it

spec:
  model_name: google/gemma-4-31B-it
  cluster_name: sophia

  router_params:
    weight: 1
    max_parallel_requests: 50

  min_replicas: 1
  max_replicas: 3

  scaling_strategy:
    strategy: LoadThresholdStrategy
    scale_up_interval_min: 120
    scale_down_age_min: 7200
    scaling_thresholds:
      - [0.0, 1]
      - [10.0, 2]
      - [20.0, 3]

  health_check_method: first_gateway.platforms.health.check_health_endpoint
  health_check_kwargs:
    timeout: 10
    health_path: "/health"

  prometheus_metrics_path: "/metrics"
  prometheus_scrape_interval_sec: 30

  launch_spec:
    served_model_name: google/gemma-4-31B-it
    num_nodes: 1
    gpus_per_node: 8

    venv_path: /lus/eagle/projects/inference_service/env/vllm-0.19.0
    weights_path: /eagle/inference_service/model_weights/google/gemma-4-31B-it
    weights_cache_path: /raid/scratch/inference_service/model_weights/google/gemma-4-31B-it

    log_dir: /eagle/inference-service/logs/
    max_startup_time: 500
    health_path: /health

    env:
      HTTP_PROXY: "http://proxy.alcf.anl.gov:3128"
      HTTPS_PROXY: "http://proxy.alcf.anl.gov:3128"
      http_proxy: "http://proxy.alcf.anl.gov:3128"
      https_proxy: "http://proxy.alcf.anl.gov:3128"
      ftp_proxy: "http://proxy.alcf.anl.gov:3128"
      TRANSFORMERS_OFFLINE: "1"
      OMP_NUM_THREADS: "4"
      VLLM_LOG_LEVEL: "WARN"
      USE_FASTSAFETENSOR: "true"
      VLLM_IMAGE_FETCH_TIMEOUT: "60"
      VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE: "shm"
      TORCHINDUCTOR_CACHE_DIR: "/raid/scratch/inference_service/model_weights/.cache/torch_inductor"
      VLLM_CACHE_ROOT: "/raid/scratch/inference_service/model_weights/.cache/vllm"
      TRITON_CACHE_DIR: "/raid/scratch/inference_service/model_weights/.cache/triton"

    serve_script_template: |
      #!/bin/bash
      set -euo pipefail

      ulimit -c unlimited
      mkdir -p "$TORCHINDUCTOR_CACHE_DIR" "$VLLM_CACHE_ROOT" "$TRITON_CACHE_DIR"

      source {{ quote(venv_path) }}/bin/activate

      exec vllm serve {{ quote(weights_cache_path) }} \
        --served-model-name {{ quote(served_model_name) }} \
        --host 127.0.0.1 \
        --port {{ port }} \
        --enable-auto-tool-choice \
        --tool-call-parser gemma4 \
        --reasoning-parser gemma4 \
        --async-scheduling \
        --tensor-parallel-size {{ gpus_per_node }} \
        --max-model-len 262144 \
        --trust-remote-code \
        --gpu-memory-utilization 0.9
```