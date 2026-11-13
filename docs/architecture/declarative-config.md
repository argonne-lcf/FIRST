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
first-admin apply tests/resource_specs/baseline/

# Inspect what's currently configured (Spec) vs. what's live (Status):
first-admin audit
alcf-ai clusters get sophia
```

## Scoped operator changes

`plan DIRECTORY` and `apply DIRECTORY` treat the directory as a **complete
catalog**: omitted resources are deleted. Never use a one-model directory as
an authoritative catalog or point at a parent containing multiple environments.

For routine additions or updates, select the exact resources to overlay on a
fresh complete live catalog. The gateway URL serves catalog reads; the separate
controller URL serves authenticated admin operations. Select both explicitly:

```bash
first-admin --base-url http://localhost:9100 --gateway-url http://localhost:8000 \
  export --output ./before-change

first-admin --base-url http://localhost:9100 --gateway-url http://localhost:8000 \
  plan ./manifests/dev \
  --only Model.example/model \
  --only PilotDeployment.tara/example-model \
  --out ./reviewed-plan.json

first-admin --base-url http://localhost:9100 --gateway-url http://localhost:8000 \
  apply --plan ./reviewed-plan.json
```

These are examples for an existing local forwarding arrangement, not universal
Dev/Production addresses. Use the approved URLs for the intended environment.
All operations retain normal authentication and permissions.

The export creates `catalog.yaml`, `runtime-state.json` and `export.json` in a
new directory; it refuses to overwrite an earlier export. It projects catalog
reads back to exact Spec fields. Manual desired replica counts and other runtime
observations are recorded separately and are not silently turned into manifests.
The new directory is private (0700), and export/saved-plan files are private
(0600). Catalogs may contain credentials: share them only through approved means.
ConfigVersion is checked around the reads, with at most three attempts. Runtime
counters/counts can still change independently and are explicitly non-atomic.
A no-change server plan verifies that the selected gateway catalog agrees with
the selected controller before it becomes an overlay base.

`--only` may be repeated. Only selected identities are taken from the source
directory; unrelated live specs remain unchanged. Required new parent resources
must be selected explicitly—dependencies are not silently imported. Deletions
are not supported in scoped mode. Deliberate removal still requires reviewing
a complete-catalog plan. Model and template selections also list dependent
deployments, including those on other clusters; a shared Model edit affects all
of those deployments.

`--out` saves the complete assembled resources, exact approved plan/base version,
target identities and both service URLs. `apply --plan` sends that payload without
replanning or rebasing. Different URLs, stale ConfigVersions and divergent plans
are rejected. Export and review a new plan after a conflict. Existing
`apply DIRECTORY` retains its interactive complete-catalog behavior.

A successful apply changes configuration, not existing model processes. Pending
or placed replicas resolve current configuration when they launch. Already-running
replicas keep their launch configuration until explicitly replaced. Applying a
spec also resets the affected reconcile/failure latches; it is not a read-only
operation. Manual desired counts remain runtime state, and `max_replicas` remains
an autoscaling bound rather than a limit to synchronize with a manual count.

Inspect one deployment or wait with an explicit deadline:

```bash
first-admin --base-url http://localhost:9100 --gateway-url http://localhost:8000 \
  status tara/example-model --wait-ready 1 --timeout 900 --json
```

Status joins replica, pilot, scheduler job, manager health, allocation and log
references. A running PBS job is not a reachable pilot manager; a reachable
manager is not a ready model. `state_ready_replicas` is a state observation, **not
a routability count**: the current catalog read schema does not identify every
draining-ready replica. Successful routed requests and independently observed
process/GPU/socket cleanup remain separate qualification requirements. Waiting
for zero state-ready replicas likewise does not prove cleanup. No command above
establishes the exact running source revision from a checkout HEAD.

## Access groups

Every `AccessGroup` must explicitly provide both `allowed_groups` and
`allowed_domains`. Missing or null fields fail validation; neither silently
becomes unrestricted. An empty list intentionally removes that restriction:

```yaml
kind: AccessGroup
name: public
spec:
  allowed_groups: []
  allowed_domains: []
```

A model's `access_group_name` must name an `AccessGroup` in the same complete
desired-state catalog, or `plan` rejects it. These per-model restrictions are
separate from the global Globus startup gates. See the
[V1-to-V2 auth cutover checklist](../deployment/docker.md#v1-to-v2-auth-cutover-gate).

## Launch templates

`LaunchTemplate` is a named resource holding the serve and optional pre/post-stop
scripts for a family of `PilotDeployment`s, along with the typed `parameters`
each deployment must supply and lifecycle defaults (`env`, `health_check`,
`max_startup_sec`, stop timeouts) that a deployment may override. Every
`PilotDeployment` references exactly one template by `launch_template_name`.

```yaml
kind: LaunchTemplate
name: vllm
spec:
  parameters:
    weights_path:
      type: str
      required: true
    max_num_seqs:
      type: int
      default: 32
      minimum: 1
  max_startup_sec: 900
  health_check:
    url: http://localhost/health
  serve_script_template: |
    exec vllm serve {{ parameters.weights_path | quote }} \
      --served-model-name {{ runtime.served_model_name | quote }} \
      --uds {{ runtime.uds | quote }} \
      --tensor-parallel-size {{ runtime.gpus_per_node }} \
      --max-model-len {{ runtime.max_model_len }} \
      --max-num-seqs {{ parameters.max_num_seqs }}
---
kind: PilotDeployment
name: tara/inkling-bf16
spec:
  cluster_name: tara
  model_name: inkling-bf16
  launch_template_name: vllm
  launch_spec:
    served_model_name: thinkingmachines/Inkling
    num_nodes: 8
    gpus_per_node: 4
    parameters:
      weights_path: /immutable/weights/inkling-bf16
```

Parameter types are `str`, `int`, `float` and `json`. Numeric types support
optional `minimum`/`maximum` bounds. Types are strict (no string-to-number
coercion) and unknown parameters are errors. A null or omitted value inherits
the template default; a `required` parameter with no default must be supplied.

`json` accepts a native YAML object or array, including nested booleans and
nulls—not a string containing JSON. FIRST preserves that structure in the
catalog, then encodes it once as canonical JSON when resolving the launch
parameter. The pilot still receives a scalar string. Quote the resolved value
directly; applying `tojson` again would double-encode it:

```yaml
parameters:
  configuration:
    type: json
    required: true
serve_script_template: |
  exec shared-launcher --configuration {{ parameters.configuration | quote }}
```

FIRST validates JSON representation and template resolution. The shared
launcher's existing configuration validator remains responsible for supported
model arguments, topology and protected settings before model initialization.
No new database column or pilot parameter-wire format is required.

Templates are Jinja2 with a strict context of three names:

- `parameters.*` — the resolved parameter values.
- `runtime.*` — values the pilot assigns at launch: `replica_name`,
  `served_model_name`, `uds`, `num_nodes`, `gpus_per_node`, `gpus_by_host`,
  the merged `env`, and the parent Model's `max_model_len` (null when the
  Model omits it, so guard with `{% if runtime.max_model_len %}` in that case).
- `quote` — `shlex.quote`, usable as `{{ value | quote }}` or `{{ quote(value) }}`.

At apply time every template is rendered once against a sample context, so a
typo, an unknown filter or a syntax error is rejected before it can reach a
pilot. Plan/apply also resolves every deployment against its template, so a
template edit that breaks a dependent deployment fails the plan. A template
edit does **not** restart running replicas: scale the deployment down and back
up when you need a rollout. A template that is still referenced cannot be
deleted.

The admin endpoint `GET /catalog/v1/launch-templates` lists templates.
Deployment detail responses expose the deployment's `launch_spec`, including
parameter values, so do not put credentials in parameters.

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

1. `POST /control/v1/plan` with the manifests → returns a
   `ResourceChangePlan` (`previous_version`, `to_add`, `to_update`,
   `to_delete`, `no_change`).
2. `POST /control/v1/apply` with the manifests **and** the approved plan.

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
audit; `GET /control/v1/config-versions/{uid}` returns one.

### Vignette: zero-downtime vLLM upgrade via canary

To upgrade vLLM on Sophia with no downtime and no SSH:

1. Add a second `PilotDeployment` on Sophia pinned to the new vLLM (a new
   `LaunchTemplate`), with `weight: 1` as a canary alongside the
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

## Sample Resources

```yaml
kind: LaunchTemplate
name: vllm-sophia
spec:
  parameters:
    venv_path:
      type: str
      required: true
    weights_cache_path:
      type: str
      required: true
  max_startup_sec: 500
  health_check:
    url: "http://localhost/health"
  env:
    HTTP_PROXY: "http://proxy.alcf.anl.gov:3128"
    HTTPS_PROXY: "http://proxy.alcf.anl.gov:3128"
    TRANSFORMERS_OFFLINE: "1"
    TORCHINDUCTOR_CACHE_DIR: "/raid/scratch/inference_service/model_weights/.cache/torch_inductor"
    VLLM_CACHE_ROOT: "/raid/scratch/inference_service/model_weights/.cache/vllm"
    TRITON_CACHE_DIR: "/raid/scratch/inference_service/model_weights/.cache/triton"
  serve_script_template: |
    #!/bin/bash
    set -euo pipefail

    ulimit -c unlimited
    mkdir -p "$TORCHINDUCTOR_CACHE_DIR" "$VLLM_CACHE_ROOT" "$TRITON_CACHE_DIR"

    source {{ parameters.venv_path | quote }}/bin/activate

    exec vllm serve {{ parameters.weights_cache_path | quote }} \
      --served-model-name {{ runtime.served_model_name | quote }} \
      --uds {{ runtime.uds | quote }} \
      --enable-auto-tool-choice \
      --tool-call-parser gemma4 \
      --reasoning-parser gemma4 \
      --async-scheduling \
      --tensor-parallel-size {{ runtime.gpus_per_node }} \
      --max-model-len {{ runtime.max_model_len }} \
      --trust-remote-code \
      --gpu-memory-utilization 0.9

---

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
    strategy: DemandThresholdStrategy
    scale_up_interval_min: 120
    scale_down_age_min: 7200
    scaling_thresholds:
      - [0.0, 1]
      - [10.0, 2]
      - [20.0, 3]

  prometheus_metrics_path: "/metrics"
  prometheus_scrape_interval_sec: 30

  launch_template_name: vllm-sophia
  launch_spec:
    served_model_name: google/gemma-4-31B-it
    num_nodes: 1
    gpus_per_node: 8
    parameters:
      venv_path: /lus/eagle/projects/inference_service/env/vllm-0.19.0
      weights_cache_path: /raid/scratch/inference_service/model_weights/google/gemma-4-31B-it
```
