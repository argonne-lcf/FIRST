# Controller Framework

FIRST allows admins to declaratively configure models with access controls,
routing policies, and multi-cluster HPC deployments.  The controllers work
continuously in the background to ensure that these deployments are enacted and
healthy.

The controller manager is a single asyncio process that hosts every controller
as one or more coroutines. There is no controller-side scaling: one process is
plenty for thousands of resources, and the data plane (API servers) is
completely independent — a wholly-down controller manager does not drop user
traffic, it just means new resources aren't reconciled until it comes back.

The next sections describe the controller framework. The actual list of
controllers FIRST will ship lives in [FIRST Controllers](#first-controllers).

## Concurrency Control

### Manager Lease

Because the controller manager is the only writer for controller-owned fields,
we need to make sure no two manager processes ever run at once.
The manager grabs a singleton lease in Postgres at startup (see `ManagerLease` in
`first_gateway.controllers.lease`).

1. On startup, attempts to claim the lease (insert or take over an expired one).
   If it can't, refuses to start any controllers and exits — supervisor (e.g.
   docker) will restart and retry.
2. Runs a single renewal coroutine that refreshes `renewed_at` every 10s.
3. If two consecutive renewals fail (network blip, contention, db down), the
   manager *kills the process* (`os._exit(1)`).

### Premised Updates

Multiple controllers (and the manager itself) may write to disjoint fields of
the same row. A single `version` column is too coarse — every reconcile would
trip every other reconciler's optimistic check, even when the changes are
unrelated.  Rules:

- Updates are incisive: only flush changes to the necessary columns.
- Updates should include in the `WHERE` clause the premises the decision was based on.
If the premise is no longer true, the UPDATE affects zero rows, which is detected and
logged as a stale update.  The next iteration reads fresh state and tries again. `IS DISTINCT FROM`
checks in the `WHERE` clause are particularly useful to prevent firing updates based on
stale premises.
- Prefer small, short-lived transactions that update one resource to avoid deadlocks
and lock contention.
- If a bulk update or multi-row `SELECT .. FOR UPDATE` is used, ensure the
rows are ordered by UID and the bulk update includes the premise of the
decision.


## Reconcile Loop

The whole design is **level-triggered**: every reconcile reads fresh state
from Postgres, decides what one step to take, takes it, writes back. Crashes,
duplicate events, missed events, and stale caches are all recovered by "the
next reconcile sees the truth and does the right thing." Edge-triggered
designs (act-on-event-X) are avoided in critical pathways.


### Poll from Postgres; Redis PubSub is just a wakeup hint

Each controller's reconcile loop is straightforward:

```text
loop forever:
    beat()
    ids = SELECT uid FROM <table> WHERE <list_actionable predicate>
    for id in ids:
        reconcile(id)
        beat()
    wait up to poll_interval seconds OR until LISTEN notification
```

- **Full resync** every `poll_interval` (default 30s) is mandatory for
  correctness.
- **Redis PubSub (WakeupDispatcher)** just shortens the resync wait when a relevant row
  changes.

If a controller is overwhelmed (its `for` loop takes longer than
`poll_interval`), no harm done — it just runs back-to-back without
sleeping. The `controller_poll_interval_used_fraction` gauge tracks this
so we notice before it matters.

### Heartbeats per loop

A controller may have several concurrent coroutines (the reconcile loop, the
resync polling sub-task, etc). A single shared `update_heartbeat()` would
mask a wedged sub-task. Instead, each spawned loop registers its own named
heartbeat token via `Worker.register_heartbeat()`, and the heartbeat
monitor in `ControllerManager._heartbeat_monitor()` cancels a worker if
any of its registered beats go stale.

### Per-resource backoff and giving up

We don't keep retry state in memory. Instead, we track it on the resource
itself. These columns are defined on the `ResourceRow` base class in
`first_gateway.database.models` and inherited by every controller-managed
table:

| Column | Type |
|---|---|
| `reconcile_failures` | `integer NOT NULL DEFAULT 0` |
| `reconcile_last_error` | `text` |
| `reconcile_retry_at` | `timestamptz` |

After each reconcile, the `Controller` base class writes back:

- success: `reconcile_failures=0, retry_at=NULL`
- failure: `reconcile_failures+=1, last_error=str(exc),
  retry_at = now() + backoff(failures)` (capped at `max_backoff`, default
  1 hour)

The backoff cap is what keeps persistently broken resources out of the hot
loop: once `failures` is large enough that `backoff(failures) >= max_backoff`,
every retry is scheduled an hour out. The `list_actionable` predicate filters
on `retry_at`, so a stuck row is reconsidered ~once per hour forever.
Transient platform breakage self-heals; persistent breakage stays cold but
is never permanently abandoned.

`reconcile_failures` is a running total, not a state flag — it keeps
climbing past the cap (9, 10, 11, ...) at the hourly cadence. Treat
`reconcile_failures >= 8` (or whatever threshold) as the "stuck" signal
for dashboards and alerts.

Resolution path for a stuck resource:

1. Operator sees the resource in the dashboard with a high
   `reconcile_failures` and `reconcile_last_error` shown verbatim.
2. They either:
    - **Fix in place**: edit the spec (e.g. correct `launch_spec`). The
     spec-apply path resets `reconcile_failures=0, retry_at=NULL`
     atomically with the spec change.
    - **Manually retry now**: `alcf-ai admin reconcile-reset <resource>` —
     same reset, no spec change. Useful when the fix was external (cluster
     filesystem permissions, etc).

Separately, `PilotDeployment.consecutive_launch_failures` counts the number of `PilotReplicas` that timed out or failed in a row for each deployment. When counter crosses a limit, the `desired_count` is pinned to 0, preventing auto-scaling or new `PilotReplicas` from starting.

This mechanism is deliberately separate from the `reconcile_failures` counter, because the error is external (not a true error in the controller) and it requires accumulating faults from replicas on the same parent resource. The counter is reset whenever a deployment succeeds or the `PilotDeployment` spec is updated.

### Reconcile function rules

- **Level-triggered.** Re-read current state from Postgres; act on what *is*, not what *changed*. If a controller crashes mid-step, the next reconcile resumes from whatever state the DB reflects.
- **Each external side effect must be idempotent.** For
  schedulers without idempotency keys (PBS): use a deterministic job name
  (`<first-pilot-prefix><resource-name>`), `qstat` to check, then `qsub` only on
  absence. Mutual exclusion is provided by the manager's single-coroutine
  rule above.
- **One step per reconcile.** If a job goes through `pending_submit ->
  submitted -> running`, do one transition per reconcile. Write back state,
  return. Next reconcile picks up the next step. Each step is independently
  recoverable.
- **Updates are premised.** See [Premised Updates](#premised-updates).
- **Postgres is the only state.** Controllers may cache nothing across
  reconcile invocations. (Redis is fine as a separate source of truth for
  high-churn fields — see below.)

### Controller base class

The `Controller` class lives in `first_gateway.controllers.controller`.

Subclasses set `resource_type` (the `ResourceRow` model class) and implement
two abstract methods:

- `list_actionable(sess) -> list[int]` — SQL query returning UIDs that need work.
- `reconcile(sess, uid)` — one step of work on a single resource.

The reconcile loop (`Controller.run`) registers a heartbeat, then loops:
query actionable rows, reconcile each one, sleep until the `poll_interval`
elapses or a `WakeupDispatcher` notification arrives. `_record_success`
resets the backoff columns; `_record_failure` increments them with a
single self-referential UPDATE. Prometheus
metrics are emitted for every reconcile attempt (see
[Observability](#observability)).

If a reconcile detects a stale premised update, it should raise `StaleReconcile`
to signal a non-failing stale outcome.

## Observers

An "observer" is just a direct `Worker` subclass that periodically reads external state (e.g. HPC scheduler) and syncs DB state.


## Soft Delete and Retention

`Cluster`, `Model`, `AccessGroup`, `PilotDeployment`, `StaticDeployment`: no
soft deletes or retention: these resources are fully-declarative and
hard-deleted as soon as the admin requests deletion. Cluster deletes cascade to
`PilotJob`.  PilotDeployment deletes cascade to `PilotReplica`.  The replica
reaper handles freeing up resources from orphaned replicas.

`PilotJob` and `PilotReplica` are controller-managed resources being continously
created and destroyed.  We utilize a soft-delete pattern with cleanup and
retention to ensure that resources are gracefully garbage-collected while
providing an operational view into the past ~7 days for postmortem.

We use a `SoftDeletable` mixin class in models.py to facilitate the same
soft-delete+sweep pattern across resources that are soft-deletable.

Flow:

1. Controller decides to `UPDATE ... SET scheduled_deletion_at = now()`.
2. The owning controller's `list_actionable` includes `scheduled_deletion_at =
   true` rows. On reconcile, it performs cleanup (terminate job, send
   stop signal to replica) and then sets `deleted_at = now()`.
3. A **retention sweeper** (a small `Worker`) runs every
   ~5 minutes and invokes the `sweep_expired()` method defined on the
   `SoftDeletable` mixin.
4. API views do not filter out `deleted_at`, so that a window of historical resources remains visible by default.


## Hybrid Postgres+Redis Status

We split state across two stores:

- **Postgres** holds the spec, semantically meaningful aggregated status
  (e.g. `health`, `state`), and anything controllers gate decisions on.
- **Redis** holds high-churn observational facts (`last_health_check`,
  `manager_health`, in-flight counts, load averages) that would otherwise
  spam triggers and balloon WAL.

The danger is Redis access scattered ad-hoc throughout the codebase.
We contain it with:

- Redis Key builder: centralized in `first_gateway.database.redis.keys`
- Router configuration managed in `first_gateway.database.redis.router_config`
- Admission controller logic / Lua scripts managed in `first_gateway.database.redis.admission`
- Cache and runtime state managed in `first_gateway.database.redis.repo.RedisRepo`

Runtime state is read from Redis and structured in `<Entity>Runtime` models
defined in `first_common.schema.resources.runtime`.  These classes are used to
delineate runtime state that changes frequently and can be fetched independently
of ORM data.

## Observability

The manager process exposes a small FastAPI on a local port (e.g.
`127.0.0.1:9100`) with three routes:

- `GET /healthz` — returns 200 iff every registered `Worker` has a fresh
  heartbeat across all its named beats. Used as the docker healthcheck.
- `GET /metrics` — Prometheus exposition format, emitted by `prometheus_client`.
- `GET /controllers` — for each worker: name, status (running/restarting),
  named heartbeats with seconds-since-last-beat, last error, restart count.

The following metrics are defined in `first_gateway.controllers.controller`
and `first_gateway.controllers.worker`:

| Metric | Type | Labels |
|---|---|---|
| `controller_reconcile_total` | counter | controller, outcome (`success`/`failure`/`stale`) |
| `controller_reconcile_duration_seconds` | histogram | controller |
| `controller_poll_interval_used_fraction` | gauge | controller |
| `controller_actionable_rows` | gauge | controller |
| `controller_worker_restarts_total` | counter | worker |
| `controller_seconds_since_last_resync` | gauge | controller |
| `controller_premised_update_stale_total` | counter | controller, table |

Logging is the primary debugging surface: structured JSONL via the existing
`first_gateway.log_config`.

The admin dashboard polls `/controllers` and renders a status pane next
to the resource list. Prometheus (run separately in our deployment) scrapes
`/metrics`, alongside the vLLM `/metrics` endpoints exposed via dynamic
service discovery from the router config.

The metrics port is bound to localhost only; in production we run behind a
reverse proxy that mediates access. No external auth needed on the metrics
endpoint itself.

## Pause and Drain

Two existing knobs cover the maintenance story:

- **Drain a deployment**: set `desired_replicas = 0` on a `PilotDeployment`.
  Replica Drainer marks replicas for deletion, router config controller
  removes them from rotation, replicas terminate in order.
- **Disable a whole cluster**: set `maintenance_notice` on the `Cluster`.
  The router config controller drops all deployments tied to that cluster
  from the data plane, so user traffic immediately routes to other clusters
  (or 503s if none remain).

Neither requires a special "controllers paused" mode. Restarting the manager
is also safe at any time — premised updates + level-triggered reconcile
mean an interrupted reconcile is just resumed by the next one.

## FIRST Controllers

The list below uses the conventions established above.

Before diving into the controller details, let's trace through the stages involved from "cold power-on" to "model is live":

1. An AutoScaler sets desired_replicas=1 on a PilotDeployment
2. The Replica Reconciler inserts a new PilotReplica
3. The Replica Placement Controller sees no PilotJobs and creates one, scheduling the Replica onto the future PilotJob.
4. The Pilot Job Controller enqueues the job that’s pending submit
5. The PilotJob Observer discovers the job has started running and sets the running manager URL
6. The Replica Launch Controller finally sees that the resources are available and the Replica is launched
7. The Pilot Replica Status Observer discovers that the replica has started successfully and populates the model_url
8. The Router Config Controller sees the deployment with a live replica and updates the global router configuration.

After this, the APIServer reacts to the router change notification and updates
its in-memory Router structure to proxy inference traffic to the new Replica.
The Redis Pubsub layer ensures that end-to-end startup proceeds faster than it
would with 9 independent sleep/polling loops.


### Health Observer
- Polls each `Cluster`'s and `StaticDeployment`'s configured `health_check`
endpoint via `perform_health_check`.
- Postgres write: `<ResourceCls>.health` (only on transition).

### Router Config Observer
- Interface to [data plane](request-routing.md): the router config is
published by the control plane and consumed by the apiserver workers of the data plane.
- Watches all of: `pilot_deployment`, `static_deployment`,
  `pilot_replica`, `model`, `access_group`, and `cluster`
  (`maintenance_notice`).
- Modeled as an `Observer`, not a `Controller`: there is one global
  router config, not a per-resource reconcile, and the work is
  "read Postgres, write Redis". The Controller base class
  (per-resource `list_actionable` + `reconcile(uid)`) doesn't fit and
  shouldn't be shoehorned in.
- On wake (any watched table changes, or every poll interval),
  rebuild the router config end-to-end from current Postgres state
  and write the result to a single Redis key.
- API servers `SUBSCRIBE` (or simply poll) that key and hot-swap their
  in-memory LiteLLM router on change.
- The rebuilt config excludes:
  - Deployments whose cluster has `maintenance_notice` set.
  - Replicas in `pending`, `terminated`, or with `scheduled_deletion_at`.
  - Replicas whose parent `PilotJob.manager_health != healthy`.
- The router config is keyed on `Model.name` and provides the full map of:
    - Model aliases: models may declare multiple non-overlapping alias names
    that resolve to the canonical name in the router.
    - Live deployment endpoints and corresponding routing parameters
    - Access Group information for pre-flight authorization

### PilotJob Observer

The PilotJob observer reads state from the cluster's job queue and discovers
pilot job manager endpoints.

At each polling iteration, it:

- Uses each Cluster with a `pilot_system` to construct the corresponding `SchedulerAdapter` (`first_gateway.platforms.schedulers.build_scheduler`) and
`first_gateway.services.pilot_submitter.PilotSubmitter` instance.
- Invokes `PilotSubmitter.get_statuses()` for each cluster.
    - Jobs from the scheduler are matched to known `PilotJob` instances in the database using `scheduler_job_id`
    - For each known `PilotJob`: premised UPDATE of
      `scheduler_state`, `time_started` (`IS DISTINCT FROM` per field).
    - For each **orphan** — a scheduler job whose name starts with
    `PilotConfig.job_name_prefix` but has no matching `PilotJob` row — issues
    `SchedulerAdapter.terminate_job(scheduler_job_id)` directly. The observer
    owns the prefix namespace and reaps orphans. No DB rows are affected, no
    zombie state exists. Log every orphan reap at INFO so operators can see it
    in `docker compose logs`.

Once the HPC job scheduler statuses are reconciled, the observer identifies actionable `PilotJob`s where `scheduler_state = running` and `manager_url IS NULL`.
These jobs require manager endpoint discovery.  If there is at least one actionable job, proceed to:

- Use `PilotSubmitter.list_ready_endpoints()` to list the readyfiles that currently exist.  Intersect the existing set with the set of actionable jobs: any jobs in this intersection are ready to have `manager_url` updated.
- For each of the ready jobs, use `PilotSubmitter.get_endpoint()` to discover the job's `AddressInfo`.  Log the discovered info and UPDATE the `AddressInfo.base_url` on `PilotJob.manager_url` to store the discovered endpoint.

### Pilot Replica Observer
- `list_actionable` (Postgres): `PilotJob` where `state = running` AND `manager_url IS NOT NULL`.
- Per job: calls `GET /status` on the pilot manager.
  - Postgres writes (premised, only on change): `PilotJob.resources`,
    `PilotJob.manager_health`, `PilotJob.manager_unhealthy_since` (set
    to `now()` on first unhealthy observation, NULL on healthy),
    `PilotJob.idle_since` (set to `now()` iff currently NULL and zero
    replicas running; set to NULL iff any replica running). Per-replica
    `PilotReplica.model_url`, `PilotReplica.observed_served_name`, `PilotReplica.state`, `PilotReplica.state_message`,
    `PilotReplica.started_at`.  Do a single row premised update per DB transaction;
    only create transactions if an update is necessary.
  - **Reap orphans**: replicas appearing in pilot manager `/status` with
    no matching `PilotReplica` row, or with a row that has a non-matching
    Pilot Job FK. Re-verify replica does not exist in DB and then issue `stop-replica`
    API control command immediately. (Consider a replica
    that is placed on PilotJob 1, then a transient DB error occurs so
    the placement is never recorded, and finally the replica is placed
    again on PilotJob 2. Now the same replica name exists in two pilot
    jobs. The first replica on Pilot Job 1 is unregistered and should
    be reaped.)
- Group successful startups and failures by PilotDeployment.  For each PilotDeployment,
update `consecutive_launch_failures` (incrementing per failed or timed-out replica and resetting to 0 on success)
   - Success reset only happens when a replica transitions to `ready`
   - Failure is counted if the launch HTTP request fails or the replica state
   transitions to `error` or `start_timeout`
- Calculate and write `PilotDeployment.state` as the aggregated `PilotDeploymentState` based on
live replica runtime states and the controller state (desired replica count, recent launch failures)

### Inflight Count Observer

Cron job: every ~15 minutes use
`AdmissionController.repair_orphaned_zsets()` to remove orphaned members of the inflight sorted sets.
Should always be a no-op under correct operation of the service. This
is merely a backstop for operational errors (accidental corruption of data in
Redis; restoring from stale backup; future development introducing buggy TTLs,
etc...).  By recounting, fixing, and alarming on detected drift, we add a layer
of defense to what should otherwise remain consistent on its own.


### PilotJob Controller
- `list_actionable`:
  ```sql
  SELECT uid FROM pilot_job
   WHERE (reconcile_retry_at IS NULL OR reconcile_retry_at < now())
     AND (deleted_at IS NULL)
     AND (
         scheduled_deletion_at IS NOT NULL
         OR state NOT IN ('queued', 'starting', 'running')
         OR (idle_since IS NOT NULL)
         OR (manager_health = 'unhealthy')
     );
  ```
- `reconcile`:
  1. If `scheduled_deletion_at`: terminate via scheduler, set
     `state = terminated`, set `deleted_at = now()`. (Cascading
     `scheduled_deletion_at` to assigned replicas is the Replica
     Reconciler's job — it picks up replicas whose parent job is in a
     terminal or deleting state.)
  2. If state is terminal: set `scheduled_deletion_at` and return.
  3. If `idle_since` exceeds the cluster's threshold: set
     `scheduled_deletion_at = now()` and return — the next reconcile handles
     teardown.
  4. If manager has been unhealthy (control APIs not responding with 200s) past debounce: set
     `scheduled_deletion_at = now()` and return.
  5. If `state = pending_submit`: check cluster's pilot_system
     `max_concurrent_jobs` and `max_num_nodes`. If all pending/submitted/starting/running jobs are
     under the caps, `PilotSubmitter.submit()`, record `scheduler_job_id`, advance state.

To make job submission idempotent in the face of crash/retry, submission should use be wrapped in the following pattern:

- `qstat` to verify that the job of the given name is not already scheduled.  If scheduled, record the Scheduler Job
ID and return.
- `qsub` only if the job was truly absent from the previous step.

- Writes: `PilotJob.scheduler_state`, `PilotJob.scheduler_job_id`,
  `PilotJob.scheduled_deletion_at` (self-set on idle/unhealthy timeout),
  `PilotJob.deleted_at`.



### Pilot Replica Reconciler

Sole writer of `PilotReplica.scheduled_deletion_at` and inserter of new PilotReplica rows.
All conditions that should drain a replica or free its resources funnel through this controller.
The reconciler drives observed count toward `desired_replicas`.

The reconciler reads all entries from `PilotDeployment`, fetching related
`PilotReplica`s and their assigned `PilotJob`s. It wakes on
`PilotDeployment.desired_replicas` changes. Filter for deployments where
`reconcile_retry_at` is null/past.

Load each deployment + its replicas (+ each replica's `pilot_job` state).
For each replica in the deployment that matches one of the following drain predicates, set `scheduled_deletion_at = now()` if not already set:

- Parent `PilotJob` is in a terminal state or has `scheduled_deletion_at`.
- Replica is in a terminal state, including `error` or `start_timeout`. The replica already stopped, but we must still free the resources through the drain pathway.

After draining the non-viable replicas above, commit the transaction.

Proceed to scan each PilotDeployment where `desired_replicas` differs from the current number of live/in-flight replicas that aren't soft-deleted or draining:

- If `num_live < desired`, use `PilotReplica.create(deploy.name)` to create `N = desired - live` new ones.
- If `live > desired`, pick `N = live - desired` to drain by priority:
    - `pending > placed > unhealthy > launching > ready`
    - tie-break: prefer to drain older replicas first (earlier `started_at`)
    - UPDATE `scheduled_deletion_at = now()` for the drained replicas.

This controller naturally supports rollouts of updated `PilotDeployments`: when
admins apply a spec, the running replicas will be stale but continue unaffected.
Admins can then temporarily use the `set-desired-replicas` API to spin up new
replicas over the current capacity.  Then, decreasing the desired count back to
the baseline causes the older stale replicas to get drained.  This enables a
zero-downtime rollout.

### Pilot Replica Placement Controller

Listener wakes on Replica creation. This Controller does not perform any RPC or
interact with the outside world; it is solely responsible for scheduling
PilotReplicas onto PilotJobs and creating new PilotJobs to meet demand up to
capacity limits.  All logic is Postgres state management.

`list_actionable`: `PilotReplica` where `state = pending` AND `scheduled_deletion_at IS NULL`.

Extract each Replica's `num_nodes` and `gpus_per_node` from the parent `PilotDeployment.launch_spec`.
Skip the replica if it's not pending or is now draining (`scheduled_deletion_at`).

Sort the `pending` Replicas using an effective submit time formula:
`t_eff = created_at − BETA * gpus_per_node`, ascending.
`BETA` is a hard-coded module-level constant equal to `timedelta(minutes=5)`.
It means that an 8-GPU job is treated as if submitted 40 minutes earlier than it was.
When many Replicas are created within a ~40 minute window, larger replicas get a head
start to facilitate bin packing efficiency.  At the same time, this heuristic ensures that
smaller replicas that have been waiting do not starve.
Multi-node replicas do not get priority over single-node replicas.

Reconcile handles the pending replicas in the above `t_eff` order to balance
fairness and sizing priority.  If the Replica is `pending`, attempt to bin-pack
it onto an existing `PilotJob` with enough free resources.


- Any `PilotJob` that is in an active/in-flight state (not `exiting` or `gone`)
and does not have `scheduled_deletion_at` is eligible for placement. This means
that replicas can be immediately placed onto pending `PilotJobs` before they
begin running.
- The full resource inventory on a `PilotJob`
is `{(node, gpu) for node in range(job.num_nodes) for gpu in range(job.gpus_per_node)}`.
- `PilotJob.claimed_gpu_ids` stores the currently claimed GPU resources on the PilotJob in the
same format: `list[tuple[int, int]]`.
- The free GPUs on a PilotJob are therefore:

```python
inventory = {(node, gpu) for node in range(job.num_nodes) for gpu in range(job.gpus_per_node)}
used = set(pilot_job.claimed_gpu_ids)
free = sorted(inventory - used)
```

Starting with a pending `PilotReplica` and the list of all eligible `PilotJobs`, placement
must follow these rules:

- Use the replica's parent PilotDeployment.launch_spec (a JSONB-encoded `PilotLaunchSpec`) to determine the replica resource requirements (`num_nodes` and `gpus_per_node`)
- If `num_nodes >= 2`, the replica requires a dedicated, empty multi-node pilot job all to itself.  No bin-packing.
- If `num_nodes == 1`, the replica can be placed into any single-node PilotJob with free GPU resources.
- A Replica may only choose from the free GPUs in a job (defined above)
- There are **no alignment or contiguity restrictions** on GPU assignment: a 4 GPU replica can use GPU IDs {0, 3, 4, 7}.  Still, prefer to fill up from the lowest free GPU indexes in order, for tidiness.
- Use **Best-fit node** selection: when placing any replica, choose the node with
the **fewest free GPUs** that still fits it (exact fit is ideal). This keeps small
replicas consolidated on partially-full nodes.

If the Replica fits in any job, confirm the assignment using
`PilotJob.assign_replica()`.  This re-reads the PilotJob using `SELECT ... FOR
UPDATE` and ensures that no invariant is violated during the Replica placement.
Update the Replica state from `pending` to `placed` and commit the DB transaction.

If the Replica cannot be placed in any existing `PilotJob`:

1. Determine if there is headroom in the cluster to add a new `PilotJob`. First read the active pilot jobs on the cluster (not scheduled_deletion; state in `{pending_submit,queued,starting,running}`).
The total number of such jobs must not exceed `Cluster.pilot_system.max_concurrent_jobs` and the sum of `num_nodes` must not exceed `Cluster.pilot_system.max_num_nodes`.
2. If there is headroom, use `PilotJob.create()` to create the new pending job. Set `num_nodes` equa to the replica's `num_nodes`. `gpus_per_node` and `walltime_min` must be taken from the cluster's `PilotConfig`.
3. Immediately place the replica on the newly-created PilotJob if it could be created, using the same transaction logic as above.  Otherwise, write write `state_message = 'AT_CAPACITY'` onto the Replica, leaving it pending for the next resync loop.


### Pilot Replica Launch Controller

This controller launches scheduled replicas (`state = placed`) onto PilotJobs
once they are running and available.

Listener wakes on Replica placement and Pilot Job manager-ready transitions,
because Pilot Job Resources becoming available/ready unblocks launching replicas.

`list_actionable`: `PilotReplica` where:

- state is `placed`
- scheduled_deletion_at is NULL
- not in reoncile cooldown
- parent pilot_job.scheduler_state = 'running' and manager_url is not null

Launch controller builds `self.client = PilotControlClient(client_state, cn="replica-drainer")`.
Use the it with `start_replica` helper to invoke
`POST /start-replica` on the pilot manager, then update `state = launching`.

Perform the API call with built-in timeout and retry.

- If the API call fails with a ReplicaStartError code, increment `PilotDeployment.consecutive_launch_failures`, set the replica state to `error` with a `state_message`
that explains what went wrong. The reconciler will drain/free its resources and try again.
- If the API call failed with 409 CONFLICT, this can be interpreted as a retry of a successful
operation.  Verify the replica was actually placed with `GET /status`, then move on successfully,
updating the `state = launching`.
- Any network or other 500 error should be logged and raised so that reconcile will cooldown and retry automatically, without penalizing the deployment or draining the replica.

*Recovery from partial failure:* Suppose `start-replica` succeeded on the backend but the
response failed to reach the controller and the replica is torn down without freeing the on-node
resources, leaving the
previously launched Replica as an **unregistered orphan** that occupies resources on the first PilotJob.
This orphan will [be reaped by the Replica Observer](#pilot-replica-observer) to address the resultant
resource leak.


### Pilot Replica Drainer

Does not write `scheduled_deletion_at` — only consumes it. The Replica Reconciler is the sole writer of that field; see above.

`list_actionable`: `scheduled_deletion_at IS NOT NULL AND deleted_at IS NULL` (retry gate).

Drainer builds `self.client = PilotControlClient(client_state, cn="replica-drainer")`.

Load replica (+pilot_job, +deployment.model_name). Early return if `deleted_at`.
Replicas with `scheduled_deletion_at` and `state == ready` must wait for eligibility gate:

- If `state != ready`: immediately eligible.
- If `state == ready`: require BOTH
  (a) `now - scheduled_deletion_at >= 20s`, AND
  (b) inflight == 0 OR `now - scheduled_deletion_at >= 300s`.
  inflight via
  `self.client_state.redis_repo.get_backend_runtime(dep.model_name,
  replica.backend_id).inflight`.
- Not eligible → return (re-checked next resync; do NOT raise).

Deletion process:

1. Use `stop_replica()` helper to call POST /control/stop-replica/{name} if the PilotJob is running.  Do this even if the Replica is in a terminal state, because the ReplicaManager continues to hold the resources for `error` and `start_timeout` replicas until they are explicitly stopped. Tolerate 404 status error (double-delete: OK).  Helpers should already have a quick built-in timeout/retry tolerance.  If other HTTP/transport errors still surface: let them raise (controller will backoff).
2. Commit transaction:
    - If the state was not already terminal, update the state to `terminated`. Preserve other terminal states like `error` and `start_timeout`.
    - Set `stopped_at` if not already set.
    - Call `PilotJob.unassign_replica()` to free the resources tracked in the DB.
    - Set `deleted_at`: the replica has now been cleaned and is ready for the retention sweeper.
   - Premise: `WHERE uid == replica.uid AND deleted_at IS NULL`. If rowcount==0
     raise `StaleReconcile`.



### Pilot Autoscaler Controller

This controller **reconciles per `Model`** and enacts scaling on each of that
model's child `PilotDeployment`s. The demand signal is inherently per-model, so
the reconcile unit is the model: sample once, then drive every child deployment
from that one signal. `resource_type = Model`.

- **Sole writer of `PilotDeployment.desired_replicas`.** This is true
  even when autoscaling is technically "disabled" for a deployment —
  the Autoscaler still runs and is the only place that pins
  `desired_replicas=0` for unhealthy deployments. Other
  controllers signal intent via separate fields (`scheduled_deletion_at`,
  `consecutive_launch_failures`); the Autoscaler is what reads those
  and writes `desired_replicas`.

`list_actionable` returns UIDs of `Model`s that have at least one
`PilotDeployment`. This controller intentionally does not consult the
`reconcile_retry_at` backoff gate — its work is pure compute plus Redis
reads/writes with no external RPC, so there is nothing to back off from, and it
must run on a fixed clock (see below).

#### Per-model signal, per-deployment action

The demand signal is **aggregated per `Model`**; the scaling decision is
**enacted per `PilotDeployment`**. A `Model` may have several `PilotDeployment`s
(typically one per cluster). Reconciling the model samples the shared signal
exactly once, then evaluates each child deployment's own ladder against it —
there is no cross-deployment coordination problem to guard against because the
sample and EWMA update happen once per reconcile by construction.

Because the deployments share one signal but each carries its own
`scaling_thresholds`, the ladder is what expresses per-deployment policy:

- Set both deployments' rungs identically and they scale up **together**,
  splitting one model's load across clusters.
- Set deployment B's rungs *above* deployment A's and you get a **two-tier
  spillover**: B only begins adding capacity once A has scaled to its top rung
  and demand keeps climbing.

#### Signal config lives on the Model; policy lives on the deployment

The config split follows the signal/action boundary:

- **`Model.demand_signal` — demand-signal generation** (one shared signal per
  model). A `DemandSignalConfig` holds `reject_window_sec`,
  `avg_request_duration_sec`, and `ewma_alpha`, plus the `calculate_demand`
  helper. These define the single EWMA every child deployment reads, so they
  cannot meaningfully differ per deployment.
- **`PilotDeployment` — scaling policy** (per deployment). The
  `scaling_strategy` (`DemandThresholdStrategy`) holds `scaling_thresholds`,
  `immediate_cold_start`, and `scale_down_sustain_sec`; `min_replicas`,
  `max_replicas`, and `max_consecutive_launch_failures` are dedicated columns.
  `immediate_cold_start` and `scale_down_sustain_sec` are deliberately
  per-deployment: one deployment can be eager (jump in on the first cold
  request, release slowly) while a sibling stays lazy (watch the signal and
  join only when the ladder says so).

#### Sampling clock

Set the `poll_interval` ClassVar to **10 seconds**, and leave `wakeup_channels`
empty so there are **no early wakes**. Unlike the other controllers — where
PubSub wakeups just shorten the resync wait and the exact cadence is
immaterial — here `poll_interval` *is the sampling clock*. Both the EWMA
(`ewma = alpha*sample + (1-alpha)*ewma`) and the reject-rate window assume a
regular ~10s tick; an irregular or event-driven cadence would distort both.

#### Reconcile order (per model)

Load the `Model` (with `DemandSignalConfig`) and its child `PilotDeployment`s.

**A. Sample the model's demand signal once:**

  1. Sample the model runtime with `RedisRepo.get_model_runtime`
     (`total_inflight`, `capacity_rejects_total`, `last_capacity_reject`).
  2. Append `(sample_ts, capacity_rejects_total)` to the model's reject-sample
     window **stored in Redis** (see [runtime schema](#autoscaler-runtime-schema)),
     and drop samples older than `reject_window_sec`.
  3. Compute the average per-model rejection rate: find the retained sample
     closest to `reject_window_sec` ago, then `reject_rate = max(0, Δrejects) /
     Δt` in rejects/sec. **Clamp `Δrejects` at 0** so a Redis flush/restore that
     resets the monotonic `capacity_rejects_total` counter produces a rate of 0
     rather than a negative spike.
  4. Compute instantaneous aggregate demand with
     `DemandSignalConfig.calculate_demand(inflight, reject_rate)`.
  5. Update the EWMA from the previous per-model value in Redis and store the
     new value (see schema below).

**B. For each child `PilotDeployment`, decide and write `desired_replicas`:**

  1. If `consecutive_launch_failures` exceeds `max_consecutive_launch_failures`,
     set `desired_replicas = 0`. Skip to the next deployment. (This is a one-way
     latch: with `desired_replicas = 0` nothing launches, so
     `consecutive_launch_failures` cannot reset on its own. It clears only on
     operator action — a spec edit — or via the reset paths described under
     [Per-resource backoff](#per-resource-backoff-and-giving-up). This is
     intended.)
  2. If `scaling_strategy` is None: skip this deployment (manual scaling).
  3. Parse/validate the `scaling_strategy` JSONB into a `DemandThresholdStrategy`.
  4. Cold start: if `desired_replicas == 0` AND `immediate_cold_start` is True
     AND (`now - last_capacity_reject < cold_start_reject_window` OR
     instantaneous demand > 0), scale immediately to `max(1, ladder(ewma))` and
     move on. (At `desired_replicas == 0` inflight is 0, so the demand signal at
     cold start comes entirely from the reject rate.) `cold_start_reject_window`
     defaults to 5 minutes.
  5. Otherwise compute the target `desired_replicas` from the ladder (below).
  6. If target > current `desired_replicas`: scale up immediately — the EWMA is
     the only gate on scale-up.
  7. If target < current `desired_replicas`: this is a candidate scale-down,
     governed by the sustain window (below). Do not lower `desired_replicas`
     yet.
  8. If target == current: no change.

Each deployment's `desired_replicas` write is its own premised update (see
below); one deployment's stale premise doesn't block its siblings.

#### Scaling threshold ladder

`scaling_thresholds` is an ordered list of `(demand_lower_bound_exclusive,
num_replicas)` rungs. The target is `scaling_thresholds[i][1]` for the rung
where `scaling_thresholds[i][0] < ewma_demand <= scaling_thresholds[i+1][0]`.
Above the top rung, target = `min(top_rung_replicas, deployment.max_replicas)`.
Below the bottom rung, target = `deployment.min_replicas`.

#### Scale-down sustain window

Scale-down is damped so a brief dip doesn't tear down capacity. On every tick,
after computing the ladder target for a deployment, maintain that deployment's
list of `ScaledownCandidate(num_replicas, starting_from)` in the per-model
Autoscaler runtime:

- If the ladder target is **below** current `desired_replicas` and there is no
  existing candidate at that target, append a candidate recording the target
  and the timestamp (`starting_from`) at which demand first fell to that rung.
- If the EWMA **lifts back above** a candidate's rung threshold, drop that
  candidate — the sustain clock resets.
- A scale-down becomes **eligible** once a candidate's target has been
  continuously held for `scale_down_sustain_sec`
  (`now - starting_from >= scale_down_sustain_sec`). When eligible, set
  `desired_replicas` to that target, then clear every candidate whose target is
  `>= desired_replicas` (they are either enacted or superseded).

Multiple candidates may accumulate if demand steps down through several rungs
during one sustain window; each carries its own `starting_from`, and the lowest
eligible one wins.

##### Worked example

Config: `scaling_thresholds = [(0.0, 1), (10.0, 2), (25.0, 3)]`,
`scale_down_sustain_sec = 120`, current `desired_replicas = 3`.

| t (s) | ewma | ladder target | candidate list action | `desired_replicas` |
|---|---|---|---|---|
| 0 | 30 | 3 | — (target == current) | 3 |
| 10 | 8 | 1 | append `(1, t=10)` | 3 |
| 20 | 12 | 2 | ewma rose above rung-1 (10.0) → drop `(1,·)`; append `(2, t=20)` | 3 |
| 30–120 | ~12 | 2 | `(2, t=20)` held | 3 |
| 140 | 12 | 2 | `(2, t=20)` held ≥120s → **eligible**; set `desired=2`; clear candidates with target ≥ 2 | 2 |
| 150 | 3 | 1 | append `(1, t=150)` | 2 |
| 270 | 3 | 1 | `(1, t=150)` held ≥120s → **eligible**; set `desired=1`; clear | 1 |

Note the drop-and-re-append at t=20: demand climbed from rung 1 back into rung
2, which resets the sustain clock for the deeper scale-down. Had ewma instead
fallen from 8 to 4 (still rung 1) at t=20, the `(1, t=10)` candidate would have
survived and become eligible at t=130.

#### Autoscaler runtime schema

Autoscaler state is a **per-model** Redis key (`Keys.autoscaler_model(model)` →
`rt:model:{model}:autoscaler`), a single JSON blob read and written through
`RedisRepo` (`get_autoscaler_model_runtime` / `set_autoscaler_model_runtime`).
One reconcile = one model = one read-modify-write of this key, so there is no
contention. Shape (`first_common.schema.resources.runtime`):

```python
class RejectSample(NamedTuple):
    ts: datetime
    rejects_total: int

class ScaledownCandidate(NamedTuple):
    num_replicas: int
    starting_from: datetime

class AutoscalerModelRuntime(BaseModel):
    """Per-model autoscaler state (one Redis key per model)."""
    demand_ewma: float = 0.0
    reject_window: list[RejectSample] = []
    # keyed by deployment name: a model's deployments scale independently
    scale_down_candidates: dict[str, list[ScaledownCandidate]] = {}
```

#### Premised writes

Each `desired_replicas` write is premised on the inputs that deployment's
decision was based on (`consecutive_launch_failures`, prior `desired_replicas`)
so a concurrent operator edit through the API can't be silently clobbered; a
stale premise yields a zero-row update for that deployment and is retried next
tick.


### Retention Sweeper
- One small `Worker`, runs every ~5 minutes.
- `DELETE FROM <each table>` where `deleted_at IS NOT NULL` and the
  retention window has elapsed.
- Logs the count per table on each pass.

### Health Alerter

A `Worker` (not a `Controller`) that polls Postgres and local host state every
30 seconds, debounces status transitions to suppress flaps, and posts
degradation/recovery batches to a Slack incoming webhook.

**Architecture:** observe → stage → flush. Each poll runs eight concurrent
check functions that return `Observation`s for unhealthy resources. A single
per-resource debounce window (45s) drives a pure state machine: a transition
must hold steady past the window before it matures and is posted, which
suppresses flapping while still alerting on sustained changes; transitions
that mature together in one poll coalesce into a single message. Recovery is
inferred by absence — a resource that was committed-unhealthy and is now
absent from a successful check is recovering. Each committed alert records the
check that produced it, so recovery is scoped to checks that actually ran and
a failed check (e.g. DB unreachable) does not falsely recover its keys.

**Persistence:** a single `HealthAlertState` Redis JSON blob stores committed
alerts, in-flight staging, daily-digest dedup, and check-failure tracking.
This state survives process restarts.

**Watched conditions:** Cluster and StaticDeployment health; PilotDeployment
state and capacity; PilotJob reconcile failures, manager health, and idle
status; PilotReplica bad states and reconcile failures; Postgres and Redis
liveness; gateway and controller healthz endpoints; local disk usage.

**Daily digest:** a status snapshot of all resource categories, posted once
per day at 13:00 UTC.

