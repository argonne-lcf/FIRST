# Controller Framework

!!! info "Aspirational design document"
    This page is the **design** for the FIRST controller stack. Treat it
    as the spec the next iteration of `first_gateway.controllers/` will
    be built against.

    What exists today:

    - The `Worker` base class and the supervising
      `first_gateway.controllers.manager.main` process (backoff,
      crash recovery, heartbeat monitor).
    - A stub `ClusterHealthController` that sleeps in a loop — registered
      so the wiring is exercised end-to-end.
    - `PilotSubmitter` and `GlobusComputePBSAdapter` — the platform layer
      that the controllers below will drive.

    Not yet implemented: the `Controller` reconcile-loop subclass, the
    manager-level lease, the shared LISTEN dispatcher, the
    Redis-backed status helpers, the retention sweeper, the manager
    `/metrics` server, and every controller listed under
    [FIRST Controllers](#first-controllers).

FIRST allows admins to declaratively configure models with access controls,
routing policies, and multi-cluster HPC deployments.  The controllers work
continuously in the background to ensure that these deployments are enacted and
healthy.

The controller manager is a single asyncio process that hosts every controller
as one or more coroutines. There is no controller-side scaling: one process is
plenty for thousands of resources, and the data plane (API servers) is
completely independent — a wholly-down controller manager does not drop user
traffic, it just means new resources aren't reconciled until it comes back.

Each controller owns a specific set of fields on one resource type or
cross-resource relationship. It observes the declared spec and current status,
performs external side effects, and writes back status to drive the system
toward the desired state.

The next sections describe the controller framework. The actual list of
controllers FIRST will ship lives in [FIRST Controllers](#first-controllers).

## Concurrency Control

### Manager Lease

Because the controller manager is the only writer for controller-owned fields,
we just need to make sure no two manager processes ever run at once (e.g. a
botched deployment, an admin starting a second instance by accident).

The manager grabs a single lease at startup:

```sql
CREATE TABLE controller_manager_lease (
    singleton       boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    holder_id       text NOT NULL,      -- UUID generated at startup
    renewed_at      timestamptz NOT NULL,
    lease_duration  interval NOT NULL DEFAULT '30 seconds'
);
```

The manager:

1. On startup, attempts to claim the lease (insert or take over an expired one).
   If it can't, refuses to start any controllers and exits — supervisor (e.g.
   docker) will restart and retry.
2. Runs a single renewal coroutine that refreshes `renewed_at` every 10s.
3. If two consecutive renewals fail (network blip, contention, db down), the
   manager *kills the process* (`os._exit(1)`). Don't try to "drain" — the
   safe assumption is that another instance may have taken over.

### Premised Updates

Multiple controllers (and the manager itself) may write to disjoint fields of
the same row. A single `version` column is too coarse — every reconcile would
trip every other reconciler's optimistic check, even when the changes are
unrelated.

Our rule, applied uniformly across the codebase:

> **Every UPDATE must include in its `WHERE` clause the premises the decision
> was based on.** If a premise is no longer true, the UPDATE affects zero rows
> and the reconciler logs which premise failed and returns. The next reconcile
> reads fresh state and tries again.

This is just SQLAlchemy core/ORM — no helper class needed.

```python
# Pattern A: ORM update with premise check
async def advance_to_running(sess: AsyncSession, job_id: int) -> bool:
    result = await sess.execute(
        sa.update(PilotJob)
        .where(
            PilotJob.uid == job_id,
            # premises read earlier in this reconcile:
            PilotJob.phase == JobPhase.submitted.value,
            PilotJob.scheduler_job_id.is_not(None),
        )
        .values(
            phase=JobPhase.running.value,
            time_started=sa.func.now(),
        )
    )
    if result.rowcount == 0:
        # Re-read so the log line names *which* premise failed.
        current = await sess.get(PilotJob, job_id)
        logger.info(
            "advance_to_running stale for job %d: "
            "phase=%r scheduler_job_id=%r (expected submitted/not-null)",
            job_id, current and current.phase,
            current and current.scheduler_job_id,
        )
        return False
    return True
```

```python
# Pattern B: bulk UPDATE in an observer, only writes rows that actually changed.
# IS DISTINCT FROM keeps the trigger from firing for unchanged rows, which
# matters for LISTEN/NOTIFY traffic.
await sess.execute(
    sa.update(PilotJob)
    .where(
        PilotJob.uid == sa.bindparam("uid"),
        PilotJob.scheduler_phase.is_distinct_from(sa.bindparam("phase")),
    )
    .values(scheduler_phase=sa.bindparam("phase")),
    updates,  # list[dict[str, Any]] — executemany
)
```

Notes on premised updates:

- **Log what failed, don't raise.** A stale update is a normal, expected event
  (the whole design assumes it). Treating it as an exception loses the most
  useful piece of diagnostic information — which premise was wrong.
- **One writer per field.** If two controllers need to contribute info to the
  same logical concept, give them distinct columns (or a relation table they
  each own exclusively). Premised updates are a safety net, not a license for
  shared writers.
- **Don't write back unchanged values.** The bulk-update pattern above is
  generalizable: include `IS DISTINCT FROM` checks for every field you're
  updating so an unchanged row doesn't fire the notify trigger and re-wake the
  loop. (See [Notification feedback loops](#notification-feedback-loops).)

### Mutual exclusion within the manager

Inside a single manager process, two coroutines belonging to the same
controller may not act on the same resource concurrently. We enforce this
structurally, not with locks:

- Every controller runs **one** reconcile coroutine. If you need finer
  concurrency, split resource IDs across an `asyncio.TaskGroup()`.
- Cross-controller concurrency *is* allowed (different controllers own
  different fields). Premised updates catch any genuine conflict.

This gives the "qsub idempotency" pattern (run `qstat` to check, then `qsub`
only if absent) a single-threaded execution context for free — no extra
locking needed.

### Sort by ID before multi-row writes

> Any time you take row locks **or** issue multiple UPDATEs in one
> transaction, sort the target IDs first. Postgres won't deadlock on locks
> taken in a consistent order.

This applies to:

- `SELECT ... FOR UPDATE` over multiple rows.
- Bulk UPDATEs touching N rows in the same table.
- Multi-table updates inside one transaction (sort within each table; if
  multiple tables, also pick a stable cross-table order).

## Reconcile Loop

The whole design is **level-triggered**: every reconcile reads fresh state
from Postgres, decides what one step to take, takes it, writes back. Crashes,
duplicate events, missed events, and stale caches are all recovered by "the
next reconcile sees the truth and does the right thing." Edge-triggered
designs (act-on-event-X) are forbidden.

### Poll from Postgres; LISTEN/NOTIFY is just a wake hint

Each controller's reconcile loop is straightforward:

```text
loop forever:
    beat()
    ids = SELECT uid FROM <table> WHERE <list_actionable predicate>
    for id in ids:
        reconcile(id)
        beat()
    wait up to resync_interval seconds OR until LISTEN notification
```

That's it. There is **no** in-memory work queue, no dirty set, no per-key
backoff data structure. The DB is the source of truth for what needs work,
and "needs work" is encoded as the `list_actionable` SQL predicate.

- **Full resync** every `resync_interval` (default 30s) is mandatory for
  correctness.
- **LISTEN/NOTIFY** just shortens the resync wait when a relevant row
  changes. If notifications were 100% lost, resync would still drive the
  system correctly within `resync_interval`.
- **Deduplication is free**: if a resource is updated 10 times between
  resyncs, the loop still does one reconcile per resync iteration.

If a controller is overwhelmed (its `for` loop takes longer than
`resync_interval`), no harm done — it just runs back-to-back without
sleeping. Add a metric for "% of resync interval spent reconciling" so we
notice before it matters.

### Shared LISTEN dispatcher

Every controller wants to be notified when its table changes. Rather than
each controller opening its own LISTEN connection, the manager owns a single
LISTEN connection and fans out to per-controller `asyncio.Event` objects,
keyed by table name:

```python
class WakeupDispatcher:
    """Single LISTEN connection in the manager; fans out per-table wakes."""

    def __init__(self) -> None:
        self._events: dict[str, asyncio.Event] = {}

    def event_for(self, table: str) -> asyncio.Event:
        return self._events.setdefault(table, asyncio.Event())

    async def run(self, conninfo: str) -> None:
        aconn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
        try:
            await aconn.execute("LISTEN resource_changes")
            async for notify in aconn.notifies():
                try:
                    payload = json.loads(notify.payload)
                    ev = self._events.get(payload["table"])
                    if ev is not None:
                        ev.set()
                except (json.JSONDecodeError, KeyError):
                    logger.warning("bad notify payload: %r", notify.payload)
        finally:
            await aconn.close()
```

The trigger:

```sql
CREATE OR REPLACE FUNCTION notify_resource_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'resource_changes',
    json_build_object('table', TG_TABLE_NAME)::text
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER pilot_job_notify
  AFTER INSERT OR UPDATE OR DELETE ON pilot_job
  FOR EACH ROW
  WHEN (
    -- Only fire for columns that drive controller behavior.
    NEW.phase IS DISTINCT FROM OLD.phase
    OR NEW.scheduled_deletion IS DISTINCT FROM OLD.scheduled_deletion
    OR NEW.manager_url IS DISTINCT FROM OLD.manager_url
    OR (TG_OP != 'UPDATE')  -- insert/delete always notify
  )
  EXECUTE FUNCTION notify_resource_change();
```

Notifications carry only the table name. That's enough: the receiving
controller's reconcile loop knows what predicate to apply.

### Notification feedback loops

Two layers of defense:

1. **Don't store high-churn observational fields in Postgres at all.** Things
   like `last_health_check`, `last_status_check`, and per-poll
   `manager_health` go to Redis (see
   [Hybrid Postgres+Redis Status](#hybrid-postgresredis-status)).
2. **Triggers only fire on watched columns.** The `WHEN` clause above
   names the columns that should wake controllers. Editing the trigger when
   you add a new such column is a forcing function — it makes you think
   about wake semantics every time.

### Heartbeats per loop

A controller may have several concurrent coroutines (the reconcile loop, the
resync polling sub-task, etc). A single shared `update_heartbeat()` would
mask a wedged sub-task. Instead, each spawned loop registers its own named
heartbeat token:

```python
class Worker(ABC):
    def register_heartbeat(self, name: str) -> Heartbeat:
        """Return a Heartbeat instance for one loop within this worker."""
        hb = Heartbeat(name=f"{self.name}.{name}", timeout=self._hb_timeout)
        self._heartbeats.append(hb)
        return hb

    def check_heartbeat(self) -> HeartbeatStatus:
        """Worker is healthy iff every registered heartbeat is fresh."""
        stale = [h for h in self._heartbeats if h.timed_out()]
        return HeartbeatStatus(timed_out=bool(stale), stale=stale)
```

Inside the controller:

```python
async def _reconcile_loop(self) -> None:
    hb = self.register_heartbeat("reconcile")
    wake = self.manager.dispatcher.event_for(self.table_name)
    while True:
        hb.beat()  # unconditional — including on empty resync ticks
        try:
            ids = await self.list_actionable()
            for uid in ids:
                try:
                    await self.reconcile(uid)
                except Exception:
                    logger.exception("%s reconcile %d failed",
                                     self.name, uid)
                    await self._record_failure(uid)
                hb.beat()
        except Exception:
            logger.exception("%s resync failed", self.name)
        try:
            await asyncio.wait_for(wake.wait(), timeout=self.resync_interval)
        finally:
            wake.clear()
```

The heartbeat monitor in `manager.py` already cancels a worker whose
heartbeat times out; that logic stays the same, it just consults the union
across registered beats.

### Per-resource backoff and giving up

We don't keep retry state in memory. Instead, we track it on the resource
itself:

```sql
ALTER TABLE pilot_job
    ADD COLUMN reconcile_failures   integer    NOT NULL DEFAULT 0,
    ADD COLUMN reconcile_last_error text,
    ADD COLUMN reconcile_retry_at   timestamptz;
-- (same columns on every controller-managed table)
```

After each reconcile, the controller writes back:

- success: `reconcile_failures=0, retry_at=NULL`
- failure: `reconcile_failures+=1, last_error=str(exc),
  retry_at = now() + backoff(failures)` (capped at `max_backoff`, default
  1 hour)

The backoff cap is what keeps persistently broken resources out of the hot
loop: once `failures` is large enough that `backoff(failures) >= max_backoff`,
every retry is scheduled an hour out. The `list_actionable` predicate filters
on `retry_at`, so a stuck row is reconsidered ~once per hour forever.
Transient platform breakage self-heals; persistent breakage stays cold but
is never permanently abandoned. No separate sweeper needed.

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
   - **Tear down**: `alcf-ai admin delete <resource>`. The owning controller
     handles cleanup as usual.
3. If the operator does nothing, the hourly retry eventually succeeds on
   its own once the underlying problem is gone.

### Reconcile function rules

- **Level-triggered.** Read current state; act on what *is*, not what
  *changed*. If a controller crashes mid-step, the next reconcile resumes
  from whatever state the DB reflects.
- **Each external side effect must be idempotent.** Build it in. For
  schedulers without idempotency keys (PBS): use a deterministic job name
  (`__FIRST_PILOT_<resource-name>`), `qstat` to check, then `qsub` only on
  absence. Mutual exclusion is provided by the manager's single-coroutine
  rule above.
- **One step per reconcile.** If a job goes through `pending_submit ->
  submitted -> running`, do one transition per reconcile. Write back state,
  return. Next reconcile picks up the next step. Each step is independently
  recoverable.
- **Updates are premised.** Always. See [Premised Updates](#premised-updates).
- **Postgres is the only state.** Controllers may cache nothing across
  reconcile invocations. (Redis is fine as a separate source of truth for
  high-churn fields — see below.)

### Controller sketch

```python
"""
Controller: a Worker subclass that polls Postgres for actionable rows,
calls reconcile() on each, and sleeps until either the resync interval
elapses or the table fires a notification.

Subclasses implement:
  - reconcile(uid)
  - list_actionable() -> list[int]
"""

class Controller(Worker):
    table_name: ClassVar[str]
    resync_interval: ClassVar[float] = 30.0
    max_reconcile_failures: ClassVar[int] = 8

    def __init__(self, name: str, client_state: ClientState) -> None:
        super().__init__(name, client_state)

    @abstractmethod
    async def reconcile(self, sess: AsyncSession, uid: int) -> None: ...

    @abstractmethod
    async def list_actionable(self, sess: AsyncSession) -> list[int]: ...

    async def run(self) -> None:
        hb = self.register_heartbeat("reconcile")
        wake = self.client_state.dispatcher.event_for(self.table_name)
        while True:
            hb.beat()
            await self._tick(hb)
            try:
                await asyncio.wait_for(wake.wait(),
                                       timeout=self.resync_interval)
            except asyncio.TimeoutError:
                pass
            finally:
                wake.clear()

    async def _tick(self, hb: Heartbeat) -> None:
        async with self.client_state.session() as sess:
            try:
                ids = await self.list_actionable(sess)
            except Exception:
                logger.exception("%s: list_actionable failed", self.name)
                return
        for uid in ids:
            hb.beat()
            await self._reconcile_one(uid)

    async def _reconcile_one(self, uid: int) -> None:
        try:
            async with self.client_state.session() as sess:
                await self.reconcile(sess, uid)
                await sess.commit()
            await self._record_success(uid)
        except Exception as exc:
            logger.exception("%s: reconcile %d failed", self.name, uid)
            await self._record_failure(uid, exc)
```

A toy subclass (illustrative; not one of the real FIRST controllers):

```python
class PilotJobController(Controller):
    table_name = "pilot_job"

    async def list_actionable(self, sess: AsyncSession) -> list[int]:
        # See FIRST Controllers / PilotJob for the real predicate.
        stmt = sa.select(PilotJob.uid).where(
            sa.or_(
                PilotJob.reconcile_retry_at.is_(None),
                PilotJob.reconcile_retry_at < sa.func.now(),
            ),
            PilotJob.phase.in_([
                JobPhase.pending_submit.value,
                JobPhase.submitted.value,
                JobPhase.running.value,
            ]),
        )
        return list(await sess.scalars(stmt))

    async def reconcile(self, sess: AsyncSession, uid: int) -> None:
        job = await sess.get(PilotJob, uid)
        if job is None:
            return  # deleted out from under us
        if job.scheduled_deletion:
            await self._terminate(sess, job)
            return
        if job.phase == JobPhase.pending_submit.value:
            await self._submit(sess, job)
            return
        # ... etc, one transition per call
```

## Observers

An "observer" is just a `Worker` with `while True: poll(); sleep()`. There's
no special base class — that wasn't pulling its weight. The polling pattern is
small enough to write inline:

```python
class HpcSchedulerObserver(Worker):
    """Polls qstat for every cluster's pilot system and updates pilot_job rows."""

    poll_interval = 30.0

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self._poll_all_clusters()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await asyncio.sleep(self.poll_interval)
```

Observers should:

- Read external state, write to Postgres via bulk premised UPDATE
  (`IS DISTINCT FROM` on every field).
- Be idempotent: polling twice with no external change is a no-op.
- Update many rows in one DB round trip when the external API returns a
  batch (e.g. `qstat` returns all jobs).
- Use Redis for per-poll timestamps/counters that would otherwise churn
  Postgres rows.

## Soft Delete and Retention

We don't use finalizers. Every resource has exactly one controller
responsible for its external cleanup, so the simpler model fits:

```sql
ALTER TABLE pilot_job
    ADD COLUMN scheduled_deletion boolean      NOT NULL DEFAULT false,
    ADD COLUMN deleted_at         timestamptz,
    ADD COLUMN retention_days     integer      NOT NULL DEFAULT 0;
```

Flow:

1. API receives delete request -> `UPDATE ... SET scheduled_deletion = true`.
2. The owning controller's `list_actionable` includes `scheduled_deletion =
   true` rows. On reconcile, it performs cleanup (terminate qsub, send
   stop signal, free certs) and then sets `deleted_at = now()`.
3. A **retention sweeper** (a small `Worker`, one per table) runs every
   ~5 minutes:
   ```sql
   DELETE FROM pilot_job
    WHERE deleted_at IS NOT NULL
      AND deleted_at + (retention_days || ' days')::interval < now();
   ```
4. API views filter out `deleted_at IS NOT NULL` rows by default; admin
   commands can opt in for postmortem.

Defaults:

- `Cluster`, `Model`, `AccessGroup`, `PilotDeployment`, `StaticDeployment`:
  `retention_days = 0` — hard-deleted as soon as cleanup is done.
- `PilotJob`, `PilotReplica`: `retention_days = 7` — keep for postmortem
  visibility into recent compute activity.

Operators can override `retention_days` per resource at delete time
(`alcf-ai admin delete --retention-days 30 ...`) for forensic cases.

## Hybrid Postgres+Redis Status

We split state across two stores:

- **Postgres** holds the spec, semantically meaningful aggregated status
  (e.g. `health`, `phase`), and anything controllers gate decisions on.
- **Redis** holds high-churn observational facts (`last_health_check`,
  `manager_health`, in-flight counts, load averages) that would otherwise
  spam triggers and balloon WAL.

The danger is Redis access scattered ad-hoc throughout the codebase. We
contain it with two abstractions:

### 1. Per-resource `StatusStore` classes

A `StatusStore` is a small typed class that owns the Redis key namespace
for one resource type and exposes Pydantic models for the read and write
paths:

```python
# packages/gateway/first_gateway/status/pilot_job.py

class PilotJobStatus(BaseModel):
    """High-churn status for a PilotJob. Lives in Redis, expires on TTL."""
    last_status_check: datetime | None = None
    manager_health: HealthEndpointStatus = HealthEndpointStatus.unknown
    last_manager_error: str | None = None


class PilotJobStatusStore(StatusStore[PilotJobStatus]):
    resource = "pilot_job"
    model = PilotJobStatus
    ttl_seconds = 3600  # rebuilt by observers within seconds


# packages/gateway/first_gateway/status/_base.py

T = TypeVar("T", bound=BaseModel)


class StatusStore(Generic[T]):
    """Typed access to one resource type's Redis-backed status."""

    resource: ClassVar[str]
    model: ClassVar[type[BaseModel]]
    ttl_seconds: ClassVar[int]

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def _key(self, name: str) -> str:
        return f"status:{self.resource}:{name}"

    async def get(self, name: str) -> T:
        raw = await self._redis.get(self._key(name))
        if raw is None:
            return self.model()  # default-valued instance
        return self.model.model_validate_json(raw)

    async def get_many(self, names: list[str]) -> dict[str, T]:
        if not names:
            return {}
        raws = await self._redis.mget([self._key(n) for n in names])
        return {
            n: (self.model.model_validate_json(r) if r else self.model())
            for n, r in zip(names, raws)
        }

    async def set(self, name: str, status: T) -> None:
        await self._redis.set(
            self._key(name),
            status.model_dump_json(),
            ex=self.ttl_seconds,
        )

    async def update(self, name: str, **changes: Any) -> T:
        """Read-modify-write helper for the common case."""
        cur = await self.get(name)
        new = cur.model_copy(update=changes)
        await self.set(name, new)
        return new
```

All Redis keys for status follow `status:<resource>:<name>`. Stores are
the only code that reaches into that namespace; controllers and API routes
get and set typed Pydantic models. `mypy` does not have to chase Redis
return types — `get()` returns `T`, full stop.

### 2. Composed read schemas

API read schemas pull from both stores and present a unified view:

```python
# packages/common/first_common/schema/resources/read.py

class PilotJobRead(BaseModel):
    # --- From Postgres (spec + aggregated status) ---
    name: ResourceName
    cluster_name: ResourceName
    phase: JobPhase
    scheduler_job_id: str | None
    manager_url: str | None
    time_started: datetime | None
    idle_since: datetime | None
    # ...

    # --- From Redis (observational, may be stale/missing) ---
    status: PilotJobStatus = Field(default_factory=PilotJobStatus)


async def load_pilot_job(
    sess: AsyncSession,
    statuses: PilotJobStatusStore,
    name: str,
) -> PilotJobRead:
    job = await PilotJob.get_by_name(sess, name)
    status = await statuses.get(name)
    return PilotJobRead.model_validate({
        **{c.name: getattr(job, c.name) for c in job.__table__.columns},
        "status": status,
    })


async def list_pilot_jobs(
    sess: AsyncSession,
    statuses: PilotJobStatusStore,
) -> list[PilotJobRead]:
    jobs = await PilotJob.list(sess)
    status_map = await statuses.get_many([j.name for j in jobs])
    return [
        PilotJobRead.model_validate({
            **{c.name: getattr(j, c.name) for c in j.__table__.columns},
            "status": status_map[j.name],
        })
        for j in jobs
    ]
```

Properties this gets us:

- All Redis access is in `first_gateway/status/`. Nothing else touches keys.
- `PilotJobRead.status` is a strongly-typed nested model. No `dict[str,
  Any]`, no `# type: ignore`.
- Status default-values when Redis is cold, so the API stays available.
- Promoting a field from Redis to Postgres (or vice versa) is a localized
  refactor: move it between the two model classes and adjust the writer.

The load-average utility in the appendix is one specific `StatusStore`-style
helper; treat it as the worked example.

## Observability

The manager process exposes a small FastAPI on a local port (e.g.
`127.0.0.1:9100`) with two routes:

- `GET /healthz` — returns 200 iff every registered `Worker` has a fresh
  heartbeat across all its named beats. Used as the docker healthcheck.
- `GET /metrics` — Prometheus exposition format, emitted by `prometheus_client`.

The same FastAPI also exposes a JSON view of controller state for the admin
dashboard to surface alongside the resource list:

- `GET /api/controllers` — for each worker: name, status (running/restarting),
  named heartbeats with seconds-since-last-beat, last error, restart count.
- `GET /api/controllers/<name>/recent` — recent reconcile log lines for one
  controller (last N records, in-memory ring buffer).

Standard metrics exported by the `Controller` base class for every subclass:

| Metric | Type | Labels |
|---|---|---|
| `first_reconcile_total` | counter | controller, outcome (`success`/`failure`/`stale`) |
| `first_reconcile_duration_seconds` | histogram | controller |
| `first_resync_interval_used_fraction` | gauge | controller |
| `first_actionable_rows` | gauge | controller |
| `first_worker_restarts_total` | counter | worker |
| `first_seconds_since_last_resync` | gauge | controller |
| `first_premised_update_stale_total` | counter | controller, table |

Logging is the primary debugging surface: structured JSONL via the existing
`first_gateway.log_config`. The tail-of-docker-logs + `jq` workflow stays the
primary day-2 tool. Metrics exist for trends and paging; logs for "what
exactly happened to this resource".

The admin dashboard polls `/api/controllers` and renders a status pane next
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

The list below uses the conventions established above. Each entry names the
table it owns (`table_name`), what its `list_actionable` predicate returns,
and what its `reconcile` does. Per-resource backoff, premised updates,
heartbeats, and LISTEN wakeups are implied — they're framework concerns.

Many small controllers beat few big ones in this design. With
poll-from-Postgres there's no per-controller queue overhead — each
controller adds one asyncio task and one SQL predicate. Splitting a fat
controller into focused ones makes each easier to test, reason about, and
restart on failure without disturbing the others.

### Observer controllers

These read external systems and write to Postgres/Redis.

#### Cluster Status Observer
- Polls each `Cluster`'s configured health endpoint.
- Postgres write: `Cluster.status` (only on transition).
- Redis write: `last_status_check` via `ClusterStatusStore`.

#### StaticDeployment Health Observer
- Polls health endpoint for each `StaticDeployment`.
- Postgres write: `StaticDeployment.health` (only on transition).
- Redis write: `last_health_check`.

#### StaticDeployment Load Observer
- Samples in-flight counts (see [Load Average utility](#load-average-utility)).
- All writes to Redis only.

#### HPC Scheduler Observer
- Polls `qstat` per cluster's pilot system.
- For each known `PilotJob`: bulk premised UPDATE of
  `scheduler_phase`, `time_started` (`IS DISTINCT FROM` per field).
- For each **orphan** — a scheduler job whose name starts with
  `__FIRST_PILOT_` but has no matching `PilotJob` row — issues `qdel`
  directly. The observer owns the `__FIRST_PILOT_` namespace; cleaning up
  inside it is part of being an observer of that namespace. No DB rows are
  inserted, no zombie phase exists.
- Logs every orphan reap at INFO so operators can see it in
  `docker compose logs`.

#### Pilot Replica Status Observer
- `list_actionable` (Postgres): `PilotJob` where `phase = running` AND
  `manager_url IS NOT NULL`.
- LISTEN wakes on both `pilot_job` and `pilot_replica`.
- Per job: calls `GET /status` on the pilot manager.
  - Postgres writes (premised, only on change): `PilotJob.resources`,
    `PilotJob.idle_since` (set to `now()` iff currently NULL and zero
    replicas running; set to NULL iff any replica running), per-replica
    `model_url`, `observed_served_name`, `phase`, `status_info`,
    `started_at`.
  - Redis writes: `PilotJob.last_status_check`, `manager_health`, per-replica
    `last_health_check`. None of these fire triggers.
- Groups successful startups and failures by PilotDeployment.  For each PilotDeployment,
update `consecutive_launch_failures` (incrementing per failed replica and resetting to 0 on success)

### Lifecycle controllers

#### PilotJob Controller (`table_name = "pilot_job"`)
- `list_actionable`:
  ```sql
  SELECT uid FROM pilot_job
   WHERE (reconcile_retry_at IS NULL OR reconcile_retry_at < now())
     AND phase NOT IN ('terminated', 'failed')
     AND (
            scheduled_deletion = true
         OR phase = 'pending_submit'
         OR (idle_since IS NOT NULL
             AND idle_since < now() - (
                pilot_max_idle_time_min || ' minutes')::interval)
         OR (manager_health = 'unhealthy'
             AND manager_unhealthy_since
                 < now() - manager_unhealthy_debounce)
     );
  ```
- `reconcile`:
  1. If `scheduled_deletion`: terminate via scheduler, set
     `phase = terminated`, set `scheduled_deletion` on every assigned
     replica (sorted-by-uid bulk UPDATE), set `deleted_at = now()`.
  2. If phase is terminal: ensure `scheduled_deletion` propagated to
     replicas, then return.
  3. If `idle_since` exceeds the cluster's threshold: set
     `scheduled_deletion = true` and return — the next reconcile handles
     teardown.
  4. If manager has been unhealthy (control APIs not responding with 200s) past debounce: set
     `scheduled_deletion = true` and return.
  5. If `phase = pending_submit`: check cluster's pilot_system
     `max_concurrent_jobs`. If under cap, `PilotSubmitter.submit()`,
     record `scheduler_job_id`, advance phase. (Cap counted via
     `SELECT count(*) FROM pilot_job WHERE cluster_name=... AND phase IN
     ('submitted','running')`.)

#### PilotJob Endpoint Discovery Controller (`table_name = "pilot_job"`)
- `list_actionable`: `PilotJob` where `phase = running` AND
  `manager_url IS NULL`. Optionally intersected with
  `PilotSubmitter.list_ready_endpoints()` if you want to skip ones the
  filesystem says aren't ready yet.
- `reconcile`: `PilotSubmitter.get_endpoint()`, set `manager_url`.
- Owns only `manager_url` on `PilotJob`. PilotJob Controller does not
  write that field.

#### PilotDeployment Controller (`table_name = "pilot_deployment"`)
- `list_actionable`: all `PilotDeployment` rows (small N; cheap to do
  unconditionally).
- `reconcile`:
  1. Aggregate health from current `PilotReplica.phase` for owned replicas;
     write `PilotDeployment.health` (premised, only on transition).
  2. Updates `consecutive_launch_failures` only by resetting it. The Replica
     Reconciler increments on failed launch; this controller resets to 0
     when at least one launch in the last hour succeeded. Reset is also
     triggered by:
     - `launch_spec` changing (spec-apply path).
     - Admin command: `alcf-ai admin reset-failures <deployment>`.
  3. Samples in-flight counts (see [Load Average Utility](#load-average-utility)).
- Owns: `PilotDeployment.health`, `PilotDeployment.consecutive_launch_failures`
  (resets only).

#### PilotReplica controllers

Split into three focused controllers, all on `table_name = "pilot_replica"`:

##### Replica Reconciler
- Drives observed count toward `desired_replicas`.
- `list_actionable`: rows where `pilot_deployment.desired_replicas` differs
  from `count(replicas where deleted_at IS NULL)`, plus any individual replica
  in a non-terminal state with `scheduled_deletion = true`.
- `reconcile`: per deployment:
  - If too few replicas: INSERT new `PilotReplica` rows in `phase=pending`
    with `pilot_job_name=NULL`. The Replica Placement controller will pick
    them up.
  - If too many: pick replicas to drain (prefer `pending` over `running`;
    among `running`, oldest first), set `scheduled_deletion = true`. The
    Drainer handles the rest.

##### Replica Placement Controller
- `list_actionable`: `PilotReplica` where `phase = pending` AND
  `pilot_job_name IS NULL` AND `scheduled_deletion = false`.
- `reconcile`: bin-pack onto an existing `PilotJob` that has free resources.
  - If a job fits: call `POST /start-replica` on the pilot manager and
    set `pilot_job_name = <job>` and `phase= 'placed'` in the same transaction. If the API call
    fails, leave `pilot_job_name = NULL` — next reconcile retries (it's
    idempotent because the pilot manager keys replicas by name).
  - If nothing fits: INSERT a new `PilotJob` in `phase = pending_submit`
    (subject to per-cluster max). Replica stays `pending`; on the next
    pass, once the new job is `running` with capacity, it gets placed.
  - If no clusters can accommodate the replica at all: write
    `status_info = 'AT_CAPACITY'`, leave pending. The full-resync loop
    picks it up periodically until capacity opens.

  *Recovery from partial failure:* If `start-replica` succeeded but the
  DB write failed, the next reconcile sees an unplaced replica and attempts
  placement on a Pilot Job again. If placed on a different pilot job, the
  unregistered first replica (now an orphan) will [be reaped](#replica-drainerreaper).
  If placed on the same pilot job, the Control API will raise a `409 CONFLICT` and
  the FK to the pilot job can be written.

##### Replica Drainer/Reaper
- Two related jobs:
  - **Drain**: replicas with `scheduled_deletion = true` and
    `phase != terminated`. Reconcile: ensure removed from router (router
    config controller does this on its own loop; here we just verify
    `deleted_at_router IS NOT NULL`), then after a 30s drain window call
    `POST /stop-replica`, set `phase = terminated`, `deleted_at = now()`.
  - **Reap orphans**: replicas appearing in pilot manager `/status` with
    no matching `PilotReplica` row, with a row that has
    `scheduled_deletion = true`, or with row that has a non-matching Pilot Job FK.
    Issue `stop-replica` immediately.  (Consider a replica that is placed on Pilot Job 1,
    then a transient DB error occurs so the placement is never recorded, and finally the replica
    is placed again on Pilot Job 2.  Now the same replica name exists in two pilot jobs. The first
    replica on Pilot Job 1 is unregistered and should be reaped.)
- Also marks replicas unhealthy or "stuck in launching > N min" with
  `scheduled_deletion = true`, which routes them through the same drain
  path.
- Also marks replicas of stopped/failed PilotJobs for deletion. (The
  `list_actionable` includes any replica whose parent job is in a terminal
  state.)

#### Replica pipeline summary

```
Autoscaler (writes PilotDeployment.desired_replicas)
        |
        v
Replica Reconciler (inserts pending replicas / marks excess for drain)
        |
        v
Replica Placement (calls start-replica + sets pilot_job_name FK, or creates new PilotJob)
        |
        v
Replica Drainer (handles scheduled_deletion: drain from router, stop-replica, mark terminated)
        |
        v
Retention Sweeper (hard-deletes after retention_days)
```

Each arrow is exactly one controller hand-off via Postgres state. Failures
at any stage are recovered by the level-triggered loop.

#### Pilot Autoscaler Controller (`table_name = "pilot_deployment"`)
- Reads load averages from Redis (1m/5m).
- Computes target `desired_replicas` per the deployment's `scaling_strategy`.
- Writes back `desired_replicas`, subject to a minimum interval between
  scale-up/scale-down events stored in Redis
  (`scaling:last_change:<deployment>`). Stops scale-up if the deployment's
  `consecutive_launch_failures` exceeds a threshold.

#### Router Config Controller (`table_name = "pilot_deployment", "static_deployment", "pilot_replica"`)
- Listens for changes to any of: deployments, replicas, models, access
  groups, cluster `maintenance_notice`.
- `list_actionable`: returns a sentinel (e.g. always `[0]`) — there's one
  global router config, not per-resource. Reconcile rebuilds it
  end-to-end from current Postgres state and writes the result to Redis.
- API servers `SUBSCRIBE` (or simply poll) the Redis key and hot-swap their
  in-memory LiteLLM router on change.
- The rebuilt config excludes:
  - Deployments whose cluster has `maintenance_notice` set.
  - Replicas in `pending`, `terminated`, or with `scheduled_deletion`.
  - Replicas whose parent `PilotJob.manager_health != healthy`.
- The router config is keyed on `Model.name` and provides the full map of:
    - Model aliases: models may declare multiple non-overlapping alias names
    that resolve to the canonical name in the router.
    - Live deployment endpoints and corresponding routing parameters
    - Access Group information for pre-flight authorization

#### Retention Sweeper
- One small `Worker`, runs every ~5 minutes.
- `DELETE FROM <each table>` where `deleted_at IS NOT NULL` and the
  retention window has elapsed.
- Logs the count per table on each pass.

### Alerting

#### Health Alert Controller
- Watches table changes to `Cluster`, `PilotJob`, `StaticDeployment`,
  `PilotReplica`, `PilotDeployment`, plus periodic checks for things not
  represented as `ResourceRow`s:
  - The Gateway API server `/health` endpoint.
  - Liveness of each `SchedulerAdapter` (for `GlobusComputePBSAdapter`,
    verifying the endpoint is online).
  - Postgres and Redis liveness.
  - Worker liveness: a failed worker (terminal crash or heartbeat
    timeout) is recorded by the manager into a small `worker_failures`
    table that the Alert controller watches.
- Owns its own table `alert_state(resource_table, resource_id,
  last_alerted_status, last_alerted_at)`.

##### Debouncing and flap suppression

Two windows interact:

1. **Per-resource debounce (60s default)**: after a resource changes
   status, we wait this long before considering it stable. Only after the
   status has held steady for the debounce window do we treat it as a
   real transition worth alerting on.
2. **Per-batch flush window (30s default)**: once at least one real
   transition is staged, wait up to this much longer to coalesce more
   transitions into one Slack message.

Concretely, the staging dict keys by `(table, resource_id)` and stores
`{first_seen_status, first_seen_at, latest_status, latest_seen_at}`. On
flush:

- If `latest_status == last_alerted_status` for that resource, **drop**
  the entry — the resource flapped and returned. No alert sent.
- Else if `latest_seen_at - first_seen_at >= debounce`, include in the
  alert batch and update `last_alerted_status = latest_status`.
- Else (status hasn't held long enough), keep in the staging dict and
  re-evaluate on the next flush tick.

A degraded->healthy flap shorter than the debounce sends nothing. A
genuine degradation that holds for the debounce window sends one Slack
message; if recovery happens before the next batch flush, the recovery
piggy-backs into the same message; if after, it sends a separate one.

## Appendix

### Load Average Utility

We measure **concurrent in-flight requests** using a Redis sorted set. Briefly,
`ZADD key score member` is like creating a Python dictionary identified as the top-level redis `key`, and setting `dict[member] = score` with the bonus that Redis keeps the entries sorted by score under the hood, making score-range queries cheap.
`ZCARD` gives you a quick O(1) count of members in the set. `ZREMRANGEBYSCORE` lets you evict members within a score range.

- On request start: `ZADD` the request id with a score = the unix time after which a leaked entry should be pruned (≈ now + 2 min).
- On request completion: `ZREM` the request id.
- On read: `ZREMRANGEBYSCORE` to evict anything past its expiry (self-cleaning against leaked/abandoned requests), then `ZCARD` for the current in-flight count.
- **Cold-start demand counts too:** a request to an offline (scale-to-zero) model `503`s immediately, but we still `ZADD` it with the ~2-minute expiry, so demand for offline models registers and can drive a scale-up.

A controller samples the noisy signal every 10 sec and buffers the last 30 samples in memory. On each sample arrival, average the last 6 (1m) and last 30 (5m).  Write these averages back into redis keys. Then redis contains a smoothed average of 1m/5m load, live-updating every 10 seconds. We may also consider recording the peak load as the max() aggregate over the same last 1m/5m worth of samples.

Finally, we don't want to store this data that changes every 10 seconds in Postgres. It's fine for it to be blown away when Redis restarts; it re-populates quickly.

API views of model deployments should read this load information out of Redis
via the deployment's `StatusStore` (see
[Hybrid Postgres+Redis Status](#hybrid-postgresredis-status)), combine it with
the Postgres data, and return the combined Pydantic Read objects to clients.

```python
import contextlib
import uuid
from typing import AsyncIterator

import redis.asyncio as redis


_START_SCRIPT = """
local key = KEYS[1]
local request_id = ARGV[1]
local ttl = tonumber(ARGV[2])

local now = tonumber(redis.call('TIME')[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
redis.call('ZADD', key, now + ttl, request_id)
redis.call('EXPIRE', key, ttl * 2)
return redis.call('ZCARD', key)
"""

_READ_SCRIPT = """
local key = KEYS[1]
local now = tonumber(redis.call('TIME')[1])
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
return redis.call('ZCARD', key)
"""


class AsyncInflightCounter:
    def __init__(self, client: redis.Redis, max_request_seconds: int = 60):
        self.client = client
        self.ttl = max_request_seconds
        self._start = client.register_script(_START_SCRIPT)
        self._read = client.register_script(_READ_SCRIPT)

    def _zkey(self, key: str) -> str:
        return f"inflight:{key}"

    @contextlib.asynccontextmanager
    async def track(self, key: str) -> AsyncIterator[int]:
        request_id = uuid.uuid4().hex
        zkey = self._zkey(key)
        count = await self._start(keys=[zkey], args=[request_id, self.ttl])
        try:
            yield int(count)
        finally:
            await self.client.zrem(zkey, request_id)

    async def count(self, key: str) -> int:
        return int(await self._read(keys=[self._zkey(key)]))

counter = AsyncInflightCounter(r, max_request_seconds=30)

# In an async handler:
async with counter.track(f"GET:/items:{api_key}") as n_inflight:
    if n_inflight > 100:
        raise TooManyInflightError()
    return await do_work()
```
