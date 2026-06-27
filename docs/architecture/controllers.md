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
    `WorkQueue`, the per-controller lease, column-level OCC helper,
    LISTEN/NOTIFY plumbing, and every controller listed under
    [FIRST Controllers](#first-controllers).

FIRST allows admins to declaratively configure models with access controls,
routing policies, and multi-cluster HPC deployments.  The controllers work
continuously in the background to ensure that these deployments are enacted and
healthy.

Each controller is a logically-independent process that owns a specific set of
fields on one resource type or cross-resource relationship. The controller
observes the declared spec and current status, performs external side effects,
and makes database updates to synchronize the status and drive the system
towards the desired state.

The next sections describe the design for the core controller framework.  The actual requirements for FIRST controllers are set out in [FIRST Controllers](#first-controllers).

## Concurrency Control

### Controller Leases

In the initial design, we avoid scaling out the controllers.  Transient outages in controllers do not impact the data plane, by design, and one asyncio controller process is likely more than sufficient to keep up with ~1000s of model deployments.

Since there is no work-construct, controllers must take out leases to prevent bugs arising from accidental duplicate instances in the infrastructure.

```sql
CREATE TABLE controller_leases (
    controller_name  text PRIMARY KEY,
    holder_id        text NOT NULL,      -- UUID generated at startup
    renewed_at       timestamptz NOT NULL,
    lease_duration   interval NOT NULL DEFAULT '30 seconds'
);
```

Each controller generates a random UUID at startup, tries to claim the lease,
and renews it periodically. Before each reconcile, it checks it still holds the
lease.

```python
async def try_acquire_lease(conn, controller_name, holder_id):
    """Claim the lease if it's expired or we already hold it."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO controller_leases
                   (controller_name, holder_id, renewed_at)
            VALUES (%(name)s, %(holder)s, now())
            ON CONFLICT (controller_name) DO UPDATE
               SET holder_id = %(holder)s,
                   renewed_at = now()
             WHERE controller_leases.holder_id = %(holder)s
                OR controller_leases.renewed_at
                   + controller_leases.lease_duration < now()
            RETURNING holder_id
            """,
            {"name": controller_name, "holder": holder_id},
        )
        row = await cur.fetchone()
        return row is not None and row[0] == holder_id
```

**QUESTION:** Do we really need a lease _per-controller_? If they all run as coroutines in one Python manager process, wouldn't it be simpler and cleaner for the manager to grab and maintain the lease at startup?

### Column-level OCC

Both row locking and [traditional optimistic concurrency
control](https://docs.sqlalchemy.org/en/21/orm/versioning.html) approaches were
considered in this design.

A single version column per row is too coarse when multiple controllers write to
the same row, even if they write disjoint fields. Instead, we perform OCC at the
column level. Each update's WHERE clause includes the _premises_ of the
decision, not just the identity of the resource. If any premise is
no longer true, the update affects zero rows and you know to re-read and
re-evaluate.  This is extracted into a small helper:

```python
# Just a sketch.  Will use SQLAlchemy.
async def guarded_update(
    conn: psycopg.AsyncConnection,
    table: str,
    resource_id: str,
    *,
    set_fields: dict[str, Any],
    preconditions: dict[str, Any],
) -> None:
    """
    Update set_fields only if preconditions still hold.
    Raises StaleVersionError if stale.
    """
    set_clause = ", ".join(f"{k} = %({k})s" for k in set_fields)
    where_clause = " AND ".join(
        f"{k} = %(pre_{k})s" for k in preconditions
    )
    params = {
        **set_fields,
        **{f"pre_{k}": v for k, v in preconditions.items()},
        "id": resource_id,
    }
    # TODO: we don't expect controllers to act on untrusted input, but we should probably
    # avoid string interpolating queries nonetheless.  If this update can't be fully parameterized without
    # string interpolation, this function needs to provide stronger guarantees against SQL injection.
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE {table}
               SET {set_clause}
             WHERE id = %(id)s AND {where_clause}
            """,
            params,
        )
        if cur.rowcount == 0:
            raise StaleVersionError
```

Then reconcilers read cleanly:

```python
try:
    await guarded_update(
        conn, "jobs", resource_id,
        set_fields={"phase": "running"},
        preconditions={
            "phase": "submitted",
            "hpc_state": "running",
        },
    )
except StaleVersionError:
    queue.done(resource_id, retry=True)
    return  # stale — next reconcile gets fresh state
```

To summarize the layered defense: the lease prevents two instances of the same
controller from running concurrently. The precondition guards prevent a single
controller from acting on stale cross-controller data. And the level-triggered
reconcile means that any time a guard trips, the recovery is just "re-read and
try again".

If a reconcile updates multiple rows in one transaction, sort by ID first to
avoid deadlocks arising from updates in different orders.

## Observer Controllers

Write dedicated observer controllers for polling external systems. It runs on a
timer, queries the external system, and updates the status of the relevant
Postgres rows. Other controllers then react to those status changes through the
normal notification pathway.

These controllers don't necessarily need to reconcile one row at a time.  For
example, `qstat` may return the status of 400 HPC-scheduled jobs.  Observer
controllers can update all rows in a single network round trip.


```python
"""
ObserverController: a Worker that polls an external system and syncs
state back into Postgres.

Use this for things like:
  - Polling an HPC scheduler for job status
  - Checking a cloud provider for instance state
  - Syncing data from an external API

The observer's job is strictly: read external state, write it to Postgres.
Other controllers then react to those Postgres changes through the normal
notification/reconcile pathway. This keeps the polling concern isolated
from the business logic.

Subclasses implement:
  - poll(): query the external system, update Postgres rows
  - poll_interval: how often to poll (seconds)
"""

import asyncio
import logging
from abc import abstractmethod

from ..settings import ClientState
from .worker import Worker

logger = logging.getLogger(__name__)


class ObserverController(Worker):
    """
    Base class for controllers that poll external systems.
    """

    poll_interval: float = 30.0  # seconds between polls

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name, client_state, heartbeat_timeout=heartbeat_timeout
        )

    @abstractmethod
    async def poll(self) -> None:
        """
        Query the external system and update Postgres with current state.

        For example, an HPC job observer might:
          1. Call the scheduler API to list all active jobs
          2. For each job, update the corresponding Postgres row's
             status fields (state, exit_code, etc.) via OCC
          3. Any status changes trigger LISTEN/NOTIFY automatically,
             which wakes up the relevant reconciling controllers

        This method should be idempotent — polling twice with no
        external change should be a no-op.
        """
        updates = await self._query_external_system()
        if not updates:
            return

        # sends all 400 statements to Postgres in a single network round trip,
        # executes them server-side, and commits once. The `IS DISTINCT FROM` clause means
        # rows where nothing changed don't fire triggers, so you might get 400 updates
        # sent but only 12 notifications — whichever jobs actually changed state.
        async with self.client_state.db_pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor() as cur:
                    await cur.executemany(
                        """
                        UPDATE jobs
                        SET hpc_state = %(state)s,
                            exit_code = %(exit_code)s,
                            last_status_check = now()
                        WHERE id = %(id)s
                        AND hpc_state IS DISTINCT FROM %(state)s
                        """,
                        updates,
                    )

    async def run(self) -> None:
        """
        Simple poll loop. Worker.supervise() handles crash recovery.
        """
        while True:
            self.update_heartbeat()
            try:
                await self.poll()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await asyncio.sleep(self.poll_interval)
```

## Reconciling Controllers

### Integrated polling and notification-driven loop

- Use a **full resync** loop to periodically list all resources, compute which
ones need work, and enqueue them.  This is mandatory for correctness;
LISTEN/NOTIFY by itself is unreliable.  Notifications provide sub-second
responsiveness most of the time; full resync occurs every ~30 seconds to catch
anything that was missed.
- Use a LISTEN/NOTIFY trigger for event-driven processing on every resource row.
Postgres automatically notifies consumers when ResourceRows change. The
notification payload is minimal: just "resource type + resource ID changed."
This is merely an optimization over re-reading the full table.  Both resync and
notification-driven resources funnel into the same deduplicating work queue.
- To avoid infinite feedback loops (change->notification->queue reconcile->change->...) reconcile functions MUST not update records unless there is a true and necessary state change. See [Column-level OCC](#column-level-occ) above for generic update strategy.

```sql
--- Install this once per table via a migration:
CREATE OR REPLACE FUNCTION notify_resource_change()
RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'resource_changes',
    json_build_object(
      'table', TG_TABLE_NAME,
      'id', COALESCE(NEW.id, OLD.id)
    )::text
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- For each resource table:
CREATE TRIGGER pilot_job_notify
  AFTER INSERT OR UPDATE OR DELETE ON pilot_job
  FOR EACH ROW EXECUTE FUNCTION notify_resource_change();
```

On the listener side:

```python
# autocommit=True is required
# The listener should use its own connection.
async def listen_for_notifications(
    conninfo: str,
    queue: asyncio.Queue[ResourceChange],
) -> None:
    """Listen on a PostgreSQL channel and forward notifications to a queue."""
    aconn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
    try:
        await aconn.execute(f"LISTEN resource_changes")
        async for notify in aconn.notifies():
            try:
                payload = json.loads(notify.payload)
                await queue.put(ResourceChange(table=payload["table"], id=payload["id"]))
            except (json.JSONDecodeError, KeyError):
                logger.warning("Malformed notification payload: %r", notify.payload)
    finally:
        await aconn.close()
```

### Work Queue

Use a workqueue with deduplication and backoff.  Don't just fire reconcile on
every Postgres notification. Maintain a queue where the key is the resource
identifier and duplicates are collapsed — if a resource changes three times
while you're already reconciling it, you just process it once more afterward
with the latest state. Add exponential backoff for resources that keep failing.

```python
@dataclass
class _BackoffState:
    attempts: int = 0
    ready_at: float = 0.0


class WorkQueue:
    def __init__(
        self,
        *,
        base_backoff: float = 1.0,
        max_backoff: float = 300.0,
        jitter: float = 0.2,
    ) -> None:
        self._base_backoff = base_backoff
        self._max_backoff = max_backoff
        self._jitter = jitter

        self._queue: asyncio.Queue[str] = asyncio.Queue()
        # Keys that are queued. Includes keys temporarily out of the physical
        # queue while deferred for backoff (see get()); they stay "dirty" so
        # duplicates remain suppressed until they return.
        self._dirty: set[str] = set()
        # Keys currently being processed.
        self._processing: set[str] = set()
        # Re-add requested while the key was in flight; re-queued in done().
        self._requeue: set[str] = set()
        # Per-key backoff tracking.
        self._backoff: dict[str, _BackoffState] = {}

    def _enqueue(self, key: str) -> None:
        # Precondition: key not already dirty/queued.
        self._dirty.add(key)
        self._queue.put_nowait(key)

    async def add(self, key: str) -> None:
        if key in self._dirty:
            return  # already queued
        if key in self._processing:
            self._requeue.add(key)  # re-process after done()
            return
        self._enqueue(key)

    async def add_many(self, keys: list[str]) -> None:
        """Bulk enqueue, e.g. from the periodic resync poller."""
        for key in keys:
            if key in self._dirty:
                continue
            if key in self._processing:
                self._requeue.add(key)
                continue
            self._enqueue(key)

    async def get(self) -> str:
        """
        Block until an item is available and its backoff has elapsed, then
        return the resource key to process.
        """
        while True:
            key = await self._queue.get()  # only suspension point
            state = self._backoff.get(key)
            if state is not None:
                now = time.monotonic()
                if now < state.ready_at:
                    # Not ready. Leave it dirty (dups stay suppressed) and put
                    # it back after the remaining delay. Exactly one deferral
                    # can be outstanding per key: it's out of the queue and
                    # add() won't re-enqueue a dirty key
                    asyncio.get_running_loop().call_later(
                        state.ready_at - now, self._queue.put_nowait, key
                    )
                    continue

            self._dirty.discard(key)
            self._processing.add(key)
            return key

    async def done(self, key: str, *, retry: bool = False) -> None:
        """
        Mark processing complete.

        retry=True schedules the key for another attempt after an exponential backoff.
        """
        self._processing.discard(key)

        requeued = key in self._requeue
        self._requeue.discard(key)

        if retry:
            state = self._backoff.setdefault(key, _BackoffState())
            state.attempts += 1
            delay = min(
                self._base_backoff * (2 ** (state.attempts - 1)),
                self._max_backoff,
            )
            if self._jitter:
                delay += random.uniform(0.0, self._jitter * delay)
            state.ready_at = time.monotonic() + delay
            logger.debug(
                "backoff for %s: attempt %d, retry in %.1fs",
                key, state.attempts, delay,
            )
        else:
            # Success clears any prior backoff.
            self._backoff.pop(key, None)

        # Re-enqueue once for whichever reason applies
        if (retry or requeued) and key not in self._dirty:
            self._enqueue(key)
```

### Reconcile Function Requirements

- Reconcile functions must be level-triggered, not edge-triggered.  In practice:
each reconcile function takes a resource ID, reads the current state from
Postgres, computes the todo, and acts on it. This means if a controller crashes
mid-operation, restarts, gets a duplicate event, or runs twice for no reason, it
just re-reads the current state and does the right thing.
- All side effects must be idempotent.  Design every reconciler so that calling
it an extra time is always safe. For example, to make `qsub` to an HPC scheduler (that doesn't provide any idempotency support natively) idempotent:
  1. Use a unique and deterministic Job_Name for every submitted job.
  2. First run `qstat` to verify that the job wasn’t already submitted.
  3. Finally, run `qsub` only after confirming that the job doesn't exist.
  4. (Ensure steps 2&3 are run with mutual exclusion, preventing concurrency)
- Only one writer per field (or per resource).  If you need multiple actors to
contribute information to a resource, give them distinct sub-fields or use
separate relation tables they each own exclusively.
- Use [column-level OCC](#column-level-occ) and incisive UPDATES to prevent lost updates
- Treat Postgres as the sole source of truth. Controllers should never cache
state locally and trust it across reconcile calls.
- Design for re-entrancy by splitting work into small state transitions. If a
reconcile needs to do three external side effects (say, provision a VM,
configure DNS, issue a TLS cert), don't do all three in one pass. Do the first,
write the intermediate status back to Postgres (e.g., phase: vm_provisioned),
and return. The next reconcile sees the new status and does step two. This way,
a crash between steps loses at most one step of work, and the reconciler picks
up cleanly. Each step is individually idempotent.  If you're just updating
Postgres fields with no external calls, you can do multiple things in one
reconcile — it's a single transaction anyway.
- Use finalizers for cleanup.
    - Finalizers is a list of strings:  when a controller touches a resource in
    a way that will require cleaning up, it adds its finalizer to the resource.
    For example `finalizers = array_append(finalizers, 'vm-controller')`.
    - When someone requests deletion, you don't `DELETE` the row: You set `deleted_at = now()` ("soft delete")
    - Every controller, on reconcile, checks: "is `deleted_at` set AND is my finalizer present?" If yes, do cleanup (delete the VM), then remove your finalizer: `finalizers = array_remove(finalizers, 'vm-controller')`.
    - A separate process checks: "is deleted_at set AND finalizers is empty?" If yes, actually DELETE the row.

### Controller Sketch

Here is a rough outline of the `Controller` parent class. It subclasses
`first_gateway.controllers.worker.Worker` to inherit supervision, heartbeat
monitoring, and auto-restart with backoff.

```python
"""
Controller: a Worker subclass that implements the reconcile-loop pattern.

Each Controller:
  - Watches a specific resource table via LISTEN/NOTIFY
  - Maintains a deduplicating work queue
  - Periodically does a full resync (listing all resources that need work)
  - Calls self.reconcile(resource_id) for each item, one at a time
  - Handles OCC retries transparently
  - Obtains a controller lease at startup; refusing to start on failure.
  - Continously refreshes the controller lease in the background
  - Verifies that it continues to hold a valid lease before each reconcile invocation.

Subclasses implement:
  - reconcile(resource_id): the level-triggered reconcile function
  - list_actionable(): returns IDs of all resources that might need work
    (used by periodic resync)

Optionally override:
  - table_name: which table's notifications to watch
  - resync_interval: how often to do a full resync (seconds)
"""

import asyncio
import logging
from abc import abstractmethod

from ..settings import ClientState
from .worker import Worker
from .workqueue import WorkQueue

logger = logging.getLogger(__name__)


class Controller(Worker):
    """Base class for resource-reconciling controllers."""

    # Subclasses set these.
    table_name: str  # e.g. "clusters", "jobs", etc.
    resync_interval: float = 60.0  # seconds between full resyncs

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        heartbeat_timeout: float = 120.0,
        max_concurrent: int = 1,
    ) -> None:
        super().__init__(
            name, client_state, heartbeat_timeout=heartbeat_timeout
        )
        self.queue = WorkQueue()
        self._max_concurrent = max_concurrent

    @abstractmethod
    async def reconcile(self, resource_id: str) -> None:
        """
        Level-triggered reconcile. Read the current state from Postgres,
        compare spec to status, perform at most one external side effect,
        write back updated status via OCC.

        Raise any Exception to trigger backoff for this resource.
        A clean return means success — backoff is cleared.
        """
        ...

    @abstractmethod
    async def list_actionable(self) -> list[str]:
        """
        Return IDs of all resources that may need reconciliation.
        Called periodically by the resync loop. Can be as simple as
        "SELECT id FROM my_table" or can filter by status.

        This is the safety net — even if every notification is lost,
        the resync will eventually enqueue everything.
        """
        ...

    # ----- Internal machinery (Worker.run implementation) -----

    async def run(self) -> None:
        """
        Main loop: run the listener, resync, and reconcile workers
        concurrently. If any crashes, the Worker.supervise() logic
        restarts the whole thing.
        """
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._listen_loop())
            tg.create_task(self._resync_loop())
            # Start N concurrent reconcile workers draining the queue.
            for i in range(self._max_concurrent):
                tg.create_task(self._reconcile_loop(worker_id=i))

    async def _listen_loop(self) -> None:
        """
        Subscribe to LISTEN/NOTIFY and feed resource IDs into the queue.
        """
        aconn = await psycopg.AsyncConnection.connect(conninfo, autocommit=True)
        try:
            await aconn.execute(f"LISTEN resource_changes")
            async for notify in aconn.notifies():
                try:
                    payload = json.loads(notify.payload)
                    if payload["table"] == self.table_name:
                        await self.queue.add(payload["resource_id"])
                except (json.JSONDecodeError, KeyError):
                    logger.warning("Malformed notification payload: %r", notify.payload)
        finally:
            await aconn.close()

    async def _resync_loop(self) -> None:
        """
        Periodically list all resources and enqueue them.
        This catches anything missed by notifications.
        """
        while True:
            try:
                self.update_heartbeat()
                ids = await self.list_actionable()
                await self.queue.add_many(ids)
                logger.debug(
                    "%s: resync enqueued %d resources", self.name, len(ids)
                )
            except Exception:
                logger.exception("%s: resync failed", self.name)
            await asyncio.sleep(self.resync_interval)

    async def _reconcile_loop(self, worker_id: int = 0) -> None:
        """
        Drain the work queue and reconcile one resource at a time.
        """
        while True:
            resource_id = await self.queue.get()
            self.update_heartbeat()

            failed = False
            try:
                await self.reconcile(resource_id)
            except Exception:
                logger.exception(
                    "%s: reconcile failed for %s",
                    self.name, resource_id,
                )
                failed = True
            finally:
                await self.queue.done(resource_id, retry=failed)
```

This is how a `Controller` subclass might look.  Note that this example is
totally fake/not related to the actual controllers in first_gateway, but it
serves to illustrate the general design patterns for Controllers:

```python
class ClusterController(Controller):
    table_name = "clusters"
    resync_interval = 60.0

    def __init__(self, client_state: ClientState) -> None:
        super().__init__("cluster-controller", client_state)

    async def list_actionable(self) -> list[str]:
        """Return IDs of clusters that might need work."""
        async with self.client_state.db_pool.connection() as conn:
            async with conn.cursor() as cur:
                # Only bother with clusters that aren't fully reconciled.
                # Or just return everything — reconcile will no-op if
                # nothing needs doing.
                await cur.execute(
                    "SELECT id FROM clusters WHERE status != 'ready'"
                )
                return [row[0] async for row in cur]

    async def reconcile(self, resource_id: str) -> None:
        """
        Level-triggered reconcile for a single cluster.

        Read current state, decide what one step to take, do it,
        write back status. If nothing to do, return immediately.
        """
        async with self.client_state.db_pool.connection() as conn:
            resource = await get_resource(conn, "clusters", resource_id)

            if resource is None:
                return  # deleted out from under us, nothing to do

            spec = resource.data
            phase = spec.get("phase", "pending")

            # -- Level-triggered state machine: check what IS, not what changed --

            if spec.get("deleted_at") and "cluster-controller" in (
                spec.get("finalizers") or []
            ):
                # Deletion requested and our finalizer is present.
                # Do cleanup, then remove our finalizer.
                await self._cleanup_external_resources(resource_id)
                await update_resource(conn, resource, {
                    "finalizers": [
                        f for f in spec["finalizers"]
                        if f != "cluster-controller"
                    ],
                })
                return

            if phase == "pending":
                # Step 1: provision the underlying infrastructure.
                external_id = await self._provision_infra(resource_id)
                await update_resource(conn, resource, {
                    "phase": "provisioning",
                    "external_id": external_id,
                })
                # Return here. Next reconcile picks up "provisioning".
                return

            if phase == "provisioning":
                # Step 2: check if provisioning finished
                # (or the observer controller already updated the status)
                if spec.get("infra_ready"):
                    await self._configure_cluster(resource_id)
                    await update_resource(conn, resource, {
                        "phase": "configuring",
                    })
                # else: nothing to do, wait for observer to update infra_ready
                return

            if phase == "configuring":
                # Step 3: finalize
                await update_resource(conn, resource, {
                    "phase": "ready",
                })
                return

            if phase == "ready":
                # Nothing to do — fully reconciled.
                return

    # -- External side effects (each individually idempotent) --

    async def _provision_infra(self, resource_id: str) -> str:
        # Idempotency: check if infra already exists with our tag
        # before creating. Return the external ID either way.
        ...

    async def _configure_cluster(self, resource_id: str) -> None:
        ...

    async def _cleanup_external_resources(self, resource_id: str) -> None:
        ...
```

# FIRST Controllers

**Cluster Status Observer:** polls cluster health endpoint and updates status/last_status_check

---
**StaticDeployment Health Observer:** polls health endpoint and updates health / last_health_check

---
**StaticDeployment Load Observer:** tracks load averages in Redis (see [Load Average utility](#load-average-utility) below)

---
**Health Alert Controller:** tracks alert state (last alerted status and time per
resource) and sends debounced Slack alerts on health changes (degradations or
recovery).  We want to track the health/status of:
   - Clusters
   - Static Deployments
   - Pilot Job Managers
   - Pilot Deployment Replicas
   - Aggregated pilot deployment health
   - The Gateway APIServer itself (/health endpoint)
   - Liveness of each `SchedulerAdapter`: (for GlobusComputePBSAdapter, verifying that the endpoint is online)
   - Liveness of Postgres and Redis
   - Failure in any controller (written by worker manager when a worker has unexpected exit/exception/heartbeat timeout)

The `ResourceRow`s managed by the system already have health/status as part of their state.
A separate DB table should be created specfically to track last-alerted status/timestamp per-resource, so
that the monitor only messages on health degradations/recoveries.

---
**HPC Scheduler Observer:**
- Polls `qstat` for each cluster's PilotSystem.
- For all matching jobs in the database, updates `phase`, `time_started`.
- Jobs that don't exist in the database but have a name matching the system
prefix (`__FIRST_PILOT_`) are inserted into the database in the **zombie**
phase with tombstone and finalizers set, so they can be reaped and deleted on the next pass.

---
**Pilot Endpoint Discovery Controller:**
- Listens for PilotJob changes
- `list_actionable` uses `PilotSubmitter.list_ready_endpoints` and takes the intersection with PilotJobs that are `running` without a set `manager_url`.
- Reconcile function: if the pilot is running and has no manager URL, then try `PilotSubmitter.get_endpoint()` and update `manager_url`.

---
**Pilot Replica Status Observer:**
- `list_actionable` returns PilotJobs that are running and have a manager URL.
- Listener: queue changes to `PilotJob` AND `PilotReplica`.  For PilotReplicas popped off the queue, simply lookup the parent `PilotJob` and queue it.
- Reconcile function: if the pilot is running and has a manager URL, hit the `get_status` endpoint of the pilot manager.
  - Update PilotJob.`resources`, `manager_health`, `idle_since`
    - `idle_since` should be updated to None if any replica is running.
    - `idle_since` should be set to the current timestamp **if and only if** it is None and zero replicas are running ([OCC](#column-level-occ))
  - For each replica in the status response, update PilotReplica.`model_url`, `observed_served_name`, `phase`, `status_info`, `last_health_check`, `started_at`.

**QUESTION:** Updating `last_health_check` on every check will create an infininte feedback loop as feared in [framework discussion above](#integrated-polling-and-notification-driven-loop).  How do we best tackle this issue?

---
**PilotJob controller:**
- Listens for PilotJob changes
- `list_actionable`: ???

**PilotJob Reconcile:**
- If the Pilot has the tombstone and submit finalizer, terminate the job and mark it terminated.
  - Set the tombstone on the replicas (sorted UPDATEs) at this time as well.
- If the Pilot has been idle for longer than `pilot_max_idle_time_min`, mark it for removal and return.
    - When we reap/cleanup Pilots and Replicas, we actually want to ensure their resources are fully cleaned up/freed, but keep them around for postmortem/visibility into recent activity (only once `now() > deleted_at + '7 days'` it gets the hard `DELETE`)
    - Consider a ClassVar `DELETE_AFTER` that controls the earliest that
    resource may be deleted after tombstone is set and finalizers all cleared
- If the job is in a terminal state, set the tombstone on the job and its replicas.
- If the job has a tombstone, return.
- If the resource is pending-submit, perform `PilotSubmitter.submit()`, record the submitted job id, and advance the phase.
    - Track total num queued+running jobs. Only submit up to a max depth, configured on the cluster pilot_system.
- If the manager has been consistently unhealthy for a debounce period (control plane not responding with 200s), set the tombstone.

---
**Pilot Deployment Controller:**
- Health aggregation (roll up from replica statuses)
- Load Average Tracking (see [Load Average utility](#load-average-utility))

---
**Pilot Autoscaling controller:**
- Follow load averages and set desired num replicas
- Responsible for scaling at a controlled rate (e.g. respect minimum intervals between scale-up/scale-down)

---
**Pilot Replica Controller:**
- Reaper: Identifies replicas running in pilot jobs that no longer have a counterpart in the DB. Sends stop signal ASAP to free up resources.
- Create new replicas when less than desired count (not placed on a job yet)
- Drain/mark for removal replicas when greater than desired count
- Drain: immediately set 0 weight in router; remove from rotation; mark as deleting state.
- Terminate after drained for ~30 sec, then delete
- Check replica health; Slack alerts when needed
- Drain/mark for removal unhealthy replicas or "stuck" in launching
- Drain/remove replicas whose pilot jobs have stopped/failed
- Watches `consecutive_launch_failures` on PilotDeployment: if a deployment has several launch failures in a row, stop hammering the scheduler with a doomed spec; backoff and eventually halt new launches.  (Question: when the problem is fixed; how do we signal it to reset and try again? Sometimes the fix is in the internal spec, sometimes it may be opaque; e.g. incorrect filesystem permissions fixed on the cluster)

---
**Pilot Replica Placement Controller:**
-  Start replicas onto pilot jobs
-  Stop replicas that are ready to terminate
-  Create/enqueue pilot jobs if a replica cannot be placed
-  If cluster is at capacity and replica cannot be placed, mark AT_CAPACITY status on

---
**Router Config Controller:**

The gateway's model routers rebuild themselves from a centralized router
configuration. The router config lists the models, endpoints, router parameters,
and access group information in a single reconstructable Redis-cached data
structure.  The controller rebuilds this data structure by listening to changes in deployment status.  It ensures that routers do not see deployments that are down, and it can assist in draining replicas before scale-down.

This enables the apiservers to simply listen for changes and rebuild their
LiteLLM routers from this Redis structure alone.  Database queries are kept out
of the hot path in the data plane.  The API servers will use this data structure
to:

- Update the singleton LiteLLM Router instance on the application
- Update the generic Router instance (non-LLM)
- Update in-memory Prometheus scraping configuration; exposed via API route to local Prometheus.
- Hot swap the above routing structures to reflect the model instances that are currently live. Routers are completely unaware of deployment mechanics; they are just a passive map of what http endpoints are currently live.
- Support "aliases" (as long as aliases are non-overlapping, a model may have
multiple alternate unique aliases) that transparently resolve to the unique
model name

# Appendix

## Load Average Utility

We measure **concurrent in-flight requests** using a Redis sorted set. Briefly,
`ZADD key score member` is like creating a Python dictionary identified as the top-level redis `key`, and setting `dict[member] = score` with the bonus that Redis keeps the entries sorted by score under the hood, making score-range queries cheap.
`ZCARD` gives you a quick O(1) count of members in the set. `ZREMRANGEBYSCORE` lets you evict members within a score range.

- On request start: `ZADD` the request id with a score = the unix time after which a leaked entry should be pruned (≈ now + 2 min).
- On request completion: `ZREM` the request id.
- On read: `ZREMRANGEBYSCORE` to evict anything past its expiry (self-cleaning against leaked/abandoned requests), then `ZCARD` for the current in-flight count.
- **Cold-start demand counts too:** a request to an offline (scale-to-zero) model `503`s immediately, but we still `ZADD` it with the ~2-minute expiry, so demand for offline models registers and can drive a scale-up.

A controller samples the noisy signal every 10 sec and buffers the last 30 samples in memory. On each sample arrival, average the last 6 (1m) and last 30 (5m).  Write these averages back into redis keys. Then redis contains a smoothed average of 1m/5m load, live-updating every 10 seconds. We may also consider recording the peak load as the max() aggregate over the same last 1m/5m worth of samples.

Finally, we don't want to store this data that changes every 10 seconds in Postgres. It's fine for it to be blown away when Redis restarts; it re-populates quickly.

API views of model deployments should read this load information out of Redis, combine it with the Postgres data, and return the combined view objects to clients.

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

## Health Alert Pseudocode
```python
# Inaccurate pseudocode; just sketching structure and patterns to borrow from:
class AlertController(Worker):
    WATCHED_TABLES = {"cluster", "pilot_job", "static_deployment", "pilot_replica", "pilot_deployment"}

    def __init__(
        self,
        client_state: ClientState,
        *,
        debounce_seconds: float = 30.0,
        slack_webhook_url: str,
    ) -> None:
        super().__init__("alert-controller", client_state)
        self._debounce = debounce_seconds
        self._webhook_url = slack_webhook_url
        self._pending: dict[tuple[str, str], dict] = {}
        self._flush_event = asyncio.Event()

    async def _listen_loop(self) -> None:
        conninfo = self.client_state.db_conninfo
        async for change in listen_for_changes(conninfo):
            if change.table in self.WATCHED_TABLES:
                await self._check_and_stage(
                    change.table, change.resource_id
                )

    async def _resync_loop(self) -> None:
        """Periodic sweep for anything missed."""

    async def _check_and_stage(self) -> None:
        """
        Read current vs last-alerted state. If there's a
        meaningful transition, stage it for the next flush.
        """
        if current == last_alerted:
            return

        self._pending[(table, resource_id)] = {
            "table": row["resource_table"],
            "id": row["resource_id"],
            "previous": last_alerted,
            "current": current,
            "message": row["health_message"],
            "changed_at": row["health_changed_at"],
        }
        self._flush_event.set()

    async def _flush_loop(self) -> None:
        """
        Wait for at least one pending alert, then wait the debounce
        window for more to accumulate, then flush everything in one
        Slack message.
        """
        while True:
            # Block until there's something to send.
            await self._flush_event.wait()
            self._flush_event.clear()

            # Debounce: wait for more changes to accumulate.
            await asyncio.sleep(self._debounce)

            if not self._pending:
                continue

            # Snapshot and clear.
            batch = dict(self._pending)
            self._pending.clear()
            self.update_heartbeat()

            # Separate degradations from recoveries.
            degradations = {...}
            recoveries = {...}
            await self._send_slack(degradations, recoveries)

            # Mark these as alerted so we don't re-alert.
            await db.executemany(
                """
                UPDATE alert_state
                    SET last_alerted_status = %(status)s,
                        last_alerted_at = now()
                    WHERE resource_table = %(table)s
                    AND resource_id = %(id)s
                """,
                [
                    {"table": k[0], "id": k[1],
                        "status": v["current"]}
                    for k, v in batch.items()
                ],
            )
```

