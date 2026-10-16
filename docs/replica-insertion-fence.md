# Replica insertion fence

The initial reconcile read is advisory. Immediately before inserting replicas,
the reconciler locks the exact `PilotDeployment.uid` with `SELECT FOR UPDATE`
in a fresh transaction, reloads related capacity, and recomputes the deficit
from the current desired count and launch-failure budget. Concurrent inserters
therefore cannot both consume the same deficit. A stale deployment name cannot
insert into a deleted/recreated deployment with a different UID.

The existing boundary remains `failures > maximum`: equality permits the final
retry. Terminal/draining replicas and replicas on dying pilots still do not
count as capacity; their replacement can begin immediately. This change does
not introduce a no-overlap policy, wait for teardown, cancel existing replicas,
or fence a separate stale writer of `desired_replicas`. Terminal pilot failure
accounting is a separate concern.

## Focused tests without live services

Use the matching FIRST environment and source packages on `PYTHONPATH`:

```bash
PYTHONDONTWRITEBYTECODE=1 python tests/test_replica_reconciler_fence_unit.py -v
```

The separate PostgreSQL suite is opt-in and never reads application DB/Redis
settings or shared pytest fixtures. Its URL validator accepts only the dedicated
`first-test-reconcile-pg-20260913` host and `first_reconcile_test` user/database,
through `FIRST_RECONCILE_TEST_DATABASE_URL`. It creates and removes the `first`
schema in that disposable database using ORM metadata, not migrations. Redis is
mocked; no scheduler, pilot, model, or GPU is invoked.

```bash
PYTHONDONTWRITEBYTECODE=1 python tests/test_replica_reconciler_fence_postgres.py -v
```

On 2026-09-13, all nine unit tests and five PostgreSQL tests passed in isolated,
new, task-labelled containers with an internal network, no published ports and
no host mounts. The PostgreSQL image was the cached pin
`sha256:7157393f508fd8eb46119937fab39813783fe3e7d4c6316c45c12ce2ea25e61d`.
The database tests hold a blocker transaction and observe PostgreSQL lock
waiters before releasing it, exercising concurrent inserters, scale-to-zero
and failure-budget commits while insertion waits, same-name UID replacement,
and immediate replacement of terminal replicas. These results do not claim a
full migration, DB/Redis integration-suite, or live deployment qualification.

## First-terminal pilot failure accounting

A pilot that reaches `exiting` or `gone` without ever publishing a manager
endpoint is a *pilot start failure*. It is counted against its **cluster**, not
against the deployments placed on it: a pilot that never comes up usually
points at the cluster or its scheduler, and charging deployments left them
`failed` after an outage or maintenance. `PilotDeployment.consecutive_launch_failures`
now counts only replica-level failures (a launcher 400, or a replica reaching
`error` or `start_timeout`).

The observer counts on the first `exiting` **or** `gone` transition. Waiting
until `gone` allowed the reconciler/controller to begin draining at `exiting`
first. A locked current PilotJob row serializes concurrent observations; the
terminal state, `PilotJob.failure_reason`, and the `Cluster.pilot_start_failures`
increment commit in one transaction. Intentional deletion, jobs that published
manager readiness, and jobs lost while their cluster is under maintenance are
not counted. `failure_reason` stays on the soft-deleted row (visible in cluster
detail until retention removes it), since cluster resets wipe
`reconcile_last_error`.

### The cluster latch

At `PilotConfig.max_consecutive_pilot_start_failures` (default 3), the observer
logs one WARNING and the cluster's pilot submissions halt: the PilotJobController
holds `pending_submit` jobs and the ReplicaPlacer creates no new PilotJobs.
Placement onto existing healthy pilots and all other reconciliation continue, and
the health alerter reports a critical `cluster/{uid}/pilot_starts` observation.

The latch is one-way. A manager coming up resets the count only while it is
still below the threshold. Once tripped, only `first-admin reconcile-reset
Cluster.<name>` or an apply that patches the cluster clears it; a no-op apply
does not. The reset deliberately lives outside `reset_reconcile_state`, which
also runs on every successful scheduler poll and would release the latch on its
own. Held jobs then submit as normal.

### Tests

The separate opt-in `tests/test_pilot_job_failure_accounting_postgres.py` suite
reuses the same guarded disposable PostgreSQL setup, with pilot RPC construction
mocked. It includes the five insertion tests plus nine accounting cases:
exiting-before-unassignment, duplicate concurrent observations, direct gone,
stale observations across both terminal states, unchanged nonterminal
transitions, one count per pilot regardless of its replicas, intentional/ready
not counted, maintenance not counted, and atomic state/counter rollback on an
injected precommit failure.

Terminal retirement stays monotonic: a locked `exiting` row cannot regress to a
nonterminal observation, and `gone` cannot regress at all. Otherwise a stale
running observation could reopen the next terminal transition's count. Ordinary
queued/starting/running transitions, including requeue, remain allowed. The
existing adapter maps suspended/staging-out jobs to `exiting`, and FIRST already
retires those allocations; this guard does not claim general PBS suspend/resume
support for an identity that FIRST has begun retiring.
