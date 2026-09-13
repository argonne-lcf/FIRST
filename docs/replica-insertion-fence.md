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

The observer now charges pre-manager failures on the first `exiting` **or**
`gone` transition. Waiting until `gone` allowed the reconciler/controller to
begin draining at `exiting`, clearing the active assignment or marking the job
for deletion before failure accounting could see it. A locked current PilotJob
row serializes concurrent observations; the terminal state and distinct affected
deployment increments remain in one transaction. Intentional deletion,
already-draining replicas, and jobs that published manager readiness retain
their no-charge behavior. This does not change scheduler cleanup ownership.

The separate opt-in `tests/test_pilot_job_failure_accounting_postgres.py` suite
reuses the same guarded disposable PostgreSQL setup, with pilot RPC construction
mocked. It includes the five insertion tests plus seven accounting cases:
exiting-before-unassignment, duplicate concurrent observations, direct gone,
distinct deployment counting, intentional/ready no-charge, already-draining
no-charge, and atomic state/counter rollback on injected precommit failure.
All 12 tests passed on the isolated PostgreSQL setup on 2026-09-13.
