# Tara quarantine coordination: FIRST client

This is an opt-in client for the shared Tara-local PBS quarantine coordinator.
It does not change the GraphQL bridge server, add a network service, take nodes
offline, or classify ordinary model failures as faulty hardware. Existing
published bundles and running jobs are not modified by this source change.

## Activation and rollout

Set `pilot_system.scheduler_config.quarantine_environment` to `dev` or `prod`
only after the shared coordinator and its version-1 protocol have been tested.
The adapter requires PBS owner `openinference_svc` and the matching native job
prefix `first_dev_tara_v2_` or `first_prod_tara_v2_`. Without that setting,
ordinary submissions retain their previous behavior. Tara ordinary cleanup
still refuses the separate `first_tara_quarantine_` namespace.

Dev-only tests do not prove cross-environment fencing while Prod still submits
unheld jobs. After review and merge, activation on both controllers must be
coordinated with the shared coordinator. Healthy running models stay running.
No database migration is required. Do not activate this by publishing a new
bundle or changing Prod fixtures before the remaining deployment review.

## Submission and cancellation

Managed model submissions use existing `JobInput.submitAsHold=true` and
`holdType=u`, with native environment tags:

```text
FIRST_QUARANTINE_VERSION=1
FIRST_QUARANTINE_KIND=model
FIRST_QUARANTINE_ENV=dev  # or prod
```

Only the shared native coordinator releases these user holds. Model resource
validation, rack constraints and all admission/security checks remain intact.

FIRST does **not** send `deleteJob` for a coordinated model. It queries the exact
source identity and submits a harmless, initially user-held CPU-only control
job named `first_tara_quarantine_c<PBS-job-number>`. This carries the version and
environment plus `FIRST_QUARANTINE_KIND=cancel`, `FIRST_QUARANTINE_SOURCE_ID`,
`FIRST_QUARANTINE_SOURCE_NAME` and `FIRST_QUARANTINE_SOURCE_CTIME`. Queue and
account are copied from the verified source job. The source ctime is derived
from GraphQL submit-time microseconds; qualification must establish that this
matches native PBS `ctime` before activation.

An acknowledgement means **cancellation requested**, not that the allocation
was released or the faulty host isolated. The existing observer continues
checking the source until PBS proves it gone. The coordinator verifies native
owner/name/ctime/environment and processes fault fencing before deleting the
source allocation. The control job never runs or initializes a GPU.

Rolling activation preserves normal stopping of existing, already-running
legacy jobs. This exception requires the exact service owner and matching
Dev/Prod model-job prefix, with **no** `FIRST_QUARANTINE_*` tags. Older immutable
admission wrappers cannot publish protocol-1 quarantine intents. FIRST uses
normal, non-forced PBS deletion for those running allocations only. Untagged
queued jobs must be reconciled/held by the coordinator before activation;
partial, malformed or wrong-protocol tags never select this fallback. All new
client submissions remain atomically held and tagged.

Before creating a cancellation request, FIRST adopts exactly one verified
matching request already in PBS. Duplicate, mismatched or unheld requests fail
closed. A lost create response is not retried within the operation; a later
reconcile must query and adopt the existing request. The shared coordinator is
the authority that serializes PBS mutations; FIRST's namespace is not a PBS
uniqueness constraint.

For a pre-manager failure, FIRST reads the registry and model statuses from the
same scheduler listing. Typed faults on the originating allocation defer its
first terminal transition while isolation is pending, failed or lost, keeping
submission capacity occupied. Once every attributed host is proven isolated
with current, unexpired evidence, FIRST allows the normal terminal transition
without charging a model-launch failure. The coordinator's separate bounded
node-fault budget limits these replacements. Unattributed model OOM, auth,
configuration and distributed failures keep the existing launch-failure counter
behavior. Intentional shutdown still cleans up normally through the coordinator.

The coordinator must publish the pending registry state before deleting the
originating allocation. This ordering is a qualification requirement, not proof
supplied by the mock tests.

## Health alerts

The coordinator mirrors bounded status in one always user-held PBS job named
`first_tara_quarantine_registry`. FIRST reads it through the existing GraphQL
scheduler and sends observations through its existing health-alert mechanism.
It requires the exact owner/name, user hold and `VERSION=1`, `KIND=registry`
tags. `FIRST_QUARANTINE_STATUS` is padded URL-safe base64 containing:

```json
{
  "schema_version": 1,
  "generation": 1,
  "updated_unix": 1700000000,
  "notification_environment": "dev",
  "gate_closed": true,
  "gate_error": null,
  "records": [
    {
      "host": "x4820c0s0b0n0",
      "origin_job": "8123.tara",
      "environment": "dev",
      "code": "missing_gpu",
      "reason": "missing_gpu",
      "evidence": "/service/evidence/node.json",
      "quarantine_job_id": null,
      "expires_unix": null,
      "status": "pending",
      "last_transition_unix": 1700000000
    }
  ]
}
```

The encoded status is at most 32 KiB; the heartbeat is at most 180 seconds old.
Statuses are `pending`, `isolated`, `failed`, `lost`, and explicitly `released`.
The same node key deduplicates repeat observations; a status change creates a
new transition. Confirmed isolation includes quarantine job ID and expiration.
Only `notification_environment` emits the shared node transitions, avoiding
normal duplicate Dev/Prod notifications. Failure to read or validate a registry
remains a check failure, not an empty healthy result. Omitting a previously
alerted unresolved node also fails, rather than falsely reporting recovery.

The shared files remain authoritative after registry deletion or restart.
Notification-owner changes require a deliberate alert-state handoff; do not
switch the owner to silence outstanding alerts. These are port/status tests,
not proof of Operations' inbound filters or GPU repair.

## Qualification gates

- Mock regression tests cover held model submission, unchanged resource
  translation, cancellation identity/idempotence, namespace protection,
  status transitions, stale/invalid registries and unresolved-record loss.
- Before live activation, demonstrate the existing bridge maps `env` to native
  `Variable_List`, honors atomic user-held submission, and returns exact owner,
  queue, account, holds and submission time matching native PBS identity.
- Controlled native tests must demonstrate queued-job fencing, exact-host
  effective exclusivity, reconciliation and guarded return to service.
- No healthy production jobs, system-wide settings or published artifacts are
  changed by the mock tests.
