# FIRST-native Tara node quarantine

FIRST isolates confirmed faulty nodes with lightweight, exact-host PBS jobs;
each VM's controller owns records, reconciliation and alerts. No Tara coordinator,
mutable registry, snapshot manager or global launch gate is used. Model jobs are
never held; normal scheduling, cancellation and model retry limits are unchanged.

Quarantine replaces bad-node exclusions and generated good-node placement, not
screening or security. Allowed-node/source ownership, same-rack placement, HSN
routing, GPU/process checks, TCP confinement and verified cleanup remain required.
Model OOM, auth/configuration errors and unexplained distributed failures do not diagnose bad nodes.

## What happens when a fault is found

1. Admission or post-cleanup screening confirms two native samples from a specific
   host in the originating allocation. A held, nonexecuting report preserves its
   source job ID/name/ctime, environment, reason, evidence and pinned policy bytes.
2. FIRST verifies the registered source and independent PBS allocation membership,
   recomputes the diagnosis, and commits a durable per-node submission intent.
   A pre-manager source retains its allocation for at most 120 seconds awaiting a request, then exits.
3. FIRST submits an exact-host, exclusive, long-walltime waiting job and verifies
   its accepted identity before the source exits. Normal replacement submission
   proceeds immediately while quarantine is queued; it does not wait for step 4.
   The waiter loads no model/GPU. Queued means **isolation pending**.
4. Isolation requires the job running on the intended host, independent full-host
   resource/all-owner census, valid worker activation and an unexpired allocation.
   The site-normalized `scatter:excl:group=tier0` is accepted only with proof that
   the physical host has one vnode and is exclusively owned by the waiting job.
5. Existing records/jobs are reconciled after restart. Lost isolation or conflicting
   duplicates alerts operators. Neither implies repair or automatic unbounded
   quarantine resubmission; ordinary model retries continue.

Already-queued jobs can win scheduling before quarantine runs. Managed admission
rejects an intersecting active quarantine before model initialization; unrelated
models can still launch/retry. The bridge lacks atomic cross-VM create-if-absent:
discovery/adoption and duplicate detection are bounded safeguards, not PBS fencing.
Failed/ambiguous guard lookups reject startup as infrastructure/admission errors,
never hardware diagnoses. If isolation is lost, FIRST still submits/retries models
and ordinary screening remains required.

## Configure the optional capability

Use `pilot_system.quarantine`, **not** `pilot_system.scheduler_config.quarantine_environment`.
The factory derives adapter mutation authority from this policy. Replace every
placeholder with the reviewed immutable commit/published SHA-256; YAML does not expand variables.

```yaml
pilot_system:
  quarantine:
    environment: dev                  # prod only on the separately approved Prod VM
    worker_path: /vast/draco/tara/projects/inference_service/deployment/artifacts/firstv2-pilot-admission/<bundle-commit>/quarantine_wait.py
    worker_sha256: <worker-sha256>
    admission_path: /vast/draco/tara/projects/inference_service/deployment/artifacts/firstv2-pilot-admission/<bundle-commit>/allocation_admission.py
    admission_sha256: <admission-sha256>
    policy_path: /vast/draco/tara/projects/inference_service/deployment/artifacts/firstv2-pilot-admission/<bundle-commit>/tara-node-admission.json
    policy_sha256: <policy-sha256>
    walltime_min: 10080
    max_submission_attempts: 3
    max_new_nodes_15min: 2
    max_active_nodes: 32
```

Keep source admission, waiter and policy pins consistent; missing/changed assets fail closed.
Other schedulers need not fake PBS assets/proof. Queue/account/workdir use existing pilot config.
Never edit published bundles, relax headroom thresholds or reintroduce a bypass.
Budgets are local, not cluster-wide; review Dev/Prod rollout and available capacity.

## VM update and database checkpoint

Use the existing checkout, project, volumes and overlays; never create a second stack
or restore old overlays/print secrets. Preserve the Dockerfile permissions workaround.

Migration `b21f0e4a7c10` adds `first.node_quarantine` and `first.quarantine_report_inbox`, not a replacement DB/model schema.
It is required before this controller starts unless already applied (Dev has it); check the target DB.
These are **Dev-only** examples, run as webportal in its existing checkout.
Review the approved revision/Dockerfile checkpoint; stop on unexpected changes. Not a Prod context:

```bash
cd /home/webportal/FIRSTv2
export COMPOSE_PROJECT_NAME=first
export COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml
git rev-parse HEAD
git status --short
sha256sum deploy/Dockerfile
docker compose ls
docker compose ps
docker compose exec -T controller-manager .venv/bin/alembic \
  -c packages/gateway/first_gateway/database/alembic.ini current
docker compose build controller-manager inference-gateway
# Only if missing, after backup and the separately coordinated idle update:
docker compose run --rm --no-deps inference-gateway .venv/bin/alembic \
  -c packages/gateway/first_gateway/database/alembic.ini upgrade b21f0e4a7c10
docker compose up -d --no-deps --no-build controller-manager inference-gateway
```

Skip migration when current already reports b21f0e4a7c10. Reuse existing tunnels
and separate CA roots. Production activation/restarts are separately coordinated.

## Operator commands: repair and return to service

Use normal auth and the selected VM's control API. These examples target Dev; get `uid`, environment, job ID and expiration from `list`:

```bash
alcf-ai auth login
first-admin --base-url http://127.0.0.1:9100 node-quarantine list
# After Operations confirms remediation; replace 1 and OPS-123 with real values:
first-admin --base-url http://127.0.0.1:9100 node-quarantine screen 1 OPS-123
first-admin --base-url http://127.0.0.1:9100 node-quarantine list
# Only while isolated and after the same reference's fresh screen_outcome=PASS:
first-admin --base-url http://127.0.0.1:9100 node-quarantine release 1 OPS-123
first-admin --base-url http://127.0.0.1:9100 node-quarantine list
# For lost/failed isolation, after reconciling any existing exact-host jobs:
first-admin --base-url http://127.0.0.1:9100 node-quarantine reestablish 1 OPS-456
```

A screen request does not release the node. A valid negative result is recorded
and its consumed carrier cleaned, but release is denied. Each nonce is write-once:
identical repeats are idempotent; conflicting results cannot overwrite a FAIL.
A PASS must remain fresh (120-second evidence window) when release is executed;
request a new screen if it expires. Release completes only after deliberate
deletion is acknowledged **and** an exact subsequent lookup proves the job gone.
Expiration/deletion/disappearance alone produces `lost`, not `released`.
Explicit reestablish creates a new bounded three-attempt operation, preserving
previous count/history. No automatic renewal/reset, GPU reset or unchecked group signaling is used.

## Protocol, ownership and alerts

Protocol 3 uses `FIRST_QUARANTINE_VERSION/KIND/ENV/CLUSTER/HOST/OPERATION` and
`SOURCE_ID/SOURCE_NAME/SOURCE_CTIME`. Waiting jobs are
`first_quar_<env>_<host>_<op8>`. Event reports are held
`first_qreport_<env>_<fault|activation>_<op8>` or
`first_qreport_<env>_screen_<nonce8>`; screen requests are held
`first_qrequest_<env>_screen_<nonce8>`. Only reports/requests are held, never models.
Report JSON is URL-safe base64, bounded to 64 KiB decoded/96 KiB encoded.

Both environments discover jobs; foreign jobs/activation are observer-only.
Only the matching environment may mutate/ack its carriers. Identity binds owner,
PBS ID/ctime, full operation, host/source/metadata. Normal cleanup protects all auxiliary
namespaces, including the legacy namespace during transition. Activation remains
available while the waiter lives; consumed reports/requests are cleaned only
after verified persistence. Ambiguous submissions are adopted by exact identity,
never blindly repeated.

The VM database holds durable records/receipts. JSONB emptiness may be SQL NULL or JSON null;
adoption, screen CAS and budgets handle both without migrations/manual resets.
Malformed reports retain an infrastructure notice, not a node diagnosis. Existing health
alerts report fault/pending, confirmed isolation/job/expiry, failed/lost isolation,
screen failures/timeouts and explicit return. Brief transitions remain latched
and deduplicated; each VM uses its configured alert destination. Configure tokens
only through approved secret files. Never post credentials in logs or chat.
