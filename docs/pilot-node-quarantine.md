# Pilot-side node quarantine (#372)

This is the simple pilot-side PBS implementation, not #371's controller database
or coordinator architecture. It changes admission of an individual attempt, not
FIRST's desired replicas, submission path, scheduling limits or retry cadence.
Dev and Prod use the same code with separate environment attribution and the
existing per-VM Slack health-alert destinations.

## Configure and stage

Use the matching corrected controller, gateway, common package and pilot. The
cluster's `pilot_system` includes:

```yaml
job_name_prefix: first_dev_tara_v2_
node_admission:
  checks: [gpu_count, gpu_memory_free, gpu_ecc_errors]
  on_failure: pbs_quarantine
  environment: dev
  quarantine_walltime_min: 10080
  max_quarantined_nodes: 8
```

Prod substitutes `prod` and `first_prod_tara_v2_`; the policy is otherwise the
same. The source allocation must match the exact prefixed pilot name and current
PBS physical-host/GPU allocation. Tara's submit preamble must include
`export PATH=/opt/pbs/bin:$PATH`: these tools are not on the default batch PATH.
The job must be able to execute and query `qstat` and `qsub` under the
service identity. Missing binaries, permissions, malformed responses and failed
lookups are infrastructure/admission errors, never hardware diagnoses.

The Tara wrapper companion retains service identity, allowed-node inventory,
same-rack placement, image and HSN checks. Its explicit `SECURITY_PASS` delegates
mandatory GPU checks to this pilot before manager initialization. Its new sealed
runtime pin must point at a clean matching FIRST source/environment. Remove the
obsolete headroom-bypass export when preparing that candidate. Existing published
bundles, model recipes, TCP confinement and the temporary VM reply route remain
unchanged. Do not activate new settings with an old pilot or old wrapper.

No database migration is added by this PR. Preserve the Dev Dockerfile
source-permissions workaround. A Dev database previously used for #371 may
retain its experimental tables and Alembic revision; do not drop or downgrade
them as part of this change, or blindly run migrations from a different graph.
Inspect and preserve that checkpoint separately before any future migration.

## What happens on a fault

1. Check active service-owned quarantine requests from either environment,
   including queued and held jobs. A matching physical host ends this attempt
   with `admission_deferred_known_quarantine` before GPU initialization or weight
   loading. Reused pilots repeat this guard before starting a new replica;
   existing healthy replicas are not stopped.
2. Obtain complete, nonce-terminated native observations on every allocated
   host. Healthy allocations need one complete sample. Suspected GPU faults
   require a second coherent sample at least one second apart. Missing ranks,
   empty/malformed queries, changing identities, unknown
   ECC, unexpected GPU clients and transient memory are infrastructure errors.
   They do not submit a quarantine. Model OOM is not part of this diagnosis.
3. A stable missing-GPU, uncorrected-ECC or unexplained headroom fault records
   paired observations, the source allocation and reason in a private evidence
   file. The free-memory threshold remains `ceil(total MiB * 0.95) + 256 MiB`.
4. Look for an existing valid request for that exact host. Names only aid
   discovery; owner, metadata, accepted host/resources and state establish
   identity. Submit at most once per operation, reusing the originating
   allocation's per-host CPU/GPU request and requesting `place=exclhost`.
   PBS enforces whole-host exclusivity. The payload is only `/bin/sleep infinity`.
5. Inspect PBS's accepted job identity, attribution, target, resources and
   walltime, then exit the failed source attempt. An ambiguous acknowledgement
   gets bounded lookup, not blind resubmission. The complete fault-action budget
   is 90 seconds, with native calls bounded to 15 seconds. Failures never retain
   the source indefinitely. Ordinary FIRST retries continue without waiting for
   the quarantine to run.

Older queued jobs may run before the quarantine; their startup guard rejects the
known node without diagnosing another fault. There are no model holds, global
locks, scaling-to-zero actions or quarantine-specific paused states. Visible
requests suppress duplicates; simultaneous Dev/Prod submissions can still race.
Duplicate requests are alerted for administrator resolution, not locked away.
The eight-node cap is a safety bound, not a new model-scheduling gate.

## Discover and verify requests

Native jobs are named `first_quar_ENV_PHYSICALHOST_OPERATION8` (at most 63
characters). Names aid discovery; a changed/truncated display name does not
override validated ownership, target and accepted resources. Attribution uses
`FIRST_QUAR_ENV`, `HOST`, `OP`, `SOURCE`, `REASON` and `EVIDENCE` variables.
Incomplete service-owned requests fail admission as infrastructure errors.
Never delete an arbitrary
job just because its name resembles a quarantine.

```bash
# Read-only inventory. Substitute an exact job ID discovered by the inventory.
qstat -f -F json
qstat -f -F json JOB_ID
pbsnodes -a -F json
```

Requested exclusivity alone is not confirmed isolation. The existing health
alerter verifies the accepted exact-host exclusive request, running placement
on the intended physical host and a nonexpired walltime. PBS is the authority
for scheduling and exclusivity. If running placement or expiration evidence is
missing, isolation stays unverified. Independent occupancy checks belong in
qualification tooling, not a second runtime scheduler/proof implementation.
Ordinary model shutdown/orphan cleanup rejects both environments' quarantine
names or metadata instead of deleting them.

## Alerts and repair

The existing health-alert worker distinguishes pending, verified isolation,
unverified/duplicate requests and lost isolation. Messages include host,
environment, source job, quarantine job, reason, evidence path and expiration
when available. Configure Slack credentials in the existing secure VM settings,
never in fixtures. Unit mocks do not establish real delivery.

Disappearance, deletion or expiry means **lost isolation, hardware health
unknown**, not recovery. FIRST continues its normal retries; admission screening
still applies, but physical exclusion is no longer guaranteed. Notifications
are deduplicated using the existing Redis alert bookkeeping. An absence warning
remains there because this simple implementation cannot authenticate an
administrator's repair procedure; it does not add a repair API/state machine.

Returning a genuinely faulty node requires Operations' remediation confirmation,
a fresh passing GPU/process/ECC screen on that exact host, and deliberate
administrator deletion of its validated quarantine job. Preserve the evidence.
While the lightweight quarantine owns the host, use authorized access to run
the reviewed pilot environment's read-only screen; do not initialize CUDA or
load a model. For Tara's four GPUs, an example on that exact node is:

```bash
# Run as the service identity on the owned quarantine host, using the pinned
# pilot Python from the admission bundle's first-pilot-runtime.json.
"$PILOT_PYTHON" -IB - <<'PY'
import json, socket, sys
from types import SimpleNamespace
from first_common.pbs_quarantine import physical_host
from first_common.schema.types import SSHDiscovery
from first_pilot.node_checks import run_node_checks
evidence = {}
config = SimpleNamespace(gpus_per_node=4, gpu_discovery=SSHDiscovery())
faults = run_node_checks(
    ['gpu_count', 'gpu_memory_free', 'gpu_ecc_errors'],
    [physical_host(socket.gethostname())], config, evidence=evidence)
print(json.dumps({'faults': faults, 'observations': evidence}, indent=2))
sys.exit(1 if faults else 0)
PY
# Only after Ops confirmation + fresh PASS + exact ownership/metadata review:
qdel JOB_ID
```

Native screening exceptions exit nonzero. Save the JSON receipt privately and
check that it belongs to the intended physical host and current repair attempt.
There is no automatic expiry-to-recovery or automatic release.

## Intentionally omitted

No admin-node daemon, dedicated coordinator node, shared status protocol,
cross-environment locking, held-submission/release machinery, quarantine DB/API
suite, delegated cancellation framework, generated good-node selections or
headroom bypass. #371's earlier qualification does not qualify this contract.
