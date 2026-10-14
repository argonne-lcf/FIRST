# Pilot-side node admission and quarantine

The pilot screens its allocation before initializing or serving models. Verified
node faults can trigger lightweight native PBS waiting jobs that keep faulty
hosts allocated. Admission affects an individual attempt, not FIRST's desired
replicas, submission path, scheduling limits or retry cadence. Dev and Prod use
the same implementation with separate environment attribution and health-alert
destinations.

## Configuration

Use compatible controller, gateway, common and pilot package versions. Configure
the cluster's `pilot_system.node_admission`. For example, Tara uses:

```yaml
job_name_prefix: first_dev_tara_v2_
node_admission:
  checks: [gpu_count, gpu_memory_free, gpu_ecc_errors]
  on_failure: pbs_quarantine
  environment: dev
  minimum_free_memory_fraction: 0.95
  memory_reserve_mib: 256
  quarantine_walltime_min: 10080
  max_quarantined_nodes: 8
```

Prod substitutes `prod` and `first_prod_tara_v2_`; the policy is otherwise the
same. Native admission verifies the owned source PBS job ID, running state and
current physical-host/GPU allocation. Its display name is not fault evidence.
Attribution comes only from `node_admission.environment`, not process environment
variables. The PBS action requires that explicit setting, the pilot's queue and
account, and a verified running allocation. The `ACTIONS` map in
`first_pilot.admission` selects `exit_only` or `pbs_quarantine`, passing fault
evidence, the bounded deadline and verified allocation context. `exit_only`
schedules nothing; both actions end the failed attempt through `NodeChecksFailed`.

With `on_failure: exit` and no checks, admission does nothing. With
`on_failure: pbs_quarantine`, the known-quarantine guard runs even if the checks
list is empty. Configure the GPU checks to screen newly allocated nodes.

Memory policy and safety limits are configurable per cluster. The values above
are Tara's choices, not universal limits: admission requires
`ceil(total MiB * minimum_free_memory_fraction) + memory_reserve_mib`, and
walltime must also be permitted by the site's PBS queue. FIRST validates positive
walltime/cap values without imposing a seven-day or eight-node maximum.

For native PBS quarantine, the pilot must be able to run `qstat` and `qsub` under
the service identity and inspect native JSON job responses. Put the site's PBS
tools on the job's `PATH`. For installations under `/opt/pbs/bin`, use
`export PATH=/opt/pbs/bin:$PATH` in the submit preamble. Missing binaries,
permissions, malformed responses and failed lookups are infrastructure/admission
errors, never hardware diagnoses. Site launch wrappers remain responsible for
allowed-node, image, topology, routing and other deployment security boundaries.

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
   file. The free-memory threshold uses the configured fraction and MiB reserve.
4. Look for an existing valid request for that exact host. Names only aid
   discovery; service ownership, accepted host/resources and state establish
   the request. Metadata supplies diagnostic context, not a separate protocol.
   Submit at most once per operation, reusing the originating
   allocation's per-host CPU/GPU request and requesting `place=exclhost`.
   PBS enforces whole-host exclusivity. The payload is only `/bin/sleep infinity`.
5. Inspect PBS's accepted job ID, exact target and exclusive request, then exit
   the failed source attempt. Diagnostic metadata need not match an independent
   full record. An ambiguous acknowledgement
   gets bounded lookup, not blind resubmission. The complete fault-action budget
   is 90 seconds, with native calls bounded to 15 seconds. Failures never retain
   the source indefinitely. Ordinary FIRST retries continue without waiting for
   the quarantine to run.

Older queued jobs may run before the quarantine; their startup guard rejects the
known node without diagnosing another fault. There are no model holds, global
locks, scaling-to-zero actions or quarantine-specific paused states. Visible
requests suppress duplicates; simultaneous Dev/Prod submissions can still race.
Duplicate requests are alerted for administrator resolution, not locked away.
The configured node cap counts distinct physical hosts with active service-owned
requests, including both environments. It bounds quarantine actions, not model
submissions or retries. Quarantine state comes from PBS jobs; there is no separate
quarantine database or persistent coordinator.

## Native PBS layout support

The native path currently requires each execution vnode to be a canonical
physical hostname, one vnode per host, with positive CPU/GPU quantities matching
the pilot configuration and node file. Tara's native `pbsnodes` inventory and
accepted allocations use that layout. The shared GraphQL adapter can consume
`allocatedMachines` mappings, but those are not available in native `qstat`.
Split entries, missing quantities and node-file mismatches fail as infrastructure
errors, not missing-GPU diagnoses. Names alone cannot detect a logical alias
that is also used in the node file: sites must establish this physical-vnode
layout before enabling native quarantine. Do not enable it on aliased/split-vnode
sites without an authoritative vnode-to-host mapping. The native implementation
does not provide that mapping.

Request `place=exclhost`. PBS on Tara may normalize this to `scatter:excl`;
vnode exclusivity is whole-host exclusivity only in the supported one-vnode-per-
physical-host layout. Validation accepts the PBS exclusivity token with generic
arrangement/group modifiers, not a hardcoded Tara grouping label. Running
confirmation still requires valid actual placement on the intended host.

## Discover and verify requests

Native jobs are named `first_quar_ENV_PHYSICALHOST_OPERATION8`. Tara's physical
hostnames keep these names short; a changed/truncated display name does not
override validated ownership, target and accepted resources. Diagnostic context uses
`FIRST_QUAR_ENV`, `HOST`, `OP`, `SOURCE`, `REASON` and `EVIDENCE` variables.
Failed or ambiguous ownership/target lookups fail admission as infrastructure
errors. Never delete an arbitrary job just because its name resembles a
quarantine.

```bash
# Read-only inventory. Substitute an exact job ID discovered by the inventory.
qstat -f -F json
qstat -f -F json JOB_ID
pbsnodes -a -F json
```

The existing health alerter reports isolation when PBS accepted the exact-host
exclusive request and reports it running on that intended physical host.
PBS is the authority for scheduling and exclusivity; runtime does not rebuild
an independent occupancy/resource census. Missing or mismatched placement stays
unverified. Expected expiration is useful operator information, not an additional
runtime proof or a hardware-health certificate.
Ordinary model shutdown/orphan cleanup rejects both environments' quarantine
names or metadata instead of deleting them.

## Alerts and repair

The existing health-alert worker distinguishes pending, running isolation and
unverified/duplicate requests. Messages include host,
environment, source job, quarantine job, reason, evidence path and expiration
when available. Configure Slack credentials in the existing secure VM settings,
never in fixtures.

When a quarantine disappears from PBS's active-job inventory, its alert clears
normally:
**"Quarantine ended; node is eligible for startup screening."** This does not
claim the hardware recovered. Physical exclusion is no longer guaranteed, and
FIRST continues normal submissions/retries with admission screening still
required. Expiration alone is not proof of repair. Notifications use the existing
deduplicated health-alert machinery; no persistent lost-isolation warning, repair
API or repair-certification record is added.

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
from first_common.schema.types import NodeAdmission, SSHDiscovery
from first_pilot.node_checks import run_node_checks
evidence = {}
config = SimpleNamespace(
    gpus_per_node=4, gpu_discovery=SSHDiscovery(),
    node_admission=NodeAdmission(
        minimum_free_memory_fraction=0.95, memory_reserve_mib=256))
faults = run_node_checks(
    ['gpu_count', 'gpu_memory_free', 'gpu_ecc_errors'],
    [physical_host(socket.gethostname())], config, evidence=evidence)
print(json.dumps({'faults': faults, 'observations': evidence}, indent=2))
sys.exit(1 if faults else 0)
PY
# Only after Ops confirmation + fresh PASS + exact job ownership/target review:
qdel JOB_ID
```

Native screening exceptions exit nonzero. Save the JSON receipt privately and
check that it belongs to the intended physical host and current repair attempt.
There is no automatic expiry-to-recovery or automatic release.

## Implementation entry points

- `first_pilot.admission`: startup guard, source-allocation validation, fault
  evidence, action dispatch and bounded native PBS submission.
- `first_pilot.node_checks`: complete command observations and confirmed GPU,
  memory/process and ECC checks.
- `first_common.pbs_quarantine`: canonical identities, accepted-request
  validation and active quarantine discovery.
- `GraphQLPBSAdapter.get_quarantine_statuses`: scheduler status and intended
  running placement for the health alerter.
- `check_quarantined_nodes`: pending, isolated, unverified and duplicate alerts;
  disappearance clears through the existing health-alert worker.
