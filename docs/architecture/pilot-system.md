# Pilot Job System

The pilot system is how FIRST extends the control plane onto HPC compute
nodes. One pilot is one scheduler job; once a pilot is running, the
gateway can place, stop, and observe model replicas inside it on demand.

This page walks through the pilot lifecycle in three pieces:

1. **Submission** — how a pilot job gets onto the cluster.
2. **Communication** — how the gateway talks to a running pilot.
3. **On-node architecture** — what runs inside the allocation.

See also the [`first_pilot` package reference](../packages/pilot.md) for
config field details, ports, and CLI entry points.


## 1. Submission via SchedulerAdapter

![Pilot Job Submission via qsub](../images/Diagrams-Control-Plane-qsub.drawio.svg)

The gateway's pilot system takes a **pluggable `SchedulerAdapter`** (the
ABC in `first_common.schema.base_scheduler:SchedulerAdapter`) so the
same controllers can drive different HPC facilities. The adapter
surface is small — `submit_job`, `get_job_statuses`, `terminate_job`,
plus `put_file`/`list_files`/`read_file` for staging the pilot's config
+ submit script onto the cluster filesystem.

`PilotSubmitter` (in `first_gateway.platforms.pilot_submitter`) is the
layer above the adapter. Per pilot-job submission it:

1. Generates a fresh per-job server cert via `certmanager.generate_server_cert`.
2. Renders a `PilotRuntimeConfig` YAML (certs, ports, allowlist, workdir,
   job name) and writes it to the cluster via `adapter.put_file`.
3. Writes a small shell script that `uvx`-launches the pinned
   `first-pilot` version with `PILOT_CONFIG_FILE` pointing at the YAML.
4. Calls `adapter.submit_job` with the resulting `JobSubmitPayload`,
   under a name prefixed `__FIRST_PILOT_` so zombie discovery can
   distinguish FIRST-owned jobs from anything else on the queue.

### Adapters shipped today

| Adapter | Status |
|---|---|
| `GlobusComputePBSAdapter` (`platforms/schedulers/globus_compute_pbs.py`) | Implemented. Just-in-time registers `_qsub`/`_qstat`/`_qdel`/`_put_file`/`_list_files`/`_read_file` as Globus Compute functions at `build()` time and dispatches each adapter call via Globus Compute. Polls for results with a `TaskPending`/asyncio sleep loop. |
| IRI / Direct PBS / others | Future adapters; the abstraction is in place. |

The adapter's only job is to **get the pilot job submitted and report
back its scheduler id**. It is *not* on the runtime path.


## 2. After the job starts: direct control-plane connection

![Pilot Control Plane](../images/Diagrams-Control-Plane-Pilot.drawio.svg)

Once the scheduler dispatches the pilot job, the `SchedulerAdapter` steps
out of the picture. The gateway and the running pilot communicate
**directly** over mTLS — no scheduler-side hop, no Globus Compute task on
the hot path.

This matters for two reasons:

- **Latency.** Replica start/stop calls are sub-second round trips, not
  scheduler-mediated tasks.
- **Blast radius.** Adapter outages do not affect already-running pilots
  — they only prevent *new* pilots from being launched.


## 3. On-node architecture

![Pilot On-Node Architecture](../images/Diagrams-Pilot-Architecture.drawio.svg)

Inside the allocation, the pilot brings up two cooperating subsystems
behind a single NGINX terminator.

### NGINX terminator

- The pilot API starts NGINX first and puts itself behind it.
- **One** external port is opened per compute node; everything else is
  loopback.
- The port is secured by **mTLS**: only the gateway, presenting a CA-signed
  client cert, can connect.
- The NGINX manager re-renders the NGINX config and `SIGHUP`-reloads
  **gracefully** as replicas come and go — in-flight traffic is not
  dropped.

### Control APIs

The pilot exposes a small FastAPI control plane reachable at
`https://<job-ip>:<external_port>/control/`. Internally it binds to
`127.0.0.1:<external_port + 1>` so NGINX is the only externally-reachable
listener; replicas live on `external_port + 2`, `+3`, … and are reverse-
proxied at `/replicas/{name}/`.

| Endpoint | Purpose |
|---|---|
| `POST /start-replica` | Body `ReplicaStartRequest` — place a replica with the given `PilotLaunchSpec` and `GpuClaim`s; fails fast on local conflict |
| `POST /stop-replica/{name}` | Terminate the replica subprocess, free its GPUs, drop its NGINX route |
| `GET /status` | `PilotJobStatus` — replica list + node/GPU inventory |
| `GET /logs/{name}` | On-demand tail (~200 lines) of `stdout`/`stderr`/user log |

### Replica manager

The replica manager renders each model's startup script and supervises
the resulting subprocess plus a daemon health monitor thread:

- **Health-driven termination.** Replicas that take too long to start, or
  that become unhealthy, are killed locally.
- **Self-healing deployments.** Once a replica is killed, the gateway's
  replica controller garbage-collects the dead row and spawns a fresh
  one — the desired-replica-count gets re-met without admin action.
- **Backoff on bad specs.** Consecutive startup failures are tracked per
  deployment; after enough in a row, the controller stops hammering the
  scheduler with a doomed spec.


## Why this shape

A few non-obvious design choices fall out of this architecture:

- **Pilots are GPU pools, not model containers.** One pilot can host
  replicas of multiple different model deployments on the same node —
  the pilot owns the allocation, the *replica* is what binds to a
  specific model recipe.
- **The gateway never reaches a replica directly.** Every request hits
  NGINX first, which authenticates the client cert and proxies to the
  right local port.
- **Certs are per-job.** The gateway's
  [certificate manager](../packages/certmanager.md) mints fresh server
  certs at submission time, so each pilot's cert lifetime tracks the
  job's max walltime.
