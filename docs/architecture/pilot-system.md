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

The gateway's pilot system takes a **pluggable `SchedulerAdapter`** so
the same controllers can drive different HPC facilities:

- **Globus Compute** — fan out submissions through Globus Compute
  endpoints (default for ALCF clusters).
- **IRI** — DOE's Integrated Research Infrastructure.
- **Direct PBS** — talk to a PBS server directly when we have a login
  shell on the cluster.
- …easy to add others; the adapter surface is small.

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

The pilot exposes a small control plane reachable at
`https://<job-ip>:<external_port>/control/`:

| Endpoint | Purpose |
|---|---|
| `POST /start-replica` | Place a replica with a given spec and GPU set |
| `POST /stop-replica/{name}` | Terminate a replica and free its GPUs |
| `GET /status` | List replicas and node inventory |
| `GET /logs/{name}` | On-demand tail of stdout/stderr/user log |

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
