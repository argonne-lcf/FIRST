# Data Model

The Postgres schema is the source of truth for the desired and current
state of every model and deployment, across all clusters. Each table has
its own controllers; see the [Controller Framework](controllers.md) for
how rows get acted on.

![Entity Relationship Diagram](../images/Diagrams-ER-Diagram.drawio.svg)

## Storage layout: one table per resource

The Spec/Status split (see [Declarative Configuration](declarative-config.md))
is an *API-shape* distinction, not a storage one. Each resource is a
**single Postgres table** (a `ResourceRow` SQLAlchemy subclass) whose
columns are the union of its Spec and Status fields.

- Embedded value objects that are not independently addressable —
  `PilotLaunchSpec`, `RouterParams`, the various `Status` blobs — are
  stored as **JSONB columns**.
- Genuinely high-churn, ephemeral state stays **out of Postgres
  entirely**: live in-flight request counts and load averages live in
  Redis, not in a column we would otherwise hammer with writes.

## Naming convention

Every table has both:

- An **integer primary key** (surrogate key).
- A **string `name`** column with a unique constraint.

The string `name` is what YAML manifests reference, so admins don't have
to manage numeric IDs by hand. Keeping the PK separate from the name
also means resources can be **renamed**, and a resource that was
destroyed and re-created with the same name is distinguishable from the
original by its PK.


## Resources

### `AccessGroup`

A set of Globus groups and domains used for access control. Other
resources point at an `AccessGroup` to delegate "who can use this."

### `Model`

A routable model name and the set of API endpoints (chat completions,
embeddings, etc.) that the model accepts.

- FK → `AccessGroup` (who is allowed to use this model).

### `Cluster`

A physical grouping for deployments — a single HPC site or a logical
group of nodes — with some shared status and configuration.

### `PilotDeployment`

An HPC-managed deployment of a model, hosted via the
[pilot job system](pilot-system.md). The pilot system handles
submission, scaling, and replica lifecycle for these.

- FK → `Model` (what to serve).
- FK → `Cluster` (where to serve it).

### `StaticDeployment`

A model deployment that is managed *externally* (already-running
endpoint), not by the pilot system. FIRST just proxies to it.

- FK → `Model`.
- FK → `Cluster`.

### `PilotJob`

A submitted scheduler job that brought up a `first_pilot` process on the
cluster. A pilot job is a **GPU pool**, not a model container — one
pilot can host replicas of several different `PilotDeployment`s on the
same node.

- FK → `Cluster` **only**. Deliberately *not* tied to one deployment.

### `PilotReplica`

A single running replica of a model inside a pilot job.

- FK → `PilotDeployment` (the recipe for starting this model).
- **Nullable** FK → `PilotJob` (which job, if any, the replica has been
  placed on). The nullable FK is what lets the placement controller
  create replicas in a "pending placement" state and bind them to a job
  as capacity opens up.

## How the relationships drive controllers

- The placement controller watches `PilotReplica.pilot_job_id IS NULL`
  rows and binds them to suitable `PilotJob`s.
- The pilot autoscaling controller compares `PilotDeployment.desired_replicas`
  to live `PilotReplica` counts to decide whether to submit more
  `PilotJob`s.
- The router config controller rolls up the `(Model, PilotDeployment,
  PilotReplica)` triple into the Redis-cached router map the data plane
  reads from.
