# Control Plane vs Data Plane

FIRST separates the system into two planes with very different
availability requirements:

- **Control plane** — everything that participates in *configuring* models,
  launching/scaling them, and tracking their health.
- **Data plane** — the path an inference request actually traverses, from
  the user's HTTP call to the model replica and back.

End users almost never touch the control plane directly. Admins drive it
through declarative manifests (see [Declarative Configuration](declarative-config.md)),
and a few read-only views let users discover what's currently running.

The two planes are kept loosely coupled so that **a control-plane outage
does not interrupt steady-state inference traffic**. If the controller
manager dies, we restart it ASAP — but requests against models that are
already running keep flowing.

## The participants

![Data Plane Participants](../images/Diagrams-data-plane.drawio.svg)

### Control plane

- **API Server** — exposes the control-plane interfaces admins use to
  declare desired state (alongside the user-facing inference routes).
- **Controller Manager** — runs the reconcile loops that *enact* the
  declared state. See the [Controller Framework](controllers.md) for
  the design.
- **Postgres** — the durable source of truth for every configured
  resource. Spec is admin-authored; status is controller-authored.
- **Redis** — caches data the system can rebuild on restart: router
  config, in-flight load counters, etc.

### Data plane

The data plane (highlighted green in the diagram above) is just the
inference request path:

```
client → API Server (auth + routing) → mTLS → NGINX on compute node → model replica
```

Once a replica is running and the router config has been published to
Redis, no control-plane component sits on the request path. The API
Server reads its router map from Redis, opens an mTLS connection to the
pilot's NGINX, and proxies straight through.

## Implications

- **Outage tolerance.** Controller-manager crashes do not page on
  user-visible symptoms. Routing config in Redis is the last-known-good
  picture; the data plane runs against it until the controller comes
  back and refreshes it.
- **Hot path stays out of Postgres.** API servers never query Postgres
  to route a request — Redis-cached router state is enough.
- **Admin reads vs user reads.** Admin views can join Postgres and Redis
  freely; user-facing discovery endpoints (list models, check load) are
  served from Redis.

!!! note "Status"
    Today the apiserver, controller-manager, Postgres, and Redis are
    all running in the Compose stack, and the control-plane admin
    endpoints (plan/apply, resource reads) are wired through them.
    The data-plane inference path described above (mTLS to NGINX on the
    compute node, Redis-cached router map) is the **target**: pilot
    integration tests exercise the mTLS leg end-to-end, but the
    router-map publishing and the inference views that consume it are
    still being built. See [Roadmap](../roadmap.md).
