# first_gateway

`first_gateway` is the user-facing server package. It runs on the Gateway VM
and is composed of two long-running processes:

- **apiserver** — the HTTPS API that authenticates requests, routes inference
  calls into the appropriate model replica, and serves the `alcf-ai` CLI.
- **controller-manager** — the reconciler stack that watches the Postgres
  resource tables and drives external state (HPC jobs, replicas, router
  config) toward the declared spec.

Both processes share a Postgres database and a Redis instance. See the
[Control Plane vs Data Plane](../architecture/control-data-plane.md) page
for how this fits into the larger system.

## Subpackages

| Subpackage | What it does |
|---|---|
| `apiserver` | FastAPI application — auth, route definitions, dependency wiring |
| `certmanager` | Library + CLI for the mTLS PKI. The gateway uses it to mint a fresh server cert for each pilot job. See [Certificate Manager](certmanager.md). |
| `controllers` | Framework and entrypoint for the control-plane reconcile loops. See [Controller Framework](../architecture/controllers.md). |
| `database` | SQLAlchemy ORM models for persistent resource state. See [Data Model](../architecture/data-model.md). |
| `platforms` | Pluggable adapters for new HPC facilities (scheduler adapters, site-specific config). This is where you extend FIRST to a new cluster. |
| `services` | Cross-cutting business logic kept out of API views — currently the declarative `plan`/`apply` implementation. Keeps API handlers thin and core functions easy to test. |
| `settings.py` | Pydantic-based settings validation; loads from env vars (and dotenv files in dev). |

## Async-native by design

The gateway is fully async — SQLAlchemy async sessions, `redis.asyncio`,
async FastAPI handlers, and `asyncio`-based controller workers under a
supervising manager process. There is no sync I/O on the request path or
in any reconcile loop.

## Running locally

See the [Developer Guide](../getting-started/developer.md) for the local
Docker Compose stack, env-file layering, and end-to-end test commands.
