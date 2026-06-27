# first_gateway

`first_gateway` is the user-facing server package. It runs on the Gateway VM
as two long-running processes:

- **apiserver** — the HTTPS API: authenticates requests, exposes the
  declarative plan/apply admin endpoints and resource-read views, and
  (per design) routes inference calls into the appropriate model replica.
- **controller-manager** — the reconciler stack that watches the Postgres
  resource tables and drives external state (HPC jobs, replicas, router
  config) toward the declared spec.

Both processes share a Postgres database and a Redis instance. See
[Control Plane vs Data Plane](../architecture/control-data-plane.md) for
how this fits into the larger system.

## Process entry points

| Process | Command |
|---|---|
| apiserver | `gunicorn first_gateway.apiserver.api:app -k first_gateway.apiserver.uvicorn_worker.UvicornWorker` |
| controller-manager | `python -m first_gateway.controllers.manager` |
| certmanager CLI | `pilot-certmanager` (see [Certificate Manager](certmanager.md)) |

Both server processes acquire their shared connection pools through the
same `Settings.build_clients()` async context manager — wrapped by
`apiserver.api.lifespan` for the API, and by `controllers.manager.main`
for the controller manager. The resulting `ClientState` (httpx, Redis,
SQLAlchemy engine + sessionmaker, Globus auth + compute clients) is the
one object that every downstream layer is given.

## Subpackages

| Subpackage | What it does |
|---|---|
| `apiserver` | FastAPI app, auth, dependency wiring, route definitions. Lives in `apiserver/routes/`. |
| `certmanager` | Library + CLI for the mTLS PKI. The library functions (`generate_server_cert`, `gen_ca_pem`, …) are what `PilotSubmitter` calls per pilot job. See [Certificate Manager](certmanager.md). |
| `controllers` | `Worker` base class + supervising `manager.main`. The framework is implemented; today only a stub `ClusterHealthController` is registered (see [Controller Framework](../architecture/controllers.md) for the design). |
| `database` | SQLAlchemy ORM (`models.py`). Each `ResourceRow` subclass auto-registers into `resource_registry` so `plan_apply` can dispatch by `kind`. All relationships are `lazy="raise"`; callers must explicitly eager-load. See [Data Model](../architecture/data-model.md). |
| `platforms` | `health.py` (cluster + endpoint health probes), `pilot_submitter.py` (renders pilot config + cert + submit script, then calls the scheduler adapter), `schedulers/globus_compute_pbs.py` (the only adapter shipped today). |
| `services` | Cross-cutting business logic kept out of API views — currently only the declarative `plan_apply` implementation. |
| `settings.py` | Pydantic-based settings validation; loads from env vars (and `.env.*` files in dev). Defines `ClientState`. |

## Request lifecycle (apiserver)

1. gunicorn `UvicornWorker` (uvloop + httptools) receives a request.
2. `log_request` middleware installs a `RequestContext` in a `ContextVar`
   for structured logging, and times the handler.
3. FastAPI dependency tree resolves: `get_state` exposes `ClientState`;
   `get_session` yields a per-request `AsyncSession` (commit-as-you-go);
   `get_auth_user` runs `GlobusAuthService.validate_access_token` (Redis-
   cached introspection + group/policy/IdP checks); `get_admin_user`
   layers the admin-group check on top.
4. The route handler runs. `FirstError` / `TaskPending` / uncaught
   exceptions are normalized into JSON envelopes by app-level handlers.
5. After the response, `log_request` fire-and-forgets an access-log
   record (with redis-`SETNX` dedupe for repeated 4xx/5xx) and any
   `UserAuthEvent` from this request.

## Async-native by design

The gateway is fully async — SQLAlchemy async sessions, `redis.asyncio`,
async FastAPI handlers, and `asyncio`-based controller workers under a
supervising manager process. There is no sync I/O on the request path or
in any reconcile loop.

## Running locally

See the [Developer Guide](../getting-started/developer.md) for the local
Docker Compose stack, env-file layering, and end-to-end test commands.
