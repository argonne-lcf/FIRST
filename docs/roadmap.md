# Roadmap

## What's done

Foundations and the gateway-side building blocks are in place:

- Local dev environment (Docker Compose) and Makefile shortcuts for
  format, lint, type-check, and test.
- UV workspace and the five-package project layout.
- Fully async-native patterns established with SQLAlchemy and Redis.
- Schemas: a good first draft of all Spec/Status pairs.
- Error hierarchy started (will extend as we go).
- Globus Auth: fully ported.
- FastAPI router and dependency structure.
- Admin API routes: declarative plan/apply and resource-reading views.
- mTLS [certificate manager](packages/certmanager.md).
- [Controller framework](architecture/controllers.md): asyncio workers
  under a supervising manager process.
- Database models for all resource types.
- Globus Compute PBS scheduler adapter.
- `first-pilot` system: integration tests cover actual pilot startup,
  NGINX boot, mTLS connection from gateway to pilot, and replica
  lifecycle management.
- Proof-of-concept with LiteLLM router forwarding direct-to-Sophia,
  including streaming and translation.


## What's left until MVP

- Improve consistency of time units across schemas (`_min` suffix for
  minutes everywhere?)
- Soft delete semantics with finalizers
- Pilot weight-cacheing / auto-downloading component
- Rest of the controller framework
- Controllers (per [Controller Framework](architecture/controllers.md))
- Hot-swapping routers (LiteLLM, generic, Prometheus
  `http_sd_config` endpoint)
- Create a Globus Compute endpoint (minimal type: ThreadPoolEngine) for
  `SchedulerAdapter`
- Add API routes that proxy through routers
- Port some Globus Compute model configurations to the new system and
  test end-to-end


## Toward production

- Read-only web UI: follow resource status more easily
- Port all fixtures to the new declarative config, under git version
  control
- DB indexing
- Docs
- Deploy in Dev; load testing
- Test the health alert controller
- Logging: revisit logged events; use LiteLLM hooks to log metrics
- Prometheus + Grafana integration
- Log export pipeline to DuckDB + archival
- Batch system
- Consider merging Riccardo and Hari's batch inference tools with the
  `alcf-ai` client, enabling users to customize and submit batch jobs
  on their own allocations
