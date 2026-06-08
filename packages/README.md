# FIRST — Package Overview

A monorepo with 5 Python packages managed by [uv](https://github.com/astral-sh/uv) workspaces.

## Quick Map

```
packages/
├── client/       # alcf_ai          — User-facing SDK + CLI
├── common/       # first_common     — Shared Pydantic schemas & types
├── gateway/      # first_gateway    — Core inference gateway (FastAPI)
├── pilot/        # first_pilot      — HPC scheduler interfaces
└── dashboard/    # first_dashboard  — (stub — not yet implemented)
```

## Package Details

### `client` — `alcf_ai`
The **user-facing package**. Provides:
- **`InferenceClient`** — an `httpx`-based SDK with automatic Globus auth, exposing cluster discovery, chat, and SAM3 (image segmentation) APIs.
- **`alcf-ai` CLI** — a `click`-based CLI (`uvx alcf-ai`) for login, listing endpoints/jobs, chatting with models, and batch SAM3 processing.
- **Globus transfer helpers** — stage data in/out of Globus collections.

### `common` — `first_common`
**Shared types and schemas** used by the gateway and other packages:
- **Schema types** — `ResourceName`, `ReplicaPhase`, `PilotJobPhase`, `DeploymentHealth`, `ClusterStatus`, etc.
- **Resource models** — `ClusterDetail`, `PilotDeploymentDetail`, `PilotReplica`, `PilotJob`, `ModelSummary`, etc.
- **Spec types** — `PilotConfig`, `PilotLaunchSpec`, `LoadThresholdStrategy`, `RouterParams`, `GpuClaim`.
- **Errors** — custom exception classes.

### `gateway` — `first_gateway`
The **core inference gateway** (FastAPI + async). Key modules:

| Module | Purpose |
|---|---|
| `apiserver/auth.py` | Globus token introspection, caching (Redis/in-memory), group resolution |
| `apiserver/api.py` | FastAPI app entry point |
| `database/models.py` | SQLAlchemy ORM models for clusters, deployments, replicas, jobs |
| `services/apply_spec.py` | YAML spec loader → Pydantic validation → DB apply pipeline |
| `controllers/manager.py` | Async controller manager with heartbeat monitoring |
| `controllers/worker.py` | Base `Worker` ABC with restart/backoff & supervision loop |
| `platforms/` | Health checks & scheduler interface integration |
| `pilot/` | Pilot health endpoint |
| `cache.py` | Redis-backed caching layer |
| `settings.py` | Configuration loading |

### `pilot` — `first_pilot`
A **stub package** — scaffold for HPC scheduler interfaces (Slurm, PBS, etc.). No code yet, but the schema in `common` already defines `PilotConfig` and `SchedulerInterface` for it.

### `dashboard` — `first_dashboard`
A **stub package** — empty, awaiting a UI/dashboard implementation.
