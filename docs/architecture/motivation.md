# Motivation

This page captures *why* FIRSTv2 looks the way it does — what limitations of
the v1 system drove the redesign, and which goals the new architecture is
held against.

## Why FIRSTv2?

The v1 system (FIRST) places Globus Compute — a Function-as-a-Service layer
— directly in the inference **data plane**. Every request is wrapped as a
Globus Compute function call and routed:

```
Gateway → Globus Compute (AWS) → manager endpoint (login node)
        → Parsl interchange → Parsl worker (compute node) → vLLM
```

That single design choice is the source of most of v1's pain. The
limitations fall into five areas.

### 1. Latency

Each request makes a round trip out to a cloud service and back through a
chain of login-node components before reaching an inference engine. The
extra hops tax every request.

### 2. Streaming is fundamentally awkward

Globus Compute is built around function calls that return a single complete
value, not a streaming generator — so token streaming cannot be expressed
inside the call. The v1 workaround spawns background threads that POST
chunks to a callback URL on the gateway. That workaround is:

- **Brittle and narrow** — deeply coupled to the OpenAI Chat Completions
  format, hard to extend to OpenAI Responses or Anthropic Messages
  streaming.
- **Network-constrained** — requires compute nodes to open connections
  *back* to the gateway, forcing bidirectional connectivity and breaking
  local development (a laptop gateway is unreachable from compute nodes).
- **Redis-resource-intensive** — streaming invokes synchronous Redis
  `LRANGE` calls from an asyncio server, which can stall the gateway under
  heavy load.

### 3. Reliability

Manager and endpoint processes run in user space on login nodes. More than
once they have died under resource pressure, taking the inference engines
behind them unreachable with them.

### 4. FaaS impedance mismatch

Function serialization/deserialization requires the gateway and endpoint
Python environments to match closely (interpreter version, dependencies) —
an operational coupling with no inherent reason to exist.

### 5. Developer and operator experience

Endpoint logic is authored as a single serialized closure, registered with
Globus Compute under a UUID the gateway is configured to invoke. As a
result:

- The source behind a registered UUID is hard to recover or audit.
- Changing one line means re-authenticating as the service account,
  re-serializing and re-registering the function, recording the new UUID,
  and updating configuration on *both* the endpoints and the gateway — a
  long deployment loop for a small change.
- A large fraction of configuration lives on the cluster and is opaque to
  the gateway: environment variables and `echo` statements buried in bash
  scripts and Globus Compute configs in the service user's home directory,
  relied on by health-check cron jobs with no explicit contract or
  validation.

### The insight

This heavy abstraction over the inference HTTP request is precisely what
prevents v1 from adopting conventional AI-proxy architectures such as
LiteLLM. If we adopt one constraint — *every AI model is reachable from
the gateway over HTTPS* — the data plane becomes ordinary HTTP proxying.
That single move lets us:

- cut latency by removing the cloud round trip and the login-node hops;
- improve reliability by deleting the failure-prone user-space components
  on the login node;
- adopt `litellm.Router` for protocol translation and streaming across all
  modern LLM dialects, essentially for free;
- move serialized closures into ordinary Python packages in our own
  codebase;
- replace per-cluster, home-directory configuration with a declarative,
  gateway-side configuration system;
- continue to scale naturally to a federated, heterogeneous fleet by
  keeping model deployment decoupled from routing.

Globus Compute does not disappear in v2: we still rely on it for
interfacing with HPC cluster schedulers, at the level of issuing **control
plane** RPCs to `qsub`, `qstat`, and `qdel`. The key shift is that once a
model is running, Globus Compute no longer participates in the data plane.
In fact, the Globus Compute endpoint could crash and the models that it
launched would remain reachable.

## Goals

### Primary goals

1. **HTTP-native data plane.** Every inference request reaches its model
   over a direct HTTPS path — no cloud round trips, no login-node
   indirection.
1. **First-class streaming across dialects.** Native token streaming for
   OpenAI Chat Completions, OpenAI Responses, and Anthropic Messages with
   no bespoke callback channel. Streaming behaves identically in local dev
   and production; supporting a new dialect is a library/config change,
   not custom plumbing.
1. **A general AI-model serving platform.** The gateway serves *any*
   model behind an HTTP interface: LLMs and embeddings today, but equally
   promptable vision models (e.g., SAM 3 for image segmentation),
   physicist-developed GNNs, and scientific foundation models not yet
   invented. `litellm.Router` handles routing and protocol translation
   *for the LLM dialects*; it is one component, not the system. Onboarding
   a non-LLM model requires no data-plane changes — only a deployment
   definition and (if it speaks a custom protocol) a pass-through route.
1. **Heterogeneous, federated serving.** A single logical model can be
   backed by deployments on multiple clusters and accelerator types
   (Sophia A100, Minerva B200, Metis SambaNova SN40L) behind one uniform
   API, with cross-cluster load balancing and fallback. Adding a cluster
   behind a model is a config change; a single-cluster outage degrades
   capacity, not availability.
1. **Declarative, gateway-side configuration.** Admins define, deploy,
   and rebalance models from the gateway — no per-model configuration in
   cluster home directories, no SSH-and-restart loop.
1. **Reliability via reconciliation and self-healing.** The control
   plane continuously reconciles desired vs. actual deployment state and
   recovers failed pilots automatically. A user-space Globus endpoint
   outage prevents new deployments but does not affect already-live
   models.
1. **Unified, cross-cluster observability.** With every model a uniform
   HTTP endpoint, the gateway aggregates time series from every inference
   instance (e.g., every vLLM process) across every cluster into a single
   view.
1. **Modern, parity-aligned application stack.** Migrate the gateway
   from Django-Ninja to FastAPI to match IRI API development. We had
   already stripped Django's middleware stack down far enough that a
   microframework is the more honest fit, and the Django ORM's lack of
   connection pooling and explicit transaction control is a poor match
   for our async, high-cadence access patterns. The gateway runs on
   FastAPI + async SQLAlchemy (asyncpg) with explicit pooling and
   transaction scoping.
1. **Container-native deployment.** Move off the current systemd +
   bare-metal venv deployment to a streamlined, reproducible container
   stack (Docker Compose for dev/single-host, Kubernetes-ready for
   production), with the explicit aim of eventually migrating the service
   into the Hermes Kubernetes cluster.
1. **Contained security boundary.** The control and data planes ride
   entirely on intra-ALCF network routability — we open no conduits to
   the outside world. The gateway requires only that a single port on
   each compute node be reachable from itself, secured end-to-end with
   NGINX, self-signed certificates, and mTLS so that no client can reach
   a model except through the gateway.
1. **Backend extensibility.** New deployment mechanisms (e.g., NVIDIA
   Dynamo) integrate behind the existing deployment abstraction without
   changes to the data plane or the client API.
1. **Agile deployments with dynamic model placement.** Sub-node models
   are bin-packed onto compute nodes for efficient utilization. Models
   can be added and removed from nodes without disrupting neighbors,
   facilitating rollouts and autoscaling.

### Continuity goals (must not regress from v1)

- **API compatibility** — OpenAI- and Anthropic-compatible endpoints
  (chat completions, completions, embeddings); existing client code and
  agent frameworks run unmodified.
- **Authentication and authorization** — Globus Auth identity with
  Globus Groups–based access control, preserved as-is.
- **Hot + on-demand models** — always-on low-latency models plus
  transparent cold-start of on-demand models.
- **Batch inference** — high-throughput batch submission retained.
- **Model availability visibility** — users can see model state
  (running / starting / queued), as the v1 `/jobs` endpoint provided.

### Non-goals

1. **A LiteLLM deployment.** We use `litellm.Router` as the LLM
   routing/translation layer, but the project is a general
   scientific-model serving platform; we are not standing up or wrapping
   LiteLLM as the product, and our design must not collapse to LLM-only
   assumptions.
1. **Sidestepping the HPC scheduler.** We integrate with PBS Pro and
   peers for resource acquisition; scheduling is not our problem to
   solve.
