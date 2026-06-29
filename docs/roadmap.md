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


## TODO: MVP

### Controllers

Build out the controllers architecture as described in [controllers.md](architecture/controllers.md)

### Hot-swapping Routers (LiteLLM, Generic, Prometheus http_sd_config endpoint)

Build out the LiteLLM Router and generic Router factories that hot-swap the application router on Router Config changes.  For responsiveness, they should listen to notifications through Redis that the router config has changed.

We also want to populate the in-memory config to serve out for Prometheus to discover metrics endpoints via `http_sd_config`.

- Listens for changes (Redis notifications) and periodically polls Redis for centralized Router configuration
- Dynamically builds and hot-swaps generic Router and LiteLLM Router on changes
- Any model with an LLM endpoint is auto-registered on LiteLLM router.  Any model with a non-LLM endpoint is auto registered on the generic Router.
- Updates Prometheus `http_sd_config` data structure in-memory, exposed via simple FastAPI route.

### Add API routes that proxy through routers

Implement the inference API routes like `/chat/completions` following the design
in [request-routing.md](./docs/architecture/request-routing.md). Postgres should
not be involved in the inference path at all.

Routing is federated/load-balanced by default, but we should also consider how to structure and provide additional API paths to side-step the router and target specific deployments.

### Test it out:

Once the code is in place we can begin testing it!

Create a Globus Compute endpoint to configure the `GlobusComputePBSAdapter`.  The endpoint
should live under the inference service account and use the minimal configuration:

```yaml
# ~/.globus_compute/test_endpoint/user_config_template.yaml.j2

engine:
  type: ThreadPoolEngine
  max_workers: 3
```

- Deploy and configure the service on the Dev VM.
- Port some Model configurations from the old `~/.globus_compute/` location on the homefilesystem
  to the new system.  We can start a branch in our fixtures repo to maintain the YAML manifests.
- Apply some model deployments and test them!

## TODO: Production

### Read-only web UI: follow resource status more easily

The continuously updating state available through the API can provide us with a rich operational view of the system. Such internal-use-only; read-only; typescript-heavy apps are low-hanging fruit for LLM generation.  Envision a UI that mirrors the resource views in the apiserver, combined with a view of the system health and controller metrics:

- AccessGroups
- Models table
    - Expand model to see static/pilot deployments
- Deployment summary tables
- Deployment details
    - Pilot Deployment detail: replicas list
        - Tail logs for any replica
- Cluster list
- Cluster detail (w/ pilot jobs)
- System status: aggregated health across resources & current alerts
- Controller status and metrics

### Pilot weight-cacheing / auto-downloading component

`first_pilot` needs a consistent strategy for handling model weights. We could
take inspiration from a multi-layer cache and imagine a design that allows for:

- Weights origin (`file://` or `huggingface://`)
- L2 cache (`file://`)
- L1 cache (`file://`)

- At Replica startup, if the weights are in L1 cache, load from there!
- If the weights are not in L1 but present in L2, initiate a copy from L2 to L1 before loading from L1.
- If the weights are not in L1 or L2, initiate a copy from origin -> L2 -> L1 before loading from L1.
- All of the above should not merely check the existence of the directory, but compare filenames and sizes (like `rsync` does it).
- Copies should run with some degree of parallelism (e.g. ~4 parallel `cp` or `rsync`)
- We should implement huggingface downloads and parallel filesystem `cp` to begin with. We should keep the design flexible enough that adding a new protocol like `s3://` later is not too hard.
- We need to be mindful of the max storage space in L2 and L1 and follow an LRU eviction policy
- We need to work efficiently and safely under concurrency.  If two different model replicas start in the same `ReplicaManager`, they should be able to download weights concurrently.  If two replicas of the same model start on the same pilot, we want to avoid bugs from racing copies (or at least make the copies idempotent).

### Port all fixtures

Fixtures and Globus Compute endpoint configurations will migrate into the unifed set of YAML manifests.
Create and test this configuration, version controlled in private GitLab repo.


### Deploy in Dev; load testing

We need a substantial period of testing where FIRSTv2 is running in dev alongside the production service.
We need a strategy for allocating enough resources to test with FIRSTv2, especially across all the pilot deployment clusters.

Testing should exercise all controller pathways.

Load testing to verify stability under heavy concurrency, and API benchmarking
should be used to measure latency overheads.

We should verify the new health alert controller functions as desired.

### Logging: revisit logged events; use LiteLLM hooks to log metrics

We will want to circle back to the structured logging and ensure that the
desired information is being logged in the framework. We can use [LiteLLM
callbacks](https://docs.litellm.ai/docs/observability/custom_callback) to
dramatically simplify the path to logging LLM completion metrics across all
request types and streaming/non-streaming.  This should be a nice improvement
over v1, but it will land after the router integrations are in place.  We want to
do something similar, albeit generic, for the generic Router.

We will continue to log the fundamental event types:
- Auth Events (user auth log)
- API access events (access log)
- Route-specific events (e.g LLM Requests would hook into LiteLLM logging/callbacks)
- System events (e.g. status change; admin changes; controller info)

### Prometheus+Grafana integration

This is less of a lift than it seems, and is mostly a deployment/configuration task!

The controllers may use `prometheus_client` to actually advertise some useful
operational metrics, but most of the metrics will come from deployments; for
each `*Deployment` that has a configured Promethus metrics path, we can register
URLs in the Prometheus HTTP Service Discovery format.  The `apiserver` provides
the master "metrics scraping list" at an API route that Prometheus polls.
Prometheus then takes care of fetching and ingesting the metrics from each of
the endpoints.

We should then be able to stand up an internal Grafana dashboard against this.  It would
provide a wealth of up-to-date performance information across all deployed models and is
definitely worth spending the time to get stood up.

### Log export pipeline to DuckDB + archival

- We want a cron job that pushes local .jsonl logs to an external analytics storage to reclaim space
- The structured logs should be split on the `stream` key and ETL'd into DuckDB tables to facilitate rapid analytics over long historical datasets
- The DuckDB datastore can be used to create a dashboard over log queries.  The types of queries answered by this dashboard are less about current system performance/health and more about historical usage trends (e.g. most popular models in 2026; how many tokens generated on Sophia)

### Batch system

Worth discussion: consider merging Riccardo and Hari’s batch inference tools
with alcf-ai client, enabling users to customize and submit batch jobs on their
own allocations?  Then, batch-mode inference is maintained as a separate capability closer
to traditional HPC "offline" batch computing, and FIRST remains focused on online serving.

If we really do want to pursue Batch inference in v2:

- It seems natural to add `batch_spec` as an (optional) sibling to `pilot_spec`  on `PilotDeploymentSpec`.
- Track `BatchJob` as a new, separate resource Kind under the `PilotDeployment`
- System can re-use `SchedulerAdapter` because that API does not make any pilot-specific assumptions
- `BatchJob` would be handled rather similarly to `PilotJob`, except batch jobs don’t host replicas and don’t serve anything (just offline inference and done)
- Integrate with inference data staging area (facilitate permissions to transfer into Guest collections, inherit from v1)
- Batch Job Controller will need to be written