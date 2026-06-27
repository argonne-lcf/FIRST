# FIRSTv2 TODO LIST

# MVP

- [ ] Improve consistency of time units across schemas (`_min` suffix for minutes everywhere?)
- [ ] Soft delete semantics with finalizers
- [ ] Pilot weight-cacheing / auto-downloading component
- [ ] Rest of controller framework
- [ ] Controllers
- [ ] Hot-swapping Routers (LiteLLM, Generic, Prometheus http_sd_config endpoint)
- [ ] Create Globus Compute endpoint (minimal type: ThreadPoolEngine) for SchedulerAdapter
- [ ] Add API routes that proxy through routers
- [ ] Port some Globus Compute Model configurations to the new system and test!

## Towards Production
- [ ] Read-only web UI: follow resource status more easily
- [ ] Port all fixtures to new declarative config; in git version control.
- [ ] DB Indexing
- [ ] Docs
- [ ] Deploy in Dev; load testing
- [ ] Test health alert controller
- [ ] Logging: revisit logged events; use LiteLLM hooks to log metrics
- [ ] Prometheus+Grafana integration
- [ ] Log export pipeline to DuckDB + archival
- [ ] Batch system
    - Consider merging Riccardo and Hari’s batch inference tools with alcf-ai client, enabling users to customize and submit batch jobs on their own allocations?

