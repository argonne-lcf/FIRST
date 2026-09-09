# Dev Tools
- Use the Makefile shortcuts (mypy, format, lint/lint-fix, test, db-up/db-down, compose-up/compose-down)
- Verify changes with make mypy/format/lint/test

# Project layout
- This is a uv workspace
  - packages/common -> `first_common` is installed everywhere for shared schema and error types.
  - packages/gateway -> `first_gateway` is installed on the user-facing server only.
  - packages/pilot -> `first_pilot` is installed on HPC systems running Pilot Jobs.
  - packages/client -> `alcf_ai` is installed by end users for a Python SDK and CLI to access the gateway.
  - packages/dashboard -> `first_dashboard` is installed on the analytics server for log aggregation, queries, and dashboard hosting
- Add tests under tests/ with common fixtures in tests/fixtures/

# Local Testing
- The tests require a database running: use `make db-up` to bring up Redis and
Postgres correctly before `make test`.
- To run the gateway stack locally, use `make compose-up`. Then test it at http://localhost:8000.
- Point the CLI tool at your local stack to test end-to-end: `alcf-ai --base-url http://localhost:8000 admin audit`
- Use `docker compose ps` and `docker compose logs <SERVICE> --since=1m` to view service logs
- Get an access token with `token=$(alcf-ai auth get-access-token)`. If this fails, ask the user to refresh their login and try again.
