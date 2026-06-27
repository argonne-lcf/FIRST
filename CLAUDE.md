# Dev Tools
- Use the Makefile shortcuts (mypy, format, lint/lint-fix, test, db-up) as needed.
- Verify changes with make mypy/format/lint/test

# Project layout
- This is a [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)
- We are focused on building out code inside `packages/`
  - packages/common -> `first_common` is installed everywhere for shared schema and error types.
  - packages/gateway -> `first_gateway` is installed on the user-facing server only.
  - packages/pilot -> `first_pilot` is installed on HPC systems running Pilot Jobs.
  - packages/client -> `alcf_ai` is installed by end users for a convenient Python SDK and CLI to access the gateway.
  - packages/dashboard -> `first_dashboard` is installed on the analytics server for log aggregation, queries, and dashboard hosting
- Add tests under tests/ with common fixtures in tests/fixtures/

# Local Testing
- The tests require a database running: use `make db-up` to bring up Redis and
Postgres correctly before `make test`.
- To run the gateway stack locally, use `make compose-up`. Then test it at http://localhost:8000.
- Point the CLI tool at your local stack to test end-to-end: `alcf-ai --base-url http://localhost:8000 admin audit`
- Use `docker compose ps` and `docker compose logs <SERVICE> --since=1m` to view service logs

# Agent Instructions
- Feel free to work autonomously within this repo.  Do not touch files outside of this project root directory.
- Never stage or git commit changes: I will review, stage, and commit myself.

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.