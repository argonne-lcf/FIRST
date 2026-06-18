- Ignore all code in the archived/ folder. It is never used or modified.
- Use the Makefile shortcuts (mypy, format, lint/lint-fix, test, db-up) as needed.
- Verify changes with make mypy/format/lint/test
- Add tests under tests/ with common fixtures in tests/fixtures/
- This is a uv workspace; we are focused on building out code in packages/common (first_common),
  packages/gateway (first_gateway), and packages/client (alcf_ai).
- Feel free to work autonomously within this repo.  Do not touch files outside of this project root directory.
- Never stage or git commit changes: I will review, stage, and commit myself.

- To run the service locally use `make compose-up`. Then test it at http://localhost:8000.
- You can use the Client CLI tool `alcf-ai --base-url http://localhost:8000 <SUBCOMMAND>` to interact with the service.
- Use `docker compose ps` and `docker compose logs <SERVICE> --since=1m` to view service logs