- Ignore all code in the archived/ folder. It is never used or modified.
- Use the Makefile shortcuts (mypy, format, lint/lint-fix, test, dev-db-up) as needed.
- Verify changes with make mypy/format/lint/test
- Add tests under tests/ with common fixtures in tests/fixtures/
- This is a uv workspace; we are focused on building out code in packages/common (first_common),
  packages/gateway (first_gateway), and packages/client (alcf_ai).
- Feel free to work autonomously within this repo.  Do not touch files outside of this project root directory.
- Please stage changes that are ready but NEVER git commit them: I will review and commit myself.