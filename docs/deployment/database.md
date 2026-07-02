# Database Migrations

Schema migrations live under
`packages/gateway/first_gateway/database/migrations/` and are managed by
[Alembic](https://alembic.sqlalchemy.org/).

Because the `alembic.ini` is inside the package tree (not at the repo
root), **every `alembic` invocation needs the `-c` flag**:

```bash
ALEMBIC=packages/gateway/first_gateway/database/alembic.ini

# Apply all pending migrations
uv run alembic -c $ALEMBIC upgrade head

# Show current revision
uv run alembic -c $ALEMBIC current

# Show migration history
uv run alembic -c $ALEMBIC history
```

## Creating a new migration

### Auto-generate from model changes

After editing `database/models.py`, generate a migration that diffs the
ORM metadata against the current database state:

```bash
uv run alembic -c $ALEMBIC revision --autogenerate -m "describe the change"
```

Always review the generated file — autogenerate doesn't catch everything
(e.g. triggers, functions, data migrations).

### Empty migration for hand-written SQL

```bash
uv run alembic -c $ALEMBIC revision -m "describe the change"
```

Edit the resulting file in `migrations/versions/` to add your `op.execute()`
calls.

## How it connects

`migrations/env.py` reads `FIRST_DB_URL` from the environment — the same
variable the application's `Settings` class uses. No additional
configuration is needed; the compose stack and the test fixtures both set
this variable.

## Dev compose stack

The `migration` service in `deploy/compose.dev.yaml` runs
`alembic upgrade head` on startup, after postgres is healthy. Because the
dev stack uses a tmpfs-backed postgres, every `docker compose up` starts
from an empty database and applies the full migration chain.

## Test fixtures

The `template_db` pytest fixture (`tests/fixtures/db.py`) runs
`alembic upgrade head` to build the template database, so integration
tests get the exact same schema and triggers that production uses.
