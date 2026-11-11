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

## Migrations in dev vs prod

The `migration` service in `deploy/compose.dev.yaml` runs
`alembic upgrade head` automatically on startup, after postgres is
healthy. The dev database lives in a named volume (`postgres-dev`) that
persists across restarts; use `make db-reset` to wipe it and re-apply the
full migration chain from empty.

In prod migrations are **manual** — there is no `migration` service in
`deploy/compose.prod.yaml`. Run `make prod-migrate` after deploying a new
image; it runs `alembic upgrade head` in a one-off container.

Some revisions must run *before* the new image starts serving. Revision
`a68035973f3a` backfills `max_nodes_per_job` into every stored
`cluster.pilot_system` (as that row's `max_num_nodes`, so behavior is
unchanged until the Cluster manifests are re-applied). The previous image
ignores the added key, but the new image cannot read a pilot cluster without
it: pull/build the new image, run `make prod-migrate`, then restart the
services. The revision also refuses to run, and changes nothing, if a stored
`ip_allowlist` would fail the new validation (a wildcard, or IPv4 wider than
`/24`); fix that Cluster with the previous version first.

## Test fixtures

The `template_db` pytest fixture (`tests/fixtures/db.py`) runs
`alembic upgrade head` to build the template database, so integration
tests get the exact same schema and triggers that production uses.
