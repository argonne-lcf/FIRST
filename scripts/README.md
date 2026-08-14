# Scripts

Helpers for running and maintaining the Compose stacks. All are safe to run
against a live stack. Backup destinations are configurable via `BACKUP_DIR`
(default `./backups`, gitignored).

| Script | What it does | Make target |
| --- | --- | --- |
| `pg-backup.sh` | `pg_dump` the database to a gzipped SQL file, keep last N | `make db-backup` |
| `prom-snapshot.sh` | Snapshot the Prometheus TSDB to a tarball, keep last N | `make prom-snapshot` |
| `dozzle.sh` | Throwaway browser log viewer for the dev stack (not in compose) | — |

Each script picks up the stack selected by `COMPOSE_FILE` (set in `.env`), so the
same commands work for dev and prod.

## Backups

```bash
# Defaults: ./backups, keep 7
make db-backup
make prom-snapshot

# Custom location / retention
BACKUP_DIR=/var/backups/first KEEP=14 ./scripts/pg-backup.sh
```

- **Postgres** — full `pg_dump` (the DB is small). Restore with:
  ```bash
  gunzip -c backups/postgres/first-<stamp>.sql.gz \
    | docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" "$POSTGRES_DB"'
  ```
- **Prometheus** — TSDB snapshot tarball for offline restore/analysis. Extract the
  tarball into a Prometheus `--storage.tsdb.path` to inspect it. Requires the admin
  API, which is enabled in `deploy/compose.yaml`.

## Nightly cron (prod)

Run backups nightly and keep the last 7. `cd` into the repo first so `COMPOSE_FILE`
from `.env` resolves and `BACKUP_DIR` lands on persistent storage. Example crontab
(`crontab -e`):

```cron
# m h dom mon dow  command
15 2 * * *  cd /opt/inference-gateway && BACKUP_DIR=/var/backups/first KEEP=7 ./scripts/pg-backup.sh    >> /var/log/first-backup.log 2>&1
30 2 * * *  cd /opt/inference-gateway && BACKUP_DIR=/var/backups/first KEEP=7 ./scripts/prom-snapshot.sh >> /var/log/first-backup.log 2>&1
```

Rotation is built in: each run deletes everything older than the newest `KEEP`.
