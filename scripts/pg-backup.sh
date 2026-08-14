#!/usr/bin/env bash
# Dump the Postgres database to a gzipped SQL file on the host and keep the
# last N backups. Safe to run against a live stack (pg_dump takes a consistent
# snapshot). Works for whichever stack COMPOSE_FILE selects.
#
#   BACKUP_DIR   host dir for backups        (default: ./backups)
#   KEEP         number of backups to retain (default: 7)
#
# The Postgres user/db are read from the container's own environment, so this
# tracks .env.compose (dev) or .env.prod (prod) automatically.
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
KEEP="${KEEP:-7}"
DEST="$BACKUP_DIR/postgres"
mkdir -p "$DEST"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="$DEST/first-$stamp.sql.gz"

echo "Backing up Postgres -> $out"
docker compose exec -T postgres \
	sh -c 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' \
	| gzip >"$out"

# Rotate: keep the newest $KEEP dumps.
ls -1t "$DEST"/first-*.sql.gz 2>/dev/null | tail -n "+$((KEEP + 1))" | while read -r old; do
	echo "Removing old backup $old"
	rm -f "$old"
done

echo "Done. $(ls -1 "$DEST"/first-*.sql.gz 2>/dev/null | wc -l | tr -d ' ') backup(s) retained."
