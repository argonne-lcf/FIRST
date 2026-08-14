#!/usr/bin/env bash
# Take a Prometheus TSDB snapshot and copy it off the host as a tarball for
# offline restore/analysis, keeping the last N snapshots. Safe to run against a
# live stack. Requires --web.enable-admin-api (set in deploy/compose.yaml).
#
#   BACKUP_DIR   host dir for snapshots       (default: ./backups)
#   KEEP         number of snapshots to keep  (default: 7)
#   PROM_URL     Prometheus base URL          (default: http://127.0.0.1:9090)
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-./backups}"
KEEP="${KEEP:-7}"
PROM_URL="${PROM_URL:-http://127.0.0.1:9090}"
DEST="$BACKUP_DIR/prometheus"
mkdir -p "$DEST"

# Ask Prometheus to snapshot its TSDB into /prometheus/snapshots/<name>.
echo "Requesting snapshot from $PROM_URL"
resp="$(curl -sS -XPOST "$PROM_URL/api/v1/admin/tsdb/snapshot")"
name="$(printf '%s' "$resp" | sed -n 's/.*"name":"\([^"]*\)".*/\1/p')"
if [ -z "$name" ]; then
	echo "Failed to create snapshot. Response: $resp" >&2
	exit 1
fi

out="$DEST/prometheus-$name.tar.gz"
echo "Archiving snapshot $name -> $out"
docker compose exec -T prometheus tar czf - -C /prometheus/snapshots "$name" >"$out"

# Remove the in-container snapshot copy so /prometheus doesn't accumulate them.
docker compose exec -T prometheus rm -rf "/prometheus/snapshots/$name"

# Rotate: keep the newest $KEEP archives.
ls -1t "$DEST"/prometheus-*.tar.gz 2>/dev/null | tail -n "+$((KEEP + 1))" | while read -r old; do
	echo "Removing old snapshot $old"
	rm -f "$old"
done

echo "Done. $(ls -1 "$DEST"/prometheus-*.tar.gz 2>/dev/null | wc -l | tr -d ' ') snapshot(s) retained."
