#!/usr/bin/env bash

set -Eeuo pipefail
umask 027

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BACKUP_DIR="/home/webportal/inference-gateway/pg_backup"
DB_USER="dataportaldev"
DB_NAME="inferencegateway"

# Two workers match the two dominant tables in this database.
JOBS="${JOBS:-2}"

# PostgreSQL 14 supports numeric gzip compression levels only.
# 0 = uncompressed; 1 = low compression effort; 9 = highest effort.
COMPRESSION="${COMPRESSION:-1}"

RETENTION_DAYS="${RETENTION_DAYS:-14}"

STAMP="$(date +'%Y-%m-%d_%H%M%S')"

FINAL_DIR="${BACKUP_DIR}/${DB_NAME}_backup_${STAMP}.dumpdir"
PARTIAL_DIR="${FINAL_DIR}.partial"
LOCK_FILE="${BACKUP_DIR}/.${DB_NAME}_backup.lock"

mkdir -p "${BACKUP_DIR}"

# Give the backup sessions a recognizable name in pg_stat_activity.
export PGAPPNAME="${DB_NAME}-backup"

# ---------------------------------------------------------------------------
# Prevent overlapping backups
# ---------------------------------------------------------------------------

exec 9>"${LOCK_FILE}"

if ! flock -n 9; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Another backup is already running." >&2
    exit 75
fi

if [[ ! "${JOBS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid JOBS value: ${JOBS}" >&2
    exit 2
fi

if [[ ! "${COMPRESSION}" =~ ^[0-9]$ ]]; then
    echo "Invalid COMPRESSION value: ${COMPRESSION}; expected 0-9." >&2
    exit 2
fi

if [[ -e "${FINAL_DIR}" || -e "${PARTIAL_DIR}" ]]; then
    echo "Backup destination already exists:" >&2
    echo "  ${FINAL_DIR}" >&2
    echo "  ${PARTIAL_DIR}" >&2
    exit 1
fi

# Remove incomplete output after a failure or interruption.
cleanup() {
    rm -rf -- "${PARTIAL_DIR}"
}

trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

START_SECONDS="${SECONDS}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting PostgreSQL backup"
echo "Database:     ${DB_NAME}"
echo "Destination:  ${FINAL_DIR}"
echo "Client:       $(pg_dump --version)"
echo "Jobs:         ${JOBS}"
echo "Compression:  gzip level ${COMPRESSION}"

# ---------------------------------------------------------------------------
# Full database dump
#
# No schema/table filters or exclusion options are used. This dumps all
# normal database-local schema and data handled by pg_dump.
# ---------------------------------------------------------------------------

pg_dump \
    --username="${DB_USER}" \
    --dbname="${DB_NAME}" \
    --format=directory \
    --jobs="${JOBS}" \
    --compress="${COMPRESSION}" \
    --verbose \
    --file="${PARTIAL_DIR}"

# Confirm that pg_restore can read the archive catalog.
# This is a basic format check, not a complete test restore.
pg_restore --list "${PARTIAL_DIR}" >/dev/null

# Publish the completed backup. This rename is atomic when both paths are
# on the same filesystem.
mv -- "${PARTIAL_DIR}" "${FINAL_DIR}"

trap - EXIT HUP INT TERM

ELAPSED_SECONDS=$((SECONDS - START_SECONDS))

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Backup completed"
echo "Elapsed seconds: ${ELAPSED_SECONDS}"
du -sh -- "${FINAL_DIR}"

# ---------------------------------------------------------------------------
# Retention
#
# Run retention only after the new backup succeeds.
# ---------------------------------------------------------------------------

find "${BACKUP_DIR}" \
    -mindepth 1 \
    -maxdepth 1 \
    -type d \
    -name "${DB_NAME}_backup_*.dumpdir" \
    -mtime "+${RETENTION_DAYS}" \
    -exec rm -rf -- {} +

# Remove legacy compressed backups according to the same retention policy.
find "${BACKUP_DIR}" \
    -maxdepth 1 \
    -type f \
    \( \
        -name "${DB_NAME}_backup_*.tar.gz" \
        -o -name "${DB_NAME}_backup_*.dump" \
        -o -name "${DB_NAME}_backup_*.sql.gz" \
    \) \
    -mtime "+${RETENTION_DAYS}" \
    -delete

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Retention cleanup completed"
