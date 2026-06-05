#!/bin/bash

# Configuration variables
BACKUP_DIR="/home/webportal/inference-gateway/pg_backup"   # Change this to your backup directory
DB_USER="dataportaldev"
DB_NAME="inferencegateway"
DATE=$(date +'%Y-%m-%d')
TMP_SQL="${BACKUP_DIR}/${DB_NAME}_backup_${DATE}.sql"
BACKUP_FILE="${BACKUP_DIR}/${DB_NAME}_backup_${DATE}.tar.gz"

# Ensure backup directory exists
mkdir -p "${BACKUP_DIR}"

# Dump the database into a temporary SQL file
pg_dump -U "${DB_USER}" "${DB_NAME}" > "${TMP_SQL}"

# Tar and compress the SQL dump
tar -czf "${BACKUP_FILE}" -C "${BACKUP_DIR}" "$(basename ${TMP_SQL})"

# Remove the temporary SQL dump file
rm "${TMP_SQL}"

# Delete backup files older than 14 days
find "${BACKUP_DIR}" -type f -name "${DB_NAME}_backup_*.tar.gz" -mtime +14 -exec rm {} \;