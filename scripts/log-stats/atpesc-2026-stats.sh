#!/usr/bin/env bash
#
# Generate ATPESC 2026 minerva stats using generic recipes.
#
# The ATPESC 2026 event window:
#   2026-08-06 17:00 – 2026-08-07 00:00  (UTC)

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <dataset_dir>"
    exit 1
fi

DATASET_DIR="$1"

echo "=== ATPESC 2026 — minerva Event Stats ==="
echo "Time range: 2026-08-06 17:00 – 2026-08-07 00:00  (UTC)"
echo "Cluster:    minerva"
echo "Dataset:    ${DATASET_DIR}"
echo "Command:    first-stats-query visualize"
echo

first-stats-query "${DATASET_DIR}" visualize inference top_users --start "2026-08-06 17:00" --end "2026-08-07 00:00" --cluster "minerva"
echo "  → top_users.svg"

first-stats-query "${DATASET_DIR}" visualize inference top_models --start "2026-08-06 17:00" --end "2026-08-07 00:00" --cluster "minerva"
echo "  → top_models.svg"

first-stats-query "${DATASET_DIR}" visualize inference top_models_users --start "2026-08-06 17:00" --end "2026-08-07 00:00" --cluster "minerva"
echo "  → top_models_users.svg"

first-stats-query "${DATASET_DIR}" visualize inference hist_users --start "2026-08-06 17:00" --end "2026-08-07 00:00" --cluster "minerva"
echo "  → hist_users.svg"

first-stats-query "${DATASET_DIR}" visualize inference hist_requests --start "2026-08-06 17:00" --end "2026-08-07 00:00" --cluster "minerva"
echo "  → hist_requests.svg"

echo
echo "Done. SVGs written to current directory."
