#!/usr/bin/env bash
#
# Generate every visualization the first-stats-query CLI can produce.
#
# Sweeps the three window presets (6m, 1m, all) across every category.
# The time-bucketed categories are also swept across all four
# granularities; the inference recipes ignore granularity (their charts
# have no time axis), so they run once per window.
#
# Usage: all-stats.sh <dataset_dir> [output_dir]
#
# SVGs are written to output_dir (default: the current directory) and
# then converted to PNG alongside them. Event windows and per-cluster
# charts stay one command away through the CLI itself.

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 2 ]; then
    echo "Usage: $0 <dataset_dir> [output_dir]"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "error: dataset directory not found: $1" >&2
    exit 1
fi

# Absolutize the dataset before moving to the output directory.
DATASET_DIR=$(cd "$1" && pwd)
OUT_DIR=${2:-.}
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

# Categories whose charts vary with --granularity.
time_categories=(cluster institution status token-breakdown)
granularities=(auto daily monthly yearly)
periods=(6m 1m all)

total=$(( ${#time_categories[@]} * ${#granularities[@]} * ${#periods[@]} + ${#periods[@]} ))
runs=0

run() {
    runs=$((runs + 1))
    echo "[$runs/$total] visualize $*"
    first-stats-query "$DATASET_DIR" visualize "$@"
}

for period in "${periods[@]}"; do
    run inference --period "$period"
    for granularity in "${granularities[@]}"; do
        for category in "${time_categories[@]}"; do
            run "$category" --granularity "$granularity" --period "$period"
        done
    done
done

shopt -s nullglob
svgs=(*.svg)
echo
echo "Converting ${#svgs[@]} SVGs to PNG..."
for svg in "${svgs[@]}"; do
    # `convert` is the ImageMagick 6 name, kept as an alias in 7.
    convert "$svg" "${svg%.svg}.png"
done

pngs=(*.png)
echo
echo "Done. ${#svgs[@]} SVGs and ${#pngs[@]} PNGs in ${PWD}."
