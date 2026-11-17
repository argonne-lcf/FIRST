#!/usr/bin/env bash
# Gateway-side half of the profiling bundle (tests/load/PROFILING.md).
# Run on the host of the V2 compose stack, with deploy/compose.profile.yaml in
# COMPOSE_FILE.
#
#   profile.sh reset                 drop earlier gauges and captures
#   profile.sh pyspy [SECONDS]       sample every gateway worker (default 60 s)
#   profile.sh top                   live py-spy view of the busiest functions
#   profile.sh collect [DIR]         copy everything to DIR on the host
#
# Environment: SERVICE (inference-gateway), RATE (samples/s per process, 100),
# NATIVE=1 (also unwind C extensions; slower sampling, may fail on stripped
# builds), GIL=1 (only samples holding the GIL), NONBLOCKING=1 (do not pause the
# workers to sample: less overhead, less exact), LABEL (capture file suffix).
set -euo pipefail

SERVICE=${SERVICE:-inference-gateway}
DIR=/tmp/first-profile
PYSPY=.venv/bin/py-spy

in_gateway() { docker compose exec -T "$@"; }

case "${1:-}" in
reset)
    # The workers keep appending to their open files: truncate, do not delete.
    in_gateway "$SERVICE" sh -c \
        'for f in /tmp/first-profile/worker-*.jsonl; do [ -f "$f" ] && : > "$f"; done
         rm -f /tmp/first-profile/pyspy-*'
    echo "Cleared $DIR in $SERVICE."
    ;;
pyspy)
    seconds=${2:-60}
    label=${LABEL:-$(date +%H%M%S)}
    flags=(--pid 1 --subprocesses --duration "$seconds" --rate "${RATE:-100}"
        --format raw --function --full-filenames)
    [ -n "${NATIVE:-}" ] && flags+=(--native)
    [ -n "${GIL:-}" ] && flags+=(--gil)
    [ -n "${NONBLOCKING:-}" ] && flags+=(--nonblocking)
    echo "Sampling the gateway workers for ${seconds}s ..."
    # Root, for CAP_SYS_PTRACE; pid 1 is the gunicorn master.
    in_gateway -u root "$SERVICE" "$PYSPY" record "${flags[@]}" \
        -o "$DIR/pyspy-$label.txt"
    in_gateway -u root "$SERVICE" chmod a+r "$DIR/pyspy-$label.txt"
    echo "Wrote $DIR/pyspy-$label.txt in $SERVICE."
    ;;
top)
    worker=$(in_gateway "$SERVICE" sh -c \
        "grep -l '^PPid:[[:space:]]*1\$' /proc/[0-9]*/status | head -1 | cut -d/ -f3")
    echo "Attaching to worker pid $worker (one of several); q to quit."
    docker compose exec -u root "$SERVICE" "$PYSPY" top --pid "$worker"
    ;;
collect)
    out=${2:-loadtest-runs/profile-$(date +%Y%m%d-%H%M%S)}
    mkdir -p "$out"
    docker compose cp "$SERVICE:$DIR/." "$out"
    echo "Copied the gateway profile to $out/:"
    ls -l "$out"
    ;;
*)
    sed -n '2,14p' "$0"
    exit 2
    ;;
esac
