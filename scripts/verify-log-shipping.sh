#!/usr/bin/env bash
# End-to-end check of log shipping on this host. Run from the repo root with
# the stack up (it uses the stack selected by .env, like `docker compose`):
#
#   ./scripts/verify-log-shipping.sh                  # V2, and V1 if deployed
#   ./scripts/verify-log-shipping.sh --write-v1-line  # also append a test line
#                                                     # to the V1 log (not on prod)
#
# It sends one marked item down each path and waits for the far end to
# acknowledge it:
#   prompts   a test file is uploaded to S3 and deleted locally
#   V2 logs   a marked request reaches the S3 archive (its access line) and
#             ClickHouse (its ResponseLog)
#   V1 logs   lines V1 wrote meanwhile reach both (or the test line, see above)
#
# The S3 archive is written every 5 minutes, so allow about 7. Test objects
# stay in the bucket (the prompt test files under .../verify/).
set -euo pipefail

WRITE_V1_LINE=false
[[ ${1:-} == --write-v1-line ]] && WRITE_V1_LINE=true

MARKER="verify-$(date +%s)"
FAILED=0
dc() { docker compose "$@"; }
ok() { echo "  ok    $*"; }
fail() { echo "  FAIL  $*"; FAILED=1; }
has_service() { dc ps --services --status running | grep -qx "$1"; }

# Value of one Vector metric for one component (0 if it has no series yet).
metric() {
    dc exec -T vector wget -q -O - http://127.0.0.1:9598/metrics |
        awk -v m="vector_$1" -v c="component_id=\"$2\"" \
            'index($0, m "{") == 1 && index($0, c) { v = $2 } END { print v + 0 }'
}

# wait_sent <component> <count before> <timeout secs>: the component has sent
# more events than before. For a sink, "sent" means the far end accepted them.
wait_sent() {
    local deadline=$((SECONDS + $3))
    while ((SECONDS < deadline)); do
        (($(metric component_sent_events_total "$1") > $2)) && return 0
        sleep 5
    done
    return 1
}

# --- prompts -----------------------------------------------------------------
verify_prompts() { # <service> <path under /data>
    local service=$1 file=$2
    echo "$service: moving a test file to S3"
    dc exec -T "$service" sh -c "
        mkdir -p \"\$(dirname /data/$file)\" && echo $MARKER > /data/$file &&
        touch -d @\$((\$(date +%s) - 3600)) /data/$file"
    if ! dc exec -T "$service" /bin/sh /ship-prompts.sh once; then
        fail "$service: rclone move failed (see the output above)"
        return
    fi
    if dc exec -T "$service" test -e "/data/$file"; then
        fail "$service: the test file is still on local disk"
    else
        ok "$service: local copy deleted"
    fi
    if dc exec -T "$service" sh -c \
        "/bin/sh /ship-prompts.sh rclone cat \"s3:\$S3_BUCKET/\$DEST_PREFIX/$file\"" |
        grep -q "$MARKER"; then
        ok "$service: test file is in S3"
    else
        fail "$service: test file is not in S3"
    fi
}

verify_prompts prompt-shipper "large-requests/verify/$MARKER.json"
if has_service prompt-shipper-v1; then
    verify_prompts prompt-shipper-v1 "$MARKER.json"
fi

# --- structured logs ---------------------------------------------------------
ch_before=$(metric component_sent_events_total clickhouse)
s3_before=$(metric component_sent_events_total s3_v2)
s3_v1_before=$(metric component_sent_events_total s3_v1)
v2_before=$(metric component_sent_events_total lines_v2)
v1_before=$(metric component_sent_events_total lines_v1)

echo "V2 logs: sending a marked request through the gateway ($MARKER)"
# An unknown path: the 404 is logged as an access line (archive only) and as a
# ResponseLog (archive and ClickHouse). /health would produce no ResponseLog.
dc exec -T inference-gateway .venv/bin/python -c "
import urllib.error, urllib.request
try:
    urllib.request.urlopen('http://localhost:7000/$MARKER')
except urllib.error.HTTPError:
    pass"

if $WRITE_V1_LINE; then
    echo "V1 logs: appending a marked line to the V1 log"
    dc exec -T prompt-shipper-v1 true # the service must exist
    line="{\"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%S.000000+00:00)\", \"level\": \"INFO\", \"logger\": \"verify\", \"message\": \"$MARKER\"}"
    dc run --rm -T --no-deps --entrypoint sh \
        -v "${V1_LOG_DIR:-/home/webportal/raw-prod-logs}:/v1" prompt-shipper-v1 \
        -c "echo '$line' >> /v1/out.log.\$(date -u +%Y-%m-%d)"
fi

if wait_sent lines_v2 "$v2_before" 60; then
    ok "Vector read the V2 line from Docker's log"
else
    fail "Vector read nothing from the V2 gateway's Docker log (DOCKER_ROOT_DIR? json-file driver?)"
fi
if has_service prompt-shipper-v1; then
    if wait_sent lines_v1 "$v1_before" 60; then
        ok "Vector read new V1 lines"
    else
        fail "Vector read no new V1 lines in 60s (no V1 traffic? try --write-v1-line on a test host)"
    fi
fi
if wait_sent clickhouse "$ch_before" 60; then
    ok "ClickHouse accepted an insert"
else
    fail "ClickHouse accepted nothing in 60s (see: docker compose logs vector)"
fi
echo "waiting for the next S3 archive objects (up to 7 minutes)"
if wait_sent s3_v2 "$s3_before" 420; then
    ok "S3 accepted a V2 archive object"
else
    fail "S3 accepted no V2 archive object in 7 minutes (see: docker compose logs vector)"
fi
if has_service prompt-shipper-v1; then
    if wait_sent s3_v1 "$s3_v1_before" 120; then
        ok "S3 accepted a V1 archive object"
    else
        fail "S3 accepted no V1 archive object (see: docker compose logs vector)"
    fi
fi

# Read the marker back from the archive, using the prompt shipper's rclone.
in_archive() { # <archive directory>
    dc exec -T prompt-shipper sh -c "
        /bin/sh /ship-prompts.sh rclone cat -q --max-age 15m --include '*.jsonl.gz' \
            \"s3:\$S3_BUCKET/$1/\$(date -u +%Y/%m/%d)/\" | zcat" |
        grep -c "$MARKER" || true
}
sleep 5
if [[ $(in_archive v2/raw/logs) -ge 1 ]]; then
    ok "the V2 line is in the S3 archive"
else
    fail "the V2 line is not in the S3 archive"
fi
if $WRITE_V1_LINE; then
    if [[ $(in_archive v1/raw/vector-logs) -ge 1 ]]; then
        ok "the V1 line is in the S3 archive"
    else
        fail "the V1 line is not in the S3 archive"
    fi
fi

echo
echo "To see the rows in ClickHouse, run there (the test request is a 404 response):"
echo "  SELECT stream, status_code, count(), max(ts) FROM gateway_logs"
echo "  WHERE ts > now() - INTERVAL 15 MINUTE GROUP BY stream, status_code"
echo
if ((FAILED)); then
    echo "FAILED: see the lines marked FAIL above"
    exit 1
fi
echo "All log shipping paths verified."
