#!/bin/sh
# Move large request/response bodies from /data to S3, forever.
#
# Each cycle:
#   1. `rclone move`, oldest files first: upload, then delete the local file.
#      Each upload carries its MD5 and S3 rejects a body that does not match.
#      A file that fails to upload stays and is retried next cycle.
#   2. Hard local limit (MAX_AGE_DAYS > 0): delete files older than that even
#      if they never uploaded. This is data loss; the health alerter warns for
#      days beforehand. Never in the shipper's first day, nor while it is
#      still working off a backlog: the uploads get their chance first.
#   3. Write /status/prompt-shipper-<name>.json for the health alerter
#      (check_log_shipping). The numbers only; the reasons are in this log.
#
# `ship-prompts.sh once` runs a single cycle and exits with rclone's status.
# `ship-prompts.sh rclone ...` runs rclone with the same remote (named `s3:`).
#
# Settings (.env.secret.log-shipping, and `environment:` in compose):
#   SHIPPER_NAME   names the status file (v2, v1)
#   S3_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#   S3_REGION, S3_CA_FILE   optional
#   DEST_PREFIX    files land under s3://$S3_BUCKET/$DEST_PREFIX/
#   MAX_AGE_DAYS   0 disables the hard limit
#
# Cost: a few uploads a second and one MD5 pass over each file (seconds of CPU
# for a day's ~40 GB). Compose caps the container at one CPU.
set -u

: "${SHIPPER_NAME:?}" "${S3_ENDPOINT:?}" "${S3_BUCKET:?}" "${DEST_PREFIX:?}"
SRC=/data
DEST="s3:${S3_BUCKET}/${DEST_PREFIX}"
STATUS=/status/prompt-shipper-${SHIPPER_NAME}.json
INTERVAL_SECS=${INTERVAL_SECS:-300}
# Files younger than this may still be being written by the gateway.
MIN_AGE_MINS=${MIN_AGE_MINS:-5}
MAX_AGE_DAYS=${MAX_AGE_DAYS:-0}
EXPIRE_GRACE_SECS=${EXPIRE_GRACE_SECS:-86400}

# An rclone remote named `s3`, configured from the environment alone.
export RCLONE_CONFIG=/tmp/rclone.conf
: >"$RCLONE_CONFIG"
export RCLONE_CONFIG_S3_TYPE=s3
export RCLONE_CONFIG_S3_PROVIDER=Other
export RCLONE_CONFIG_S3_ENV_AUTH=true # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
export RCLONE_CONFIG_S3_ENDPOINT="$S3_ENDPOINT"
export RCLONE_CONFIG_S3_REGION="${S3_REGION:-us-east-1}"
export RCLONE_CONFIG_S3_NO_CHECK_BUCKET=true # the bucket exists; no CreateBucket
[ -z "${S3_CA_FILE:-}" ] || export RCLONE_CA_CERT="$S3_CA_FILE"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) prompt-shipper-${SHIPPER_NAME}: $*"; }

started_at=$(date +%s)
exit_code=0
consecutive_failures=0
last_success_at=0
expired_deleted_total=0
pending=0
oldest_age=0

write_status() {
    printf '{"updated_at": %d, "last_success_at": %d, "exit_code": %d, "consecutive_failures": %d, "pending_files": %d, "oldest_pending_age_secs": %d, "expired_deleted_total": %d}\n' \
        "$(date +%s)" "$last_success_at" "$exit_code" "$consecutive_failures" \
        "$pending" "$oldest_age" "$expired_deleted_total" >"$STATUS.tmp" &&
        mv "$STATUS.tmp" "$STATUS"
}

cycle() {
    # --no-check-dest: upload without first listing or probing the
    #   destination, which holds hundreds of thousands of new objects a day
    #   (millions in V1's flat prefix). Re-uploading a file is harmless.
    # --max-duration: a large backlog is worked off over several cycles, so
    #   the status file stays fresh. rclone then exits 10.
    rclone move "$SRC" "$DEST" \
        --min-age "${MIN_AGE_MINS}m" --no-check-dest \
        --order-by modtime,ascending \
        --transfers 8 --retries 3 --low-level-retries 10 \
        --max-duration 20m \
        --stats 5m --stats-one-line --stats-log-level NOTICE
    exit_code=$?
    now=$(date +%s)
    draining=false
    if [ "$exit_code" -eq 10 ]; then
        # Out of time, not failing: uploads worked and more are waiting.
        draining=true
        exit_code=0
        log "backlog: stopped after 20 minutes, continuing next cycle"
    fi
    if [ "$exit_code" -eq 0 ]; then
        consecutive_failures=0
        last_success_at=$now
    else
        consecutive_failures=$((consecutive_failures + 1))
        log "rclone move failed (exit $exit_code, $consecutive_failures in a row)"
    fi

    if [ "$MAX_AGE_DAYS" -gt 0 ] && ! $draining &&
        [ $((now - started_at)) -gt "$EXPIRE_GRACE_SECS" ]; then
        expired=$(find "$SRC" -type f -mtime "+$MAX_AGE_DAYS" -print -delete | wc -l)
        if [ "$expired" -gt 0 ]; then
            expired_deleted_total=$((expired_deleted_total + expired))
            log "DELETED $expired files older than $MAX_AGE_DAYS days that were never uploaded"
        fi
    fi

    # Day directories the gateway has finished with. Not rclone's
    # --delete-empty-src-dirs, which could remove today's between the
    # gateway's mkdir and its write.
    find "$SRC" -mindepth 1 -type d -mmin +1440 -exec rmdir {} + 2>/dev/null

    # What is left that should have gone: the backlog.
    pending=$(find "$SRC" -type f -mmin "+$MIN_AGE_MINS" | wc -l)
    oldest=$(find "$SRC" -type f -mmin "+$MIN_AGE_MINS" -exec stat -c %Y {} + | sort -n | head -n 1)
    oldest_age=0
    [ -z "$oldest" ] || oldest_age=$((now - oldest))

    write_status
    return "$exit_code"
}

if [ "${1:-}" = rclone ]; then
    shift
    exec rclone "$@"
fi

if [ "${1:-}" = once ]; then
    STATUS=/tmp/once.json # leave the running shipper's status alone
    cycle
    exit $?
fi

trap 'exit 0' TERM INT
log "moving $SRC to $DEST every ${INTERVAL_SECS}s"
write_status # "started": the first cycle may take a while on a backlog
while true; do
    cycle
    sleep "$INTERVAL_SECS" &
    wait $!
done
