#!/bin/sh
# Vector `exec` secrets backend: resolves SECRET[env.NAME] in the config from
# this container's environment (.env.secret.log-shipping).
#
# Vector 0.58 no longer interpolates ${VARS} in config files. This is the
# supported replacement, limited to the names below; add a name here when the
# config needs a new one. A name with no default must be set, or Vector
# refuses to start. Vector also refuses an empty value.
#
# Protocol: {"version":"1.0","secrets":["A","B"]} on stdin,
#           {"A":{"value":"...","error":null},...} on stdout.
set -eu

CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

default_for() {
    case "$1" in
        CLICKHOUSE_CA_FILE | S3_CA_FILE) echo "$CA_BUNDLE" ;;
        S3_REGION) echo us-east-1 ;;
        CLICKHOUSE_ENDPOINT | CLICKHOUSE_DATABASE | CLICKHOUSE_TABLE | \
            CLICKHOUSE_USER | CLICKHOUSE_PASSWORD | S3_ENDPOINT | S3_BUCKET)
            return 1 # required
            ;;
        *) return 2 ;; # not a setting
    esac
}

json_string() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

names=$(sed -e 's/.*"secrets"[^[]*\[//' -e 's/\].*//' | tr -d '" \n' | tr ',' ' ')

out=""
for name in $names; do
    value="" error=""
    if default=$(default_for "$name"); then
        eval "value=\${$name:-\$default}"
    elif [ $? -eq 1 ]; then
        eval "value=\${$name:-}"
        [ -n "$value" ] || error="$name is not set"
    else
        error="$name is not a log-shipping setting (see settings.sh)"
    fi
    if [ -n "$error" ]; then
        out="$out\"$name\":{\"value\":null,\"error\":\"$error\"},"
    else
        out="$out\"$name\":{\"value\":\"$(json_string "$value")\",\"error\":null},"
    fi
done
printf '{%s}\n' "${out%,}"
