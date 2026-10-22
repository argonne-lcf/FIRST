#!/usr/bin/env bash
#
# Generate a local CA and server certificates for the analytics sample.
#
# Edit clickhouse-san.cnf and grafana-san.cnf first: every name a client
# dials must appear in the SAN list.
#
# Writes:
#   certs/{ca.key,ca.crt,ca.srl,*.csr}
#   clickhouse/certs/{server.key,server.crt,ca.crt}
#   grafana/certs/{server.key,server.crt,ca.crt}
#
# WARNING: the server keys are left world-readable so the containers can read
# them regardless of runtime. In production, hand each key to its container user
# (ClickHouse uid 101, Grafana uid 472) and chmod 0600.
set -euo pipefail
cd "$(dirname "$0")"

CA_DAYS=3650
CERT_DAYS=825

mkdir -p ../clickhouse/certs ../grafana/certs

echo "Generating CA..."
openssl genrsa -out ca.key 4096
openssl req -x509 -new -key ca.key -config ca.cnf -days "$CA_DAYS" -out ca.crt

sign() {
    # sign <name> <san.cnf> <output-dir>
    local name="$1" cnf="$2" outdir="$3"

    echo "Generating $name server certificate..."
    openssl genrsa -out "$outdir/server.key" 2048
    openssl req -new -key "$outdir/server.key" -config "$cnf" -out "$name.csr"
    openssl x509 -req -in "$name.csr" -CA ca.crt -CAkey ca.key \
        -CAserial ca.srl -CAcreateserial -days "$CERT_DAYS" \
        -extfile "$cnf" -extensions v3_req -out "$outdir/server.crt"
    cp ca.crt "$outdir/ca.crt"

    chmod 0644 "$outdir/server.crt" "$outdir/ca.crt"
    chmod 0644 "$outdir/server.key"  # see WARNING above
}

sign clickhouse clickhouse-san.cnf ../clickhouse/certs
sign grafana grafana-san.cnf ../grafana/certs

echo
echo "Done."
echo "Copy clickhouse/certs/ca.crt to VM1 as vector/certs/ca.crt."
