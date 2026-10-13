# Analytics tier sample — Vector, ClickHouse, Grafana

A deployable sample of the pipeline designed in
[`docs/architecture/structured-logging-clickhouse.md`](../../docs/architecture/structured-logging-clickhouse.md):
Vector ships the gateway's structured JSONL off VM1 into a ClickHouse landing
table on VM2, where Grafana reads the usage tables.

The design doc is the source of truth. Where this tree and the doc disagree, the
difference is listed under [Deviations](#deviations-from-the-design).

The gateway stack's own Prometheus + Grafana (`deploy/compose.yaml`) is
unrelated and stays where it is; this is a second, separate analytics tier.

## Architecture

```text
VM1 — inference                          VM2 — analytics
-----------------                        ---------------
inference-gateway (compose)
  stdout JSONL
       │
       ▼
vector/ ──── HTTPS 8443 (TLS) ────►  clickhouse/
  docker_logs                          events_raw
       │                                 ├─ mv → usage_inference
       └──── S3 raw archive ──► S3       └─ mv → usage_daily
       (optional)                                    ▲
                                                     │ native 9440 (TLS)
                                                grafana/
```

The gateway emits four streams: `user`, `request`, `response`, `inference`. Only
the last three carry an `event_id`, so only they are inserted into ClickHouse —
the `user` auth events and every unstructured line are archive-only.

## Layout

| Path | Runs on | Purpose |
| --- | --- | --- |
| `certs/` | both | CA and SAN configs; `generate.sh` creates the certificates |
| `clickhouse/` | VM2 | server, TLS config, `schema.sql`, `.env.example` |
| `grafana/` | VM2 | Grafana, ClickHouse datasource, dashboards dir |
| `vector/` | VM1 | `docker_logs` → ClickHouse (+ optional S3 archive) |

## Prerequisites

- Docker Compose v2 or podman + podman-compose on both VMs.
- VM1 can reach VM2 on the ClickHouse HTTPS port; the design requires that
  nothing else can.
- The gateway runs in Compose, so Vector can read its stdout through the Docker
  socket.

## 1. Certificates

Edit the SAN lists in `certs/clickhouse-san.cnf` and `certs/grafana-san.cnf` so
every name a client dials is listed, then:

```bash
./certs/generate.sh
```

Copy `clickhouse/certs/ca.crt` to VM1 as `vector/certs/ca.crt`.

## 2. ClickHouse (VM2)

```bash
cd clickhouse
cp .env.example .env     # set the three passwords and the storage paths
docker compose up -d
export CLICKHOUSE_VECTOR_PASSWORD=... CLICKHOUSE_GRAFANA_PASSWORD=...
envsubst < schema.sql | docker compose exec -T clickhouse clickhouse-client --multiquery
```

`schema.sql` creates the two least-privilege users: `vector` (INSERT on
`events_raw` only, with `async_insert`) and `grafana` (SELECT on `analytics`).

## 3. Vector (VM1)

```bash
cd vector
cp .env.example .env     # set CLICKHOUSE_HOST, the vector password, the CA path
mkdir -p certs           # copy clickhouse/certs/ca.crt here
docker compose up -d
docker compose logs -f
```

Set `include_containers` in `vector.yaml` to the gateway container's name
(`docker compose ps` shows it). To also ship the raw archive, add
`"--config", "/etc/vector/vector-archive.yaml"` to the `command` in
`compose.yaml` and set the `S3_*` variables.

## 4. Grafana (VM2)

Paste `clickhouse/certs/ca.crt` into `tlsCACert` in
`provisioning/datasources/clickhouse.yaml`, then:

```bash
cd grafana
cp .env.example .env     # set the admin password, the grafana password, the root URL
docker compose up -d
```

## What the design specifies but this sample does not build

- **Usage API** (FastAPI, read-only user, 60 s per-user cache) — not
  implemented; `packages/dashboard` is the intended home.
- **Dashboards and the loss alert** — `grafana/dashboards/` is empty. The
  loss-detection query is at the bottom of `clickhouse/schema.sql`.
- **Dictionaries** for pricing and user→project joins — not implemented.
- **The body off-line path** — the design writes bodies ≥ 2 KB to
  `/payloads/{kind}/YYYY/MM/DD/{event_id}.log` and records `body_ref`,
  `body_bytes`, `body_sha256`; the app currently writes them under its own
  `storage_dir`, keyed by `request_id`, and records `body` + `body_stored`. The
  schema carries both sets of columns; the rclone uploader is not written.
- **The non-blocking Docker log driver** — `deploy/compose.yaml` uses the
  `local` driver. The design wants `json-file` with `mode: non-blocking` so a
  stalled Vector cannot back-pressure inference; the snippet is in
  `vector/compose.yaml`.

## Deviations from the design

- `usage_inference` uses `ReplacingMergeTree(timestamp)`; the doc's
  `ReplacingMergeTree(event_id)` is rejected by ClickHouse because a UUID
  cannot be a version column.
- The 13-month TTL has no `TO VOLUME 'cold'` clause — a cold volume must exist
  in the server's storage config first.
- The S3 raw archive is a second, optional config file instead of part of the
  main pipeline, so the sample runs without NetApp credentials.
- Storage paths default to `/srv/clickhouse/...`; the design does not name
  them.
