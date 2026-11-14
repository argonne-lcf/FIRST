# Docker Deployment

The gateway runs as a small Docker Compose application. A single **base**
file defines the services that exist everywhere; thin **overlay** files add
the differences for each environment. The base file never runs alone.

| File | Role |
|---|---|
| `deploy/compose.yaml` | Base services common to dev and prod: apiserver, controller-manager, postgres, redis, prometheus. |
| `deploy/compose.dev.yaml` | **Dev** overlay — nginx container, auto-migration, dev data volumes, dev env (`.env.compose`). |
| `deploy/compose.prod.yaml` | **Prod** overlay — publishes apiserver on `127.0.0.1:7001`, external data volumes, prod env (`.env.prod`); no nginx, no auto-migration. |
| `deploy/compose.tunnel.yaml` | Opt-in **dev** overlay — SOCKS tunnel + `*PROXY` injection. |
| `deploy/Dockerfile` | Image for both apiserver and controller-manager (same image, different `command`). |
| `deploy/Dockerfile.tunnel` | Tiny image (openssh) for the SOCKS tunnel container. |
| `deploy/nginx.conf` | **Dev** reverse-proxy config; publishes the apiserver on `:8000`. |
| `deploy/nginx.prod.conf.example` | Template for the **host** nginx in prod (TLS, proxy to `127.0.0.1:7001`). |
| `deploy/first.service.example` | Template systemd unit to run the prod stack on boot. |
| `deploy/prometheus.yml` | Prometheus scrape config (controller-manager + discovered backends). |
| `deploy/compose.v1-logs.yaml` | Opt-in overlay — ships the V1 gateway's logs until V1 retires. See [Log Shipping](log-shipping.md). |
| `deploy/log-shipping/` | Vector config and the rclone script for the `vector` and `prompt-shipper` services (`log-shipping` profile). |
| `deploy/compose.dev-analytics.yaml` | Opt-in dev-gateway overlay — exposes Prometheus privately to FIRST-analytics. |

## Architecture

```mermaid
flowchart TB
    classDef gw fill:#e8f0ff,stroke:#3b6ea8,color:#1a2a3a
    classDef store fill:#ffffff,stroke:#888,color:#222
    classDef proxy fill:#fff5e6,stroke:#d49a3a,color:#3a2a10
    classDef obs fill:#eafaf1,stroke:#3a9a6a,color:#10301f
    classDef opt fill:#f3f0ff,stroke:#7a5ad4,color:#241a3a,stroke-dasharray: 4 3

    Client([client / alcf-ai])

    subgraph host[Host]
      NX["nginx<br/>dev: container :8000<br/>prod: host :443"]:::proxy
      subgraph net["Compose network (first_gateway)"]
        API["inference-gateway<br/>apiserver :7000"]:::gw
        CM["controller-manager<br/>:9100"]:::gw
        PG[("postgres:18")]:::store
        RD[("redis:8")]:::store
        PROM["prometheus :9090"]:::obs
        TUN["tunnel :1080<br/>(dev, opt-in)"]:::opt
      end
    end
    Pilot([HPC pilot backends])

    Client --> NX --> API
    API --> PG & RD
    CM --> PG & RD
    PROM -->|scrape| CM
    PROM -->|http_sd + scrape| Pilot
    ANALYTICS["Analytics Grafana<br/>separate VM"]:::obs
    ANALYTICS -->|private :9090| PROM
    API -.egress via.-> TUN
    CM -.egress via.-> TUN
    TUN -.SSH -D.-> Pilot
```

## Components

- **inference-gateway (apiserver)** — the user-facing API (gunicorn/uvicorn on
  `:7000`). Fronted by nginx. Dev doesn't publish `:7000` (nginx proxies it
  inside the network); prod publishes `127.0.0.1:7000` for the host nginx.
- **controller-manager** — runs the control loops; exposes metrics, pilot
  service-discovery and the **admin API** on `127.0.0.1:9100`: the control plane
  (`/control/v1`), the full catalog (`/catalog/v1`), the admin console
  (`/admin-console/`) and its OpenAPI docs (`/docs`). Nothing admin is served by
  the apiserver or proxied by nginx; operators reach it on the host or through
  an SSH tunnel (`ssh -L 9100:localhost:9100 <host>`). Same image as the
  apiserver.
  It binds `0.0.0.0` inside the Compose network so
  Prometheus can scrape it; the port is published only on the host's loopback
  (`127.0.0.1:9100`). Same image as the apiserver.
- **postgres:18** — application database. Published on `127.0.0.1:5433` for host
  access. Healthcheck gates the apiserver/controller startup.
- **redis:8** — control-plane state / coordination on `127.0.0.1:6380`. Tuned for
  latency and throughput over durability (light RDB, no AOF); data is persistent
  but expendable and **not** backed up.
- **prometheus** — scrapes the controller-manager and the pilot backends it
  discovers. Bounded footprint (retain ~15 days / 5 GB) with the admin API enabled
  for snapshots. Published on `127.0.0.1:9090`. Managed gateway deployment paths
  can additionally publish a private-VM listener for Analytics Grafana. That
  plaintext, admin-capable endpoint must be firewalled to the Analytics VM only.
- **Analytics Grafana** — deployed separately in FIRST-analytics. It provisions
  the control-plane and vLLM dashboards and queries gateway Prometheus over the
  private listener. No standalone Grafana runs in this Compose stack.
- **nginx** — reverse proxy + rate limiting + SSE-friendly buffering. A container
  in dev; the **host's** nginx in prod (see below).
- **vector**, **prompt-shipper** *(`log-shipping` profile)* — move structured logs
  and large request/response bodies off the VM. See [Log Shipping](log-shipping.md).
- **tunnel** *(dev, opt-in)* — an SSH SOCKS proxy so a laptop with no direct route
  to the HPC systems can still reach pilot backends.
- **migration** *(dev only)* — one-shot `alembic upgrade head` on startup. Prod
  migrations are manual.

## Filesystem dependencies

The stack bind-mounts a few host paths. They must exist before `up`:

- **`pki/`** (repo root, `.gitignore`d) — pilot mTLS material, mounted as Docker
  secrets: `ca.crt` into every service, plus one client pair per service
  (`first-router` → `inference-gateway`, `first-control` → `controller-manager`,
  `first-metrics` → `prometheus`), always at `/run/secrets/pilot_client.{crt,key}`.
  `make pki` generates a dev set; prod certificates are issued offline (see the
  [Certificate Manager](../packages/certmanager.md)). The CA key is never
  deployed. See the ownership note below for Linux hosts.
- **`pki/upstream/`** — client certificate sets for StaticDeployments whose
  externally-managed upstream requires mutual TLS (unrelated to the pilot CA).
  One directory per set, typically one per cluster, shared by all of that
  cluster's deployments:

        pki/upstream/<set>/ca.crt       # CA that signed the upstream's server cert
        pki/upstream/<set>/client.crt
        pki/upstream/<set>/client.key

  The whole directory is bind-mounted read-only at `/run/secrets/upstream/` in
  `inference-gateway`, `controller-manager` and `prometheus`, so adding a set
  needs no compose change. Reference the in-container paths from the spec:

        mtls:
          ca_file: /run/secrets/upstream/<set>/ca.crt
          cert_file: /run/secrets/upstream/<set>/client.crt
          key_file: /run/secrets/upstream/<set>/client.key

  The controller checks that the set loads at plan time. The certificates secure
  proxied requests and the deployment's health check; to scrape its metrics, add
  a Prometheus job for the set (template at the end of `deploy/prometheus.yml`).
  Files are read when clients are built: after rotating a set, restart
  `inference-gateway` and `controller-manager`. `make pki` creates the empty
  directory.
- **`packages/admin-console/dist`** — the built admin UI, mounted into
  `controller-manager`, which serves it at
  `http://localhost:9100/admin-console/`. Build it (`npm run build` in
  `packages/admin-console`) before `compose up`, in dev and prod alike, and
  restart `controller-manager` after the first build. The console's Globus
  login redirects to `http://localhost:9100/admin-console/callback`, so tunnel
  to local port 9100.
- **`.env.secret.gateway`, `.env.secret.controller`** (and in prod
  **`.env.secret.postgres`**; repo root, `.gitignore`d) — per-service secrets,
  required by the stack; see [Environment files](#environment-files).
- **`backups/`** — created on demand by the backup scripts (`.gitignore`d);
  override with `BACKUP_DIR`.

!!! warning "Key file ownership on Linux"
    Compose file-based secrets are bind mounts: they keep the host file's owner
    and mode. On a Linux host, each `0600` key must be owned by the container
    user that reads it: `app` (uid 900, see `deploy/Dockerfile`) for
    `first-router.key` and `first-control.key`, and `nobody` (uid 65534) for
    `first-metrics.key`, which Prometheus reads.

        sudo chown 900 pki/first-router.key pki/first-control.key
        sudo chown 65534 pki/first-metrics.key

    For `pki/upstream/`, the gateway services run as uid 900 and Prometheus
    reads through its supplementary group 900 (`group_add` in the overlays), so
    keys are owner- and group-readable only:

        sudo chown -R 900:900 pki/upstream
        sudo chmod 0750 pki/upstream pki/upstream/*
        sudo chmod 0644 pki/upstream/*/*.crt
        sudo chmod 0640 pki/upstream/*/client.key

    Docker Desktop on macOS maps file ownership, so a dev stack works as
    generated by `make pki`.

## Environment files

`env_file` layering. The base loads only what is valid everywhere; each overlay
adds its environment-specific file, so **no dev value can leak into prod**:

1. `.env.default` — common, non-secret defaults, checked in (base).
2. Per-service secrets, `.gitignore`d (base, see below).
3. `.env.compose` — **dev only** (added by `compose.dev.yaml`): the dev
   `FIRST_DB_URL` and in-network `redis`/`postgres` URLs.
4. `.env.prod` — **prod only** (added by `compose.prod.yaml`): non-secret
   service URLs and prod settings.

`.env.local` is **not** loaded inside Compose; it only helps host-side test runs
reach the published `localhost` ports. `.env` (dev, `.gitignore`d) selects the
stack via `COMPOSE_FILE` and holds dev conveniences like `TUNNEL_SSH_TARGET`.

All settings use the `FIRST_` prefix with `__` for nesting — e.g. `FIRST_DB_URL`,
`FIRST_GLOBUS__APP_ID`. See
[`settings.py`](https://github.com/argonne-lcf/inference-gateway/blob/main/packages/gateway/first_gateway/settings.py).

### Secrets are scoped per service

Each service reads only the secrets it needs. Above all, the internet-facing
`inference-gateway` holds **no credential that can submit work to a cluster**:
the Globus Compute client and the Keycloak impersonation clients live only in
`controller-manager`. A compromised gateway yields data-plane access only (the
read-only `first-router` pilot identity, static backend API keys, user-token
introspection).

Copy each example to its secret file and fill it in:

| Secret file | Example | Read by | Holds |
|---|---|---|---|
| `.env.secret.gateway` | `.env.gateway.example` | `inference-gateway` | Globus resource-server app, data-staging client, static backend API keys, `FIRST_DB_URL` |
| `.env.secret.controller` | `.env.controller.example` | `controller-manager` | Globus resource-server app (control-server admin API), Globus Compute client, Keycloak impersonation clients, Slack token, static backend API keys (health checks), `FIRST_DB_URL` |
| `.env.secret.postgres` | `.env.postgres.example` | `postgres` (prod) | `POSTGRES_PASSWORD` |
| `.env.secret.log-shipping` | `.env.log-shipping.example` | `vector`, `prompt-shipper*` | see [Log Shipping](log-shipping.md) |

`postgres` gets nothing but `POSTGRES_DB`/`POSTGRES_USER` (set in the overlay)
and `POSTGRES_PASSWORD` (prod: `.env.secret.postgres`; dev: a throwaway value
in `compose.dev.yaml`). The dev `migration` service reads only `.env.compose`,
for `FIRST_DB_URL`. Prometheus, Redis, nginx and the tunnel get no secrets file.

The gateway fails closed to keep it that way: it refuses to start if it is
configured with `FIRST_GLOBUS__COMPUTE_CLIENT_SECRET` or any
`FIRST_KEYCLOAK_CLIENTS` entry, so a compose regression breaks the deployment
instead of passing silently.

Replica logs follow from this: the API tails a **live** replica's log through
its pilot, but a finished replica's log stays on the cluster (`409`, naming the
path), since reading it would take a scheduler credential. The data-staging
routes use their own Transfer-only Globus client (`FIRST_DATA_STAGING_CLIENT_*`)
and return `503` when it is not configured.

## Dev environment

Set `.env` at the repo root:

```ini
COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml
COMPOSE_PROJECT_NAME=first
inference_base_url=http://localhost:8000
```

```bash
make compose-up      # build + start the dev stack
make watch-logs      # apiserver logs
make compose-down    # stop
make db-reset        # wipe DEV db/redis volumes and re-run migrations (prompts)
```

Once up: apiserver via nginx at <http://localhost:8000> and Prometheus at
<http://localhost:9090>. Postgres and Redis are on `127.0.0.1:5433` /
`127.0.0.1:6380`. Use the consolidated FIRST-analytics Grafana for dashboards.

### Dev-gateway Analytics listener

On `inference-api-dev`, append the Analytics overlay to the existing selection
in `.env`:

```ini
COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml:deploy/compose.tunnel.yaml:deploy/compose.dev-analytics.yaml
COMPOSE_PROJECT_NAME=first
```

Then deploy with the existing target:

```bash
make compose-up
```

The overlay preserves `127.0.0.1:9090` for local snapshot tooling and adds
`140.221.33.155:9090` for the consolidated Analytics Grafana. The conduit and
host firewall must allow that plaintext listener only from
`inference-olap-01`. Ordinary local development does not select the VM-specific
overlay and remains loopback-only.
### Dozzle (browser log viewer)

Dozzle is intentionally **not** in the Compose stack. Run it on demand:

```bash
./scripts/dozzle.sh          # http://127.0.0.1:8080 ; Ctrl-C to stop
```

It attaches to the `first_gateway` network and reads the Docker socket read-only.

### SOCKS tunnel (opt-in)

On a laptop with no route to the HPC systems, append the tunnel overlay and set
the SSH target:

```ini
# .env
COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml:deploy/compose.tunnel.yaml
TUNNEL_SSH_TARGET=you@bastion.example.org
```

The overlay both defines the `tunnel` service and injects `*_PROXY` into the
apiserver, controller-manager, and Prometheus. Because the tunnel uses one-time
passwords, you attach once to authenticate:

```bash
make compose-up
make attach-tunnel   # shows recent logs, then attaches; enter your OTP
                     # press Ctrl-X to detach (leaves it running)
```

Prod never references this file, so the tunnel simply does not exist there.

## Prod environment

Prod differs from dev in three ways: **nginx runs on the host**, **migrations are
manual**, and **data volumes are external** (so `docker compose down -v` cannot
delete them).

### First-time setup

```bash
# 1. Write the per-service secret files from their examples (see
#    "Secrets are scoped per service" above):
#      .env.secret.gateway, .env.secret.controller, .env.secret.postgres
#    FIRST_DB_URL (gateway and controller) carries POSTGRES_PASSWORD:
#      FIRST_DB_URL=postgresql+psycopg://first_gateway:...@postgres:5432/first_gateway
#    Copy the prod ca.crt and client cert pairs (issued offline, see
#    packages/certmanager.md) into pki/.

# 2. Create the external data volumes once:
make prod-init

# 3. Bring up the stack and apply migrations:
make prod-up
make prod-migrate

# 4. Configure the HOST nginx (see below).
```

`make prod-*` targets select the prod overlay explicitly, so `.env` can stay on
the dev path. Day-to-day:

```bash
make prod-up        # start / apply changes
make prod-migrate   # after deploying a new image
make prod-down      # stop (never uses -v)
```

The prod overlay preserves `127.0.0.1:9090` for local snapshots and additionally
publishes `140.221.33.25:9090` for consolidated Analytics Grafana queries. Keep
that private listener restricted to `inference-olap-01` in the conduit and host
firewall: it is plaintext HTTP and includes the enabled Prometheus admin API.

### V1-to-V2 auth cutover gate

This is a cutover checklist, not authorization to switch production traffic.
Keep the current V1 request path until the normal review and Dev qualification
gates pass. These changes do not require a database migration.

1. Back up the deployed V1/V2 auth configuration securely and record the current
   application versions and NGINX upstream. Confirm the V1 `.env` is the file
   actually loaded by V1; account for any deployment-specific overrides.
2. Audit the **complete** Dev and Prod fixture catalogs. Every `AccessGroup` must
   explicitly declare both lists (including `[]` on `public`), and every Model
   must reference an existing group. Preview the full plan; never apply a
   partial catalog as desired state.
3. Copy these three settings from V1 into V2 **in one configuration change**, not
   successive access-policy changes:

   | V1 `.env` | V2 environment |
   | --- | --- |
   | `AUTHORIZED_IDP_DOMAINS` | `FIRST_GLOBUS__AUTHORIZED_IDP_DOMAINS` |
   | `AUTHORIZED_GROUPS_PER_IDP` | `FIRST_GLOBUS__AUTHORIZED_GROUPS_PER_IDP` |
   | `AUTHORIZED_GLOBUS_SERVICE_USERNAMES` | `FIRST_GLOBUS__AUTHORIZED_SERVICE_USERNAMES` |

   V1 domain/policy/group text lists use whitespace or semicolons. V1's per-IDP
   JSON maps domains to comma-separated group **strings**; V2 maps them to JSON
   **lists**. V2 list settings, including service usernames, are JSON lists.
   Preserve exact identifiers and membership. Policy and global user groups
   were already equal in the November 8 snapshot, but verify them again rather
   than assuming that remains true.
4. Check the rendered production overlay, without writing its secrets to an
   artifact. From the V2 checkout, with the workspace dependencies installed:

   ```bash
   set -o pipefail
   COMPOSE_FILE=deploy/compose.yaml:deploy/compose.prod.yaml \
     docker compose config --format json \
     | uv run python scripts/check-auth-parity.py \
         --v1-env /secure/path/to/v1/.env --v2-compose -
   ```

   Require exit `0` and `No differences in the five auth gates.` before the
   NGINX switch. Exit `1` names differing gates; exit `2` means invalid input.
   The checker neither sources the V1 file nor prints gate values or secrets.
   It rejects unresolved `${...}` in V1 gate values: provide the effective,
   resolved values in a protected comparison file if the deployment uses
   interpolation. File input is also supported with `--v2-compose FILE`;
   rendered Compose files contain secrets, so protect and remove them.
   Use the documented JSON list/map settings, not deeper `__` overrides within
   a gate; the checker rejects those rather than silently comparing partial values.
5. Recreate the affected V2 gateway/controller services with the reviewed
   settings **before** switching traffic (a restart alone does not reload
   Compose environment changes). Use the established staging/listener layout;
   do not take V1's live port merely to run this check. Confirm startup and
   repeat parity against the exact configuration used to create those services.
   V2 refuses to start with empty policies or authorized-domain lists, or with a
   per-IDP group key that is not an authorized domain. Fix the key or the domain
   list; do not broaden access to make it start.
6. On Dev, verify approved-domain access, per-IDP group admission and rejection,
   service clients, invalid tokens, model-specific restrictions and admin access.
   A rejected inference request must log only metadata, never its body or a body
   file. Keep normal authenticated request logging intact. Do not bypass V1's
   startup policy guard or relax allowlists to make Dev start.
7. Record parity and acceptance results as production cutover prerequisites.
   Change NGINX only through the separately approved cutover. If qualification
   fails, retain V1 traffic and restore the saved V2 configuration; after an
   approved switch, rollback restores the recorded V1 upstream and versions.

### Host nginx

In prod the host runs nginx and proxies to the loopback-published apiserver.
`deploy/nginx.prod.conf.example` mirrors the dev config (SSE-friendly buffering)
but terminates TLS, targets `127.0.0.1:7001`, takes the client address from
`X-Forwarded-For` only for requests from the front-end proxies (`real_ip`), and
rate-limits per client address rather than per token:

```bash
sudo cp deploy/nginx.prod.conf.example /etc/nginx/conf.d/first.conf
# edit server_name and TLS cert paths
sudo nginx -t && sudo systemctl reload nginx
```

Host nginx serves only the user-facing API. The admin console and admin API
stay on `127.0.0.1:9100` (see [Components](#components)); never add a
`proxy_pass` to that port.

### Grafana consolidation

FIRST-analytics hosts authenticated dashboards at
<https://inference-api.alcf.anl.gov/dashboard/> and provisions the FIRST
control-plane and vLLM sources. FIRST Compose no longer creates a Grafana
service or its data volumes.

Removing the Compose service does **not** stop an already-running container.
After validating the unified deployment, explicitly stop and remove each legacy
Grafana container and retain `grafana_data_prod` and `first_grafana-dev` through
a rollback window; neither is deleted by this change.

### Run on boot (systemd)

`deploy/first.service.example` ties the prod stack's lifecycle to boot. Compose's
own `restart: unless-stopped` keeps containers alive while dockerd runs; the unit
adds ordering after `docker.service` and a clean start/stop.

```bash
sudo cp deploy/first.service.example /etc/systemd/system/first.service
# edit WorkingDirectory (repo path) and User
sudo systemctl daemon-reload
sudo systemctl enable --now first.service
```

Run `make prod-init` once before the first start. Migrations remain manual
(`make prod-migrate`) so a schema change never runs unattended on boot.

### Disk alerts

The health alerter in `controller-manager` alerts when a block device it can
see is over 70% (info), 80% (warn) or 90% (crit) full, once per device, e.g.
`/dev/sda2 88% full (docker-data)`. A container sees a host filesystem only
through something mounted from it, so `compose.yaml` mounts one empty,
read-only directory per filesystem to watch, under `/run/disk-watch/<label>`:

| Label | Host directory | Filesystem on the VMs |
|---|---|---|
| `docker-data` | the `disk-watch` named volume, in Docker's data root | `/dev/sda2` (`/`) |
| `checkout` | `deploy/disk-watch/` in this checkout (committed, empty) | `/dev/mapper/vg01-webportal` (`/home/webportal`) |

Both exist without any setup step, on the VMs and on a laptop. To watch
another filesystem, mount an empty directory from it the same way.

## Backups

Both scripts are safe against a live stack and pick up the stack selected by
`COMPOSE_FILE`. Destinations are configurable via `BACKUP_DIR` (default
`./backups`); each keeps the newest `KEEP` (default 7) and prunes older ones.

```bash
make db-backup       # pg_dump -> backups/postgres/first-<ts>.sql.gz
make prom-snapshot   # TSDB snapshot -> backups/prometheus/prometheus-<name>.tar.gz
```

Restore Postgres:

```bash
gunzip -c backups/postgres/first-<ts>.sql.gz \
  | docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" "$POSTGRES_DB"'
```

Redis is **not** backed up by design (expendable state).

### Nightly cron (prod)

`cd` into the repo first so `.env` resolves; point `BACKUP_DIR` at persistent
storage. Example crontab (`crontab -e`):

```cron
# m h dom mon dow  command
15 2 * * *  cd /opt/inference-gateway && BACKUP_DIR=/var/backups/first KEEP=7 ./scripts/pg-backup.sh    >> /var/log/first-backup.log 2>&1
30 2 * * *  cd /opt/inference-gateway && BACKUP_DIR=/var/backups/first KEEP=7 ./scripts/prom-snapshot.sh >> /var/log/first-backup.log 2>&1
```

See [`scripts/README.md`](https://github.com/argonne-lcf/inference-gateway/blob/main/scripts/README.md)
for details.

## Troubleshooting

```bash
docker compose ps                                  # status
docker compose logs inference-gateway --since=1m   # recent logs
docker compose logs controller-manager --since=1m
make db-reset                                      # DEV only: wipe + re-migrate
```

!!! warning
    `make db-reset` removes the dev volumes by name (`first_postgres-dev`,
    `first_redis-dev`). Prod volumes are `external` and immune to
    `docker compose down -v` — but never run `db-reset` against a prod stack.

## Beyond a single host

Container-native deployment is a primary goal of v2 (see
[Motivation](../architecture/motivation.md)); the eventual target is the ALCF
Hermes Kubernetes cluster. The Compose prod overlay is a single-host staging step —
the same image, settings, and `Settings` loading carry over.
