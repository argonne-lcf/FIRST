# First V2 → V1 Bridge Deployment

The bridge is a long-lived sidecar (`manage.py first_v2_bridge`) that polls the
V2 controller's Redis for its `router-cfg` blob and reconciles V1 `Endpoint`
rows.

The V1 gateway proxies inference to V2-managed pilot backends over
mTLS (optionally through a SOCKS/HTTP proxy in the absence of a conduit).
Requests to `/{cluster}/api/v1/...` and `/{cluster}/jobs` are then served by the
`FirstV2Endpoint` / `TaraCluster` adapters.

Note that the bridge and the V1 gateway use **two distinct Redis instances**!
The bridge reads `router-cfg` from V2's Redis; it never writes to it. All
writes go to the V1 Postgres `Endpoint`/`Cluster` tables.


## Environment variables

Both the gateway and the bridge run with the same workdir, and the same
`inference_gateway.settings` module loads the `.env` file for both.
Here are the new environment variables for the V2-V1 bridge.

### Required

| Variable                    | Example                         | Notes |
| --------------------------- | ------------------------------- | ----- |
| `FIRST_V2_REDIS_URL`        | `redis://v2-host:6379/1`        | Dedicated connection to V2's Redis. Must differ from `REDIS_URL`. |
| `FIRST_V2_CA_CERT_PATH`     | `/etc/first/pki/ca.crt`         | CA that signed the backend server certs. |
| `FIRST_V2_CLIENT_CERT_PATH` | `/etc/first/pki/first-pilot.crt`| Client cert presented to backends (mTLS). |
| `FIRST_V2_CLIENT_KEY_PATH`  | `/etc/first/pki/first-pilot.key`| Private key for the client cert. Must be readable by the service `User`. |

`REDIS_URL` (V1's own Redis) is required by the gateway independently and is assumed already set.

### Optional

| Variable                    | Default | Notes |
| --------------------------- | ------- | ----- |
| `FIRST_V2_PROXY_URL`        | *(unset → direct)* | SOCKS/HTTP proxy to reach backends, e.g. `socks5h://localhost:1080`. `socks5h://` requires httpx ≥ 0.28 (pinned in `uv.lock`). |
| `FIRST_V2_CHECK_HOSTNAME`   | `false` | TLS hostname verification. Off because backends are reached by IP (mirrors V2's own client). |
| `FIRST_V2_POLL_INTERVAL_SEC`| `10`    | Seconds between reconcile ticks. |

The cert/proxy values are baked into each managed `Endpoint`'s `config` at
reconcile time, so the gateway process reads them from the DB row at request
time.

## Systemd unit

The unit is `deploy/first_v2_bridge.service`. It mirrors
`gateway_async.service`: same `User`/`Group`/`WorkingDirectory`, runs the
management command directly from the venv, `Restart=always`.

Because config comes from `.env` (loaded by `load_dotenv`), the unit itself
declares no `FIRST_V2_*` variables — it only sets `PATH` and
`PYTHONUNBUFFERED`.

### Install

```sh
# From the repo root on the gateway host (adjust paths if not /home/webportal):
sudo cp deploy/first_v2_bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now first_v2_bridge.service
```

### Verify

```sh
systemctl status first_v2_bridge.service
journalctl -u first_v2_bridge.service -f
# Healthy ticks log e.g.:
#   first_v2_bridge tick: version=12 desired=1 created=0 updated=1 deleted=0
```

### One-shot dry run

Before enabling the service, confirm connectivity and reconciliation with a
single tick:

```sh
cd /home/webportal/inference-gateway
.venv/bin/python manage.py first_v2_bridge --once
```

### Operations

```sh
sudo systemctl restart first_v2_bridge.service   # after changing .env or certs
sudo systemctl stop first_v2_bridge.service      # pause reconciliation
```

Stopping the bridge leaves the last-reconciled `Endpoint` rows in place; the
gateway keeps serving them until the bridge runs again and reconciles (deletes
rows whose backends have disappeared).
