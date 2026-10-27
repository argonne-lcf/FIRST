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

### One reconcile tick (writes)

The following runs real reconciliation and can create, update and delete
bridge-managed rows. It is **not a dry run**. Review the model defaults and
expected endpoint changes before using it:

```sh
cd /home/webportal/inference-gateway
.venv/bin/python manage.py first_v2_bridge --once
```

### Access-control limits

- **Before V2 adds an access-control dimension beyond groups/domains**, it must be
  modelled in `first_v2_bridge/router_config.py` and enforced in V1; until then the
  bridge silently ignores it and V1 has no column or check.
- A parse failure aborts the tick rather than writing, so the last-reconciled rows
  (including a permissive one) persist. Audit after an outage or a V2 schema change.
- `_reconcile_endpoints` matches rows by `endpoint_slug` only, so it can overwrite
  a same-slug row created by another adapter.
- V1's adapter cache has a 60 s TTL, so a reconciled restriction can take up to
  a minute to apply.

### Long-context requests and V1 limits

`FirstV2Endpoint` allows up to **900 seconds without upstream response data**
for both unary and streaming inference. This is a read-inactivity timeout, not
a total request duration limit; connect/pool waits remain 5 seconds and writes
60 seconds. Check the gateway's frontdoor body/read limits too. Final streaming
logs and token accounting run after the stream finishes, errors or is closed,
not while a long prefill is still running.

The temporary bridge uses this same, deliberately limited mapping in every
environment and cluster:

| V1 field | Source |
| --- | --- |
| `tpm_model` | Always `0`: V2 has no corresponding aggregate model-wide TPM cap. |
| `tpm_user` | `usage_limits.default.tpm` from that model in V2's Redis RouterConfig, including `0`. |

User-ID, username and group overrides are intentionally ignored. V1 does not
attempt to reproduce V2's per-user policy resolution. A limited default stays
limited for every V1 user even if a V2 override exempts one user; an unlimited
default stays unlimited even if a V2 override restricts one user. V1 retains its
existing completed-token window, not V2's GCRA reservations or bursts. V2 RPM,
token-burst and per-user concurrency policies are not added to the public V1
path. Authentication and access-group/domain checks are separate and unchanged.

Set unlimited TPM in the **model entry**, not in bridge code or environment
variables: `usage_limits.default.tpm: 0`. Set `rpm: 0` there too when native V2
request-rate limits should be disabled. Preserve the entry's other admission
settings and access controls. A Model resource shared by multiple clusters has
one default policy for all those routes; review that scope before changing it.
There is no cluster-specific TPM branch, opt-in setting, manual SQL procedure or
request-time override resolver in the bridge.

Every normal reconcile writes both TPM values on creation, update and
recreation. An endpoint that disappears with its last healthy backend is
recreated with the current V2 default, not V1's database defaults or an old
manual value. Unrelated legacy adapters and global database defaults are not
changed. The model's default TPM must be a nonnegative integer fitting V1's
signed SQL integer column (at most `2147483647`); a missing, malformed or
unrepresentable default fails the tick before endpoint writes. Overrides are
not parsed and cannot change that projection.

Zero disables TPM rejection, not token recording or request/usage accounting.
The gateway's independent global per-user request throttle
(`RATE_LIMIT_PER_SEC_PER_USER`, default 10 requests/second), frontdoor security
throttles, authentication, access controls and timeouts remain in effect.

Allow the 60-second adapter-cache TTL after the last DB update for a limit
change to take effect in existing gateway workers. No gateway/model restart
or clearing of caches/sessions is necessary for a TPM-row update. Deploying new
bridge code requires a bridge-only restart, not a gateway or model restart.
Check V2's effective RouterConfig before deployment, and after a reconcile tick
and cache expiry use the normal authenticated `GET /resource_server/{cluster}/models`
route. Positive defaults appear in `rate_limit_token_per_minute_per_user`;
zero-valued TPM metadata keys are absent. This verifies the gateway's cached
endpoint configuration without submitting inference or exhausting a quota.

### Operations

```sh
sudo systemctl restart first_v2_bridge.service   # after changing .env or certs
sudo systemctl stop first_v2_bridge.service      # pause reconciliation
```

Stopping the bridge leaves the last-reconciled `Endpoint` rows in place; the
gateway keeps serving them until the bridge runs again and reconciles (deletes
rows whose backends have disappeared).
