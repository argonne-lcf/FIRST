# Certificate Manager

`pilot-certmanager` is a small [Typer](https://typer.tiangolo.com/) CLI, shipped
with `first-gateway`, that wraps `openssl` to run the private CA for the
gateway ⇄ pilot mTLS link. It is an **offline** tool: an administrator runs it
on a trusted host, and no running service imports it or holds a CA key.

This page is the issuing and renewal runbook. For why the model looks like
this, see the [F-01 response](../security/control-plane-mtls-response.md).


## Identities

Every environment (dev, prod) has its own CA and one fixed set of leaf
certificates. The subjects are constants in `first_common.schema.pilot`
(`PILOT_SERVER_CN`, `PILOT_SERVER_SAN`, `PilotClientRole`):

| Subject | EKU | SAN | Holder | Pilot role |
|---|---|---|---|---|
| `CN=first-pilot` | serverAuth | `DNS:first-pilot.internal` | pilot NGINX, every cluster | — |
| `CN=first-control` | clientAuth | — | `controller-manager` | control |
| `CN=first-router` | clientAuth | — | `inference-gateway` (API server) | router |
| `CN=first-metrics` | clientAuth | — | `prometheus` | metrics |

Pilot NGINX maps the client subject to a role and authorizes every request
against it, denying by default:

| Role | Permitted |
|---|---|
| control | All paths |
| router | `/replicas/…` (inference); `GET /control/logs/…` |
| metrics | `GET /replicas/…/metrics` only |
| anything else | Nothing (403) |

Clients do **not** verify the pilot's hostname: compute nodes have no DNS
names, and the node a pilot lands on is chosen by the scheduler after the
certificate must already exist. The pilot is identified by chaining to the
environment's CA with a `serverAuth` certificate, which only pilots hold.
Prometheus sets `server_name: first-pilot.internal` to satisfy its TLS client.


## Issuing the standard set

Run on a **trusted admin host**, never on the gateway or a cluster. Requires
OpenSSL 3.x. Keys are EC P-256, certificates SHA-256, and private keys are
unencrypted (services start unattended).

```bash
# CA (if absent) + first-pilot server + the three client certificates
pilot-certmanager standard --dir pki-prod/ --ca-name "FIRST Pilot CA (prod)"
```

The command creates `ca.{key,crt}` only if the directory has no `ca.crt`, then
(re)issues `first-pilot`, `first-control`, `first-router` and `first-metrics`,
each as `<cn>.{crt,key}` (`--days`, default 730, sets leaf validity). For local development, `make pki` runs it into the
repo's `.gitignore`d `pki/`, which is all the Compose stack and the tests need.

The lower-level `ca`, `server <cn>` and `client <cn>` commands remain for
one-off issuing; see `pilot-certmanager --help`.


## Where each file goes

| File | Secret | Destination |
|---|---|---|
| `ca.key` | **yes** | Admin host / offline storage only. Never deployed. |
| `ca.crt` | no | Gateway `pki/` and every cluster's runtime config |
| `first-pilot.{crt,key}` | key | Every cluster's runtime config only |
| `first-control.{crt,key}` | key | Gateway `pki/` → `controller-manager` |
| `first-router.{crt,key}` | key | Gateway `pki/` → `inference-gateway` |
| `first-metrics.{crt,key}` | key | Gateway `pki/` → `prometheus` |

### Gateway host

Copy `ca.crt` and the three client pairs into `pki/` at the repo root
(`chmod 600 *.key`; on Linux also match the key owners to the container
users, see [Docker](../deployment/docker.md#filesystem-dependencies)).
`deploy/compose.yaml` declares them as Docker secrets and
mounts a **different identity behind the same names** in each service:

| Service | `/run/secrets/pilot_client.{crt,key}` |
|---|---|
| `inference-gateway` | `first-router` |
| `controller-manager` | `first-control` |
| `prometheus` | `first-metrics` |

Every service also gets `/run/secrets/pilot_ca.crt`. The gateway reads these
through `pilot_ca_crt_file`, `pilot_client_crt_file` and `pilot_client_key_file`
(env `FIRST_PILOT_CA_CRT_FILE`, `FIRST_PILOT_CLIENT_CRT_FILE`,
`FIRST_PILOT_CLIENT_KEY_FILE`), which default to those paths, so code never
knows which role it holds. No CA material belongs in `.env.secret`.

### Each cluster

Every cluster runs its pilots from a pre-staged `PilotRuntimeConfig` YAML at
`PilotConfig.pilot_config_path` (required). It carries the PEMs inline:

```yaml
ca_crt: |
  -----BEGIN CERTIFICATE-----
  ...
server_crt: |
  -----BEGIN CERTIFICATE-----
  ...
server_key: |
  -----BEGIN PRIVATE KEY-----
  ...
```

These three are the only fields the file must carry. The submitter supplies
every other required field per job as `PILOT_*` environment overrides, taken
from the cluster's `PilotConfig` and the job (`PILOT_JOB_NAME`,
`PILOT_EXTERNAL_PORT`, `PILOT_NGINX_PATH`, `PILOT_IP_ALLOWLIST`,
`PILOT_WORKDIR`, `PILOT_NODE_FILE_ENV`, `PILOT_GPU_DISCOVERY`,
`PILOT_NUM_NODES`, `PILOT_GPUS_PER_NODE`, `PILOT_WALLTIME_MIN`); those override
any value in the file. The optional `network_interface` is not overridable and
can only be set in the file. Make the file readable only by the pilot service
account (`chmod 600`).


## Renewal

Leaf certificates default to 1 year; the CA to 10. Renewal re-issues leaves
against the **same CA**, so nothing else needs to change:

1. Re-run `pilot-certmanager standard` against the environment's CA directory.
2. Replace the client pairs in the gateway's `pki/` and restart the stack
   (`docker compose up -d --force-recreate`).
3. Replace `server_crt`/`server_key` in each cluster's runtime config. Running
   pilots keep their old certificate until they exit; new submissions pick up
   the new one.

Two checks surface expiry before it bites:

- The `controller-manager`'s `health_alerter` checks its mounted client
  certificate (`first-control`): a `warn` alert when it expires in under 30
  days, `crit` once expired, posted to Slack when configured. The client
  certificates are issued together, so one alert covers the set.
- A pilot refuses to start if its server certificate expires before
  `now + walltime_min`. Renew the server certificate at least one job walltime
  ahead of expiry.


## CA rotation (suspected leak)

There is no CRL. If any private key may have leaked, replace the CA and the
whole standard set for that environment:

1. Run `pilot-certmanager standard` into an empty directory; a new CA is
   created because none is present.
2. Stage the new `ca.crt` and `first-pilot` pair in every cluster's runtime
   config.
3. Replace the gateway's `pki/` contents and recreate the stack.
4. Terminate running pilot jobs; they still trust the old CA. The controller
   resubmits them with the new config.
5. Destroy the old CA key.

Rotation at the CA's 10-year expiry follows the same steps.
