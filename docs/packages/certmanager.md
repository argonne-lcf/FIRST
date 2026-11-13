# Certificate Manager

`pilot-certmanager` is a small [Typer](https://typer.tiangolo.com/) CLI, shipped
with `first-gateway`, that wraps `openssl` to run the private CA for the
gateway ⇄ pilot mTLS and analytics Grafana ⇄ Prometheus TLS links. It is an
on a trusted host, and no running service imports it or holds a CA key.

This page is the issuing and renewal runbook. For why the model looks like
this, see the [F-01 response](../security/control-plane-mtls-response.md).


## Identities

Every environment (dev, prod) has its own CA, one pilot server certificate per
pilot cluster, one Prometheus HTTPS server certificate, and one fixed set of
client certificates. The pilot subjects and names come from
`first_common.schema.pilot` (`PILOT_SERVER_CN`, `pilot_server_name`,
`PilotClientRole`); Prometheus identity constants live in the certmanager:

| Subject | EKU | SAN | Holder | Pilot role |
|---|---|---|---|---|
| `CN=first-pilot` | serverAuth | `DNS:<cluster>.first-pilot.internal` | pilot NGINX on `<cluster>` | — |
| `CN=first-prometheus` | serverAuth | `DNS:prometheus`, `DNS:localhost`, `IP:127.0.0.1`, plus analytics addresses | Prometheus HTTPS API/UI | — |
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

Compute nodes have no DNS names, and the node a pilot lands on is chosen by the
scheduler after the certificate must already exist, so clients dial pilots by
IP. They verify the pilot by its **cluster's name** instead of its address:
every gateway client verifies `<cluster>.first-pilot.internal` as the TLS
hostname (`pilot_ssl_context(settings, cluster)` pins it on every handshake),
and Prometheus sets it as `server_name` in one scrape job per pilot cluster. The
name never has to resolve. A server certificate staged on one cluster is
therefore rejected by clients of every other cluster, although all chain to the
same CA.

The cluster name is part of that TLS name, so a Cluster with a `pilot_system`
must be named by a DNS label (`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`);
`plan` rejects any other.


## Issuing the standard set

Run on a **trusted admin host**, never on the gateway or a cluster. Requires
OpenSSL 3.x. Keys are EC P-256, certificates SHA-256, and private keys are
unencrypted (services start unattended).

```bash
# CA (if absent) + server identities + the three client certificates
pilot-certmanager standard --dir pki-prod/ --ca-name "FIRST Pilot CA (prod)" \
    --cluster tara --cluster sophia \
    --prometheus-dns data-portal-dev-vmw-01.cels.anl.gov
```

The command creates `ca.{key,crt}` only if the directory has no `ca.crt`, then
(re)issues `pilot-server-<cluster>.{crt,key}` for each `--cluster` (at least
one is required), `first-prometheus.{crt,key}`, and the three client identities.
Prometheus always includes `DNS:prometheus`, `DNS:localhost` and `IP:127.0.0.1`;
repeat `--prometheus-dns` / `--prometheus-ip` to add its analytics addresses.
`--days` (default 365) sets leaf validity. For local development, `make pki`
uses `PILOT_CLUSTERS` (default `tara sophia`), `PROMETHEUS_DNS`, and
`PROMETHEUS_IP` in the repo's `.gitignore`d `pki/`.

To issue one cluster's server certificate alone:

```bash
pilot-certmanager pilot-server tara --dir pki-prod/ [--days 365]
```

To issue only the Prometheus HTTPS pair, including its analytics FQDN:

```bash
pilot-certmanager prometheus-server --dir pki-prod/ \
    --dns data-portal-dev-vmw-01.cels.anl.gov [--days 365]
```

The lower-level `ca` and `client <cn>` commands remain for one-off issuing;
see `pilot-certmanager --help`.


## Where each file goes

| File | Secret | Destination |
|---|---|---|
| `ca.key` | **yes** | Admin host / offline storage only. Never deployed. |
| `ca.crt` | no | Gateway `pki/` and every cluster's runtime config |
| `pilot-server-<cluster>.{crt,key}` | key | That cluster's runtime config only |
| `first-control.{crt,key}` | key | Gateway `pki/` → `controller-manager` |
| `first-router.{crt,key}` | key | Gateway `pki/` → `inference-gateway` |
| `first-metrics.{crt,key}` | key | Gateway `pki/` → `prometheus` |
| `first-prometheus.{crt,key}` | key | Gateway `pki/` → `prometheus` HTTPS server |

### Gateway host

Copy `ca.crt`, the three client pairs, and the `first-prometheus` pair into `pki/`
at the repo root (`chmod 600 *.key`; on Linux also match the key owners to the
container users, see [Docker](../deployment/docker.md#filesystem-dependencies)).
`deploy/compose.yaml` declares them as Docker secrets and
mounts a **different identity behind the same names** in each service:

| Service | `/run/secrets/pilot_client.{crt,key}` |
|---|---|
| `inference-gateway` | `first-router` |
| `controller-manager` | `first-control` |
| `prometheus` | `first-metrics` |

The three gateway services above also get `/run/secrets/pilot_ca.crt`. The gateway
reads these through `pilot_ca_crt_file`, `pilot_client_crt_file` and
`pilot_client_key_file` (env `FIRST_PILOT_CA_CRT_FILE`,
`FIRST_PILOT_CLIENT_CRT_FILE`, `FIRST_PILOT_CLIENT_KEY_FILE`), which default to
those paths, so code never knows which role it holds. No CA material belongs in
`.env.secret`.

Prometheus also receives `first-prometheus` as
`/run/secrets/prometheus_server.{crt,key}`. Copy the **public** `ca.crt` for each
environment into FIRST-analytics as documented there; never copy `ca.key`.
Analytics Grafana validates the chain and the analytics FQDN, while the internal
log-shipping health check validates the `prometheus` SAN against the same CA.

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
network_interfaces: [hsn0]
```

These four are the only fields the file must carry. The submitter supplies
every other required field per job as `PILOT_*` environment overrides, taken
from the cluster's `PilotConfig` and the job (`PILOT_JOB_NAME`, `PILOT_CLUSTER`,
`PILOT_EXTERNAL_PORT`, `PILOT_NGINX_PATH`, `PILOT_IP_ALLOWLIST`,
`PILOT_WORKDIR`, `PILOT_NODE_FILE_ENV`, `PILOT_GPU_DISCOVERY`,
`PILOT_NUM_NODES`, `PILOT_GPUS_PER_NODE`, `PILOT_WALLTIME_MIN`); those override
any value in the file. `network_interfaces` (see the [pilot docs](pilot.md)) is
not overridable and can only be set in the file. Make the file readable only by
the pilot service account (`chmod 600`).

Stage each cluster's **own** `pilot-server-<cluster>` pair. A pilot refuses to
start if its server certificate does not name `PILOT_CLUSTER`, and the gateway
would reject it anyway.


## Renewal

Leaf certificates default to 1 year; the CA to 10. Renewal re-issues leaves
against the **same CA**, so nothing else needs to change:

1. Re-run `pilot-certmanager standard --cluster …` (every pilot cluster) against
   the environment's CA directory, repeating the same `--prometheus-dns` and
   `--prometheus-ip` options so the analytics SANs are preserved.
2. Replace the client pairs and `first-prometheus` pair in the gateway's `pki/`
   and restart the stack (`docker compose up -d --force-recreate`).
3. Replace `server_crt`/`server_key` in each cluster's runtime config with
   that cluster's `pilot-server-<cluster>` pair. Running
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


## Adding a pilot cluster

1. Name the Cluster with a DNS label (see [Identities](#identities)).
2. `pilot-certmanager pilot-server <cluster>` against the environment's CA, and
   stage it in the new cluster's runtime config.
3. Add a `pilot-<cluster>` scrape job to `deploy/prometheus.yml` (copy an
   existing one; set `pilot_cluster=<cluster>` and
   `server_name: <cluster>.first-pilot.internal`). Without it, the cluster's
   replicas are not scraped. The gateway needs no change.


## CA rotation (suspected leak)

There is no CRL. If any private key may have leaked, replace the CA and the
whole standard set for that environment:

1. Run `pilot-certmanager standard` into an empty directory; a new CA is
   created because none is present.
2. Stage the new `ca.crt` and each cluster's `pilot-server-<cluster>` pair in
   that cluster's runtime config.
3. Replace the gateway's `pki/` contents and recreate the stack.
4. Terminate running pilot jobs; they still trust the old CA. The controller
   resubmits them with the new config.
5. Destroy the old CA key.

Rotation at the CA's 10-year expiry follows the same steps.

After rotating either environment's CA, stage its new public `ca.crt` in
FIRST-analytics before validating dashboards again.
