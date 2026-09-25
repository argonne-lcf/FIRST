# first-pilot

The **pilot** is the per-job agent that runs *inside* an HPC allocation and
exposes the GPUs on that allocation to the central Inference Gateway over
mTLS. One pilot process owns one scheduler job; the gateway treats each
running pilot as an ephemeral inference endpoint that can host one or more
model replicas.

```
                          mTLS
   Inference Gateway  ─────────────▶  NGINX (external_port)
       (client)                            │
                                           ├──▶ /control          → Control Plane API (external_port + 1)
                                           └──▶ /replicas/{name}/ → model replica   (external_port + 2 … N)
                                                                    (vLLM, SGLang, …)
```

The gateway never reaches a model replica directly. NGINX terminates TLS,
authenticates the caller's client cert, authorizes the request by the cert's
subject (see NGINX manager below), and
reverse-proxies to either the control API or to a replica's local HTTP port.


## System requirements

* `uv` on `PATH`
* `nginx` binary path available
* outbound HTTPS access (for `uvx` to fetch the locked package)
* `nvidia-smi` on `PATH` (if absent the pilot reports zero GPUs)


## Entrypoint

The pilot is meant to be launched as the body of an HPC scheduler script.
There is nothing to install on the cluster beyond `uv` and `nginx`:

```bash
PILOT_CONFIG_FILE=/path/to/config.yaml uvx first-pilot
```

This invokes `first_pilot.control_api:entrypoint`, which loads the YAML config
and runs the FastAPI app under uvicorn on a private Unix domain socket.  NGINX,
started by the app's lifespan, is what listens on `external_port` and
reverse-proxies inbound mTLS traffic to it.


## Configuration

`PilotRuntimeConfig` (defined in
`first_common.schema.pilot:PilotRuntimeConfig`) is the on-disk YAML
contract between the gateway and the pilot. It is loaded once at startup
from the path in `$PILOT_CONFIG_FILE`, then overlaid with any `PILOT_*`
environment variables. An admin pre-stages one such file per cluster
(`PilotConfig.pilot_config_path`), and the gateway's submitter passes the
per-job fields as `PILOT_*` overrides:

| Field | Meaning |
|---|---|
| `ca_crt` | Root-CA PEM (inline string) the pilot trusts for incoming mTLS clients |
| `server_crt`, `server_key` | `CN=first-pilot` server cert + key PEMs (inline strings), issued offline and shared by every pilot on the cluster |
| `walltime_min` | Job walltime (`PILOT_WALLTIME_MIN`). The pilot refuses to start if `server_crt` expires before `now + walltime_min` |
| `external_port` | Single externally-exposed TCP port. NGINX listens here; control API and replicas live on `+1`, `+2…` internally |
| `network_interfaces` | IPv4 interfaces (e.g. `[hsn0, hsn1, hsn2, hsn3]`) NGINX binds, in preference order, alongside `127.0.0.1`. Interfaces that are missing, down, or have no IPv4 address are skipped; at least one must resolve. The first resolved address is advertised as the pilot endpoint and in replica URLs. Under GraphQL PBS the gateway dials an unordered `hsn_ips[0]`, so list every HSN interface |
| `nginx_path` | Absolute path to the `nginx` binary on the compute node |
| `ip_allowlist` | NGINX `allow` ACL — typically the gateway's egress range |
| `workdir` | Rendezvous directory: pidfiles, ready-file, replica workdirs, nginx tmp, audit |
| `node_file_env` | Name of the env var (e.g. `PBS_NODEFILE`) that holds the scheduler's host list |
| `job_name` | Unique pilot job name, used in file naming and the ready-file |

Only `ca_crt`, `server_crt`, `server_key` and `network_interfaces` must live in the staged file;
the gateway supplies the other fields in the table per job through the
environment, overriding the file. `network_interfaces` can only be set in the
file. The gateway holds no CA key and never issues certificates.
See the [Certificate Manager](certmanager.md) runbook for issuing and renewal.


## Subsystems

### NGINX manager — `nginx_manager.py`

Boots a private `nginx` master from a rendered config in a per-job tmpdir,
exposes `external_port` over TLS, and `SIGHUP`-reloads when the set of
replicas changes. Two location classes:

* `/control` → control plane API (`127.0.0.1:external_port + 1`)
* `/replicas/{name}/` → that replica's local port (`external_port + 2 + i`)

NGINX is also what enforces the IP allowlist, the mTLS client-cert
requirement (`ssl_verify_client on`), and per-role authorization. A `map` on
`$ssl_client_s_dn` assigns each caller a role (the CNs are
`first_common.schema.pilot.PilotClientRole`), and a server-level rule returns
403 for any role/method/path combination not listed:

| Client CN | Role | Permitted |
|---|---|---|
| `first-control` | control | All paths |
| `first-router` | router | `/replicas/…`; `GET /control/logs/…` |
| `first-metrics` | metrics | `GET /replicas/…/metrics` only |
| anything else | none | Nothing (403) |

Matching uses the normalized `$uri`. Replicas must expose Prometheus metrics
at `/metrics` for the metrics role to scrape them. See the
[F-01 response](../security/control-plane-mtls-response.md) for rationale.

### Replica manager — `replica_manager.py`

`ReplicaManager` is the local placement controller. It self-discovers
GPUs by `ssh <host> nvidia-smi …` over the host list read from the
scheduler's node-file env var, then tracks the full `(host, gpu_id)`
inventory plus what's claimed vs free. The inventory query is cached
(`@ttl_cache(ttl=60)`); placement bookkeeping (`_replicas`, `_claimed`,
`_used_ports`) is guarded by a single asyncio lock. A pending placement
is held in `_replicas` as a `_RESERVED` sentinel between the validate
and construct steps so concurrent `start_replica` calls cannot race on
the same GPUs or port.

Each `Replica` owns the model subprocess (launched as a process group
via `start_new_session=True`) and a daemon thread that polls
`http://127.0.0.1:<port>/<health_check.health_url path>` every 0.4 s. State transitions:
`launching → ready` on first healthy hit (or `start_timeout` after
`max_startup_sec`), `ready → unhealthy` after 10 consecutive failed
checks, recovery back to `ready` on success, and `error` if the process
exits with nonzero status. Termination escalates SIGTERM → 8 s grace →
SIGKILL across the whole process group.

### Control plane API — `control_api.py`

FastAPI app, served on `127.0.0.1:external_port + 1`, reachable through NGINX
via `https://<job-ip>:<external_port>/control/`.

| Endpoint | Purpose |
|---|---|
| `POST /start-replica` | Place a `ReplicaStartRequest` (name + `ResolvedLaunchSpec` + requested GPUs); fails fast on GPU conflict |
| `POST /stop-replica/{name}` | Terminate the replica subprocess, free its GPUs, drop its nginx route |
| `GET  /status` | List `ReplicaInfo` and node status |
| `GET  /logs/{name}` | On-demand tail (~200 lines) of `stdout`, `stderr`, and the user log file. Not scraped on an interval — admins pull when needed |

Resource bookkeeping is **mirrored**: the pilot rejects local conflicts,
and the gateway's placement controller tracks the same inventory upstream
so it doesn't try to place two replicas on the same GPU in the first
place.

### Control-plane audit trail

These live under `workdir`, not the per-job tmpdir, so they outlive the allocation:

* **`/control/` requests** — one JSON line per request in
  `<workdir>/audit/<job_name>.control-access.jsonl`, including the mTLS subject
  NGINX authenticated (`$ssl_client_s_dn`). A request counts as a control
  request by its normalized path, and is recorded whatever its outcome:
  requests denied by role (403) or by a failed client-certificate check (400)
  are included.
* **All requests** — the full NGINX access log (`combined` format, control and
  data plane) in `<workdir>/audit/<job_name>.access.log`.
* **Replica scripts** — the rendered `serve.sh`, `pre-stop.sh` and
  `post-stop.sh` in `<workdir>/replicas/<name>/`. That directory is never
  removed, and replica names are unique per launch. If a name is nonetheless
  reused with a different script, the earlier file is kept as
  `<script>.<UTC time it was written>`.

Joining them by DN and timestamp — with the gateway's
`ConfigVersion.applied_by` — attributes a start to its caller and script; the
source address alone cannot, because every allowed host presents a CA-signed cert.

### Service discovery

On startup the pilot writes `<workdir>/readyfiles/<job_name>.ready.json`
containing the control URL that will be used to reach the pilot from the
gateway.  The gateway watches the filesystem for that file to learn the
job's control host/port.


## Lifecycle

1. Scheduler runs the submission script. The script `uvx`-launches the
   pilot with the cluster's pre-staged config plus per-job `PILOT_*`
   overrides. The pilot checks its server cert covers the walltime.
2. Pilot starts NGINX, waits for it to bind `external_port`, writes the
   ready-file.
3. Gateway reads the ready-file, opens an mTLS client (as `first-control`) to
   `https://<ip>:<external_port>/control/`, and starts placing replicas.
4. Replicas come up; nginx is reloaded as each replica reaches `ready`;
   the gateway proxies user traffic to
   `https://<ip>:<external_port>/replicas/<name>/`.
5. On shutdown (or job-end signal) the pilot stops every replica, then
   stops NGINX.
