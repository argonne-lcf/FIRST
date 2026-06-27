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
authenticates the gateway's client cert, and reverse-proxies to either the
control API or to a replica's local HTTP port.


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

This invokes `first_pilot.control_api:entrypoint`, which loads the YAML
config and runs the FastAPI app under uvicorn on
`127.0.0.1:control_port_internal`. NGINX, started by the app's lifespan,
is what listens on `external_port` and reverse-proxies inbound mTLS
traffic to it.


## Configuration

`PilotRuntimeConfig` (defined in
`first_common.schema.pilot:PilotRuntimeConfig`) is the on-disk YAML
contract between the gateway and the pilot. It is loaded once at startup
from the path in `$PILOT_CONFIG_FILE`. The control plane (not the
cluster) renders this config at **job submission time** and stages it
into the allocation's working directory:

| Field | Meaning |
|---|---|
| `ca_crt` | Root-CA PEM (inline string) the pilot trusts for incoming mTLS clients |
| `server_crt`, `server_key` | Server cert + key PEMs (inline strings), JIT-issued so the cert's lifetime tracks the job's max walltime. |
| `external_port` | Single externally-exposed TCP port. NGINX listens here; control API and replicas live on `+1`, `+2…` internally |
| `nginx_path` | Absolute path to the `nginx` binary on the compute node |
| `ip_allowlist` | NGINX `allow` ACL — typically the gateway's egress range |
| `workdir` | Rendezvous directory: pidfiles, ready-file, replica workdirs, nginx tmp |
| `node_file_env` | Name of the env var (e.g. `PBS_NODEFILE`) that holds the scheduler's host list |
| `job_name` | Unique pilot job name, used in file naming and the ready-file |

Because everything except the **root CA** is rendered per-job, admins do
not need to maintain pilot config files on the HPC cluster. Server certs
are ephemeral and re-issued for every submission via
`first_gateway.certmanager.generate_server_cert` (called from
`PilotSubmitter.submit`); see the [Certificate Manager](certmanager.md) docs.


## Subsystems

### NGINX manager — `nginx_manager.py`

Boots a private `nginx` master from a rendered config in a per-job tmpdir,
exposes `external_port` over TLS, and `SIGHUP`-reloads when the set of
replicas changes. Two location classes:

* `/control` → control plane API (`127.0.0.1:external_port + 1`)
* `/replicas/{name}/` → that replica's local port (`external_port + 2 + i`)

NGINX is also what enforces the IP allowlist and the gateway's
mTLS client-cert requirement (`ssl_verify_client on`).

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
`http://127.0.0.1:<port><health_path>` every 0.4 s. Phase transitions:
`launching → ready` on first healthy hit (or `start_timeout` after
`max_startup_time`), `ready → unhealthy` after 10 consecutive failed
checks, recovery back to `ready` on success, and `error` if the process
exits with nonzero status. Termination escalates SIGTERM → 8 s grace →
SIGKILL across the whole process group.

### Control plane API — `control_api.py`

FastAPI app, served on `127.0.0.1:external_port + 1`, reachable through NGINX
via `https://<job-ip>:<external_port>/control/`.

| Endpoint | Purpose |
|---|---|
| `POST /start-replica` | Place a `ReplicaStartRequest` (name + `PilotLaunchSpec` + requested GPUs); fails fast on GPU conflict |
| `POST /stop-replica/{name}` | Terminate the replica subprocess, free its GPUs, drop its nginx route |
| `GET  /status` | List `ReplicaInfo` and node status |
| `GET  /logs/{name}` | On-demand tail (~200 lines) of `stdout`, `stderr`, and the user log file. Not scraped on an interval — admins pull when needed |

Resource bookkeeping is **mirrored**: the pilot rejects local conflicts,
and the gateway's placement controller tracks the same inventory upstream
so it doesn't try to place two replicas on the same GPU in the first
place.

### Service discovery

On startup the pilot writes `<workdir>/readyfiles/<job_name>.ready.json`
containing the control URL that will be used to reach the pilot from the
gateway.  The gateway watches the filesystem for that file to learn the
job's control host/port.


## Lifecycle

1. Scheduler runs the submission script. The script `uvx`-launches the
   pilot with a freshly-rendered config + freshly-issued certs in the
   rendezvous dir.
2. Pilot starts NGINX, waits for it to bind `external_port`, writes the
   ready-file.
3. Gateway reads the ready-file, opens an mTLS client to
   `https://<ip>:<external_port>/control/`, and starts placing replicas.
4. Replicas come up; nginx is reloaded as each replica reaches `ready`;
   the gateway proxies user traffic to
   `https://<ip>:<external_port>/replicas/<name>/`.
5. On shutdown (or job-end signal) the pilot stops every replica, then
   stops NGINX.
