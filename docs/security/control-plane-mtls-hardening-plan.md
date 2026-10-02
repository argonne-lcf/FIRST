# Plan: pilot mTLS hardening (F-01)

Implements [control-plane-mtls-response.md](control-plane-mtls-response.md).
Goal: the pilot authorizes callers by certificate subject, each service holds
exactly one leaf key, and no process on the gateway host holds a CA key. The
change should leave the code smaller than it is today.

## Identities

Defined once in `first_common` and imported by both the certmanager CLI and the
pilot's NGINX template:

| Subject | EKU | Holder | Role |
|---|---|---|---|
| `CN=first-control` | clientAuth | `controller-manager` | control |
| `CN=first-router` | clientAuth | `inference-gateway` (API server) | router |
| `CN=first-metrics` | clientAuth | `prometheus` | metrics |
| `CN=first-pilot` | serverAuth | pilot NGINX, one per cluster | — |

Only the server certificate carries `DNS:first-pilot.internal`, which Prometheus
uses as `server_name`.

## 1. Certificate tooling (`first_gateway.services.certmanager`)

- `_issue_leaf_pem`: add `subjectAltName` only when `eku == "serverAuth"`.
- The CLI stays the offline issuing tool. Add one command that issues the
  standard set (CA if absent, `first-pilot` server, and the three clients) into
  a directory, using the identity constants.
- `make pki`: runs that command into `pki/` for local development.
- The library functions remain in use by the CLI and test fixtures only; no
  service code imports them after this change.

## 2. Gateway: load leaf certificates from files

**Settings** (`first_gateway/settings.py`)

- Remove `pilot_ca_key` and `pilot_ca_crt`.
- Add `pilot_ca_crt_file`, `pilot_client_crt_file`, and `pilot_client_key_file`
  as `Path`s, defaulting to `/run/secrets/pilot_ca.crt`,
  `/run/secrets/pilot_client.crt` and `/run/secrets/pilot_client.key`.
- The same setting names are used in every service. Compose decides which
  identity sits behind the path, so the code has no notion of roles.

**One SSL-context helper**

- Add `pilot_ssl_context(settings) -> SSLContext`: CA from file,
  `check_hostname = False` (with a comment giving the scheduler-placement
  rationale), and `load_cert_chain` from the two files. No temp files and no
  `openssl` subprocesses.
- `BackendClientManager._get_ssl_context` and `PilotControlClient.__init__`
  both call it.
- Drop the `cn=` parameter from `PilotControlClient` and its six call sites
  (`catalog.py`, `replica_launcher`, `pilot_job_observer`,
  `pilot_replica_observer`, `replica_drainer`). The identity now comes from the
  mounted certificate.
- `catalog.py`'s logs route currently mints a certificate per request. It
  should reuse a process-wide client or context instead.

## 3. Gateway: one pilot submission path (`services/pilot_submitter.py`)

- Remove the `ca_crt`/`ca_key` constructor arguments and the per-job
  `generate_server_cert` branch. Also remove `_BlockStringDumper`, the YAML
  rendering, and the `put_file` of the config.
- `PilotConfig.pilot_config_path` becomes required, so every cluster runs from a
  pre-staged runtime config, as Tara already does.
- Build the `PILOT_*` env overrides and script body once for every adapter.
  Only delivery differs: inline `script` for GraphQL-PBS, and `put_file` plus
  `script_path` for Globus Compute, which requires a path.
- Update `PilotSubmitter` construction in `pilot_job_controller.py` and
  `pilot_job_observer.py`.

## 4. Pilot: identity-based authorization (`first_pilot/nginx_manager.py`)

Add the following to the `http` block, generated from the identity constants:

```nginx
map $ssl_client_s_dn $role {
    "CN=first-control"  control;
    "CN=first-router"   router;
    "CN=first-metrics"  metrics;
    default             none;
}
map "$role:$request_method:$uri" $authorized {
    default                                   0;
    "~^control:"                              1;
    "~^router:[A-Z]+:/replicas/"              1;
    "~^router:GET:/control/logs/"             1;
    "~^metrics:GET:/replicas/.+/metrics$"     1;
}
```

- In the `server` block, add `if ($authorized = 0) { return 403; }`. This is a
  server-level default deny, so future locations inherit it.
- Keep `ssl_verify_client on`, `ssl_verify_depth 1`, and the per-location IP
  allow-lists unchanged.
- Match on `$uri` (normalized), never on `$request_uri`.
- Pilot deployments must expose Prometheus metrics at `/metrics`. Every current
  spec already does; document this next to `prometheus_metrics_path`.

**Expiry check at startup**

- Add `walltime_min` to `PilotRuntimeConfig` and `_JOB_ENV_OVERRIDE_FIELDS`,
  and pass it from the submitter.
- The pilot refuses to start, with a clear error, if the server certificate
  expires before `now + walltime`.

## 5. Deployment

**`deploy/compose.yaml`**

- Declare top-level `secrets:` with `file:` sources under `../pki/`.
- Map them per service to the same in-container names: `pilot_ca.crt`,
  `pilot_client.crt`, and `pilot_client.key`.
  - `inference-gateway` gets the router pair.
  - `controller-manager` gets the control pair.
  - `prometheus` gets the metrics pair.
- Replace Prometheus's `pki` bind mounts with secrets, and update the paths in
  `prometheus.yml`.
- `compose.prod.yaml` needs no change if prod keeps `pki/` at the same relative
  location. Otherwise, override the secret `file:` paths there.
- Remove `FIRST_PILOT_CA_KEY` and `FIRST_PILOT_CA_CRT` from `.env.secret` on
  every host.

**Expiry alert**

- Add one `health_alerter` check that parses the mounted client certificate and
  alerts when it expires in less than 30 days.
- Both the API server and controller mount theirs; the controller-side check
  covers the control certificate. Router and metrics certificates are issued
  together with it, so they expire together.

## 6. Tests

- **Fixture** (`tests/fixtures/`): an in-process throwaway CA that issues the
  full identity set into `tmp_path`. It also provides one extra client
  certificate with an unknown CN.
- **`LocalSchedulerAdapter`**: stage a pre-baked runtime config. Read
  `PILOT_CONFIG_FILE` from the submitted environment instead of deriving the
  config path from `script_path`.
- **Unit test**: the rendered NGINX config contains `ssl_verify_client on`, both
  maps, and the server-level deny.
- **Integration test** (`test_pilot_integration.py`): a real pilot NGINX,
  asserting the matrix below. Expected results:

  | Caller | Request | Expected |
  |---|---|---|
  | control | start / stop / status / logs / replica | allowed |
  | router | replica inference | allowed |
  | router | `GET /control/logs/…` | allowed |
  | router | `/control/start-replica`, `/control/stop-replica/…`, `/control/status` | 403 |
  | router | `/replicas/x/../../control/start-replica`, `//control/...` | 403 |
  | metrics | `GET /replicas/<r>/metrics` | allowed |
  | metrics | `POST /replicas/<r>/v1/chat/completions`, any `/control/` | 403 |
  | unknown CN | anything | 403 |
  | server cert as client | anything | 400 (NGINX certificate verification error) |

- Update `test_pilot_submitter.py`, `test_pilot_job_controller.py`, and
  `test_pilot_job_observer.py` for the constructor and settings changes.
- Verify with `make mypy format lint test`.

## 7. Documentation

- `docs/packages/certmanager.md`: rewrite as the offline issuing and rotation
  runbook covering:
  - issuing the standard set,
  - where each file goes (gateway `pki/` and each cluster's pre-baked config),
  - renewal,
  - CA rotation after a suspected leak.
- `docs/architecture/pilot-system.md` (line 82 and around line 130), plus
  `docs/packages/pilot.md` and `docs/getting-started/developer.md`: replace
  dynamic issuance with pre-staged certificates, and state that `/control/`
  authorizes by subject. Include the role table.

## Rollout

The system is pre-production, so configuration is re-rolled in one step:
suspend, reconfigure, restart. Pilots are ephemeral and the controller
recreates them.

1. Merge sections 1–7.
2. Offline, generate the CA and the standard set for each environment
   (`dev`, and later `prod`). Keep CA keys off every server.
3. Stage the runtime config on Tara and Sophia with that environment's CA and
   `first-pilot` server certificate. Point each cluster's `PilotConfig` at it via
   `pilot_config_path`.
4. Deploy the gateway with the new `pki/` secrets and the pilot package
   version. Remove the CA values from `.env.secret`.
5. Terminate running pilot jobs. The controller resubmits them with the new
   config.
6. Verify end to end:
   - inference through the API server,
   - replica logs through the API server,
   - Prometheus targets are up,
   - a router-certificate `curl` to `/control/status` returns 403.
