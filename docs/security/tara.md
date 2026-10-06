# Tara Inference Service Integration

This document describes the security architecture for integrating the **Tara**
cluster as a backend for the ALCF Inference Service.
The design reuses several mechanisms already vetted and in production for
the Inference Service and the IRI API project.

![Tara Architecture](./TaraArch-Bridge.drawio.png)

## 1. Overview

Like Minerva, Tara is a dedicated inference-service cluster. Unlike Minerva,  where the
gateway proxies to a *statically configured* login-node NGINX and does not
participate in model startup - Tara is driven by the Inference Service's **V2
control plane**, which launches and scales model replicas inside PBS **pilot
jobs** and publishes the resulting backends into the existing, internet-facing
**V1 API server** through a one-way **bridge**.


Two independent planes make up the integration, with very different
availability and exposure profiles:

| Plane | Path | Exposure | Auth |
| ----- | ---- | -------- | ---- |
| **Data plane** (inference) | Client → CloudFlare → CELS NGINX → V1 API server → Tara pilot NGINX → vLLM | Public internet (via the vetted V1 API server) | Globus Auth + per-endpoint Globus-group gating; mTLS to backend with the `first-router` client identity |
| **Control plane** (model lifecycle) | V2 controller → PBS GraphQL API (job launch) and → pilot NGINX `/control/` (replica start/stop) | **Internal only** - Inference Service CELS VMs, firewalled | Keycloak (Tara realm) token for job launch; mTLS with the `first-control` client identity for replica control |

As always, external users can only interact with the existing V1 API server,
which enforces uniform Globus authentication and authorization for
every model backend.  The compute nodes of Tara shall only be reachable from the Inference Service VMs over mutually authenticated TLS.

Backend access is enforced with **three layers** at the pilot NGINX:

1. **Mutually authenticated TLS.** The client must present a certificate signed
   by the environment's CA, and the pilot presents a `serverAuth` certificate
   from the same CA. A request without a valid client certificate is never
   proxied.
2. **Authorization by certificate identity.** mTLS alone only establishes that
   a caller holds *a* certificate from the CA. The pilot therefore maps the
   client certificate subject to a role (`control`, `router`, or `metrics`) and
   checks every request against one deny-by-default policy table. The
   credential that serves inference cannot start or stop replicas, and the
   monitoring credential can scrape metrics and nothing else.
3. **IP allow-listing.** NGINX applies a default `deny all` policy with
   exceptions only for the Inference Service Gateway VM IP address(es), so even
   a client holding a valid certificate is rejected unless it originates from an
   approved source.

All three must pass before a request reaches any backend service; any one
failing rejects the request at the NGINX layer. These layers are described in
detail in Sections 5 and 6.

The control plane that launches jobs is fully decoupled from the data plane services and never exposed outside of the Inference VM host.


Today, Tara access is gated to an internal developer Globus group and reachable
only from the firewalled Inference Service Dev VM.

The purpose of this review is to enable
Tara for general inference through the **existing, production V1 API server**.

The V2 Pilot Deployment system replaces static configuration (where
`https://minerva-login-01.alcf.anl.gov:9443` is manually written into v1
PostgreSQL) with dynamic model placement and service discovery (admins configure
desired models; control plane launches replicas, monitors health, recovers from
outages, and registers healthy IPs dynamically with the gateway).  Tara is
positioned as the first adopter of this control plane because it removes any
single cluster node as a point of failure and is resilient to dynamic IP churn
even in the absence of DNS.

## 2. Data plane

The front of the data plane is **unchanged from production**: external traffic
arrives through the CloudFlare CDN to a CELS-managed NGINX, which forwards to
the gateway VM's NGINX. That NGINX terminates SSL and reverse-proxies to
gunicorn on `localhost:7000` running the V1 Inference API server. User
authentication and authorization (Globus Auth, per-endpoint
`allowed_globus_groups`) is handled identically for Tara as for every other
backend - the model backend is transparent to the auth layer.

For a Tara model, the V1 API server proxies the inference request to a Tara
pilot's NGINX over **HTTPS with mutual TLS**, using the `FirstV2Endpoint`
adapter. The gateway reads the target backend URLs from its `Endpoint` row and
presents its **client certificate** (`CN=first-router`) on every backend
connection to a Tara Compute node. That identity is authorized for the replica
inference paths and for reading replica logs; it cannot reach the replica
start/stop API (Section 6.1).

This transport mirrors the Minerva integration, which
also uses mTLS from the gateway into NGINX.
Inside each Tara allocation, the pilot NGINX is the **only** externally
reachable listener; it terminates mTLS and reverse-proxies to vLLM replicas
that are bound to **Unix domain sockets in temporary folders with 700 filesystem permissions**. This single listener is guarded by the three layers noted in Section 1 — mutually authenticated TLS, authorization by certificate identity, **and** an NGINX `deny all` policy that admits only the Gateway VM IP(s). There is no TCP port on
which the vLLM API server is directly reachable, not even from `localhost`.
This ensures that traffic _must_ be authenticated through the service account's NGINX and requests can therefore only be initiated from the trusted CELS gateway.

For this reason, unlike Minerva, HTTPS is not used in the Tara NGINX-to-vLLM connection. There is never a TCP socket involved in this connection; intercepting traffic would require access to the service user account's private files on the vLLM host itself.

NGINX does not forward an open path prefix to vLLM. Each replica is exposed
only at the exact paths its `Model` resource declares in `supported_endpoints`,
plus its Prometheus metrics path (Section 6.3). The rest of the vLLM API
surface (tokenizer, adapter load/unload, and so on) is not reachable through
the proxy.

## 3. Remote job submission: PBS GraphQL + Keycloak

Model replicas run inside PBS **pilot jobs**. The V2 controller submits,
queries, and terminates those jobs on Tara through a **PBS GraphQL API**
endpoint. This is the most security-relevant new surface, so it is described in
detail.

**Network isolation.** The PBS GraphQL endpoint sits behind the same internal
bridge server that already gates all the other GraphQL endpoints used in
production for the IRI API project (Polaris and Crux). Unlike IRI, remote job submission on Tara is **not** exposed through any user-facing API. Access to this
GraphQL API is restricted to the ALCF network.

**Token authentication.** The PBS GraphQL server requires an access token in
the request headers to authorize job submission and to determine which ALCF
user will own the job. As with the IRI API, all tokens sent to GraphQL are
minted by the ALCF Keycloak server. A **dedicated Tara Keycloak realm** has
been created specifically for this endpoint: only Keycloak tokens minted for
Tara are accepted. Tokens generated to submit jobs on other production systems
are rejected by Tara, and Tara tokens are rejected elsewhere. This confines the
blast radius of any single token to a single system. The ALCF PBS
administrator has confirmed that the Tara GraphQL endpoint accepts **only**
Tara-realm tokens; a token from any other realm, including the general ALCF
realm, is rejected.

**Service-account impersonation.** To run jobs as the `openinference_svc`
service account, the controller uses a **confidential Keycloak impersonation
client**, the same pattern already in production for the IRI API. The client
performs a two-step OAuth flow — client-credentials to obtain the service
account token, then RFC 8693 token exchange to obtain a token for the requested
subject — implemented in `keycloak_client.py` (`KeycloakServiceTokenAuth`),
with proactive refresh and single-retry re-authentication on a 401.

On Tara, the impersonation client is **configured to allow impersonation of
`openinference_svc` only**. The ALCF Keycloak administrator has confirmed that
`openinference_svc` is the only entry in the Tara realm's user list, so the
impersonation client cannot mint a token for any other user. This is a tighter
policy than a general impersonation client and is the intended steady state for
the integration.

**Submission authorization model.** Taken together, job submission to Tara is
closed to everyone but the service account by construction, not by a rule that
has to be maintained:

- there is no submit host; the PBS GraphQL endpoint is the only way work
  reaches the cluster,
- that endpoint accepts only Tara-realm tokens, and
- the Tara realm holds one user, `openinference_svc`.

**Impersonation client secret.** The Keycloak client secret is the submission
boundary: whoever holds it can submit a job script that runs as
`openinference_svc`. It is handled as follows:

- It is stored only on the Inference Service VMs in CELS, under the `webportal`
  service account, in the `.gitignore`d secrets environment file read by the
  controller.
- The secret was first delivered to two service admins as files in their home
  directories; those files have been deleted. Future delivery will use the
  ALCF Vault service.
- There is no scheduled rotation. We are not aware of an ALCF policy that sets
  a rotation interval, and a stable secret avoids service downtime. The secret
  is rotated on suspected compromise.

We accept this single secret as the intended design (Section 10).

**Job submission mechanics.** The GraphQL adapter (`graphql_pbs.py`) submits a
job by base64-encoding the inline pilot launch script (URL-safe base64,
preserving the script verbatim) and issuing a `createJob` mutation with the
requested walltime, node/task counts, GPUs per node, queue, accounting id, and
log paths. Every field is passed as a **GraphQL variable** (`$input:
JobInput!`); nothing is interpolated into the query text.
It reads job state and, for actively placed jobs, the head node's high-speed-network (HSN) IP via
`get_job_statuses`, and terminates jobs via `deleteJob`. Only these three
operations (submit, status, terminate) are used.

When a job begins running, the head node's HSN IPs are surfaced in the job
status query (the PBS `hsn_ips` resource) and associated to the pilot job. The
gateway makes mTLS HTTPS requests directly to the first address PBS reports.
Tara compute nodes have four HSN interfaces, numbered from zero: `hsn0`,
`hsn1`, `hsn2`, and `hsn3`. PBS does not report their addresses in interface
order, so the pilot NGINX on Tara listens on all four (Section 6), and never
on the wildcard address. These are the same interfaces the Conduit request
covers (Section 8). As everywhere the backend is reachable, that listener
enforces the three layers from Section 1.

The job name becomes part of the PBS log path, so resource names are restricted
to a strict pattern: every `/`-separated segment must be non-empty and start
with an alphanumeric character. `..`, `.`, and absolute paths are rejected, so
a name cannot traverse out of the pilot's working directory.

Overall, the job submission flow reuses the infrastructure and authentication
mechanisms in place for IRI, but with a substantially reduced exposure:

- unlike IRI, which can impersonate any ALCF user, the Inference Service client will only
submit jobs as `openinference_svc`
- unlike IRI, which provides an internet-facing API for users to submit arbitrary scripts, the
Inference Service control plane has no inbound connectivity and submits only the trusted, service-user-controlled `first-pilot`
application to the PBS GraphQL API.

The GraphQL integration also provides significant advantages over the Globus Compute integration with Sophia, since it
keeps all traffic internal to ALCF (no hop through AWS) and does not require the deployment of a Globus Compute Endpoint manager
process and Parsl HTEX Interchange process _per-model_ on a login node. This results in improved reliability and availablilty:
as long as the PBS endpoint is reachable, the system does not rely on any single Tara service node to remain available.

### 3.1 Pilot Job Configuration

All software, configuration, and weights used by the Tara inference backend are
stored under `/vast/draco/tara/projects/inference_service`,
which has access restricted to the owner service user `openinference_svc` and
group `inference_service` (`770` permissions).

The pilot job submitted to PBS runs the `first-pilot` Python entrypoint,
which reads its runtime configuration from a file with `600` permissions.  The service starts the Pilot Job's control FastAPI server, bound to a local unix domain socket in a temporary directory with `700` permissions.  This process then starts and manages the lifecycle of the NGINX server, placing itself behind the `/control/` API path and refreshing the proxy configuration as model replicas are started and stopped on the allocation.

**The pre-staged pilot configuration file.** The gateway has no filesystem
access to Tara and holds no CA key, so it cannot write trust material onto the
cluster. Instead, each cluster and environment has one pre-staged
`PilotRuntimeConfig` YAML file, referenced by path
(`PilotConfig.pilot_config_path`) from the version-controlled cluster manifest.
This file is deliberately a small, manually managed configuration file kept
outside version control:

- **What it carries.** Exactly four fields: the CA certificate (`ca_crt`), the
  pilot server certificate and key (`server_crt`, `server_key`), and the list
  of interfaces to bind (`network_interfaces`).
- **Who produces it.** An Inference Service admin issues the certificates
  offline with `pilot-certmanager` (Section 5) and writes the file by hand,
  once per cluster and per environment (dev, production).
- **Who can change it.** The file is owned by `openinference_svc` with `600`
  permissions, so only the service account (and the admins who can act as it)
  can read or modify it.
- **When it changes.** Only at certificate renewal or CA rotation.

Everything else the pilot needs (job name, external port, NGINX path, IP
allow-list, working directory, node/GPU counts, walltime) comes from the
version-controlled manifests and is passed per job as `PILOT_*` environment
assignments in the submitted script. Those assignments can override only an
explicit list of non-sensitive fields: the three certificate fields and
`network_interfaces` are **not** on that list, so a job cannot substitute its
own trust material or change where the listener binds, and key material never
appears on a command line.


## 4. The V2 → V1 bridge

In the upcoming V2 Inference Gateway, the control plane writes healthy model backend metadata into a Redis registry, and the API servers continually update an in-memory snapshot of that registry to forward requests to live models.  While the V2 gateway is in development, we will deploy a temporary,
lightweight, standalone **bridge** (`manage.py first_v2_bridge`): a sidecar service that lets the
V2-managed Tara backends appear in the production V1 API server without coupling
the two systems' state.  This is a thin, temporary adapter between the V2 control plane and V1 API server while the V2 API server is in development.

The bridge **polls V2's Redis** for its `router-cfg` blob and reconciles V1
`Endpoint` rows in PostgreSQL to match the set of healthy pilot backends.
It manages one row in the `Endpoint` table per Tara model
containing the healthy backend URLs.

Because the bridge is one-directional (V2 Redis → V1 Postgres) and only ever
publishes **healthy** backends, a failed or drained model simply stops being
advertised: the corresponding V1 `Endpoint` row is deleted on the next
reconcile and the V1 server stops routing to it. A control-plane or bridge outage does not interrupt in-flight inference — the last-reconciled rows keep serving until
the bridge next runs.

**Access control fails closed.** The router-config blob carries each model's
`allowed_groups` and `allowed_domains`, which the bridge writes into the V1
`Endpoint` row. The bridge's copy of the schema declares both fields as
**required, with no default**. If the V2 side ever renames, moves, or drops
them, parsing fails and the reconcile tick aborts without writing; the existing
rows stay as they were and the bridge retries on the next poll. A schema
mismatch cannot silently produce an empty allow-list, which V1
reads as "public". (An empty list remains the deliberate way to express a
public model, so V1's check is unchanged.)

**No new authorization dimensions in V1.** The bridge enforces Globus-group and
IdP-domain restrictions only. We will not add further access-control
dimensions to the V1 path: the bridge is a short-lived shim, and any new
authorization mechanism will land in the full V2 data plane, where the control
plane and API server share one schema by design. Until the bridge is retired,
no access-control dimension beyond groups and domains will be introduced on the
V2 side for bridged models.

**Redis transport.** The bridge reads the router-config blob from the V2 Redis
instance over a plaintext, unauthenticated connection. Like PostgreSQL, Redis
is published only on the VM's loopback interface (`127.0.0.1`). **The bridge
always runs on the same VM as the V2 Redis instance it reads**; a deployment
that places them on different hosts is not supported. The safety of this
connection rests on that loopback publishing: Redis is not reachable from any
other host, and no Conduit flow is requested for it.

## 5. Transport security: mTLS and PKI

All communication between the Inference Service VMs and Tara — both the
inference data plane and the replica-control plane — is protected by **mutual
TLS**. This ensures that only the Inference Service gateway can talk to a Tara
pilot, and that the gateway is talking to a genuine pilot.

- **Certificate authority.** A self-signed root CA signs all leaf certs
  (`pilot-certmanager`). The self-signed nature of the CA is carried as a
  residual risk (Section 10), to be addressed if a future scan identifies it.
- **One CA per environment.** Development and production use two entirely
  independent root CAs, so no certificate is valid across environments. Only
  the development CA exists today; the production CA and its certificates will
  be generated offline at first production deployment. Within an environment,
  the CA is shared by every cluster running under the pilot system. Today that
  is Tara alone: **Minerva has its own, distinct root CA beacuse it is not managed by the V2 control plane at this time**.
  Unifying Sophia and Minerva under the pilot system (and therefore under this
  CA) will be the subject of a follow-up assessment.
- **The CA private key is not on any service host.** The gateway no longer
  issues certificates at run time. An admin issues them offline with the
  `pilot-certmanager` CLI on their own workstation, where the CA key stays. No
  running service, on the gateway VMs or on Tara, holds a signing key.
- **One identity per role.** Each environment has a fixed set of four leaf
  certificates, each with a distinct subject:

  | Subject | Key usage | Holder | Role at the pilot |
  | ------- | --------- | ------ | ----------------- |
  | `CN=first-pilot` | `serverAuth` | Pilot NGINX on the cluster | (server) |
  | `CN=first-control` | `clientAuth` | V2 controller | `control` |
  | `CN=first-router` | `clientAuth` | API server (inference data plane) | `router` |
  | `CN=first-metrics` | `clientAuth` | Prometheus | `metrics` |

  Each component is given exactly one client key. In the V2 Compose stack the
  keys are mounted as Docker Compose secrets, so they do not appear in
  container metadata or environment variables.
- **Client and server certificates are not interchangeable.** Client and
  server leaves carry disjoint extended key usage. Pilot NGINX rejects a
  `serverAuth` certificate presented as a client certificate, so a pilot's
  server key cannot be used against another pilot's control API, and a client
  key cannot impersonate a pilot.
- **Server certs.** One server certificate per cluster and environment is
  shared by every pilot job on that cluster. The certificate and key live in
  the pre-staged pilot configuration file (Section 3.1) under
  `/vast/draco/tara/projects/inference_service`,
  owned by the `openinference_svc` service account with `600` permissions.
  Per-job certificate issuance is deliberately not used: it would require a
  signing key on the live gateway, which is the larger risk.  Distinct
  top-level subdirectories are used to separate development and production
  environments. While these environments exist under distinct root CAs, the
  server certificates on Tara are owned by the same service account. The
  corresponding client certificates are fully isolated on the gateway side:
  the development host only possesses client certificates for the development
  pilot server, and the production host is similarly restricted to the matching
  pilot environment on Tara.
- **Certificate lifetime.** Leaf certificates are issued for at most **365
  days**, which is also the `pilot-certmanager` default. Two checks surface
  expiry ahead of time: the controller raises an alert when its client
  certificate is within 30 days of expiry, and a pilot refuses to start if its
  server certificate would expire before the end of the job's walltime.
- **No revocation; full reissue instead.** There is no CRL or OCSP. If any
  private key is suspected to have leaked, the response is to replace the CA
  and the whole certificate set for that environment: a new CA, one server
  certificate per cluster, and three client certificates. The new files are
  staged on the gateway VM and on each cluster, running pilot jobs are
  terminated and resubmitted, and the old CA key is destroyed. The set is small
  and fixed, which keeps this practical.
- **Hostname verification.** The gateway's clients verify the pilot's
  certificate chain and key usage but not its hostname. Compute node DNS names
  are not visible to the gateway, which therefore dials them by IP address.
  Moreover, job hostnames are not known to the control plane at submission time; the
  node a job lands on is assigned by PBS later.
  The pilot is identified by holding a `serverAuth` certificate from the
  environment's CA, which only pilots hold. (Prometheus additionally checks the
  fixed name `first-pilot.internal`, which only the server certificate carries.)
- **Algorithm.** Certificates use **ECDSA** (elliptic-curve) P-256 keys.
- **TLS version.** The pilot NGINX is configured for **TLSv1.3 only**
  (`ssl_protocols TLSv1.3;`); TLSv1.2 and below are not offered.
- **Client verification.** The pilot NGINX enforces mandatory client-cert
  verification (`ssl_verify_client on; ssl_verify_depth 1;`) against the
  environment's CA, and then authorizes the request by the certificate's
  subject (Section 6.1).

The gateway's client keys live on the Inference Service VM and are stored under
the `webportal` service user account with `600` permissions. Section 9 lists
which host holds which credential.

## 6. On-node architecture and vLLM security posture

Inside each PBS allocation, the `first-pilot` agent brings up a single NGINX
terminator and supervises one or more vLLM **replicas**:

- **Single entrypoint per node.** The pilot opens **one** external port per
  compute node (mTLS-protected, above). Every reverse-proxied service binds to a local **Unix domain socket** in a temporary directory with `700` permissions.  This
  ensures that even other users located on the same node cannot bypass mTLS.
  NGINX reverse-proxies `/control/` to the FastAPI control endpoints and each
  replica's declared paths under `/replicas/{name}/` to the local vLLM instance.
- **Explicit listener binding.** NGINX does not listen on the wildcard
  address. The pilot configuration must name at least one interface
  (`network_interfaces`), and NGINX binds only the addresses of those
  interfaces plus loopback. On Tara the list is `hsn0`, `hsn1`, `hsn2`, and
  `hsn3`. An interface that is missing or down is skipped,
  and the pilot refuses to start if none resolves. The interface list is set
  only in the pre-staged configuration file (Section 3.1) and cannot be
  overridden per job.
- **Authorization by certificate identity.** Every request is checked against
  the role policy in Section 6.1 before it is proxied.
- **IP allow-listing at NGINX.** In addition, each proxied location is
  wrapped in an `allow <gateway-ip>; … deny all;` block, so only the Inference
  Service VMs (and loopback) can reach the control and replica routes even if
  they possessed a client cert. The control and replica locations share one
  allow-list; the separation between them is enforced by identity, not by
  network.
- **Service account.** Pilots, NGINX, and vLLM all run as the
  `openinference_svc` service account, isolating production services from user
  accounts (as on Sophia/Minerva).  All software and weights are stored under a
  directory accessible only to the owning service account and `inference_service` group.
- **Replica supervision.** The replica manager runs each vLLM as its own
  process group with a health-monitor thread; replicas that fail to start or go
  unhealthy are torn down locally and garbage-collected/replaced by the
  controller.

### 6.1 Authorization by certificate identity

Pilot NGINX maps the client certificate subject (`$ssl_client_s_dn`) to a role
and applies one policy table to every request at the server level:

| Role (client subject) | Permitted |
| --------------------- | --------- |
| `control` (`CN=first-control`) | All paths |
| `router` (`CN=first-router`) | `/replicas/…` (inference); `GET /control/logs/…` |
| `metrics` (`CN=first-metrics`) | `GET /replicas/…/metrics` only |
| Anything else | Nothing (`403`) |

A pilot deployment's metrics path must end in `/metrics`; a manifest that
declares any other path is rejected when it is applied.

- **Deny by default.** The check sits in the `server` block, not in individual
  locations, so a location added to the template later inherits it and cannot
  bypass authorization by omission.
- **Normalized paths.** Matching uses NGINX's normalized `$uri`, so
  dot-segments, duplicate slashes, and percent-encoding cannot move a request
  from a permitted prefix into `/control/`.
- **What this separates.** Only the controller can start or stop replicas, and
  therefore run a launch script. Compromise of the internet-facing API server
  yields the `router` identity: inference access and log reads, not control.
  The Prometheus credential permits scraping metrics and nothing else.

**No second credential behind NGINX.** We considered and declined adding
application-layer authentication to the control API (for example a bearer
token) and a per-replica vLLM `--api-key`:

- Every component that would hold such a credential already holds the
  matching client key, in the same process and under the same service account.
  The controller would hold both the control certificate and the control token;
  the API server would hold both the router certificate and the vLLM API key.
  A second credential would not separate any principal from any capability,
  while adding a secret to provision, rotate, and keep in sync on every pilot.
- A process that can reach the control API's or a replica's Unix socket
  directly already runs as the pilot's service account and has equivalent
  authority.

The concern behind a second layer is that a template edit or misconfiguration
could silently return the system to a single point of failure. We address that
with regression tests that run in CI:

- a unit test on the rendered NGINX configuration asserting that client
  verification and the policy table are present, and
- an integration test that starts a real pilot NGINX and asserts the full
  authorization matrix: every role, an unknown subject, a server certificate
  presented as a client certificate, and path-normalization bypass attempts.

### 6.2 Control-plane audit trail

The control plane has two hops. The first, a person changing a resource through
the gateway's admin API, was already recorded: every applied change writes a
`ConfigVersion` database row with the authenticated Globus identity
(`applied_by`), the time, and the full change. The second hop, controller to
pilot, is now recorded at the pilot:

- **Who called.** Every `/control/` request writes one JSON line to a dedicated
  audit log, including the client certificate subject NGINX authenticated, the
  source address, time, request, and status. Requests that are denied (by role,
  or by a failed certificate check) are recorded as well.
- **What ran.** The launch script each replica ran is kept as rendered
  (`serve.sh`, with its pre-stop and post-stop scripts) in the replica's
  working directory. Replica names are unique per launch, and if a name is ever
  reused with a different script the earlier file is renamed and kept rather
  than overwritten.
- **Retention.** Both are written under the pilot's working directory on the
  shared filesystem (`<workdir>/audit/` and `<workdir>/replicas/<name>/`), not
  the per-job temporary directory, so they outlive the allocation.

Joining the audit log with `ConfigVersion` by subject and time distinguishes a
replica start that came from an authorized resource change from one that did
not. Job submission itself is logged by PBS and the PBS GraphQL API, which are
facility services.

### 6.3 Replica path pinning

For each replica, NGINX renders one **exact-match** location per path the
replica's `Model` declares in `supported_endpoints` (for example
`/v1/chat/completions`), plus the deployment's Prometheus metrics path.
Anything else under `/replicas/` returns `404`. The upstream path is written in
full in the NGINX configuration, so no client-supplied path remainder is ever
forwarded to vLLM.

The exposed set is therefore **per model** and **derived** from the same
declaration the gateway routes on, so it cannot drift from what the gateway
actually calls. The replica name and each path are validated against a strict
character set at the schema boundary and again immediately before the NGINX
configuration is rendered, so neither can inject directives into the
configuration.

### 6.4 vLLM security posture

The following posture items mirror the vLLM security questions raised in the
Minerva review and are answered here pre-emptively:

- **Protected endpoints only:**  The Minerva deployment of NGINX on `minerva-login-01` necessitates a network connection to the vLLM server backends, which is secured with TLS and HTTP Authorization header-based access control (`vllm serve --api-key`). In contrast, the v2 Pilot system deployed on Tara enforces that vLLM replicas bind to a private local unix domain socket owned by the service user. This prevents access to the vLLM API from other network hosts or other user accounts on the same host. Therefore, vLLM on Tara is not configured with HTTP Authorization header-based access control. The `700` filesystem permissions on the UDS control *reachability*; what a caller that has cleared NGINX may *invoke* is controlled by certificate-identity authorization (Section 6.1) and per-model path pinning (Section 6.3). A per-replica API key would be held by the same process that holds the `router` certificate and so would add no separation.
- **No tool / MCP servers:** No tool or MCP servers are enabled. Tara is dedicated to serving models. Any future change will be evaluated with CELS Cybersecurity first.
- **No gRPC:**  The gRPC interface is not used.
- **Cache-directory security:**  The vLLM cache and model directories are owned by `openinference_svc` and protected by the `inference_service` Linux group with `770` permissions.
- **Dedicated nodes:**  The Tara nodes used by the service are dedicated to inference.
- **Distributed (multi-node) inference:** **In scope and required** — individual Tara nodes are too weak to host the larger models, so multi-node/model-parallel replicas (e.g. Pilot Job 3 in the diagram) are essential to the utility of the service. Tara is a locked-down, dedicated cluster: only trusted service admins can access it, the sole network entrypoint is the mTLS-protected, firewalled pilot NGINX, and inter-node traffic stays within a single PBS allocation on the cluster's high-speed network. This is the **same risk that was formally documented and accepted for Minerva**, and we request the same path here.

## 7. Distributed Inference

Section 6 covers the on-node posture for a single compute node. Larger models,
however, exceed the memory of any single Tara node and must be sharded across
several nodes within one PBS allocation. This section describes how those
multi-node replicas are launched and, more importantly, how the inter-node
traffic they generate is isolated.

![Tara Multinode Architecture](./ServiceArch-Tara-Multinode.drawio.png)


Within one PBS allocation, PALS starts a vLLM node worker on each compute node.
The head node's `NGINX 3` terminates mTLS and reverse-proxies to the local vLLM
API over a Unix domain socket (Section 6). Other nodes are headless, but still
have distributed/RPC TCP listeners. Inter-node model traffic uses the HSN:
native GPU collectives are protected by Slingshot VNI isolation, while
coordination TCP is protected by per-job firewall rules.

### 7.1 Launch mechanics

Cross-node process launch on Tara uses **Cray PALS/MPI**, but vLLM itself runs
with its **multiprocessing (`mp`) executor, not Ray**. The division of
responsibility is deliberate:

- **PALS/MPI** provides process startup and rank assignment. `mpiexec`
  launches one node-worker entrypoint on each allocated compute node.
  **MPI is not the model tensor-transport layer.**
- **vLLM (`--distributed-executor-backend mp`)** owns distributed inference
  and starts the engine/GPU worker processes.
- **NCCL** carries inter-node GPU collectives using the **AWS OFI NCCL plugin**
  over Slingshot / libfabric CXI (`NCCL_NET=OFI`, `FI_PROVIDER=cxi`).
  Inkling also uses its native intra-node Lamport collective path.

The current Inkling vLLM 0.29 recipe runs across **8 nodes × 4 GPUs/node**,
configured as:

- **Tensor parallel: 4** (the 4 GPUs within each node)
- **Data parallel: 8** (across the 8 nodes)
- **Pipeline parallel: 1**
- **Expert parallelism enabled**
- Rank 0 hosts the Unix-domain-socket API and is fronted by NGINX; all other
  node ranks run **headless**.

A simplified view of the launch:

```bash
# PALS launches one worker entrypoint on each allocated node
mpiexec ... model-node-worker.sh

# Each PALS rank then runs approximately:
FI_PROVIDER=cxi \
NCCL_NET=OFI \
NCCL_NET_PLUGIN=ofi \
OFI_NCCL_PROTOCOL=SENDRECV \
vllm serve "$WEIGHTS" \
  --served-model-name inkling-bf16 \
  --distributed-executor-backend mp \
  --nnodes 8 \
  --node-rank "$PALS_RANKID" \
  --master-addr "$MASTER_ADDR" \
  --master-port "$MASTER_PORT" \
  --tensor-parallel-size 4 \
  --pipeline-parallel-size 1 \
  --data-parallel-size 8 \
  --data-parallel-size-local 1 \
  --data-parallel-address "$MASTER_ADDR" \
  --data-parallel-rpc-port "$DATA_PORT" \
  --enable-expert-parallel
```

Rank 0 additionally receives `--uds "$UDS"` (so NGINX can reach it over the
Unix domain socket); every other node rank receives `--headless`. The actual
launch wrapper also activates the verified TCP port-confinement library
described in Section 7.3.

Model TCP consistently uses **`hsn0`**, not `hsn[1-3]` or `bond0`.
`MASTER_ADDR` is the head's `hsn0` address; `VLLM_HOST_IP` is the local
`hsn0` address. Gloo and NCCL TCP bootstrap select the same interface, and
the launcher verifies the HSN route to the master.
Nodes stay within one rack
(`x4818`, `x4819`, or `x4820`), not necessarily one chassis.

### 7.2 Secure operating boundary: Slingshot VNI isolation

The network isolation boundary is the **PBS job**. For native Slingshot/RDMA
traffic, PBS/PALS provisions the job's **Virtual Network Identifier (VNI)** and
CXI service. The NIC permits communication between authorized endpoints and
drops native packets with a non-matching VNI. This protects the NCCL/OFI
collectives; it does **not** establish isolation for ordinary TCP/IP over HSN.

Native OFI/CXI traffic is not encrypted by this deployment. Its protection
relies on site-managed fabric isolation rather than transport encryption.
See HPE's [VNI overview documentation](https://support.hpe.com/hpesc/public/docDisplay?docId=dp00006842en_us&page=operations/vni_overview.html)
and [CXI services documentation](https://support.hpe.com/hpesc/public/docDisplay?docId=dp00008089en_us&page=operations/cxi_services.html)
for more details.
The separate coordination-TCP control is described below.

### 7.3 Inter-node traffic paths

vLLM uses native NCCL/OFI collectives and **TCP** rendezvous, bootstrap, and
distributed/RPC coordination. Both use HSN, but their access controls differ.

**Per-job iptables rules are installed in the PBS prologue** on every
allocated node before model startup. For the assigned coordination ports,
new connections are accepted only from the **HSN addresses of nodes in that
same PBS allocation**, not the whole HSN subnet. **All** other remote sources are
dropped. Rules preserve local/loopback communication and legitimate established
replies. The **PBS epilogue removes the job-specific permissions after workload
cleanup**, preventing stale peer access when nodes are reassigned.

The listener contract assigns one slot to an instance across all its nodes;
headless workers share that slot:

| Instance slot | Optional HTTP API | Rendezvous / DP startup | Other internal TCP listeners |
| --- | --- | --- | --- |
| 0 | 8000 | 20000–20001 | 50000–50255 |
| 1 | 8001 | 20002–20003 | 50256–50511 |
| 2 | 8002 | 20004–20005 | 50512–50767 |
| 3 | 8003 | 20006–20007 | 50768–51023 |

Thus the coordination ranges are **20000–20007 and 50000–51023**, with each
instance confined to its assigned subset. Normal FIRST serving uses UDS,
so 8000–8003 need no remote HTTP allowance but are retained as an optional
fallback path. NCCL RAS remains **loopback-only on 28028**.

A checksum-verified C library, injected only at the vLLM engine-launch boundary
after PALS startup, confines native TCP listeners to this contract. Activation
is checked in engine/worker processes; invalid activation, unsupported required
socket options, and range exhaustion fail rather than widening the pool.
This library controls port selection; it is **not a security sandbox** and does
not prevent deliberate bypass. **The per-job firewall is the TCP security
boundary.**

Some native listeners bind wildcard addresses. The rules therefore protect
these ports on all ingress interfaces and are ordered so earlier broad
ACCEPT rules do not bypass them.

The complete traffic picture for a multi-node replica:

| Traffic | Path | Protection |
| ------- | ---- | ---------- |
| Production/Dev VM → Model (ingress) | HTTPS/mTLS to the head-node NGINX | mTLS; certificate-identity authorization; IP allow-listing (Sections 5, 6) |
| NGINX → vLLM API | Local Unix domain socket; no network TCP | Private service-owned runtime-directory/socket permissions (Section 6) |
| vLLM node-to-node coordination | TCP over `hsn0`, in the assigned port ranges | Per-job PBS prologue/epilogue firewall rules, restricted to allocated peer HSN addresses |
| NCCL inter-node collectives | OFI/CXI over Slingshot HSN (`hsn[0-3]`) | Job-scoped Slingshot VNI/CXI isolation |
| NCCL RAS | Loopback TCP 28028 | Loopback-only bind; no remote exception |

The internal TCP channels are not TLS-encrypted; they rely on the job-scoped
firewall, not VNI. These controls, an mTLS-protected single ingress path,
Slingshot VNIs, and per-job firewall rules, isolate the allocation from
outside-job traffic. The result is that every byte leaving a compute node
is protected through the above controlling measures.


## 8. Conduit requests

The following network conduits are required between the Inference Service VMs in
CELS and the Tara / bridge endpoints. All connections are outbound from the CELS
source VMs to the destinations below.

**Sources**

| IP | Host |
| -- | ---- |
| `140.221.33.155` | `inference-api-dev.cels.anl.gov` — the Dev VM in CELS (a genuine development host) |
| `140.221.33.25` | `data-portal-dev-vmw-01` — the **Production** VM in CELS (the `dev` in the name is legacy) |

**Destinations / Ports**

| Destination | Resolved IP / CIDR | Port |
| ----------- | ------------------ | ---- |
| `graphql-bridge-dev-01.lab.alcf.anl.gov` (a **production** host; the `dev` in the name is legacy) | `10.140.50.167` | 443 |
| All Tara compute nodes' `hsn*` interfaces (Tara South) | `10.124.128.0/21` (`10.124.128.0`, mask `255.255.248.0`) | 9443 |
| All Tara compute nodes' `hsn*` interfaces (Tara North) | `10.124.160.0/21` (`10.124.160.0`, mask `255.255.248.0`) | 9443 |

Port 443 to the GraphQL bridge carries the PBS job-submission control plane
(Section 3). Port 9443 to the Tara compute-node `hsn*` interfaces carries
**three** consumers of the pilot NGINX (Sections 2, 6), each with its own
client identity:

| Consumer | Client identity | Traffic |
| -------- | --------------- | ------- |
| API server | `first-router` | Inference requests to replicas |
| V2 controller | `first-control` | Replica start/stop and status on `/control/` |
| Prometheus | `first-metrics` | Metrics scrapes of each replica |

All three security layers from Section 1 (mutually authenticated TLS,
authorization by certificate identity, and NGINX IP allow-listing) apply to
every connection on this port.

The Dev VM keeps this access permanently: new features are developed and
released from it. It holds certificates from the development CA only, which
production pilots do not trust (Section 5).

All flows are initiated from CELS. The pilot has no outbound HTTP client and
never initiates a connection to the Inference Service VMs. No flow is requested
for Redis or PostgreSQL, which are local to the Inference Service VM.

The subnet-scoped destinations are appropriate because Tara is dedicated in its
entirety to the Inference Service and the two `/21` networks contain only Tara
compute-node interfaces. That dedication is a predicate of this request: if
Tara ever runs other workloads, the request must be revisited.

## 9. Accounts and credentials

### 9.1 Service accounts and the credentials they hold

| Host | Service account | Runs | Credentials held |
| ---- | --------------- | ---- | ---------------- |
| Dev Gateway VM (`inference-api-dev.cels.anl.gov`) | `webportal` | Gateway stack: API server, bridge, V2 controller, Prometheus | Tara Keycloak impersonation client secret; **dev** CA certificate and the three dev client certificate/key pairs (`first-router`, `first-control`, `first-metrics`) |
| Production Gateway VM (`data-portal-dev-vmw-01.cels.anl.gov`) | `webportal` | Gateway stack: API server, bridge, V2 controller, Prometheus | Tara Keycloak impersonation client secret; **production** CA certificate and the three production client certificate/key pairs (distinct CA from dev) |
| Tara (`north-asn-01.head.north.tara.alcf.anl.gov` and compute nodes, shared filesystem) | `openinference_svc` | Pilot, pilot NGINX, vLLM | Pilot server certificate/key pair for dev; pilot server certificate/key pair for production |
| Admin workstation (offline) | Inference Service admin | `pilot-certmanager` | Dev and production CA signing keys. Never placed on a service host. |

A client certificate acts at the pilot with the authority of its role, and
anything the `control` role starts runs as `openinference_svc`, regardless of
which account on the gateway VM holds the key.

### 9.2 Accounts on Tara

Job submission is closed to everyone but `openinference_svc` (Section 3), so
the accounts below concern access to the nodes, not the ability to submit work.
The ALCF Operations team has reduced the accounts on Tara to those that are
needed. HPE staff who were present only to bring the machine into service have
been removed. The accounts that remain are accepted:

| Group | Privilege | Who | Why retained |
| ----- | --------- | --- | ------------ |
| `tara_openinference_svc_sudo` | Can act as `openinference_svc` | Inference Service admins | Operate the service |
| `tara_infra_sudo`, `tara_storage_sudo`, `tara_aig_sudo` | Administrative | ALCF Operations | Operate the cluster, storage, and PBS |
| `tara_hpe_sudo` | Administrative | HPE Site Team and ALCF Operations | Ongoing on-site support of the machine |
| (no sudo) | Login only | ALCF Performance Engineering | Tara acceptance work |
| (no sudo) | Login only | ALCF AI/ML team | Help the Inference Service admins improve serving |

Access to Tara is granted by the facility's account process rather than by the
Inference Service, so this list is a point-in-time record; the audit will be
re-run at production enablement.

### 9.3 Globus admin group

Authorization for the gateway's admin API is a Globus Auth bearer token plus
membership of the Inference Service admin Globus group. The admin API can write
launch-script templates, which run on Tara as `openinference_svc`, so a Globus
identity is one factor in a path that ends at code execution on facility
compute. This is intended, and bounded as follows:

- The admin API is published on the gateway VM's loopback interface only
  (Section 9.4), so a Globus identity in the admin group cannot reach it
  without first having access to the VM.
- Membership of the Globus group is controlled entirely by the Inference
  Service admin team.
- The team will review the group's membership every six months.

We accept this dependency with these controls (Section 10).

### 9.4 Local services on the Inference Service VM

Four services on the VM are published on `127.0.0.1` only and are not proxied
by the host's front-end NGINX, so none is reachable from outside the VM:

| Service | Access control | Position |
| ------- | -------------- | -------- |
| Gateway admin (control) API | Loopback publishing **and** Globus Auth token with admin-group membership | Kept as is. Its safety depends on both; the loopback publishing lives in the Compose file and must be preserved by anyone editing it. |
| Prometheus | Loopback publishing | The admin API stays enabled in production: it is used to take periodic database snapshots for backup. |
| Grafana | Loopback publishing only (anonymous admin) | Temporary. It will be replaced by a unified Grafana instance, covering Prometheus and ClickHouse log analytics, that enforces Globus Auth SSO. Until then we accept loopback publishing as the only control. |
| Redis, PostgreSQL | Loopback publishing | Plaintext transport; see Section 4. |

## 10. Residual risks and accepted positions

The following are stated as decisions rather than left implicit.

- **Single job-submission secret.** The Keycloak impersonation client secret is
  the submission boundary (Section 3). Its compromise allows a job to be
  submitted as `openinference_svc`. This is the intended design; safeguarding
  the secret is the Inference Service team's responsibility.
- **Globus credential in the path to code execution.** Accepted, with the
  bounds and membership review in Section 9.3.
- **No second credential behind pilot NGINX.** Declined for the reasons in
  Section 6.1; regression tests guard the single boundary.
- **Static, shared server certificate.** One server certificate per cluster and
  environment, valid for up to 365 days and shared by all pilot jobs, in
  exchange for removing the CA key from every service host. Any pilot's server
  key could impersonate another pilot of the same environment to an on-path
  attacker, since hostname verification cannot be used. Server keys are held
  only on the cluster filesystem, readable by the service account.
- **No certificate revocation.** A leaked key is handled by reissuing the CA
  and the full certificate set (Section 5).
- **Shared IP allow-list.** The control and data planes use one port and one
  allow-list; they are separated by certificate identity.
- **Plaintext Redis and loopback-only Grafana.** Both rest on loopback
  publishing on the gateway VM (Sections 4, 9.4). Grafana is temporary.
- **Development VM access.** The Dev VM keeps permanent network access to the
  Tara compute range. With separate CAs per environment, its credentials are
  not accepted by production pilots.
- **Inter-node protection is external to the software.** vLLM's inter-node
  channels are unauthenticated and unencrypted; protection is provided by the
  per-job VNI and the PBS prologue/epilogue firewall rules (Section 7). A
  change to the coordination ports requires a matching change to those rules.
- **Cluster dedication.** The Conduit scope and the co-residency arguments in
  this document assume Tara remains dedicated to the Inference Service.
- **Cluster access is granted outside the project.** See Section 9.2.
- **Self-signed CA.** Trust derives from a locally managed CA rather than one
  issued through ALCF channels. No exemption is sought; to be addressed if a
  future scan identifies it.

## Appendix: Tara vs Minerva

| Aspect | Minerva | Tara |
| ------ | ------- | ---- |
| Model control | Custom PBS launcher + registry on the login node; gateway not involved | V2 controller launches/scales replicas in PBS **pilot jobs** via GraphQL |
| Backend registration | Static gateway config (`models.json`). New model additions require `manage.py loaddata` on Gateway VM. | **Bridge** reconciles healthy backends from V2 Redis into V1 `Endpoint` rows |
| Job submission | Manual tools invoke PBS from Minerva login node under `openinference_svc` user | **PBS GraphQL API** + dedicated **Tara Keycloak realm**, impersonating `openinference_svc` |
| Entrypoint | Login-node NGINX (`:9443`), mTLS | Per-job pilot NGINX (one port per head compute node), mTLS plus authorization by client certificate identity |
| PKI | Its own root CA | Separate root CA per environment (dev, production), not shared with Minerva; CA key kept offline |
| vLLM binding | HTTPS to `0.0.0.0:8000` + shared API key, firewalled | **HTTP over Unix domain sockets** in temporary directories with `700` permissions (no TCP port). Only each model's declared endpoints are proxied. |
| Data-plane transport | Direct HTTPS/mTLS from gateway | Direct HTTPS/mTLS from gateway |
| Multi-node inference | In scope, risk accepted | In scope, same risk-acceptance path requested |

## Appendix: Code References

- [Pilot Control API](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/pilot/first_pilot/control_api.py)
- [Pilot NGINX Manager](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/pilot/first_pilot/nginx_manager.py)
- [Pilot schemas: client roles, runtime config, replica start request](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/common/first_common/schema/pilot.py)
- [NGINX configuration unit tests](https://github.com/argonne-lcf/FIRST/blob/firstv2/tests/test_nginx_manager.py)
- [Pilot integration tests, including the authorization matrix](https://github.com/argonne-lcf/FIRST/blob/firstv2/tests/test_pilot_integration.py)
- [SSL Cert Manager](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/gateway/first_gateway/services/certmanager/__init__.py)
- [Certificate issuing, renewal, and CA rotation runbook](../packages/certmanager.md)
- [Gateway Compose stack (per-service client keys, loopback publishing)](https://github.com/argonne-lcf/FIRST/blob/firstv2/deploy/compose.yaml)
- [Tara Keycloak Client Auth Flow](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/gateway/first_gateway/services/keycloak_client.py)
- [GraphQL Scheduler Adapter](https://github.com/argonne-lcf/FIRST/blob/firstv2/packages/gateway/first_gateway/platforms/schedulers/graphql_pbs.py)
- [v2 -> v1 Bridge](https://github.com/argonne-lcf/FIRST/blob/main/resource_server_async/management/commands/first_v2_bridge.py)
- [Bridge deployment notes](https://github.com/argonne-lcf/FIRST/blob/main/deploy/first_v2_bridge.md)

## Appendix: Assessment actions

Where each action from the CELS Cybersecurity assessment (V1, 2026-09-22) is
addressed in this document.

| Action | Finding | Position | Section |
| ------ | ------- | -------- | ------- |
| A-01 | F-01 | Fixed: per-role identities, authorization by subject, CA key removed from services. Second credential declined; regression tests instead. | 5, 6.1 |
| A-02 | F-02 | Fixed: bridge fields required, fails closed. No new authorization dimensions in V1. | 4 |
| A-03 | F-03 | Fixed: replica name validated at the schema boundary and before rendering. | 6.3 |
| A-04 | F-05 | Fixed: explicit interface binding, required configuration. | 6 |
| A-05 | F-06 | Fixed: `createJob` uses GraphQL variables; names that become paths cannot contain `..` or empty segments. | 3 |
| A-06 | — | Confirmed and documented: Tara realm only, one user. | 3 |
| A-07 | — | Accounts reduced by Operations; remainder accepted. | 9.2 |
| A-08 | F-04 | Position stated: static certificate, 365 days, full reissue, configuration file ownership. | 3.1, 5 |
| A-09 | F-07 | Confirmed: separate CA per environment, not shared with Minerva. | 5 |
| A-10 | F-08 | Document and code aligned (interfaces, certificate lifetime, `/control/` protection). | 3, 5, 6.1 |
| A-11 | F-02 | Position stated: plaintext Redis, loopback only. | 4 |
| A-12 | F-09 | Fixed: per-model path pinning. Per-replica API key declined. | 6.1, 6.3 |
| A-13 | F-11 | Fixed: subject logged, scripts retained, logs outlive the job. | 6.2 |
| A-15 | F-10, F-08 | Section 7 describes the per-job firewall; Prometheus listed as a consumer of 9443. | 7, 8 |
| A-16 | F-01 | Documented. | 9.1 |
| A-17 | RR 6 | Accepted with controls. | 9.3 |
| A-18 | F-12 | Positions stated. | 9.4 |
