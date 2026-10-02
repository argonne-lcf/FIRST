# Response to F-01: control-plane mTLS authenticates the CA, not the caller

**Disposition:** Accepted. Remediation planned in
[control-plane-mtls-hardening-plan.md](control-plane-mtls-hardening-plan.md).

We agree with the finding's conclusion: any holder of any CA-signed client key
that can reach a pilot's port can start replicas, and therefore run arbitrary
commands as the pilot's service account. We will close this by making the pilot
authorize on certificate identity, issuing a distinct identity per caller, and
removing the CA private key from the gateway host. We decline the proposed
application-layer second credential, and substitute regression tests that
address the concern it was meant to cover (see [Declined](#declined)).

## Assessment of the chain

Verified against `c6b3915` (no changes to this area since `a869eb4`).

| # | Claim | Assessment |
|---|---|---|
| 1 | NGINX verifies issuer, not subject | Confirmed |
| 2 | Client and server certificates are mutually interchangeable | **Not reproduced** — see below |
| 3 | Every leaf carries the same SAN | Confirmed |
| 4 | Hostname verification disabled on every client | Confirmed; rationale below |
| 5 | Control API has no authentication of its own | Confirmed |
| 6 | Control and data planes share one allow-list and credential | Confirmed |
| 7 | `/start-replica` executes caller-supplied bash | Confirmed |
| — | CA private key resident in the internet-facing API process | Confirmed (also in `controller-manager`) |
| — | Prometheus holds a control-capable credential | Confirmed |

**On (2).** Client and server leaves carry disjoint extended key usage
(`clientAuth` vs `serverAuth`), and OpenSSL enforces it by default on both
sides of the handshake. Using certificates from the project's own
`certmanager`, an `openssl s_server -Verify 1` handshake succeeds with the
client certificate and fails with the server certificate (`unsuitable
certificate purpose`, alert 43); `openssl verify -purpose` rejects the
converse. Pilot NGINX records the same verification failure but completes the
handshake and answers every request with `400 The SSL certificate error`, so
nothing reaches the control API or a replica; the integration test asserts
this. A pilot's server key therefore
cannot be used against another pilot's control API, and a client key cannot
impersonate a pilot. This narrows the finding without changing its conclusion:
**every client certificate** the CA has issued is control-capable, which is the
substance of (1) and (6).

**On (4), hostname verification.** Hostname verification is off because it
cannot be made meaningful in this environment, not only because of (3):

- Compute nodes are not reachable by DNS name; the gateway reaches pilots by
  the IP address the pilot advertises in its readyfile.
- Pilot NGINX instances start on a large and shifting range of compute hosts.
  The host a job lands on is chosen by the cluster scheduler *after*
  submission, whereas the server certificate must exist before the job starts.

A per-host name cannot be bound into a certificate that must be issued before
the host is known. After remediation, the peer's identity is established by
certificate subject and key usage rather than hostname: only pilots hold
`serverAuth` certificates, and each caller is authorized by its subject. The
residual gap is listed under [Accepted residual risk](#accepted-residual-risk).

## Remediation

Each item maps to one of the finding's "break the chain" options.

1. **Distinct identities.** One certificate per role, each with a fixed
   subject: `first-router` (inference data plane, held by the API server),
   `first-control` (held by the controller), `first-metrics` (held by
   Prometheus), and `first-pilot` (pilot NGINX server). Client certificates no
   longer carry the shared SAN.

2. **The pilot authorizes on identity.** Pilot NGINX maps `$ssl_client_s_dn` to
   a role and applies one policy table to every request, denying by default:

   | Role | Permitted |
   |---|---|
   | `control` | All paths |
   | `router` | `/replicas/…` (inference); `GET /control/logs/…` |
   | `metrics` | `GET /replicas/…/metrics` only |
   | anything else | Nothing (403) |

   Matching uses NGINX's normalized `$uri`, so dot-segment and duplicate-slash
   tricks cannot move a request from a permitted prefix into `/control/`. The
   IP allow-list is retained as an outer layer. A router credential can no
   longer start or stop replicas, and the monitoring credential permits
   scraping and nothing else.

3. **The CA private key leaves the service.** Dynamic certificate issuance is
   removed from the gateway. Certificates are issued offline by an
   administrator with the existing `certmanager` CLI and placed on the gateway
   host and on each cluster's filesystem. The gateway process holds only its
   own leaf key, never a signing key. This was already the operating model for
   Tara; Sophia moves to it. As a side effect, pilot server keys no longer
   transit Globus Compute task payloads on every Sophia job submission.

4. **Keys are scoped per service.** Each container is mounted exactly one
   client key: the API server receives the router key, the controller receives
   the control key, and Prometheus receives the metrics key. Compromise of the
   internet-facing API server yields data-plane access and log reads, not
   control.

5. **One CA per environment, created offline.** The system is pre-production;
   the only CA that exists today is the development CA. The production CA and
   its certificates will be generated offline at first deployment, separately
   for each environment, so that no certificate is valid across environments.

## Declined

**Application-layer authentication on the control API.** The recommendation is
a second credential behind NGINX. Every component that would hold such a
credential already holds the corresponding client key in the same process
under the same service account — the controller would hold both the control
certificate and the control token. A second credential therefore does not
separate any principal from any capability, while adding a secret to
provision, rotate and keep in sync on every pilot. Processes that can reach the
control API's Unix socket directly run as the pilot's service account and
already possess equivalent authority, so application authentication would not
constrain them either.

The finding's underlying concern is that a template edit or misconfiguration
could silently return the system to a single point of failure. We address that
directly with tests rather than a runtime credential:

- a unit test on the rendered NGINX configuration asserting client
  verification and the policy table are present, and
- an integration test that stands up a real pilot NGINX and asserts the full
  authorization matrix for every role, an unknown subject, a server
  certificate presented as a client, and path-normalization bypass attempts.

A regression of the kind the finding describes fails CI instead of passing
silently.

**Per-replica vLLM API key.** The key would be held by the API server alongside
the router certificate, so it separates nothing that the router certificate
does not. After remediation, reaching `/replicas/` already requires the router
or control identity.

## Accepted residual risk

- **Server certificates are interchangeable with each other.** Any pilot's
  server key can impersonate any other pilot to an on-path attacker, since
  hostname verification cannot be used (see above). Server keys are held only
  on cluster filesystems readable by the pilot service account.
- **No revocation.** There is no CRL. A leaked key is remediated by rotating
  the CA and reissuing the small, fixed set of certificates per environment
  (one CA, one server certificate per cluster, three client certificates).
- **Longer certificate lifetimes.** Manually issued certificates live longer
  than today's job-scoped server certificates. Expiry is surfaced by a
  gateway-side alert ahead of expiry and by the pilot refusing to start when
  its server certificate would expire within the job's walltime.
- **Shared IP allow-list.** Data-plane and control-plane locations still share
  one network allow-list; separation is enforced by identity, not network.

## Documentation

The architecture documentation describes `/control/` as protected by mTLS. It
will be updated to state that the pilot authorizes callers by certificate
subject, and to document the role table above.
