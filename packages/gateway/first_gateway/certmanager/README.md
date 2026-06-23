# mTLS Certificate Tool

A small [Typer](https://typer.tiangolo.com/) CLI that wraps `openssl` to manage a
private certificate authority and issue server/client certificates for **mutual TLS**.

Built for one specific topology:

```
   Inference Gateway       ──mTLS──▶   NGINX (ephemeral HPC host)
presents  client.crt                   presents  server.crt
verifies  server.crt  ◀── same CA ──▶  verifies  client.crt
```

The gateway is the **TLS client**; the HPC NGINX instances are the **TLS servers**.
Both sides authenticate each other against a single private CA.

* The **gateway** presents `client.crt` and verifies that the server's certificate
  chains to `ca.crt`.
* Each **NGINX server** presents `server.crt` and *requires* a client certificate
  that chains to `ca.crt`.

Because HPC servers launch on **random, ephemeral IPs**, the gateway does **not**
check the server's hostname/IP against the certificate.


## Prerequisites

* **OpenSSL 3.x** (`openssl version`) — the tool uses `-addext` and `-copy_extensions`.

Keys are **EC P-256**, certificates use **SHA-256**, and private keys are written
**unencrypted** (required for unattended machine-to-machine startup).


## Quick start

Run these on a **trusted admin host** — this is the only machine that should ever
hold the CA private key.

```bash
# 1. Create the Root CA (default 10 years). Do this once.
python mtls.py ca --name "FIRST Inference Root CA"

# 2. Issue the server certificate (default 2 years). Logical name, not a host.
python mtls.py server inference-server --name server

# 3. Issue the gateway's client certificate (default 2 years).
python mtls.py client inference-gateway --name client
```

Override a lifetime with `--days`, e.g. `python mtls.py server inference-server --name server --days 365`.

All files land in `./pki/` by default (`--dir` to change it).


## Output files

| File | Contents | Secret? | Deploy to |
|---|---|---|---|
| `ca.key` | CA **private** key | **YES — crown jewel** | Admin host only. Never deploy. |
| `ca.crt` | CA public certificate (trust anchor) | No | **Both** gateway and HPC |
| `ca.srl`| CA serial ledger | No (keep with `ca.key`) | Admin host only |
| `server.key` | Server **private** key | **YES** | HPC filesystem only |
| `server.crt` | Server public certificate | No | HPC filesystem |
| `client.key` | Client **private** key | **YES** | Gateway only |
| `client.crt` | Client public certificate | No | Gateway |

Anyone holding `ca.key` can mint trusted **server and client** certificates and
impersonate either side. Keep it off all production machines.

**Private keys** (`*.key`) are owner-read-only secrets. Never commit them, never log
them, never copy `ca.key` off the admin host.

## Deployment

### A. Inference Gateway (FastAPI + httpx)

Copy these three files to the gateway (e.g. `/etc/inference/tls/`):

* `ca.crt` — public (to verify ephemeral servers)
* `client.crt` - public (presented to HPC servers)
* `client.key` — secret gateway's identity

```bash
chmod 644 client.crt ca.crt
chmod 600 client.key
```

Build the httpx client with an SSL context that **verifies the CA but skips the
hostname check** (this is what makes random IPs work):

```python
import ssl
import httpx

TLS = "/etc/inference/tls"

ctx = ssl.create_default_context(cafile=f"{TLS}/ca.crt")
ctx.check_hostname = False                       # ephemeral IPs: trust the CA, not the host
ctx.load_cert_chain(f"{TLS}/client.crt", f"{TLS}/client.key")  # present our client cert

# Reuse one client; the context still requires a CA-signed server cert.
client = httpx.Client(verify=ctx, timeout=30.0)

# The pilot system tells the gateway the ephemeral host:port.
resp = client.get(f"https://{ephemeral_ip}:8443/v1/health")
resp.raise_for_status()
```

### B. HPC NGINX servers

Stage these on the HPC filesystem where the pilot job system can read them:

* `server.crt`, `server.key` — the server's identity
* `ca.crt` — to verify incoming client certs

```bash
chmod 600 server.key          # see the shared-filesystem note below
chmod 644 server.crt ca.crt
```

Minimal NGINX server block enforcing mTLS and proxying to the local backend:

```nginx
server {
    listen 8443 ssl;
    server_name _;                                   # no fixed name needed

    ssl_certificate           /hpc/secure/server.crt;
    ssl_certificate_key       /hpc/secure/server.key;

    ssl_client_certificate    /hpc/secure/ca.crt;    # CA that client certs must chain to
    ssl_verify_client         on;                    # REQUIRE a valid client cert
    ssl_verify_depth          1;                     # leaf signed directly by the root

    ssl_protocols             TLSv1.2 TLSv1.3;

    location / {
        # optional: surface client identity to the backend
        proxy_set_header X-Client-Verify  $ssl_client_verify;
        proxy_set_header X-Client-DN      $ssl_client_s_dn;
        proxy_pass http://127.0.0.1:8000;            # the real inference server
    }
}
```

With `ssl_verify_client on`, NGINX rejects any connection that doesn't present a
client certificate signed by your CA — so only the gateway can reach the backend.

---

## Rotating a certificate

Rotation is just re-issuing against the **same CA** — the CA and the other side are
untouched.

```bash
# Rotate the server cert (new key + new serial, same CA):
python mtls.py server inference-server --name server
# redeploy server.crt + server.key to the HPC filesystem, reload NGINX.

# Rotate the gateway client cert:
python mtls.py client inference-gateway --name client
# redeploy client.crt + client.key to the gateway, restart the gateway.
```

Re-running a command overwrites the existing `.key`/`.crt` for that name and mints a
fresh serial number. Because both sides validate by **CA chain**, a rotated cert is
trusted immediately with no coordination — as long as the CA itself hasn't changed.

Rotating the **CA** (`ca.crt`) is a fleet-wide event: you must redistribute the new
`ca.crt` to *both* ends and re-issue all leaf certs. Plan for it before the 10-year
expiry.
