# FIRST: httpx / NGINX Configuration Spec

## Why HTTPS + NGINX? (Transport Rationale)

Before the configuration details, the architectural question: why link the gateway to
per-node model servers over mutually-authenticated HTTPS through NGINX, rather than a
custom gRPC or ZeroMQ transport — and why does the gateway initiate every connection
rather than have the pilot dial out?

### The thesis: be a thin, faithful proxy for APIs we don't own

The gateway's job is to be a faithful proxy for three evolving API surfaces — Anthropic
Messages, OpenAI Chat Completions, and OpenAI Responses. The cheapest way to stay faithful
is to **re-encode nothing**: `vllm serve` (and SGLang, TGI) already implement these APIs
over HTTP/JSON/SSE, so the gateway passes the request body through raw (`aiter_raw`, no
decode/re-chunk) and lets the upstream own the spec.

Any non-HTTP transport forces us to own a transcoding layer for a spec we don't control:
HTTP→custom at the gateway, custom→HTTP at a node-side sidecar, maintained forever as
tool-call formats, usage accounting, and reasoning/thinking deltas churn upstream. That is
permanent work for zero functional gain. **Passthrough over HTTP is the thinnest possible
design — there is nothing to beat.**

### Supporting arguments

1. **Streaming is already solved by HTTP.** SSE-over-chunked-HTTP/1.1 *is* the
   token-streaming primitive: framing, backpressure, and client-disconnect propagation come
   for free. The rest of this doc leans on this — `proxy_ignore_client_abort off` means a
   gateway abort closes the upstream, vLLM aborts, and the **GPU is freed**. A custom
   transport would re-implement flow control *and* have to get GPU reclamation right (which,
   as the streaming sections below show, is subtle).

2. **Gateway-as-client is the right directionality.** One connection direction means one
   firewall rule to reason about, fail-fast connect semantics, and per-backend connection
   pools sized to concurrency. It is simpler to develop and far simpler for cyber/ops to
   secure.

3. **HTTPS + mTLS is the *lingua franca* — a security feature, not just convenience.**
   Cyber/ops already know how to reason about "TLS 1.3 mutual auth, client cert from a
   private CA, IP allowlist, deny all," and have tooling for cert lifecycle, TLS scanning,
   and audit. A bespoke ZeroMQ CURVE socket or custom gRPC auth interceptor is an unaudited
   attack surface whose CVEs we would own. On a shared HPC system, *nothing novel to
   security-review* is a real win.

4. **Debuggability under HPC constraints.** You often cannot attach a debugger to a running
   batch job. "Can I `curl --cert` it from the login node?" reproduces any request;
   tcpdump/wireshark decode it; standard load-testers work; HTTP status codes mean what
   everyone expects. gRPC needs grpcurl + reflection; ZeroMQ needs tooling we would write.

5. **Stateless horizontal scale.** Request/response HTTP carries no shared session state —
   scaling is just more uvicorn workers, each with its own pools, with capacity arbitrated
   in Redis (the Admission Controller). This is exactly what the architecture is built
   around.

### Why not gRPC?

Its real advantages are HTTP/2 multiplexing and typed schemas. But vLLM doesn't speak our
protobuf, so we would transcode on both ends anyway; the typed-schema benefit is marginal
when the schema is "whatever OpenAI/Anthropic define" (already JSON, not ours to own); and
HTTP/2's single-connection head-of-line blocking is *worse* than HTTP/1.1's
many-connections model for many parallel token streams over a long-haul link. HTTP/1.1 with
large warm keepalive pools + TLS session resumption (below) recovers most of the handshake
savings without the HOL risk. And if we ever *do* want multiplexing, the upgrade is HTTP/2
— still HTTP, still NGINX, still curl-able — not gRPC.

### Why not ZeroMQ?

Lowest wire latency, but we would build request/response correlation, streaming, auth
(CURVE is niche and unrecognized by ops), health checks, load balancing, and observability
from sockets up — maximal maintenance and security burden, no vLLM compatibility. And the
latency argument doesn't survive contact with reality: protocol overhead is tens of
milliseconds against *seconds* of GPU generation. It optimizes the wrong term.

### Where this default is too myopic (honest caveats)

None of these argue for gRPC/ZeroMQ; two say "keep HTTP, revisit a sub-choice," and one is
a genuine assumption to verify.

1. **The reachability assumption is load-bearing.** Gateway-as-client requires a network
   path from the gateway to each compute node's NGINX. The pilot's
   `discover_service_endpoint()` finds an externally-routable IP via the UDP-connect trick —
   which *assumes such an IP exists and is reachable from where the gateway runs*. On many
   HPC sites, compute nodes are only reachable from a login node. If direct
   gateway→compute-node TCP isn't available in production, the answer is still not a custom
   protocol — it's an **HTTP-preserving tunnel**: co-locate the gateway on a service/login
   node with fabric access, or reverse-tunnel via SSH `-L` / an HTTP CONNECT proxy. This is
   the one place pilot-initiated connections earn a real look — and even then the cleanest
   dial-out is a plain HTTPS reverse tunnel, not a bespoke channel. Full pilot dial-out is
   rejected as the *default* because carrying many concurrent streams back over one
   pilot-initiated connection means re-inventing HTTP/2 flow control, loses per-backend
   pooling, and gives every batch job outbound credentials into the control plane (a larger
   blast radius than the gateway holding client certs and reaching in).

2. **HTTP/1.1 specifically — not HTTP — may need to become HTTP/2 at extreme fan-out.** One
   `AsyncClient` per backend per worker × 64 connections is a lot of pooled TLS connections
   and a burst of handshakes on scale-from-zero. Fine at current scale (resumption + warm
   pools handle steady state); at thousands of backends, HTTP/2 multiplexing is the natural
   evolution — cheap to reconsider without touching the architecture.

3. **"Unidirectional" is a current simplification, not a transport constraint.** Because the
   pilot never dials out, the gateway *polls* status (see the pilot observer's 10s interval).
   If push-based status/telemetry is ever wanted, add a pilot→gateway **webhook over plain
   HTTP POST** — still HTTP, still mTLS, no new protocol — without abandoning the
   client-initiated data path.

**Net:** delegate to vLLM so we never own a codec for an API we don't control; HTTP because
streaming/disconnect/GPU-reclaim are already solved; mTLS + NGINX because it's the auditable
lingua franca; gateway-as-client because it's the simplest thing that scales statelessly —
with the standing footnote that the whole model rests on the gateway being able to route to
compute nodes.

## Configuration Spec

- Use HTTP/1.1 protocol with large keepalive pools on the gateway->NGINX hop
- Maintain one `AsyncClient` per backend URL in each worker's memory.  Never create a client per-request: the mTLS handshake is expensive!

## Client Configuration

```python
import ssl, socket, httpx
from contextlib import asynccontextmanager

# Detect half-open connections from crashed nodes (HPC nodes do die).
# httpx does NOT enable TCP keepalive by default.
SOCKET_OPTS = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15),
    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4),
]

LIMITS = httpx.Limits(
    max_connections=64,            # per backend, per worker (should be similar to backend's max concurrency)
    max_keepalive_connections=64,  # keep all connections warm; mTLS handshakes are expensive
    keepalive_expiry=30.0,         # MUST be < NGINX keepalive_timeout
)

# TIMEOUT logic
# connect: 5s fail fast on firewall issue or mTLS timeout
# write: 60s to upload large prompt body
# pool: 5s to wait if all connections are currently in use for the backend
# streaming read: 120s to wait for first token response
# unary read: 15 minutes to wait for a large non-streaming response
STREAM_TIMEOUT = httpx.Timeout(connect=5.0, read=120.0, write=60.0, pool=5.0)

# STREAM_TIMEOUT is the default; apply timeout=UNARY_TIMEOUT per-request for non-streaming invocations
UNARY_TIMEOUT  = httpx.Timeout(connect=5.0, read=900.0, write=60.0, pool=5.0)

def make_client(base_url: str) -> httpx.AsyncClient:
    transport = httpx.AsyncHTTPTransport(
        verify=SSL_CTX,
        retries=2,                 # connect-phase retries ONLY
        limits=LIMITS,
        http2=False,
        socket_options=SOCKET_OPTS,
    )
    return httpx.AsyncClient(
        base_url=base_url,
        transport=transport,
        timeout=STREAM_TIMEOUT,    # default; unary calls override per request
        headers={"Accept-Encoding": "identity"},  # never let upstream compress token streams
    )

CLIENTS: dict[str, httpx.AsyncClient] = {}

@asynccontextmanager
async def lifespan(app):
    for node in discover_backends():
        CLIENTS[node.origin] = make_client(node.origin)
    yield
    for c in CLIENTS.values():
        await c.aclose()           # drains pools cleanly on worker shutdown
```

## Streaming Requests

Streaming httpx API: do not use `async with client.stream(...)` — the context
manager can't span the lifetime of a `StreamingResponse`. Use the build/send
pattern with cleanup in `finally`:

```python
import anyio
from fastapi.responses import StreamingResponse, JSONResponse

async def proxy_stream(origin: str, payload: dict):
    client = CLIENTS[origin]
    req = client.build_request("POST", "/v1/chat/completions", json=payload)
    upstream = await client.send(req, stream=True)

    if upstream.status_code != 200:
        body = await upstream.aread()          # small error body; safe to buffer
        await upstream.aclose()
        return JSONResponse(status_code=upstream.status_code, content=json.loads(body))

    async def relay():
        try:
            async for chunk in upstream.aiter_raw():   # raw passthrough, no decode/re-chunk
                yield chunk
        finally:
            # Runs on: normal completion, upstream error, AND client disconnect
            # (Starlette cancels this generator on disconnect → CancelledError).
            # The close itself must be shielded or the pending cancellation
            # interrupts it and leaks the connection.
            with anyio.CancelScope(shield=True):
                await upstream.aclose()

    return StreamingResponse(
        relay(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

## Non-streaming

```python
async def proxy_unary(origin: str, payload: dict):
    client = CLIENTS[origin]
    r = await client.post("/v1/chat/completions", json=payload, timeout=UNARY_TIMEOUT)
    return Response(content=r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))
```

## Retry Policy

| Condition | Action |
|---|---|
| `ConnectError` / `ConnectTimeout` | Record error and retry on another backend, ≤3 attempts |
| `PoolTimeout` | Should never happen, because if backend saturated, Admission Controller will not select the backend. Admission Controller selects the first available backend, or 503 + `Retry-After` if none |
| 502/503 from NGINX, no bytes relayed to client | Retry on another node |
| `ReadTimeout` (non-streaming) | **Do not retry.** The GPU likely did (or is doing) the work; retrying doubles cost and feeds overload. Return 504 |
| Any error after first byte streamed to the client | **Never retry** — partial tokens already delivered; surface the error in-stream |

The AdmissionController `record_error()` creates a per-backend circuit breaker and ensures that consistently failling backends are removed from the admission rotation for a bench duration.    Backends that recover are naturally returned to the rotation.  Backends that remain truly unhealthy will fail the health check and be stopped/replaced by the control plane.


**NGINX side:** `proxy_next_upstream off;`. Never let NGINX replay an inference POST.

## NGINX Configuration

```nginx
# ---- Resource footprint: this box's CPUs belong to inference ----
worker_processes 2;                 # a bounded per-node proxy doesn't need 'auto'
worker_cpu_affinity auto;           # or pin via systemd CPUAffinity away from vLLM cores
worker_rlimit_nofile 65536;

events {
    worker_connections 8192;
    multi_accept on;
}

http {
    # Compute-node disk I/O is precious; buffer logs (or log at the gateway only)
    access_log /var/log/nginx/access.log main buffer=64k flush=5s;
    error_log  /var/log/nginx/error.log warn;

    tcp_nodelay on;                 # push token frames immediately
    gzip off;                       # NEVER compress SSE — it buffers tokens
    reset_timedout_connection on;

    # ---- Gateway-facing keepalive (must outlive httpx keepalive_expiry!) ----
    keepalive_timeout  300s;
    keepalive_requests 10000;       # default 1000 forces churn at high QPS

    # ---- Upstream: local vLLM ----
    upstream vllm {
        # Prefer a Unix socket: no loopback TCP overhead, no ephemeral-port
        # exhaustion, no TIME_WAIT churn. (vLLM/uvicorn: --uds /run/vllm/http.sock)
        server unix:/run/vllm/http.sock;
        # server 127.0.0.1:8000;    # TCP fallback

        keepalive 128;              # pooled conns to vLLM; >= expected concurrency
        keepalive_requests 10000;
        keepalive_timeout 60s;      # MUST be < vLLM/uvicorn keep-alive
    }

    server {
        listen 443 ssl reuseport;
        # http2 intentionally NOT enabled — gateway speaks HTTP/1.1

        # ---- mTLS termination ----
        ssl_certificate         /etc/nginx/node.crt;
        ssl_certificate_key     /etc/nginx/node.key;
        ssl_client_certificate  /etc/nginx/gateway-ca.pem;
        ssl_verify_client       on;
        ssl_protocols           TLSv1.3;
        ssl_session_cache       shared:SSL:20m;   # resumption slashes reconnect cost
        ssl_session_timeout     4h;
        ssl_session_tickets     on;               # fine on a private fabric
        # ssl_early_data off (default): 0-RTT replay is unacceptable for POSTs

        # ---- Request bodies (prompts can be MBs of JSON) ----
        client_max_body_size    32m;
        client_body_buffer_size 1m;    # avoid spooling prompts to disk

        location / {
            proxy_pass http://vllm;

            # -- Upstream keepalive prerequisites (both lines REQUIRED) --
            proxy_http_version 1.1;
            proxy_set_header Connection "";

            proxy_set_header Host $host;
            proxy_set_header X-Request-ID $http_x_request_id;   # trace propagation

            # -- Streaming correctness --
            proxy_buffering off;            # relay tokens the moment they arrive
            proxy_request_buffering off;    # stream large prompts; no disk spool
            proxy_cache off;

            # -- Timeouts: strictly ABOVE the gateway's, so the gateway
            #    always times out first and owns error semantics --
            proxy_connect_timeout 5s;
            proxy_send_timeout    60s;
            proxy_read_timeout    920s;     # > httpx unary read (900s)

            # -- Liveness & resource reclaim --
            proxy_socket_keepalive on;          # reap half-open peers
            proxy_ignore_client_abort off;      # default, but load-bearing:
                                                # gateway abort → close upstream
                                                # → vLLM aborts → GPU freed
            proxy_next_upstream off;            # never replay inference (§6)
        }

        location = /health {                    # cheap probe for gateway health checks
            proxy_pass http://vllm/health;
            proxy_read_timeout 2s;
        }
    }
}
```

## Keepalive races

Idle-connection races (client reuses a connection the server is concurrently closing → `RemoteProtocolError` / RST) are eliminated by making each *client* side expire idle connections strictly before its *server* side does:

| Hop | Client-side idle expiry | Server-side idle timeout | Rule |
|---|---|---|---|
| httpx → NGINX | `keepalive_expiry = 30s` | `keepalive_timeout 300s` | 30 ≪ 300 ✓ |
| NGINX → vLLM | upstream `keepalive_timeout 60s` | uvicorn keep-alive ≥ 120s | 60 < 120 ✓ |

⚠️ **vLLM's embedded uvicorn defaults to a 5-second keep-alive timeout**, which silently defeats NGINX upstream pooling (every pooled connection dies after 5 idle seconds). Raise it — recent vLLM exposes `VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120` (env) or the equivalent uvicorn `--timeout-keep-alive`; verify against your vLLM version.

## Gateway uvicorn

Gateway uvicorn: `--loop uvloop --http httptools --backlog 2048`, a graceful shutdown timeout longer than your longest tolerated stream, and 4 workers is reasonable for an I/O-bound proxy — scale workers on CPU saturation from JSON/TLS work, not on concurrency (asyncio handles that).

## Streaming settlement

Starlette/uvicorn give you a strong guarantee: the generator feeding a StreamingResponse is always finalized.

- Any bare await in the finally gets re-cancelled immediately (the task is still in a cancelled state).
Cleanup awaits **must be shielded**: `with anyio.CancelScope(shield=True): await upstream.aclose()`.
- The settlement work itself should not be awaited there at all. Keep the finally body synchronous and non-raising: flip a flag, shielded-close the upstream, and schedule a detached task that does parsing, ac.settle(), and logging. (This is exactly what `finalize_stream()` does in the example `usage-adapters.py`).