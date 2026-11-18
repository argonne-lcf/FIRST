# FIRST: Backend HTTP Client / NGINX Configuration Spec

## Why HTTPS + NGINX? (Transport Rationale)

Before the configuration details, the architectural question: why link the gateway to
per-node model servers over mutually-authenticated HTTPS through NGINX, rather than a
custom gRPC or ZeroMQ transport — and why does the gateway initiate every connection
rather than have the pilot dial out?

### The thesis: be a thin, faithful proxy for APIs we don't own

The gateway's job is to be a faithful proxy for three evolving API surfaces — Anthropic
Messages, OpenAI Chat Completions, and OpenAI Responses. The cheapest way to stay faithful
is to **re-encode nothing**: `vllm serve` (and SGLang, TGI) already implement these APIs
over HTTP/JSON/SSE, so the gateway relays the backend's response bytes as received (no
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
   scaling is just more uvicorn replicas, each with its own pools, with capacity arbitrated
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
   advertised IP comes from the first configured `network_interfaces` entry —
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
   client per backend per worker × 64 connections is a lot of pooled TLS connections
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
- Maintain one `BackendClient` (an aiohttp session and its connection pool) per backend in each worker's memory.  Never create a client per-request: the mTLS handshake is expensive!

## Client Configuration

The gateway's backend client is `BackendClient` in
`apiserver/backend_client_manager.py`, built on aiohttp.  It replaced httpx
after the 2026-11 load tests: httpcore's pool rescans every connection for every
waiting request on each acquire and release, and probes each idle socket, which
took 83% of gateway CPU at 512 requests in flight to one backend; and about 64%
of busy CPU while relaying streams sat in httpx's per-read path.  aiohttp's pool
takes and returns a connection in constant time and its response parser is in C.
(The controller and pilot-control clients still use httpx: they make few calls.)

```python
import socket
import aiohttp

# Detect half-open connections from crashed nodes (HPC nodes do die).
# Applied through the connector's socket_factory on direct connections.
SOCKET_OPTS = [
    (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
    (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60),
    (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15),
    (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 4),
    (socket.IPPROTO_TCP, socket.TCP_USER_TIMEOUT, 60_000),  # Linux; see below
]

# total=None: no end-to-end deadline.
# sock_connect: 5s fail fast on a firewall issue or mTLS timeout
# connect: 10s for a free pool slot plus the connection (TCP + TLS)
# sock_read: 900s of upstream inactivity, including a silent long prefill
#   (it applies to each read, not the total request duration)
BACKEND_TIMEOUT = aiohttp.ClientTimeout(
    total=None, connect=10.0, sock_connect=5.0, sock_read=900.0
)

connector = aiohttp.TCPConnector(
    limit=max_backend_concurrency,   # per backend, per worker
    limit_per_host=0,
    keepalive_timeout=30.0,          # MUST be < NGINX keepalive_timeout
    ssl=ssl_context,                 # pilot mTLS / static mTLS / system CAs
    socket_factory=keepalive_socket, # sets SOCKET_OPTS
)
session = aiohttp.ClientSession(
    connector=connector,
    timeout=BACKEND_TIMEOUT,
    headers={"Accept-Encoding": "identity"},  # never let upstream compress token streams
)
```

When `ALL_PROXY`/`HTTPS_PROXY` applies to a backend (not exempted by
`NO_PROXY`, which may list CIDR networks), the connector is an
`aiohttp_socks.ProxyConnector` instead; `socks5h://` resolves names at the
proxy.  TCP keepalive is then not set on the proxied socket.

`BackendClient.post()` retries twice, 0–0.5 s apart, when no connection could be
made (the request was never sent, so repeating it is safe).  It returns at the
response headers; the caller reads the body and releases or closes the response.

aiohttp has no write timeout: it starts the read timer once the request body is
written.  On Linux the direct-connection sockets also set `TCP_USER_TIMEOUT` to
60 s, so a peer that stops acknowledging a large prompt fails the request after
60 s rather than after the kernel's retransmission limit (~15 min).  A live peer
acknowledges promptly, keepalive probes included, so a silent prefill never
trips it.  Sockets through the egress proxy do not get it.

The 900-second read budget applies both before the first upstream response data
and between later reads. A stream making progress can run longer than 900
seconds; this is not an end-to-end deadline. FIRST does not inject heartbeat
events into the model's SSE stream. A measured single long prefill taking about
655 seconds fits this budget, but four concurrent long prefills are not
guaranteed to produce data within it.

The `alcf-ai` SDK also defaults to a 900-second HTTP read inactivity limit; its
10-second connect/write/pool values are unchanged. The CLI's `chat --timeout`
controls cold-start waiting, not the HTTP timeout. Callers using another SDK or
an explicit shorter timeout must configure it separately.

The Dev frontdoor (`deploy/nginx.conf`) and Production host example
(`deploy/nginx.prod.conf.example`) allow 16 MiB and 20 MiB request bodies
respectively, and use 900-second
proxy read/send and downstream send inactivity limits. Their existing connect
timeout is unchanged. These templates are not evidence of deployed settings:
inspect the selected Compose mounts and running host/edge configurations before
applying them. An outside proxy or client can impose a shorter limit. The
compute-side proxy retains its 32 MiB body allowance and 920-second read budget.
Neither these settings nor the backend client's timeouts extend backend-client
retirement or replica-drain grace periods; deploy/restart only when idle or with
separately coordinated draining.

## Streaming Requests

`InferenceService._handle_streaming` sends the request, checks the status at the
response headers, and hands a generator over the open response to
`StreamingResponse`.  The response cannot live inside an `async with`: the
context would close before Starlette iterates the body.  Cleanup goes in the
generator's `finally`:

```python
async def proxy_stream(client: BackendClient, body: bytes):
    upstream = await client.post("/v1/chat/completions", body)

    if upstream.status != 200:
        async with upstream:
            error = await upstream.read()      # small error body; safe to buffer
        return Response(error, status_code=upstream.status)

    async def relay():
        try:
            async for chunk in upstream.content.iter_any():  # whatever each read returned
                yield chunk
        finally:
            # Runs on: normal completion, upstream error, AND client disconnect
            # (Starlette cancels this generator on disconnect).  A stream read to
            # its end has already returned its connection to the pool; close()
            # drops one abandoned midway.  It is synchronous, so cancellation
            # cannot interrupt it.
            upstream.close()

    return StreamingResponse(
        relay(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
```

## Non-streaming

The backend's JSON bytes are relayed as received; the gateway parses them only
to read `usage`.

```python
async def proxy_unary(client: BackendClient, body: bytes):
    async with await client.post("/v1/chat/completions", body) as r:
        content = await r.read()
    return Response(content, status_code=r.status, media_type="application/json")
```

## Retry Policy

| Condition | Action |
|---|---|
| No connection (`ClientConnectorError`, `ConnectionTimeoutError`, proxy errors) | Two more connect attempts on the same backend, then record the error and retry on another backend, ≤3 backends |
| Waiting for a free pool slot | Should never happen, because if backend saturated, Admission Controller will not select the backend. Admission Controller selects the first available backend, or 503 + `Retry-After` if none. Bounded by the 10 s `connect` timeout |
| 502/503/504 from NGINX, no bytes relayed to client | Retry on another node |
| Read inactivity (`SocketTimeoutError`) on a task (`/tasks`) | **Do not retry.** The GPU likely did (or is doing) the work; retrying doubles cost and feeds overload |
| Read inactivity on other unary requests | Currently retried on another backend, like other transport errors |
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

    # ---- Gateway-facing keepalive (must outlive the gateway client idle expiry!) ----
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
            proxy_read_timeout    920s;     # > gateway client read inactivity (900s)

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
| Gateway client → NGINX | connector `keepalive_timeout = 30s` | `keepalive_timeout 300s` | 30 ≪ 300 ✓ |
| NGINX → vLLM | upstream `keepalive_timeout 60s` | uvicorn keep-alive ≥ 120s | 60 < 120 ✓ |

⚠️ **vLLM's embedded uvicorn defaults to a 5-second keep-alive timeout**, which silently defeats NGINX upstream pooling (every pooled connection dies after 5 idle seconds). Raise it — recent vLLM exposes `VLLM_HTTP_TIMEOUT_KEEP_ALIVE=120` (env) or the equivalent uvicorn `--timeout-keep-alive`; verify against your vLLM version.

## Gateway uvicorn

Each gateway container runs one uvicorn process (`--loop uvloop --http
httptools`), with no gunicorn; Compose runs 4 replicas and restarts any that
exit.  Scale replicas on CPU saturation from JSON/TLS work, not on concurrency
(asyncio handles that).  Uvicorn's graceful shutdown is 20s, under the
container's 30s `stop_grace_period`.

nginx balances requests, not connections, across the replicas: it sees each as
its own upstream peer and `least_conn` sends every request to the one with the
fewest in flight, even though its keepalive connections to them last a long
time.  In dev the service name resolves to every replica (`resolve` keeps up as
they restart); in prod each replica has its own loopback port.  Earlier, one
gunicorn with 4 workers shared one listening socket: the kernel handed most new
connections to whichever worker woke first, nginx kept those connections open,
and one worker served most of the requests (94/15/23/64% busy at 64 streams in
the load tests).  Per-worker `SO_REUSEPORT` sockets spread connections but not
requests, since a hot keepalive connection stays on one worker.  Don't run
`uvicorn --workers N` in one container for the same reason: its workers share
one socket.

Replacing gunicorn loses its `--timeout` heartbeat, which killed a worker whose
event loop was blocked; Compose's restart policy acts only when a process
exits.  `apiserver/loop_watchdog.py` fills the gap: a thread exits the process
when the loop has not run for `FIRST_LOOP_WATCHDOG_TIMEOUT` seconds (30), and
`restart: unless-stopped` brings it back.  Meanwhile nginx's `max_fails` takes
the stuck replica out of rotation.

## Streaming settlement

Starlette/uvicorn give you a strong guarantee: the generator feeding a StreamingResponse is always finalized.

- Any bare await in the finally gets re-cancelled immediately (the task is still in a cancelled state).
Cleanup awaits **must be shielded**: `with anyio.CancelScope(shield=True): ...`.  Closing the
upstream response needs no shield: aiohttp's `close()` is synchronous.
- The relay settles in that shielded block: one `settle` round trip (which also updates the
  token-estimation statistics) and the `InferenceLog` emit, after the last chunk has been relayed.
  Settling inline rather than in a detached task frees the user's concurrency slot before
  the next request on the same connection can be admitted.
