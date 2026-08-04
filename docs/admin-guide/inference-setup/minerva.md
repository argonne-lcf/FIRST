# Minerva Direct API Affinity

FIRST exposes one endpoint per logical Minerva model even when the Minerva
gateway runs several vLLM replicas. FIRST derives opaque routing and cache
values on every request so NGINX can preserve replica-local KV-cache reuse
without learning the authenticated user or caller's raw session identifier.

## Required Secret

Provision a dedicated secret in FIRST's normal secret store:

```bash
openssl rand -base64 48
```

Set the generated value as `MINERVA_AFFINITY_HMAC_KEY` in the production
environment. It must contain at least 32 bytes and must be separate from
`SECRET_KEY`, `MINERVA_DIRECT_API_KEY`, and all TLS private keys. Restart FIRST
workers after changing it. A missing or short value fails closed only for
Minerva inference and returns an actionable configuration error.

Rotation changes every derived routing key and cache namespace. Treat rotation
as a planned cold-cache event: drain or announce the change, rotate the secret,
restart FIRST, then validate non-streaming and streaming Minerva requests.

## Client Session Header

Clients may add this optional header to a FIRST inference request:

```text
X-ALCF-Session-ID: conversation-or-branch-id
```

Use the same value for every turn in one conversation. Use different values
for independent conversations or parallel branches when they may use different
replicas. The value may contain at most 128 visible ASCII bytes; spaces,
control characters, non-ASCII text, and longer values are rejected with HTTP
400. An absent or empty value produces a stable user-level fallback.

Do not use the OpenAI `user` request field for this purpose. The session value
is always namespaced by the authenticated FIRST user and logical model. FIRST
does not forward or log the raw user or session value.

Example:

```bash
curl -X POST "$FIRST_URL/resource_server/v1/chat/completions" \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'X-ALCF-Session-ID: project-a-conversation-17' \
  -d '{
    "model": "nemotron-3-ultra",
    "messages": [{"role": "user", "content": "Continue our analysis."}]
  }'
```

## Forwarding And Cache Isolation

For Minerva requests, FIRST derives and overwrites:

- `X-Minerva-Affinity-Key`, an opaque user/model/session HMAC used only for
  consistent-hash replica selection;
- `X-Request-ID`, the existing FIRST access-log request ID; and
- `cache_salt`, an opaque stable user/model HMAC when the selected vLLM request
  schema supports it.

Caller-supplied affinity headers or `cache_salt` fields cannot override these
server-derived values. The values are built in per-request dictionaries;
cached endpoint adapters and shared HTTP client defaults are not mutated.
Streaming captures an immutable request body and headers before returning its
generator, preventing another request from changing them.

The installed compatibility policy is:

| vLLM operation | Session affinity header | `cache_salt` |
| --- | --- | --- |
| chat/completions, completions, responses | yes | yes |
| embeddings, pooling, classify, score | yes | yes |
| Anthropic messages | yes | no |
| health, metrics, unknown operations | yes when routed through the Minerva adapter | no |

Anthropic messages can retain replica-local reuse through affinity, but FIRST
does not claim salted cross-user cache isolation for that protocol. Do not add
unknown body fields or a vLLM monkeypatch to change this table; update it only
when the installed vLLM request schema explicitly supports the field.

## Endpoint Fixtures And Capacity

A fixture row represents a logical model, not a replica. Its `api_url` remains
the one NGINX route, for example:

```text
https://minerva-login-01.minerva.alcf.anl.gov:9443/models/nemotron-3-ultra/v1
```

Adding or removing Minerva replicas requires no FIRST fixture or database
change. Retiring a logical model does require targeted deletion of its endpoint
row plus endpoint-cache invalidation; restore the row only after its Minerva
route is healthy again.

## Security And Validation

Keep TLS hostname checking enabled and retain the configured client
certificate, private key, and CA. FIRST sends only opaque derived values to
Minerva. Logs must not contain bearer tokens, TLS keys or certificates, raw
session IDs, raw user IDs, affinity keys, cache salts, prompts, or bodies.

After a deployment or secret rotation, verify:

1. the same authenticated user/model/session derives stable behavior;
2. different sessions can reach different replicas;
3. different users cannot share the same cache namespace;
4. both non-streaming and streaming requests succeed; and
5. non-Minerva Direct API adapters receive no Minerva-specific headers or body
   fields.
