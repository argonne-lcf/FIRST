# Retiring a model on one cluster

Use the same sequence for every retirement: verify the replacement, add a
cluster/model mapping, retire only the source deployment, validate on Dev,
obtain review and owner-operated production confirmation, then drain and stop
the source. A merge alone is not permission to stop a serving job.

## Retirement mapping and authorization

`packages/gateway/first_gateway/apiserver/retirements.py` holds the small
`MODEL_RETIREMENTS` mapping, keyed by `(source_cluster, source_model)`.
Each `ModelRetirement` specifies a message, replacement cluster/model, verified
public API base URL, and explicit retained group/domain restrictions. Copy the
**old access policy**, not a tombstone's placeholder group. Empty lists mean
unrestricted within normal FIRST authentication; gated models need their real
policy. Review these retained restrictions when access policies change.

Global authentication runs first. The map checks its retained policy and, when
present, the shared logical Model's current policy. It returns the normal error
envelope with HTTP 410 and `error.code: model_retired`, before backend selection,
admission, streaming headers or dispatch. It still works when the original
deployment or logical model is absent. It does not create a deployment, redirect,
replay, grant replacement access or pass requests to another cluster.

Both `/resource_server/{cluster}/{framework}/v1/{endpoint}` and
`/deployments/{cluster}~{model}/v1/{endpoint}` check retirement for Chat
Completions, Responses and Messages. Existing logical model names and aliases
are preserved. Explicit alias entries are needed if guidance must survive
deletion of the alias metadata too. Federated requests do not target the retired
cluster and continue through normal routing to available deployments.

The legacy FIRST main server has the equivalent policy in
`resource_server_async/retirements.py`, enforced after global authentication and
the source-cluster ACL but before endpoint lookup. Keep the entries consistent
while both generations serve users. No database migration or fake available
deployment is needed.

## Reusable operator checklist

1. Verify the replacement's exact model name, running status, URLs and current
   capabilities using authenticated FIRST listings and bounded requests. Check
   all advertised APIs and streaming; note modality/position-specific limits.
   Record dated evidence rather than treating a source catalog as live health.
2. Record the source's fixture primary key, access policy, live deployment,
   environment, arguments, PBS allocation, node/port mapping, registry incarnation
   and exactly one successor. Keep rollback weights/environments available and
   credentials out of evidence. Establish a capacity-aware rollback plan.
3. Add the mapping and focused tests for all three APIs/stream modes,
   missing/invalid/expired auth, retained/live ACLs, deployment/model deletion,
   unrelated requests and continued replacement access. Assert no dispatch,
   admission, redirect or SSE headers on 410.
4. In gateway-api-fixtures, preserve the legacy primary key, rename its model
   to `removed-pk-<pk>` and slug to `<cluster>-<framework>-removed-pk-<pk>`, and
   restrict its groups to `aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`. Deletion from
   JSON alone does not delete a live database row. In complete v2 catalogs,
   remove **only** the source deployment. Preserve the shared Model, replacement
   deployment, monitoring and every unrelated resource. Regenerate capability
   documentation. Review fresh live plans instead of applying stale catalogs
   over operational drift.
5. Validate FIRST and fixtures on Dev, including after original identity removal.
   Isolated Dev apps/test databases/backends avoid disrupting shared Dev work.
   Keep auth real for the final HTTP check; mocked auth belongs in regressions.
6. After review, owners deploy FIRST first, then retirement fixtures to production.
   They confirm authenticated production 410 JSON for all APIs and both stream
   modes **after fixture removal**, no advertised source deployment, and working
   replacement access. Production deployment remains owner-operated; do not
   access the Prod VM/tunnel or create production StaticDeployments for this.
7. Only after that confirmation, refresh live PBS/registry state. Suppress only
   the source successor before drain. Prevent the source heartbeat from
   re-registering; remove only its exact owned incarnation; wait for route
   withdrawal and bounded in-flight completion. A startup registration-hold file
   does not drain a registered replica. Gracefully terminate the verified
   job-owned supervisor and use its normal cleanup. Check all allocated nodes,
   orphan processes, GPU ownership, sockets, job-scoped temporary files, PBS
   state, registry and per-instance metrics removal. Never use a broad process
   kill, shared-cache deletion or GPU reset. Preserve other models, affinity,
   TLS, authorization, hardware metrics on port 8008 and FIRST monitoring.

Rollback after physical shutdown first requires free capacity: restore the
recorded environment/settings and a verified single successor, qualify HTTPS
and bounded inference privately, re-register, then have the owner restore only
the old source fixture/deployment and remove the mapping. Before shutdown, only
the API/fixture rollback is needed. Do not release stale dependency-held jobs or
revert unrelated catalog changes. Report unavailable capacity as a blocker.

## Inkling replacement verified on 2026-11-06 UTC

Tara's authenticated catalog and job listing reported `inkling-bf16` running.
Bounded text requests returned HTTP 200 and complete final output for all three
APIs, streaming and non-streaming, with reasoning disabled. The verified base
URL is `https://inference-api.alcf.anl.gov/resource_server/tara/api/v1`:

| API | Append to the base URL |
| --- | --- |
| Chat Completions | `/chat/completions` |
| Responses | `/responses` |
| Messages | `/messages` |

Use `model: "inkling-bf16"` and normal FIRST bearer authentication. Explicitly
change the client's cluster/base URL; requests are not automatically migrated.
For Chat use `messages` and `max_tokens`; for Responses use `input` and
`max_output_tokens`; Messages uses `messages` and required `max_tokens`.

Tara advertises a 1,048,576-token context, streaming, tools, separate reasoning,
text/image/audio input, and efforts `none`, `minimal`, `low`, `medium`, `high`,
`xhigh`, `max` (default `high`). This is not proof of every combination or a
maximum-context qualification. Tara's fixture qualification documents audio
only in Chat Completions, not the Responses/Messages schemas, and no images in
tool results with the pinned Inkling tokenizer. Ordinary image input is a
different feature. No image/audio/tool/context stress testing was performed for
this retirement verification.

Tombstone Minerva legacy PK65 and remove only `minerva/inkling-bf16` from the
Dev v2 catalog. Keep the shared `inkling-bf16` Model and `tara/inkling-bf16`
deployment. The exact response message is:

```json
{
  "error": {
    "code": "model_retired",
    "message": "Inkling has been retired on Minerva. Use inkling-bf16 on Tara.",
    "info": {
      "replacement_cluster": "tara",
      "replacement_model": "inkling-bf16",
      "replacement_api_url": "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1"
    }
  }
}
```
