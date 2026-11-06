# Retire a model on one cluster

Retirement is a deliberate API change, not a redirect or a failed backend.
Keep the model serving until replacement checks, review, Dev validation and the
owner-operated production API/fixture deployment are complete. Physical model
shutdown is a separate, gated operation. This procedure also applies to future
retirements; substitute the source and replacement cluster/model identities.

## 1. Verify the replacement and save rollback information

Use authenticated FIRST model and job listings to verify the replacement is
currently running. Check its exact model identifier, public API URLs, context,
modalities, reasoning modes and API-specific limitations. Send bounded requests
through each advertised API, including streaming. Do not infer capabilities from
the retired model, repository catalogs alone, or a successful health probe.
Record the time and results; availability can change.

Save the original fixture row/primary key, live deployment specification,
access policy, job/node/port mapping, registry incarnation, launch environment,
effective arguments, logs and successor dependency. Retain referenced weights,
environments and caches for rollback. Do not record credentials in the report.

## 2. Add a retirement entry and tests in FIRST

`resource_server_async/retirements.py` contains `MODEL_RETIREMENTS`, keyed by
`(source_cluster, source_model)`. Add a `ModelRetirement` with the human-readable
message, replacement cluster/model, verified public API base URL and the **old
endpoint's group/domain restrictions**. An empty policy means unrestricted
within FIRST's existing authentication policy; do not copy it to a gated model.
The live source-cluster policy is also enforced. Aliases requiring guidance
after deletion need their own explicit mapping entries.

The mapping is independent of database endpoint identities. It raises
`model_retired` / HTTP 410 before endpoint lookup, admission, backend dispatch
or SSE headers, using FIRST's ordinary error envelope. It does not create an
endpoint, authorize access to the replacement, transfer a request or retry
against another cluster. Global authentication remains mandatory. Unknown
models and other clusters retain their normal behavior. The mapping remains
after tombstoning; do not delete it with the fixture.

FIRST v2 uses the equivalent map in
`packages/gateway/first_gateway/apiserver/retirements.py`. Its V1 compatibility
and deployment-scoped inference routes check the map before deployment lookup.
It retains the old access policy and also checks the shared logical model's
current policy when present. Federated requests are not a request for the
retired cluster and continue through ordinary routing to available deployments.
Keep both server generations consistent while both remain in use; no fake
deployment is needed in either. Review retained ACLs when access policy changes.

Test all three APIs with `stream: false` and `stream: true`, missing/invalid/
expired authentication, restricted policies, original deployment present and
absent, unrelated models, and continued replacement-cluster dispatch. Assert
JSON 410, no redirect/SSE response, and no backend/admission call on retirement.

## 3. Retire only the source fixture

Follow gateway-api-fixtures' legacy tombstone convention: preserve the primary
key; rename its model to `removed-pk-<pk>` and slug to
`<cluster>-<framework>-removed-pk-<pk>`; set `allowed_globus_groups` to the
placeholder-only group `aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa`. Preserve the rest
of that row and every unrelated row. Deleting JSON alone does not delete an
already loaded database row. Never use the tombstone's placeholder ACL for the
retirement mapping.

For v2 catalogs, remove only the source deployment from its complete catalog.
Keep the shared `Model`, replacement deployment and all unrelated resources.
Review a fresh live plan; do not apply a stale full catalog that overwrites
manual counts, monitoring or unrelated drift. Regenerate the capability table.

## 4. Dev, review and production handoff

Validate the candidate on FIRST Dev, including after source-fixture removal.
Use isolated test databases/backends where possible, never production failure
injection. Verify authentication, 410 before streaming, unchanged other-model
behavior, replacement access, and absence of a fake advertised source deployment.
Preserve monitoring, token refresh and unrelated Dev settings.

Have the owners review and deploy FIRST **before** applying retirement fixtures
on production. Production FIRST and fixture deployment are owner-operated.
No production StaticDeployment creation is needed. The owner must confirm the
real authenticated production paths return the new 410 on all three APIs and
both stream modes after fixture removal, while replacement requests still work.
A merge or successful Dev test is not this confirmation.

## 5. Drain and stop the source only after confirmation

Refresh the live mapping again; job IDs in an earlier report can be stale.
Record a capacity-aware rollback before stopping anything. Suppress the source
job's exact successor chain first, so a retirement does not relaunch it.
Withdraw only the source serving instance(s) from Minerva's registry, using
incarnation/ownership checks and preventing their heartbeat from re-registering
during drain. Wait for NGINX to remove their routes and for in-flight work to
finish within a bounded, recorded drain window. A startup registration hold by
itself does **not** drain an already registered replica.

Gracefully stop only verified job-owned supervisors, letting existing traps
clean up their workers and job-scoped temporary files. Escalate only against
verified remaining job-owned processes if graceful cleanup fails. Check every
allocated node: processes, GPU ownership, sockets, temporary files, PBS state
and no newly runnable successor. Never use broad `pkill`, shared-cache deletion
or a GPU reset. Remove only expired/owned serving leases; verify the model's
serving and per-instance metrics targets disappear. Keep physical-node metrics
on port 8008 and their discovery inventory/reconciliation untouched. Preserve
all other models, affinity, TLS, authorization and FIRST monitoring.

## Rollback

Before physical shutdown, the owner can restore only the original source
fixture/deployment and deploy FIRST without the source retirement entry.
After shutdown, first ensure the recorded nodes are available; recover the
verified model environment/settings and exactly one valid successor, validate
private HTTPS/inference, then re-register it. Only then restore its advertisement
and remove the 410 mapping through the owner workflow. Do not release a stale
dependency-held job blindly or undo unrelated catalog changes. If capacity is
unavailable, report that blocker instead of stopping another model.

## Inkling: verified Tara migration guidance (2026-11-06 UTC)

Authenticated FIRST reported Tara's **`inkling-bf16` running**. Bounded text
requests completed with HTTP 200 on all three URLs below, streaming and
non-streaming, using no-reasoning mode. No maximum-context, image, audio or tool
qualification was performed as part of this retirement check.

| API | Replacement URL |
| --- | --- |
| Chat Completions | `https://inference-api.alcf.anl.gov/resource_server/tara/api/v1/chat/completions` |
| Responses | `https://inference-api.alcf.anl.gov/resource_server/tara/api/v1/responses` |
| Messages | `https://inference-api.alcf.anl.gov/resource_server/tara/api/v1/messages` |

Use `model: "inkling-bf16"` and the same FIRST authentication workflow. Change
the cluster/base URL explicitly in the client; FIRST does not redirect requests.
For example, with an existing `FIRST_TOKEN` (do not print it):

```bash
export FIRST_BASE_URL=https://inference-api.alcf.anl.gov/resource_server/tara/api/v1
curl --fail-with-body --connect-timeout 15 --max-time 120 \
  -H "Authorization: Bearer $FIRST_TOKEN" -H 'Content-Type: application/json' \
  "$FIRST_BASE_URL/chat/completions" \
  -d '{"model":"inkling-bf16","messages":[{"role":"user","content":"Reply with OK"}],"reasoning_effort":"none","max_tokens":32}'
```

Tara's live catalog advertises 1,048,576 tokens, streaming, tool calling,
separate reasoning, and text/image/audio input. Advertised efforts are `none`,
`minimal`, `low`, `medium`, `high`, `xhigh`, `max` (default `high`). These are
advertised ceilings/features, not proof of every combination. The Tara fixture
qualification notes limit audio to Chat Completions; Responses/Messages schemas
cannot represent that audio input, and the pinned Inkling tokenizer does not
support images inside tool results. Ordinary image input is not that case.
Consult Tara's current capabilities before migrating a multimodal workload.

Minerva's legacy endpoint primary key is **65**. Tombstone only that row; remove
only `minerva/inkling-bf16` from the Dev v2 catalog. Keep Tara's deployment and
the shared `inkling-bf16` Model. This documentation/fixture change does not stop
the live Minerva job; that remains gated on production confirmation above.
