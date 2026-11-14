# Retiring a deployment without retiring its model

Retirement metadata belongs to the shared V2 Model catalog. A removed backend
is not an available endpoint. V1's existing `first_v2_bridge` mirrors a small,
non-secret snapshot into `Cluster.model_retirements`, separately from serving
Endpoint rows. There is no per-model condition in FIRST source and no fake
Endpoint or StaticDeployment for the migration response.

## Catalog change

Keep the shared Model and its other deployments. Remove only the retiring
deployment resource and record its exact former identity on the Model:

```yaml
kind: Model
name: inkling-bf16
spec:
  access_group_name: public
  supported_endpoints: [chat/completions, responses, messages]
  retired_deployments:
    - name: minerva/inkling-bf16
      cluster_name: minerva
  # Preserve the rest of this Model's existing settings.
```

Do not set `retired: true` for a cluster-only retirement. That flag means the
entire logical model is retired, including every cluster. A globally retired
Model must remain as a catalog tombstone with no deployments; deleting the
Model would discard its migration guidance. V2 validates these invariants.

V1 reads the current cluster ACL and last reconciled Model ACL before returning
HTTP 410. The legacy endpoint's placeholder tombstone ACL is not substituted
for the shared Model ACL. Review this carefully if a legacy endpoint had stricter
permissions than the shared Model: retain the required restriction in the
catalog before retirement, rather than broadening access for guidance.

## Replacement with a different model identifier

An optional explicit replacement on a deployment tombstone tells clients to
change both model and API base URL. It is guidance, not an alias or routing rule:

```yaml
kind: Model
name: beam-1.0-bf16
spec:
  access_group_name: public
  supported_endpoints: [chat/completions, responses, messages]
  retired_deployments:
    - name: minerva/beam-1.0-bf16
      cluster_name: minerva
      replacement:
        cluster_name: tara
        model_name: beam-1.0-fp8
        base_url: https://inference-api.alcf.anl.gov/resource_server/tara/api/v1
```

Retain the source Model even if its final deployment is removed; do not set
`retired: true` for this deployment-only migration. The target is a separate,
existing Model and deployment. V2 validates those references. V1 checks the
current target Endpoint and cluster ACLs before including its name or URL in
guidance. An inaccessible or unlisted target does not turn the source's 410
into a dispatch or disclose the replacement. Requests still authorize against
the source cluster and catalog ACLs first.

The JSON error includes `info.replacement_cluster`, `replacement_model` and
`replacement_base_url` only when an authorized alternative is listed. Without
`replacement`, existing same-model discovery and response fields are unchanged.
Both applications and bridges must be updated before publishing this optional
field; it uses existing JSON retirement columns, not a new database migration.
Preview a complete desired-state catalog, preserving unrelated resources and
existing tombstones. Never apply this example alone as a complete catalog.

## API behavior

All normal authentication still applies. Chat Completions, Responses and
Messages return ordinary JSON 410 before backend initialization, dispatch or
streaming headers, even for `stream: true`. Completions/Embeddings use the same
guard. Aliases work too.

Retired identities are also hidden from V1 model/job listings before adapter
initialization, including the interval before the legacy fixture is tombstoned.
Retiring one deployment does not withdraw another deployment on the same cluster.

Cluster-only retirement uses `error.code: deployment_retired` (and the matching
OpenAI/Anthropic-compatible `type`). `error.info` includes `model`,
`retired_deployment`, `retired_cluster` and `alternatives`. The message tells
the caller to change the base URL. Alternative paths are relative to the same
public FIRST origin and come only from existing, authorized V1 endpoints and
their cluster ACLs. They are not promises that a backend cannot fail later.
V1 does not recommend a federated route that it does not itself provide.

For Minerva Inkling, the verified Tara identifier is `inkling-bf16` and the
public replacement base URL is:

```text
https://inference-api.alcf.anl.gov/resource_server/tara/api/v1
```

Append `chat/completions`, `responses` or `messages`. Keep the normal FIRST
bearer token; explicitly change the client's cluster/base URL. Tara's modalities,
reasoning options and context limits must be verified independently, not inferred
from Minerva. No retired request is automatically redirected, replayed or proxied.

No replacement currently listed? The request still returns 410, but says there
is no listed alternative. An authenticated caller lacking Model/cluster access
receives the existing authorization error without migration details. `model_retired`
is reserved for global retirement.

## Review, Dev validation and owner-operated rollout

1. Verify the replacement's current catalog, API URLs, access and bounded
   inference on each relevant API, including streaming. Record capabilities and
   any differences; do not promise untested feature combinations.
2. Review FIRST V2 PR #409 and this V1 companion. Deploy the V2 database migration
   and code, plus V1 migration `0015_cluster_model_retirements`, API and bridge
   code, in that order before applying new catalog fields. The bridge and API
   must both use the new V1 code. No inference model restart is needed.
3. On Dev, remove the source deployment and add its Model tombstone; allow one
   bridge tick. Verify the persisted snapshot and JSON 410 for all three APIs,
   both stream modes, aliases, missing/invalid auth and denied Model/cluster ACLs.
   Confirm unrelated models and the replacement still dispatch normally.
4. Apply the legacy fixture's established restricted tombstone: retain its
   primary key, rename model to `removed-pk-<pk>` and endpoint slug accordingly,
   and use the placeholder-only group. Recheck 410 after the original identity
   is gone, no source advertisement, and continued replacement access.
5. Review gates and Dev qualification precede production. Production FIRST and
   fixture deployment are owner-operated. Do not create a production
   StaticDeployment or access Prod to work around deployment permissions.
6. Keep the source serving job until the owner confirms the production 410 and
   replacement access. Then refresh live jobs/registry, record rollback placement,
   environment and arguments, suppress the exact held successor, prevent lease
   renewal, withdraw the owned replica, drain active requests, and stop gracefully.
   Verify job-scoped workers, GPU allocations, listeners and temporary files,
   and removal of serving/per-instance metrics registrations. Leave physical
   hardware monitoring, unrelated models, authorization and affinity unchanged.

The bridge publishes endpoint changes and retirement snapshots in one database
transaction. A missing, unreachable or invalid RouterConfig leaves the previous
snapshot intact. Changes to Model ACLs or retirement metadata therefore follow
the bridge polling interval; audit bridge health after an outage. Cluster ACLs
are read on each request. Fixture reloading Cluster rows can clear the defaulted
snapshot; allow a successful bridge reconciliation and verify 410 after fixture
application. Do not hand-edit this bridge-owned field.

## Rollback

Before physical shutdown, restore the original deployment resource and remove
only its `retired_deployments` entry, reconcile, and restore the legacy fixture's
original identity/ACLs. Confirm private serving health and normal FIRST dispatch.
After shutdown, first recover approved capacity and the recorded environment,
placement and model settings with exactly one valid successor, then restore
advertisement. Do not release stale held jobs or interrupt other models blindly.
Keep weights, caches and rollback environments until no live job references them.

Rolling back V1 code requires stopping the new bridge/API before reversing its
database migration; otherwise new processes still reference the removed column.
Prefer leaving this additive column in place during an application rollback.
