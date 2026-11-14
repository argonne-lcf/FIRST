# Deployment retirement and replacement guidance

Retire a deployment, not its shared Model, when only one cluster stops serving
it. Remove the deployment resource from the complete desired-state catalog and
retain its exact former identity in the source Model's `retired_deployments`.
Do not set `retired: true` unless retiring the logical Model everywhere.

An optional explicit replacement supports migration to a different Model:

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

This is a fragment, not a complete catalog. Preserve every unrelated resource,
source Model setting and existing retirement record. The target Model, cluster
and deployment must exist, and the target Model must not be globally retired.
The URL must be HTTPS without credentials, query parameters or a fragment.

Requests retain source authorization. A retired deployment returns JSON HTTP
410 `deployment_retired`, including for streaming requests before backend
dispatch or event-stream headers. Authorized explicit alternatives add
`replacement_cluster`, `replacement_model` and `replacement_base_url` to
`error.info`. The message instructs clients to change both model and base URL.
There is no alias, redirect, replay, server-side tool execution or proxying.

Replacement guidance is filtered by target Model access. A declared deployment
may be cold or temporarily unhealthy: guidance does not promise a healthy
backend or create a fake serving entry. Inaccessible/unlisted targets are not
disclosed. Omitting `replacement` preserves existing same-model behavior.

The legacy V1 bridge must include matching cross-model support. It mirrors
non-secret retirement and target Model ACL metadata separately from serving
Endpoint rows, then checks the current V1 cluster and any existing Endpoint
restrictions. Updating this optional JSON metadata needs no new SQL migration;
the original retirement migrations must already be applied. Never run V2
Alembic migrations against a V1 database or replace public V1 with V2 to deploy
this feature.

Validate on Dev first: all three APIs, streaming/non-streaming, missing/invalid
authentication, denied source/target access, cold targets, bridge round-trip,
removal of the old fixture identity, continued replacement and unrelated-model
access. Preview the complete catalog plan before applying it. Only stop the
old serving job after production 410 and actual replacement inference pass.

Rollback restores only the source deployment and its legacy fixture identity,
removes that deployment's tombstone and reconciles the bridge. Recover and
verify serving capacity before restoring advertisement. Preserve shared Models,
other tombstones, weights and environments throughout.
