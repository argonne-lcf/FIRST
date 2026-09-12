# GraphQL PBS placement constraints

The adapter translates a deliberately small subset of `scheduler_flags` into
GraphQL job resources. Unsupported flags fail before submission; they are not
passed through to a shell or silently ignored. Empty flags with no custom
resources preserve the previous resource request.

Supported flags are repeated `-l VALUE` or `-lVALUE`, where VALUE contains
comma-separated `place` and/or `select` assignments (each at most once):

- `place=scatter:excl:group=RESOURCE` or `scatter:exclhost:group=RESOURCE`.
- `select=[N:]ngpus=G[:tier1=GROUP][:host=HOST][+...]`, also allowing a bare
  count or omitted `ngpus`. Counts must total FIRST's node request; any GPU
  count must match FIRST. No CPU/memory or arbitrary flag overrides are accepted.
- Explicit hosts are temporary HPE xname selections: one unique host per task,
  all tasks covered, with `group=tier1` and a matching constrained `tier1` on
  every chunk. Host matching uses the xname's cabinet/chassis prefix, not DNS.

Static custom host resources belong in `pilot_system.scheduler_config` and are
added to **every** task resource chunk, independently of the requested node count:

```yaml
task_resources:
  tier1: x4820c7
```

Combine this with `scheduler_flags: "-l place=scatter:exclhost:group=tier1"`.
GraphQL receives `jobPlacement=4`, `jobPlacementSharing=2`,
`jobPlacementRescGroupName=tier1`, and per-task `customResources` name/value pairs.
Custom resource keys and values must be simple strings; built-in resource
overrides are rejected. Chunk constraints cannot override configured `tier1`.
Resources must already exist and be requestable at the site.

For a temporary two-node selection, append
`-l select=1:host=x4820c7s3b1n0+1:host=x4820c7s4b0n0`.
Remove that selection before changing topology; it cannot silently override
FIRST's node count. There is no automatic host picker or negative-host syntax.

ALCF's [placement documentation](https://docs.alcf.anl.gov/aurora/running-jobs-aurora/#placement)
identifies `tier0` as rack/cabinet and `tier1` as rack plus chassis. Constraining
`tier1=x4820c7` is stricter than same-rack placement. This does not establish
Tara's network isolation policy; operators must confirm the allowed boundary.

Inventory query filters do not constrain submitted jobs. Long-term quarantine
can use an operator-managed, requestable health resource through `task_resources`;
creating or updating that resource is an operations responsibility. These
constraints supplement, not replace, all-node pre-model health checks. Offline
unit tests cover the resource mapping; field definitions come from the bundled
GraphQL schema documentation. Live bridge/scheduler acceptance must be verified
with a bounded model-free job before model launch.
