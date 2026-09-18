# PBS execution-head identity

`allocatedMachines` describes allocation membership, not PBS execution order. The
Tara bridge returned this list in hostname order for job 6349, although PBS and
the pilot readyfile identified a different first execution host. Treating the
list's first element as the pilot host sent control requests to the wrong node.

Both FIRST status queries now request the existing `Job.extension` JSON field.
The live bridge exposes PBS `exec_vnode` there, preserving its ordered chunks.
FIRST validates the bounded expression, selects its first vnode, and requires
exactly one `allocatedMachines` entry whose `name` or `hostname` matches it.
The selected machine's existing `hsn_ips` resource supplies the address. No host
sorting, requested-placement inference, management-network fallback, or probing
of other allocated nodes is used. Pilot submission and endpoint protocol are
unchanged; no pilot, database, or bridge-server change is needed for this bridge.

The documented GraphQL schema already contains `Job.extension: JSON`, and the
current Tara service was checked read-only for this exact field before the fix.
`temp_graphql` on FIRST's old `graphql` branch contains client examples, not the
external bridge implementation. Other bridge versions must expose the same PBS
extension for multi-node endpoints. Missing metadata is compatible only with
an unambiguous single-machine allocation. Present but malformed metadata, an
unmatched first vnode, or multiple matching machine entries never yields an
endpoint, even for a single-machine allocation. Scheduler state remains available
so normal lifecycle disposal still works. The warning contains only the job ID,
never extension contents or job environment data.
