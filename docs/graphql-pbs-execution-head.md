# PBS execution-head identity

For placed jobs, both GraphQL status queries select `allocatedMachines[0]` as the
pilot head, preserving the order returned by PBS. Operations verified this order
against the first native PBS execution host. FIRST does not sort the allocation
or depend on `Job.extension`, which remains in the schema but is not populated.

The selected machine's existing `hsn_ips` resource supplies its first HSN address;
its `hostname` is retained as host identity, not used as a management-address
fallback. No other allocated node is scanned or probed. Missing allocation data
leaves head identity and address unset. Missing HSN data leaves the address unset.
Scheduler state remains available for normal lifecycle disposal; a discovery
failure is not a hardware-fault diagnosis. Queued and finished jobs do not
advertise head addresses from stale allocation data.
