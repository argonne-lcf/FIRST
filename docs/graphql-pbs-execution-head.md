# PBS execution-head identity

The GraphQL `allocatedMachines` list does not tell us which machine is the pilot
head. Its order can differ from native PBS `exec_host`, whose first host is the
head: on job 11055, `exec_host` began with `x4820c7s5b0n0` while FIRST dialed the
job's other node. `Job.extension`, which would carry `exec_vnode`, remains in the
schema but is not populated.

FIRST therefore treats every allocated machine as a candidate. Both GraphQL status
queries report all allocated machines, and the pilot job observer probes each one
concurrently. The machine whose manager answers `/status` over mTLS becomes the
job's `manager_url`. mTLS verifies the peer as a pilot of this cluster, and
exclusive allocation means the only pilot on these machines is this job's.

Each machine's existing `hsn_ips` resource supplies its first HSN address. The
order of `hsn_ips` does not matter because pilot NGINX listens on every configured
interface. A machine's `hostname` is retained as host identity, not used as a
management-address fallback, and a machine without HSN data is not a candidate.
Scheduler state remains available for normal lifecycle disposal; a discovery
failure is not a hardware-fault diagnosis. Queued and finished jobs do not
advertise addresses from stale allocation data.
