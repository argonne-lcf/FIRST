# GraphQL PBS pilot termination

Ordinary FIRST pilot disposal requests `deleteJob(jobId: ..., input: {force: false})`. The controller calls this after marking an idle, unhealthy or otherwise retiring pilot for deletion. It does not call a pilot-manager shutdown API; none exists in the current control client.

The mutation acknowledgement is not allocation-release evidence. FIRST records `exiting` and continues scheduler observation until the exact job is gone. If deletion reports an error, the adapter checks that exact job including history: explicit absence or a terminal state is accepted; a still-live job remains an error. There is no automatic forced-deletion retry.

ALCF [advises against making forced deletion the default](https://docs.alcf.anl.gov/running-jobs/) because it can skip cleanup. The [PBS administrator guide](https://help.altair.com/2024.1.0/PBS%20Professional/PBSAdminGuide2024.1.pdf) describes ordinary termination as TERM followed by a bounded kill delay and KILL. This is an opportunity for graceful shutdown, not a guarantee.

There is a documentation discrepancy about forced deletion: [published PBS 2021.1.1 administrative guidance](https://help.altair.com/2021.1.1/PBSProfessional/PBS2021.1.1.pdf) describes immediate KILL when MoM is reachable, while Tara's installed `qdel(1B)` manual says force is ignored for a reachable MoM and discards server state when unreachable or exiting. Neither establishes the exact behavior of Tara's current PBS 2025.2.2 bridge. The GraphQL schema exposes an optional Boolean `force` field, but does not document its implementation mapping. Do not infer an exact signal sequence from the field name or an exit status alone; qualify the actual bridge/site behavior with an owned, model-free job.

A pilot ready file is normally removed by its application lifespan cleanup. GraphQL endpoint discovery uses live scheduler state and the allocated head-node address, not that file; the adapter has no remote file-deletion operation. A forced kill or failed lifespan can therefore leave a stale file after scheduler/DB disposal. This change does not add stale-file recovery, a shutdown endpoint, or a larger grace period.

Offline regressions check normal deletion, no force escalation, exact-ID validation, acknowledgement versus release, and lost-ack handling for absent, terminal, running and exiting jobs. Live graceful-pilot and ready-file cleanup qualification remains required.
