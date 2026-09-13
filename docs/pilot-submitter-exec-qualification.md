# Pilot batch-shell signal boundary

The focused change is qualified for the bounded Tara lifecycle below; this is
not a general shutdown guarantee or production-performance qualification.

Nemotron PBS 6289 completed its request and both model cleanup hooks. Its idle
pilot was then deleted through the ordinary controller scheduler path. The
pilot log ended without Uvicorn's first `Shutting down` message; PBS recorded
exit 137, and the ready file remained. This does not establish which signal
reached the pilot, and does not implicate `manager.stop()` as the observed cause.

The submitter previously invoked the pilot as the batch shell's final child.
Both rendered paths now use `exec`, preserving the batch-shell PID through the
admission wrapper's own `execve` into FIRST. Quoting, environment values and
exit status remain unchanged. This removes a signal-delivery boundary; it does
not create a SIGKILL handler or guarantee PBS grants a shutdown grace period.
It also means preamble EXIT traps no longer run after the pilot: the preamble
is setup, not a post-pilot cleanup mechanism.

CPU tests execute only a short temporary stand-in, not FIRST or a model. They
verify both adapters, shell/stand-in PID equality, quoted paths/environment,
and exit statuses 0 and 7. No scheduler, database or Redis is used.

## Separately reviewed, operator-run A/B

`tests/fixtures/pilot-signal-{plain,exec}.pbs` each request one exclusive
PBS-selected c6 node for 120 seconds. The only intentional process-boundary
difference is `exec`; labels identify the variant. No FIRST pilot, model,
PALS launcher, GPU query, CUDA import, network call or shared runtime write is
made. The Python witness exits after 105 seconds if no SIGTERM arrives.
These are manual, non-FIRST job names and must never enter controller ownership.

Do not submit both at once. From this reviewed clean worktree, the operator
submits one variant with output directed to a new private evidence path,
records the exact returned job ID and selected host, and waits for its `ready`
JSON line. Confirm its job ID/variant and source SHA match, and compare its PID
to `pilot_signal_shell`. Only then issue one default `qdel` for that exact owned
ID. Capture full PBS accounting and the complete output before proceeding to
the other variant. Do not use owner-wide deletion, altered signal options or
manual PID signals. Do not rerun automatically after failure.

Expected distinguishing evidence is `term_handler` and `finally` from the same
recorded PID. Exit 137 alone cannot show whether SIGTERM was delivered. If
both variants omit those markers, investigate PBS/bridge termination policy;
the exec change alone is not qualified as a fix. If both receive SIGTERM, the
shell hypothesis is insufficient and a no-model FIRST/Uvicorn witness is the
next bounded diagnostic. Any deadline exit is inconclusive, not PASS.

Separately, the current lifespan skips ready-file unlink if manager teardown
raises, and nginx kill is not followed by a bounded reap. Those are distinct
hardening candidates; this patch intentionally changes neither, nor any model
cleanup receipt requirement. An inert ready file must not be removed merely
to convert the existing canary result into PASS.

## Executed Tara qualification, 2026-09-13 UTC

The sequential model-free witnesses distinguished the shell boundary: PBS 6290
(plain child) ended 143 without its child's TERM/finally markers; PBS 6291
(`exec`) ended 0 with TERM and finally from the batch PID. Empty real-FIRST
PBS 6293 then completed its application lifespan and removed its ready file and
nginx prefix after one normal GraphQL deletion. These witnesses did not run a
model and do not determine the semantics of forced deletion.

The subsequent controller-owned Nemotron allocation, PBS **6295**, passed one
full-model request and cleanup with the combined shutdown changes. Exact pins:

- Submitter source: `bde6f9a3a973b0fc11fea33324d44ae3506501a1`.
- Normal GraphQL deletion: `a2c8d12d990a747353aad59a90c714d5494db65f`.
- Deployed combined controller: `5bccd83ff260e8ad7ca487f9f2e90c204dce39ca`.
- Unchanged FIRST pilot: `a869eb4578449854ac304537043ee8933c2de218`.
- Fixture baseline: `93644d61c5ab32ab2611d6773653a2d8f2a0f20b`.
- Admission bundle: `07da3687f04b7efb1ad63673fbac49218234acf1`.
- Model bundle: `df1f118d206c6cbc7920a31a4bbefc5386016782`;
  backend SHA-256 `5fb7c7a223924c1c3b43a85ea9ce0731673a9b14bb55d03a30168ccfb89e4e0c`.
- Guard SHA-256: `b6ea5c7c7f88d0924b167d26a69e54b0ddae01383d980ba88aa35bbff139a793`.

ConfigVersion 11 temporarily bounded Tara to one pilot/two exclusive c6 nodes,
90-minute walltime and one-minute idle release; Nemotron's failure limit was
zero and GPT-OSS autoscaling was paused. All seven desired/failure counters
started at zero. Nemotron retained TP4/PP2/DP1, four GPUs per node, context
262144, max sequences 1, GPU utilization 0.95 and `enforce_eager=false`; no
model pin, graph, fusion or runtime-environment changes were made for this run.

Admission bound the exact pilot `tara-pilot-e08a13af` (UID 8) and PBS allocation;
the replica was `tara/nemotron-3-ultra/replica/a177b6de` (UID 8). Both nodes
matched kernel `6.4.0-150700.53.63-64kb`, page size 65536 and driver 580.65.06;
all eight GPUs had no compute processes or uncorrected ECC errors. HSN-only
bootstrap and OFI/cxi/SENDRECV completed all four cross-node PP communicator
pairs. Both stages loaded weights and captured graphs before readiness.

Scale-one completed at 04:41:14; readiness at 04:46:07; the single request
returned HTTP 200 at 04:46:24; scale-zero completed at 04:46:25. Both normal
FIRST model hooks and exact per-node quiesce receipts passed, with fresh
all-eight-GPU checks showing no compute processes/ECC errors and absence of the
checked model/PALS groups and UDS/TCP listeners. **Both model engine cleanups
used bounded KILL escalation**: this was not a claim of graceful engine exit
without escalation.

The distinct pilot lifespan completed gracefully: PID 422399, also the PBS
session PID, logged shutdown, stopping zero replicas at 04:48:04, application
shutdown completion and process finish. Its exact ready file and nginx prefix
were absent. PBS accounting was terminal F/143; the ordered application proof,
not that exit code alone, established cleanup. The guard recorded PASS/cleanup
PASS at 04:48:18, with the owned pilot/replica deleted and all claims empty.

At 04:49:31, one conditional restoration returned the full 28-resource fixture
baseline as ConfigVersion **12**, preserving all seven deployment identities,
zero desired/failure counters and all job/replica rows. The original 6289 result
remains incomplete and unchanged. This is one full-model lifecycle smoke, not
throughput, sustained load, general fault recovery or production qualification.
Private evidence remains operator-held; no raw logs, TLS material or credentials
are included in this note.
