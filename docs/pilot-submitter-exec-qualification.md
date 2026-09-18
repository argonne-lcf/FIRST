# Pilot batch-shell signal boundary

Candidate only; no source deployment or PBS qualification is implied.

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
