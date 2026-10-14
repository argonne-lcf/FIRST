# Node admission and quarantine

Before a pilot serves anything, it can check every node of its allocation. A
confirmed node fault triggers the configured action before the attempt exits.
FIRST's normal submission and retry policy remains unchanged; replacements do
not wait for quarantine isolation.

```yaml
pilot_system:
  node_admission:
    checks: [gpu_count, gpu_memory_free, gpu_ecc_errors]
    on_failure: pbs_quarantine   # or "exit" (default): just exit
    environment: dev            # required for native PBS quarantine submission
    minimum_free_memory_fraction: 0.95
    memory_reserve_mib: 256
    quarantine_walltime_min: 10080
    max_quarantined_nodes: 8
```

With no `checks` and `on_failure: exit`, admission does nothing. In quarantine
mode, active-request lookup and the startup guard still run. See
[pilot-side admission and quarantine](pilot-node-quarantine.md) for allocation
requirements, fault confirmation, supported PBS layouts and operator commands.

## Writing a check

A check lives in `packages/pilot/first_pilot/node_checks.py`. It is a command
that runs once on every node, and a function that judges one node's output:

```python
@node_check("kernel", command=["uname", "-r"])
def kernel(output: str, _config: PilotRuntimeConfig) -> str | None:
    if output.strip() != "6.4.0-150600.23.25-default":
        return f"unexpected kernel {output.strip()}"
    return None
```

- Return `None` for a passing observation, or a short reason for a node fault.
- Only a returned reason marks a node bad. If the command cannot run, or the
  function raises, the pilot fails to start and no node is blamed.
- Built-in GPU checks confirm suspected faults in a second coherent sample;
  memory checks also require no compute clients. Custom checks must establish
  sufficient evidence before returning a node-specific fault.
- The command reaches the nodes the same way GPU discovery does (local, SSH or
  PALS, from `gpu_discovery`). Checks with an identical command share a launch.
- The function is pure, so its test is a string in and a reason out. See
  `tests/test_pilot_node_admission.py`.

## The `pbs_quarantine` action

The pilot submits a job named `first_quar_ENV_PHYSICALHOST_OPERATION8` that
requests exactly that host, exclusively, and sleeps. Names aid discovery;
service ownership, target, operation and accepted exclusivity validate the
request. While the job runs, PBS keeps the host unavailable to other allocations.

- The quarantine jobs in PBS are the only record of quarantined nodes. There
  is no separate quarantine database or coordinator.
- Pending, held and running requests reject model initialization on their target
  hosts, including new replicas on reused pilots. Visible requests suppress
  duplicates; `max_quarantined_nodes` caps distinct actively targeted hosts, not
  job count or normal model submissions.
- An uncertain submission acknowledgement gets bounded operation lookup, not
  another `qsub`. Submission failure does not retain the failed attempt forever.
- After Operations confirms remediation and a fresh screen passes, an
  administrator deliberately deletes the validated job with `qdel`. Deletion or
  expiration clears the quarantine condition, not the hardware diagnosis. The
  next allocation must still pass startup screening.
- The gateway reports each quarantine job to Slack through the health alerter
  (`check_quarantined_nodes`), distinguishing pending requests from intended
  running isolation. Disappearance clears with "Quarantine ended; node is
  eligible for startup screening." Fault evidence is recorded in the pilot
  workdir and its location appears in the failed attempt's log.
