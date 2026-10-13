# Node admission and quarantine

Before a pilot serves anything, it can check every node of its allocation. If
a node fails, the pilot takes one configured action for that node and exits.
The gateway sees an ordinary failed pilot job and submits a replacement.

```yaml
pilot_system:
  node_admission:
    checks: [gpu_count, gpu_memory_free, gpu_ecc_errors]
    on_failure: pbs_quarantine   # or "exit" (default): just exit
    quarantine_walltime_min: 10080
    max_quarantined_nodes: 8
```

With no `checks`, nothing runs and the pilot starts as before.

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

- Return `None` for a healthy node, or a short reason for a bad one.
- Only a returned reason marks a node bad. If the command cannot run, or the
  function raises, the pilot fails to start and no node is blamed.
- The command reaches the nodes the same way GPU discovery does (local, SSH or
  PALS, from `gpu_discovery`). Checks with an identical command share a launch.
- The function is pure, so its test is a string in and a reason out. See
  `tests/test_pilot_node_admission.py`.

## The `pbs_quarantine` action

The pilot submits a job named `first_quar_<host>` that requests exactly that
host, exclusively, and sleeps. While the job runs, PBS cannot place anything
else on the node.

- The quarantine jobs in PBS are the only record of quarantined nodes. There
  is no database table and no message passing.
- A node is skipped if it already has a quarantine job, and nothing is
  submitted once `max_quarantined_nodes` jobs exist.
- To return a node to service, `qdel` its job. A job also ends at its
  walltime. Either way the next pilot to land on the node checks it again and
  quarantines it again if it is still bad.
- The gateway reports each quarantine job to Slack through the health alerter
  (`check_quarantined_nodes`). The reason is in the failed pilot job's log.
