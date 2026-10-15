# Node admission and quarantine

Before a pilot serves anything, it can check every node of its allocation. If
a node fails, the pilot takes the configured action and exits. The gateway
sees an ordinary failed pilot job and submits a replacement.

```yaml
pilot_system:
  node_admission:
    checks: [gpu_count, gpu_memory_free, gpu_ecc_errors]
    on_failure: pbs_quarantine   # or "exit" (default): just exit
    environment: dev             # required by pbs_quarantine
    minimum_free_memory_fraction: 0.95
    memory_reserve_mib: 256
    quarantine_walltime_min: 10080
    max_quarantined_nodes: 8
```

With no `checks`, nothing runs and the pilot starts as before. A GPU passes
`gpu_memory_free` with at least `ceil(total MiB * minimum_free_memory_fraction)
+ memory_reserve_mib` free.

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

The action receives every screen's result:

- For each failing node, the pilot submits a job named
  `first_quar_<environment>_<host>`. The job requests exactly that host
  (`select=1:host=<host>`, `place=exclhost`) and sleeps. Once it runs, PBS
  cannot place anything else on the node.
- For each passing node, the pilot deletes that environment's quarantine jobs
  targeting it. Those jobs are still queued, because the pilot holds the node,
  and the node has just passed.

The quarantine jobs in PBS are the only record of quarantined nodes:

- `qsub -v FIRST_QUAR_*` variables identify a job: `ENV` and `HOST`, plus
  `REASON` (the failed checks), `SOURCE` (the pilot's PBS job) and `EVIDENCE`
  (a JSON file of the failed screen's observations in the pilot workdir). The
  name is only for people.
- Dev and Prod share the PBS account. Each environment dedupes, counts, deletes
  and reports only its own quarantine jobs, so Dev's experimental checks cannot
  clear Prod's quarantines. A running quarantine still blocks the node for both.
- A node with an existing quarantine job is not submitted again. Nothing is
  submitted once `max_quarantined_nodes` nodes are quarantined.
- A failure to run `qstat`, `qsub` or `qdel` is logged and does not change the
  outcome: a healthy pilot still starts, and a failed one still exits.
- To return a node to service, `qdel` its job. A job also ends at its walltime.
  Either way, the next pilot to land on the node checks it again and
  quarantines it again if it is still bad.
- The gateway reports each quarantine job to Slack through the health alerter
  (`check_quarantined_nodes`): "quarantine job X on host Y is queued/running",
  with the reason, source and evidence in the thread. When the job is gone, the
  alert clears with "Quarantine ended; node is eligible for startup screening."

The pilot needs `qstat`, `qsub` and `qdel` on its `PATH` (for example
`export PATH=/opt/pbs/bin:$PATH` in the submit preamble). `select=1:host=<host>`
assumes each PBS vnode is a whole physical host, as on Tara.
