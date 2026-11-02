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
`gpu_memory_free` with at least `ceil(total MiB * minimum_free_memory_fraction) + memory_reserve_mib` free.

## IPv4 rendezvous self-connect

Enable `tcp_self_connect` alongside the GPU checks, and explicitly configure an
unused rendezvous port approved by the site. For example, where 20005 is approved:

```yaml
node_admission:
  checks: [tcp_self_connect, gpu_count, gpu_memory_free, gpu_ecc_errors]
  tcp_probe_port: 20005
  on_failure: pbs_quarantine
  environment: dev
  minimum_free_memory_fraction: 0.95
  memory_reserve_mib: 256
```

Before the pilot manager or any model starts, the existing local/SSH/PALS runner
executes a GPU-free Python probe on each allocated node. It uses the first
available IPv4 address from that node's configured `network_interfaces`, in the
same order as the pilot server. Configure only the intended HSN interfaces:
there is no DNS, management-address or localhost fallback.

The probe binds a disposable wildcard IPv4 listener, like a rendezvous server,
on the configured port, then connects through the node's own selected address.
It verifies accept and a small payload exchange and closes all sockets before
returning. The temporary wildcard listener is not HSN-only exposure; the site
must approve its port. It does not change host networking or load a model.

A connect/accept timeout counts as a node fault only after one bounded repeat
confirms it on the same host, address and port. The existing action records the
node, originating PBS allocation and both observations in the admission evidence.
An occupied port, bind/listen error, missing interface, command failure, incomplete
output or changing result is an infrastructure error: admission fails without
quarantining a node. The check does not require IPv4 loopback to work.

This checks one configured address and port, not peer connectivity or every
possible listener. It runs at pilot startup; upgrading a pin does not rerun it
inside an existing pilot or a reused allocation. Fresh pilot startup is required
to exercise a newly enabled check. Matching pilot and control-plane common-schema
versions must preserve `tcp_probe_port` when preparing the job configuration.

## Writing a GPU check

A check lives in `packages/pilot/first_pilot/node_checks.py`. It is a function
that judges one node's GPUs and is registered by name in `CHECKS`. The GPUs are
`GpuInfo` records from `first_pilot.gpu_inventory`, the same `nvidia-smi` query
the pilot reports to the gateway as its resources:

```python
def gpu_driver(gpus: list[GpuInfo], config: PilotRuntimeConfig) -> Faults:
    if gpus[0].driver_version != "580.82.09":
        return {None: f"unexpected driver {gpus[0].driver_version}"}
    return {}
```

To check a value that the query does not yet collect, add its `nvidia-smi`
field to `gpu_inventory.QUERY_FIELDS` and to `GpuInfo`.

- Return faults keyed by GPU index (`None` for the node as a whole), or `{}`
  for a pass. Only a returned fault marks a node bad. If sampling fails or the
  function raises, the pilot fails to start and no node is blamed. Optional
  `GpuInfo` fields are `None` where the GPU reports `[N/A]`; a check that needs
  one must raise rather than pass, as `gpu_ecc_errors` does.
- The pilot samples every node at once, the same way GPU discovery reaches
  them (local, SSH or PALS, from `gpu_discovery`), and all checks judge the
  same sample.
- A fault counts only if a second sample a second later repeats it on the same
  GPUs. Add a check to `IDLE_CHECKS` if it is only meaningful with no compute
  clients on any GPU, like `gpu_memory_free`.
- The function is pure, so its test is a parsed GPU list in and faults out. See
  `tests/test_pilot_node_admission.py`.

## The `pbs_quarantine` action

The action receives every screen's result:

- For each failing node, the pilot submits a job named
  `first_quar_<environment>_<host>` to the pilot's own queue and account,
  which are known to reach the node. The job requests exactly that host
  (`select=1:host=<host>`, `place=exclhost`) and sleeps. Once it runs, PBS
  cannot place anything else on the node.
- For each passing node, the pilot deletes that environment's queued quarantine
  jobs only when this screen covered every recorded failed check. A GPU-only
  pass cannot withdraw a networking quarantine; missing or unknown reasons are
  retained for operator review. These jobs are still queued because the pilot
  holds the node.

The quarantine jobs in PBS are the only record of quarantined nodes:

- `qsub -v FIRST_QUAR_*` variables identify a job: `ENV` and `HOST`, plus
  `REASON` (the failed checks), `SOURCE` (the pilot's PBS job) and `EVIDENCE`
  (a JSON file of the failed screen's observations in the pilot workdir). The
  name is only for people. `qsub -v` separates values with commas, so the
  pilot replaces commas, quotes, whitespace and other unusual characters in
  `REASON`, `SOURCE` and `EVIDENCE` with `_`, and refuses a host whose name
  would need it.
- Dev and Prod share the PBS account. Each environment dedupes, counts, deletes
  and reports only its own quarantine jobs, so Dev's experimental checks cannot
  clear Prod's quarantines. A running quarantine still blocks the node for both.
- A node with an existing quarantine job is not submitted again. Nothing is
  submitted once `max_quarantined_nodes` nodes are quarantined.
- A failure to run `qstat`, `qsub` or `qdel` is logged and does not change the
  outcome: a healthy pilot still starts, and a failed one still exits.
- After Operations confirms remediation and a fresh screen covers and passes
  the recorded faults, deliberately `qdel` its job. A job also ends at its walltime.
  Either way, the next pilot to land on the node checks it again and
  quarantines it again if it is still bad.
- The gateway reports each quarantine job to Slack through the health alerter
  (`check_quarantined_nodes`): "quarantine job X on host Y is queued/running",
  with the reason, source and evidence in the thread. When the job is gone, the
  alert clears with "Quarantine ended; node is eligible for startup screening."

The pilot needs `qstat`, `qsub` and `qdel` on its `PATH` (for example
`export PATH=/opt/pbs/bin:$PATH` in the submit preamble). `select=1:host=<host>`
assumes each PBS vnode is a whole physical host, as on Tara.
