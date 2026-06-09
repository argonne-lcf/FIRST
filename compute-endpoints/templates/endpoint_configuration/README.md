# Configuring Template Endpoint and Get UEP Status

This folder includes an example of how to configure a Globus Compute templated endpoint and an example of how to retrieve the status of all spawned user-endpoints (UEPs). Below are some notes.

### `config.yaml`

Make sure to add a subscription ID to the templated endpoint. Otherwise it won't start.

### `user_config_template.yaml.j2`

Here we can use the `display_name` field as a mean to recover which AI models are running.

`idle_heartbeats_soft` is the number of idle heartbeats the UEP will count before the *endpoint* shuts down. `max_idletime` is the idle time before the *HPC job* stops. If the endpoint shuts down before `max_idletime`, the job will be killed. To make sure this does not happen, `idle_heartbeats_soft` needs to be larger than `max_idletime / 30 sec`. For persistently served model, this should be longer than the jobs's walltime.

### `submit_task.py`

Example on how to submit to the templated endpoint.

### `get_endpoints_status.py`

Script to query the templated endpoint status and recover the status of each spawned UEP. An example of the output is:

```bash
user_endpoint_id: 622851b6-2d2e-e1f0-e14e-945603d37ae5
  - models: openai/gpt-oss-12b,openai/gpt-oss-20b
  - job IDs: []
  - ready: False

user_endpoint_id: e8057605-6862-c706-2020-86f88b0ab967
  - models: nvidia/nemotron-3-super-120b
  - job IDs: ['158234.sophia-pbs-01.lab.alcf.anl.gov']
  - ready: True

user_endpoint_id: 4b4428a9-66db-153f-9359-0fa21fae0921
  - models: meta-llama/Llama-4-Maverick-17B-128E-Instruct
  - job IDs: ['158271.sophia-pbs-01.lab.alcf.anl.gov']
  - ready: True
```