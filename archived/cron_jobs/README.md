# Setting Cron Jobs for Inference VM

### Update Batch Status

This is to periodically update the status of all ongoing batch jobs, and add the results in the database while they are available from the Globus server (which deletes task results after 3 days).

On the VM, as the `webportal` user, add a crontab with the following command:
```bash
crontab -e
```
and include the following line to execute the update command every 10 minutes:
```bash
*/10 * * * * /home/webportal/inference-gateway/cron_jobs/update_batch_status.sh >> /home/webportal/inference-gateway/cron_jobs/update_batch_status_output.log 2>> /home/webportal/inference-gateway/cron_jobs/update_batch_status_error.log
```

Make sure you set execution permission:
```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/update_batch_status.sh
```

### Dashboard

This is to periodically update the materialized views for the dashboard.

On the VM, as the `webportal` user, add a crontab with the following command:
```bash
crontab -e
```
and include the following line to execute the update command every 4 hours:
```bash
0 */4 * * * /home/webportal/inference-gateway/cron_jobs/refresh_materialized_views.sh
```

Make sure you set execution permission:
```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/refresh_materialized_views.sh
```

### Check Endpoint Status

This is to periodically check if all compute endpoints are online. It alerts admin by email when endpoints become offline.

In `/home/webportal/inference-gateway/cron_jobs/`, create the following `.env` file (make sure to add your compute endpoint credentials):
```bash
ENDPOINTS="
sophia-vllm-qwen-qwq25-vl-72b-instruct:57d963f5-2f73-4f0a-898c-3cc05311764f
sophia-vllm-qwen-qvq-72b-preview:94248103-c719-4320-ac6b-6a885bf61d99
sophia-vllm-qwen-qwq-32b-endpoint:11509894-55f4-4c22-8866-d0382cab7f18
sophia-vllm-auroragpt-endpoint:4f1275f4-e7f3-4f1f-8254-85141f42af49
sophia-vllm-allenai-Llama-3.1-Tulu-3-405B:9ce1abd4-ee0b-475b-a90a-716f79c18af4
sophia-vllm-llama3.3-70b-instruct:c3daad56-7af1-4ba4-bca4-63e4ce72f268
sophia-infinity-nv-embed-v2:da5f2dbb-f265-4c61-a62a-2c1ec44c29eb
sophia-vllm-llama3.2-90B-vision-instruct:499a85ea-c9d0-400e-a836-aef6c0f2d43b
sophia-vllm-qwen-qwq-32b-preview:1e0ff07b-6468-496c-8024-5cb3911517ab
sophia-vllm-qwen2-vl-72B-instruct:fb539f97-a72b-4515-a87c-7c8fe431dfc4
sophia-vllm-llama3.1-405b-instruct:d39cbc54-b6ed-4d1f-894f-499dff5b1dc8
sophia-vllm-nemotron-4-340B-instruct:34fa20a8-0d4a-49d2-8a73-39dc85bd2ed4
sophia-vllm-qwen-multi-endpoint:7ec80f1f-ef0a-4297-a74d-58a26521fb69
sophia-vllm-mistral-large-instruct-2407:33bc18db-752b-4f60-81ad-2c7756990184
sophia-vllm-llama3.1-multi-endpoint:f69909d6-62de-4e45-8c2a-4c37e0b6b11e
sophia-vllm-mixtral-8-22b-instruct:46a57aff-68a7-48b2-8157-e27347d06740
sophia-vllm-llama3-mistral-multi-endpoint:a5ac731e-e49a-4951-90d2-7709a05b3d6a
sophia-vllm-batch-endpoint:177b2ebc-e126-47d5-b753-6070cdbcae81
"
CLIENT_ID="<compute-endpoint-client-id>"
CLIENT_SECRET="compute-endpoint-client-secret"
```

On the VM, as the `webportal` user, add a crontab with the following command:
```bash
crontab -e
```
and include the following line to execute the update command every 12 hours:
```bash
0 */12 * * * /home/webportal/inference-gateway/cron_jobs/check_endpoints.sh
```

Make sure you set execution permission:
```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/check_endpoints.sh
```

### Query Match Status

This is to periodically query the qstat endpoint for each inference cluster, and add the result in the database so that the jobs/ URL can reuse the cached information instead of re-trigerring the qstat function.

On the VM, as the `webportal` user, add a crontab with the following command:
```bash
crontab -e
```
and include the following line to execute the update command every minute:
```bash
*/1 * * * * /home/webportal/inference-gateway/cron_jobs/query_model_status.sh
```

Make sure you set execution permission:
```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/query_model_status.sh
```

### Check Cluster Maintenance Status

This is to periodically check ALCF cluster status and cache the results in Redis for consumption by the Django Ninja API. It queries the ALCF facility API and stores cluster status (up/down/error) with a 10-minute TTL.

On the VM, as the `webportal` user, add a crontab with the following command:
```bash
crontab -e
```
and include the following line to execute the update command every 10 minutes:
```bash
*/10 * * * * /home/webportal/inference-gateway/cron_jobs/check_maintenances.sh >> /home/webportal/inference-gateway/cron_jobs/check_maintenances.log 2>&1
```

Make sure you set execution permission:
```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/check_maintenances.sh
```

### Direct Health Monitor (Sophia + Metis)

Runs internal health checks against the active Sophia Globus Compute models and Metis API models. The script bypasses the public API, calls the underlying endpoints directly, and posts a summary to Slack.

#### Requirements

- `.env` (or environment variables) must include:
  - `WEBHOOK_URL`: Slack incoming webhook URL.
  - `METIS_STATUS_URL` and `METIS_API_TOKENS` (JSON mapping) for Metis access.
  - Globus Compute credentials already configured for the Django app (same as production service).
- Python dependencies are already available in the project virtual environment.

#### Cron setup

```bash
crontab -e
```
Add the following entry to run every 5 minutes:

```bash
*/5 * * * * /home/webportal/inference-gateway/cron_jobs/direct_health_monitor.sh >> /home/webportal/inference-gateway/cron_jobs/direct_health_monitor.log 2>&1
```

Ensure the wrapper script is executable:

```bash
chmod u+x /home/webportal/inference-gateway/cron_jobs/direct_health_monitor.sh
```

The wrapper activates the project virtual environment and invokes `direct_health_monitor.py`. Logs contain the latest Slack payload for auditing.
- Posts a consolidated summary to Slack via `WEBHOOK_URL` (no truncation).
- Also runs VM health checks: Redis, PostgreSQL, Globus client, and the Django `/resource_server_async/health` endpoint.