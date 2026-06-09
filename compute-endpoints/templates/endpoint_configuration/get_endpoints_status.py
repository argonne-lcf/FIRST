import os

import yaml
from dotenv import load_dotenv
from globus_compute_sdk import Client

load_dotenv()

TEMPLATE_ENDPOINT_ID = os.environ.get("TEMPLATE_ENDPOINT_ID")

# Globus Compute client
gcc = Client()

# Query status of user endpoints spawned by main templated endpoint
response = gcc._compute_web_client.v3.get(
    f"/v3/endpoints/{TEMPLATE_ENDPOINT_ID}/console",
    query_params={"include_fields": "user_endpoint_id,node_info,config"},
)

# For each user endpoint ...
for endpoint in response.data.get("user_endpoints", []):
    # Extract model(s) served
    models = "model names not defined"
    if "config" in endpoint and endpoint["config"] is not None:
        config_dict = yaml.safe_load(endpoint["config"])
        models = config_dict.get("display_name", "model names not defined")

    # Extract node info to see whether the job is running
    ready = True if endpoint.get("node_info", {}) else False
    node_info = endpoint.get("node_info", {})
    job_ids = [job_id for job_id in node_info]

    # Print status
    print()
    print(f"user_endpoint_id: {endpoint['user_endpoint_id']}")
    print(f"  - models: {models}")
    print(f"  - job IDs: {job_ids}")
    print(f"  - ready: {ready}")
