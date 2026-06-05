# Connecting to Globus Compute Backends

This guide describes how to connect the Gateway to existing backends powered by Globus Compute single-user endpoints.

## Endpoint Configuration

If you adopted our Globus Compute configuration, you can simply reuse the `GlobusComputeEndpoint` endpoint adaptor and add your entries to the `fixtures/endpoints.json` file. Each entry should respect the following the data structure:

```json
{
    "model": "resource_server_async.endpoint",
    "pk": 1,
    "fields": {
        "endpoint_slug": "your-cluster-api-your-model-70b",
        "cluster": "your-cluster",
        "framework": "api",
        "model": "Your-Model-70B",
        "endpoint_adapter": "resource_server_async.endpoints.globus_compute.GlobusComputeEndpoint",
        "config": {
            "api_port": 8000,
            "endpoint_uuid": "your-backend-globus-compute-endpoint-uuid",
            "function_uuid": "your-backend-globus-compute-function-uuid"
        }
    }
}
```

Make sure that `endpoint_slug` has the following format: `cluster-framework-model` (with no `/` or `.` character, all lower case). For example, the `meta-llama/Meta-Llama-3.1-70B-Instruct` model hosted on `my-cluster` and served with `my-framework` should have the following slug: `my-cluster-my-framework-meta-llamameta-llama-31-70b-instruct`. You can also use the Django `slugify` tool.
```python
from django.utils.text import slugify
endpoint_slug = slugify(" ".join([cluster, framework, model.lower()]))
```

## Cluster Configuration

If you adopted our Globus Compute configuration, you can simply reuse the `GlobusComputeCluster` cluster adaptor and add your entries to the `fixtures/clusters.json` file. Each entry should respect the following the data structure:

```json
{
    "model": "resource_server_async.cluster",
    "pk": 1,
    "fields": {
        "cluster_name": "your-cluster",
        "cluster_adapter": "resource_server_async.clusters.globus_compute.GlobusComputeCluster",
        "frameworks": [
            "vllm"
        ],
        "openai_endpoints": [
            "chat/completions",
            "completions",
            "embeddings",
        ],
        "config": {
            "qstat_endpoint_uuid": "your-backend-globus-compute-qstat-endpoint-uuid",
            "qstat_function_uuid": "your-backend-globus-compute-qstat-function-uuid"
        }
    }
}
```
