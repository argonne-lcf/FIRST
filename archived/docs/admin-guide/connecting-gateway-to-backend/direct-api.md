# Connecting to Direct API Backends

This guide describes how to connect the Gateway to existing OpenAI-compatible backend APIs.

## Endpoint Configuration

You can simply reuse the `DirectAPIEndpoint` endpoint adaptor and add your entries to the `fixtures/endpoints.json` file. Each entry should respect the following the data structure:

```json
{
    "model": "resource_server_async.endpoint",
    "pk": 1,
    "fields": {
        "endpoint_slug": "your-cluster-api-your-model-70b",
        "cluster": "your-cluster",
        "framework": "api",
        "model": "Your-Model-70B",
        "endpoint_adapter": "resource_server_async.endpoints.direct_api.DirectAPIEndpoint",
        "config": {
            "api_url": "https://your-targetted-api.com/v1/your-model/chat/completions",
            "api_key_env_name": "YOUR_MODEL_70B_API_KEY"
        }
    }
}
```

Here, `YOUR_MODEL_70B_API_KEY` is an environment variable that includes the actual API key. Such variable can have arbitrary names.

Make sure that `endpoint_slug` has the following format: `cluster-framework-model` (with no `/` or `.` character, all lower case). For example, the `meta-llama/Meta-Llama-3.1-70B-Instruct` model hosted on `my-cluster` and served with `my-framework` should have the following slug: `my-cluster-my-framework-meta-llamameta-llama-31-70b-instruct`. You can also use the Django `slugify` tool.
```python
from django.utils.text import slugify
endpoint_slug = slugify(" ".join([cluster, framework, model.lower()]))
```

If you need to incorporate additional logics, you can create an extention adaptor that inherits from the `DirectAPIEndpoint` class. Make sure that you change the `endpoint_adapter` path in `fixtures/endpoints.json` to point to your new adaptor class. In the function re-definitions, you can modify the input data, make additional checks, modify the API URL (via the `self.set_api_url(your_new_url)` function), ect. Below is an example of how an adaptor extention can be built:

```python
from resource_server_async.endpoints.endpoint import BaseEndpoint, SubmitTaskResponse

class CustomEndpoint(DirectAPIEndpoint):
    """Custom endpoint implementation of DirectAPIEndpoint."""

    # Class initialization
    def __init__(self,
        id: str,
        endpoint_slug: str,
        cluster: str,
        framework: str,
        model: str,
        endpoint_adapter: str,
        tpm_model: int,
        tpm_user: int,
        allowed_globus_groups: List[str] = None,
        allowed_domains: List[str] = None,
        config: dict = None
    ):
        super().__init__(
            id,
            endpoint_slug,
            cluster,
            framework,
            model,
            endpoint_adapter,
            tpm_model,
            tpm_user,
            allowed_globus_groups,
            allowed_domains,
            config
        )

    # Inject custom logics to required submit_task function
    async def submit_task(self, data: dict) -> SubmitTaskResponse:
        """Add custom logic before calling the parent submit_task function."""

        # Do some checks with model status [recommended to avoid overloading the backend API]
        response = await self.get_endpoint_status()
        if response.error_message:
            return SubmitStreamingTaskResponse(
                error_message=response.error_message,
                error_code=response.error_code
            )

        # Modify input data to be compliant with the backend API
        api_request_data = {**data["model_params"]}
        api_request_data["stream"] = False

        # Additional logging
        log.info(f"Making API call to model {self.model}")

        # Call sumbit_task function of the parent DirectAPIEndpoint class
        return await super().submit_task(api_request_data)
```

## Cluster Configuration

A cluster adaptor that inherits from the `BaseCluster` class must be created in order to add the `get_jobs` function logic, which is designed to list the state (e.g., `running`) of each model hosted in the backend. Entries in the `fixtures/clusters.json` file should respect the following the data structure:

```json
{
    "model": "resource_server_async.cluster",
    "pk": 1,
    "fields": {
        "cluster_name": "your-cluster",
        "frameworks": [
            "vllm"
        ],
        "openai_endpoints": [
            "chat/completions"
            "completions"
        ],
        "cluster_adapter": "resource_server_async.clusters.your_cluster.YourCluster",
        "config": {}
    }
}
```

Below is an example of how such cluster adaptor can be defined:

```python
from resource_server_async.clusters.cluster import BaseCluster, GetJobsResponse

class CustomCluster(BaseCluster):
    """Custom implementation of BaseCluster."""

    # Class initialization
    def __init__(self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: List[str],
        openai_endpoints: List[str],
        allowed_globus_groups: List[str] = [],
        allowed_domains: List[str] = [],
        config: Dict = None
    ):
        # [Optional] Do something with custom config if needed
        self.config = config

        # Initialize the rest of the common attributes
        super().__init__(
            id,
            cluster_name,
            cluster_adapter,
            frameworks,
            openai_endpoints,
            allowed_globus_groups,
            allowed_domains
        )

    # [Required function]
    async def get_jobs(self, auth: User) -> GetJobsResponse:
        """Provides a status of the cluster as a whole, including which models are running."""

        # Get cluster status
        cluster_status = await some_utils.fetch_status()

        # Format and return model status
        try:
            return GetJobsResponse(jobs=cluster_status)
        except Exception as e:
            return GetJobsResponse(
                error_message=f"Error: Could not generate GetJobsResponse: {e}",
                error_code=500
            )
```