# Building Custom Adaptors

This guide describes how to create new adaptors from scratch in order to fully customize the integration of your backends.

## Custom Endpoint Adaptors

Each endpoint adaptor must inherits from the `BaseEndpoint` class:

```python
from resource_server_async.endpoints.endpoint import BaseEndpoint

class CustomEndpoint(BaseEndpoint):
    """Custom endpoint implementation of BaseEndpoint."""

    def __init__(self,
        id: str,
        endpoint_slug: str,
        cluster: str,
        framework: str,
        model: str,
        endpoint_adapter: str,
        allowed_globus_groups: List[str] = None,
        allowed_domains: List[str] = None,
        config: dict = None
    ):
        # Assign custom config dictionary
        # From config fields in endpoints.json
        self.config = config

        # Initialize the rest of the common attributes
        super().__init__(
            id,
            endpoint_slug,
            cluster,
            framework,
            model,
            endpoint_adapter,
            allowed_globus_groups,
            allowed_domains
        )
```

### Required Functions

Each adaptor must define the following required functions:

```python
from resource_server_async.endpoints.endpoint import (
    SubmitTaskResponse,
    SubmitStreamingTaskResponse
)

async def submit_task(self, data: dict) -> SubmitTaskResponse:
    """Submits a single interactive task to the compute resource."""
    pass

async def submit_streaming_task(self, data: dict, request_log_id: str) -> SubmitStreamingTaskResponse:
    """Submits a single interactive task to the compute resource with streaming enabled."""
    pass
```

In these functions, while you can introduce any logics you want, you must return an object in the requested format to ensure proper integration with the rest of the Gateway API codes.

```python
from pydantic import BaseModel
from typing import Optional
from django.http import StreamingHttpResponse

class SubmitTaskResponse(BaseModel):
    result: Optional[str]
    task_id: Optional[str]
    error_message: Optional[str]
    error_code: Optional[int]

class SubmitStreamingTaskResponse(BaseModel):
    response: Optional[StreamingHttpResponse]
    task_id: Optional[str]
    error_message: Optional[str]
    error_code: Optional[int]
```

### Optional Functions

The optional batch mode, which is deactivated by default, can be activated by re-defining the following functions:

```python
from resource_server_async.endpoints.endpoint import (
    SubmitBatchResponse,
    GetBatchStatusResponse
)

def has_batch_enabled(self) -> bool:
    """Return True if batch can be used for this endpoint, False otherwise."""
    pass

async def submit_batch(self, batch_data: dict, username: str) -> SubmitBatchResponse:
    """Submits a batch job to the compute resource."""
    pass

async def get_batch_status(self, batch: BatchLog) -> GetBatchStatusResponse:
    """Get the status and results of a batch job."""
    pass
```

As for the required functions, you can introduce any logics you want, but you must return an object in the requested format to ensure proper integration with the rest of the Gateway API codes.

```python
from pydantic import BaseModel
from typing import Optional

class BatchStatusEnum(str, Enum):
    pending = 'pending'
    running = 'running'
    failed = 'failed'
    completed = 'completed'

class SubmitBatchResponse(BaseModel):
    batch_id: Optional[str]
    task_ids: Optional[str]
    status: Optional[BatchStatusEnum]
    error_message: Optional[str]
    error_code: Optional[int]

class GetBatchStatusResponse(BaseModel):
    status: Optional[BatchStatusEnum]
    result: Optional[str]
    error_message: Optional[str]
    error_code: Optional[int]
```

## Custom Cluster Adaptors

Each cluster adaptor must inherits from the `BaseEndpoint` class:

```python
from resource_server_async.clusters.cluster import BaseCluster

class CustomCluster(BaseCluster):
    """Custom implementation of BaseCluster."""

    def __init__(self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: List[str],
        openai_endpoints: List[str],
        allowed_globus_groups: List[str] = [],
        allowed_domains: List[str] = [],
        config: dict = None
    ):
        # Assign custom config dictionary
        # From config fields in endpoints.json
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
```

### Required Functions

Each adaptor must define the following required functions:

```python
from resource_server_async.clusters.cluster import GetJobsResponse

async def get_jobs(self, auth: User) -> GetJobsResponse:
    """Provides a status of the cluster as a whole, including which models are running."""
    pass
```

The goal of this function is to query the backend and report the status of each model to help users identify which models are ready to be used. Below is an example of the expected data format for the response (taken from the ALCF Metis cluster):

```json
{
  "running": [
    {
      "Models": "gpt-oss-120b-131072",
      "Framework": "api",
      "Cluster": "metis",
      "Model Status": "running",
      "Description": "gpt-oss-120b-131072 - gpt oss 131K",
      "Model Version": 1
    },
    {
      "Models": "Llama-4-Maverick-17B-128E-Instruct",
      "Framework": "api",
      "Cluster": "metis",
      "Model Status": "running",
      "Description": "Llama-4-Maverick-17B-128E-Instruct - maverick",
      "Model Version": 4
    }
  ],
  "queued": [],
  "stopped": [],
  "cluster_status": {
    "cluster": "metis",
    "total_models": 2,
    "live_models": 2,
    "stopped_models": 0
  }
}
```

While there is some flexibility in what can be displayed in the response, the following structure must be included:

```python
from pydantic import BaseModel
from typing import Optional, List

class JobInfo(BaseModel):
    Models: str
    Framework: str
    Cluster: str
    # Open dictionary that allow more fields
    model_config = {"extra": "allow"}

class Jobs(BaseModel):
    running: List[JobInfo]
    queued: List[JobInfo]
    stopped: List[JobInfo]
    others: List[JobInfo]
    private_batch_running: List[JobInfo]
    private_batch_queued: List[JobInfo]
    cluster_status: dict

class GetJobsResponse(BaseModel):
    jobs: Optional[Jobs]
    error_message: Optional[str]
    error_code: Optional[int]
```

## Paths to Your Adaptors

Once you have your adaptors ready, make sure you point to them in the `fixtures/endpoints.json` and `fixtures/clusters.json` files. If, for example, your endpoint and cluster adaptors are located at `my_app/custom_endpoint.py` and `my_app/custom_cluster.py`, the adaptor paths would be:
```json
# In fixtures/endpoints.json
"endpoint_adapter": "my_app.custom_endpoint.CustomEndpoint"

# In fixtures/clusters.json
"cluster_adapter": "my_app.custom_cluster.CustomCluster"
```