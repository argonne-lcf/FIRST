import asyncio
import json
import logging
import time
import uuid
from typing import Any, AsyncGenerator, Optional, cast, override

from django.http import StreamingHttpResponse
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.errors import TaskPending as GlobusTaskPending
from pydantic import BaseModel

from resource_server_async import globus_utils
from resource_server_async.cache import (
    cache_item,
    is_cached,
    remove_endpoint_from_cache,
)
from resource_server_async.endpoints.endpoint import (
    BaseEndpoint,
)
from resource_server_async.streaming import (
    create_streaming_response_headers,
    format_streaming_error_for_openai,
    get_streaming_data_and_status_batch,
    get_streaming_metadata,
    prepare_streaming_task_data,
    process_streaming_completion_async,
    set_streaming_error,
    set_streaming_status,
)

from ..errors import BatchNotFound, EndpointError, TaskPending
from ..logging import get_request_context
from ..models import BatchLog
from ..schemas.batch import BatchStatus, BatchSubmit
from ..schemas.endpoints import (
    BatchStatusResult,
    SubmitBatchResult,
    SubmitStreamingTaskResponse,
    SubmitTaskAsyncResponse,
    SubmitTaskResult,
)

log = logging.getLogger(__name__)


class GlobusComputeEndpointConfig(BaseModel):
    api_port: int
    endpoint_uuid: str
    function_uuid: str
    batch_endpoint_uuid: Optional[str] = None
    batch_function_uuid: Optional[str] = None


# Extract user prompt
def extract_prompt(model_params: dict[str, Any]) -> Any:
    """Extract the user input text from the requested model parameters."""

    # Completions
    if "prompt" in model_params:
        return model_params["prompt"]

    # Chat completions
    elif "messages" in model_params:
        return model_params["messages"]

    # Embeddings
    elif "input" in model_params:
        return model_params["input"]

    # Undefined
    return "default"


# Globus Compute implementation of a BaseEndpoint
class GlobusComputeEndpoint(BaseEndpoint):
    """Globus Compute implementation of BaseEndpoint."""

    # Class initialization
    def __init__(
        self,
        id: str,
        endpoint_slug: str,
        cluster: str,
        framework: str,
        model: str,
        endpoint_adapter: str,
        tpm_model: int,
        tpm_user: int,
        config: dict[str, Any],
        allowed_globus_groups: list[str] | None = None,
        allowed_domains: list[str] | None = None,
    ):
        # Validate endpoint configuration
        self.__config = GlobusComputeEndpointConfig(**config)
        self._client_lock = asyncio.Lock()

        # Initialize the rest of the common attributes
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
        )

    @override
    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        del state["_client_lock"]  # pickle-friendly
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._client_lock = asyncio.Lock()

    # Get endpoint status
    async def get_endpoint_status(
        self,
        gcc: Client | None = None,
        check_managers: bool = False,
        for_batch: bool = False,
    ) -> dict[str, Any]:
        """Return endpoint status or an error is the endpoint cannot receive requests."""

        # Get Globus Compute client
        if gcc is None:
            gcc = globus_utils.get_compute_client_from_endpoint_id(
                self.config.endpoint_uuid
            )

        # Query the status of the targetted Globus Compute endpoint
        # NOTE: Do not await here, cache the "first" request to avoid too-many-requests Globus error
        if for_batch:
            assert self.config.batch_endpoint_uuid is not None
            endpoint_status, error_message = globus_utils.get_endpoint_status(
                endpoint_uuid=self.config.batch_endpoint_uuid,
                client=gcc,
                endpoint_slug=self.endpoint_slug + "/batch",
            )
        else:
            endpoint_status, error_message = globus_utils.get_endpoint_status(
                endpoint_uuid=self.config.endpoint_uuid,
                client=gcc,
                endpoint_slug=self.endpoint_slug,
            )
        if len(error_message) > 0 or endpoint_status is None:
            raise EndpointError(error_message)

        # Check if the endpoint is online
        if not endpoint_status["status"] == "online":
            raise EndpointError(
                f"Endpoint {self.endpoint_slug!r} is offline.", status_code=503
            )

        # If managers should be checked ...
        # This is to prevent submitting requests to an endpoint that is not ready yet
        if check_managers:
            # Extract whether managers are deployed on the online endpoint
            resources_ready = (
                int(endpoint_status.get("details", {}).get("managers", 0)) > 0
            )

            # If the compute resource is not ready (if node not acquired, worker_init not completed, or lost managers) ...
            if not resources_ready:
                # If a user already triggered the model (model currently loading) ...
                cache_key = f"endpoint_triggered:{self.endpoint_slug}"
                if is_cached(cache_key):
                    # Send an error to avoid overloading the Globus Compute endpoint
                    # This also reduces memory footprint on the API application
                    error_message = f"Error: Endpoint {self.endpoint_slug} online but not ready to receive tasks. "
                    error_message += "Please try again later."
                    raise EndpointError(error_message, status_code=503)

        return endpoint_status

    async def prepare_executor(self, for_batch: bool = False) -> Executor:
        # Get Globus Compute client and executor
        try:
            gcc = globus_utils.get_compute_client_from_endpoint_id(
                self.config.endpoint_uuid
            )
            gce = globus_utils.get_compute_executor(client=gcc)
        except Exception as e:
            raise EndpointError(str(e)) from e

        # Check endpoint status
        await self.get_endpoint_status(
            gcc=gcc, check_managers=True, for_batch=for_batch
        )

        return gce

    # Submit task
    @override
    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        """Submits a single interactive task to the compute resource."""
        gce = await self.prepare_executor()

        # Add API port to the input data
        model_params = data.setdefault("model_params", {})
        if isinstance(model_params, dict):
            model_params["api_port"] = self.config.api_port

        return await globus_utils.submit_and_get_result(
            gce,
            self.config.endpoint_uuid,
            self.config.function_uuid,
            data=data,
            endpoint_slug=self.endpoint_slug,
        )

    async def submit_task_async(
        self, data: dict[str, Any], endpoint_config: dict[str, Any] | None = None
    ) -> SubmitTaskAsyncResponse:
        gce = await self.prepare_executor()

        gcc = gce.client
        batch = gcc.create_batch(user_endpoint_config=endpoint_config)
        batch.add(self.config.function_uuid, args=(data,))

        async with self._client_lock:
            resp: dict[str, Any] = await asyncio.to_thread(
                gcc.batch_run,
                endpoint_id=self.config.endpoint_uuid,
                batch=batch,
            )

        task_id: str = str(resp["tasks"][self.config.function_uuid][0])
        return SubmitTaskAsyncResponse(task_id=task_id)

    async def get_task_result(self, task_id: str) -> SubmitTaskResult:
        gce = await self.prepare_executor()

        gcc = gce.client

        try:
            async with self._client_lock:
                result = await asyncio.to_thread(gcc.get_result, task_id)
        except GlobusTaskPending:
            raise TaskPending(task_id)
        else:
            result = globus_utils.unwrap_json(result)
            return SubmitTaskResult(result=result, task_id=task_id)

    # Submit streaming task
    @override
    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        """Submits a single interactive task to the compute resource with streaming enabled."""

        # Generate unique task ID for streaming
        stream_task_id = str(uuid.uuid4())
        streaming_start_time = time.time()

        # Prepare streaming data payload using utility function
        data = prepare_streaming_task_data(data, stream_task_id)

        # Add API port to the input data
        model_params = data.setdefault("model_params", {})
        if isinstance(model_params, dict):
            model_params["api_port"] = self.config.api_port
        else:
            remove_endpoint_from_cache(self.endpoint_slug)
            raise AssertionError(
                f"Error: Could not process endpoint data for {self.endpoint_slug}"
            )

        # Submit task to Globus Compute (same logic as non-streaming)
        # Assign endpoint UUID to the executor (same as submit_and_get_result)
        gce = await self.prepare_executor()

        gce.endpoint_id = self.config.endpoint_uuid

        # Submit Globus Compute task and collect the future object (same as submit_and_get_result)
        future = gce.submit_to_registered_function(
            self.config.function_uuid, args=(data,)
        )

        # Wait briefly for task to be registered with Globus (like submit_and_get_result does)
        # This allows the task_uuid to be populated without waiting for full completion
        try:
            asyncio_future: asyncio.Future[Any] = asyncio.wrap_future(future)
            # Wait just long enough for task registration (not full completion)
            await asyncio.wait_for(asyncio.shield(asyncio_future), timeout=1.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            # Timeout/cancellation is expected - we just want task registration, not completion
            pass
        except Exception:
            # Other exceptions don't prevent us from getting task_uuid
            pass

        # Get task_id from the future (should be available after brief wait)
        task_uuid = globus_utils.get_task_uuid(future) or ""

        # Cache the endpoint slug to tell the application that a user already submitted a request to this endpoint
        cache_key = f"endpoint_triggered:{self.endpoint_slug}"
        cache_item(cache_key, True, ttl=600)

        try:
            context = get_request_context()
        except LookupError:
            context = None

        # Start background processing for metrics collection (fire and forget)
        if context is not None:
            original_prompt = (
                extract_prompt(data["model_params"])
                if data.get("model_params")
                else None
            )
            asyncio.create_task(
                process_streaming_completion_async(
                    task_uuid,
                    stream_task_id,
                    context,
                    streaming_start_time,
                    original_prompt,
                )
            )

        # Create simple SSE streaming response
        async def sse_generator() -> AsyncGenerator[str, None]:
            """Simple SSE generator with fast Redis polling - P0 OPTIMIZED with pipeline batching"""
            try:
                max_wait_time = 300  # 5 minutes total timeout
                start_time = time.time()
                last_chunk_index = 0
                first_data_timeout = 30  # 30 seconds to receive first chunk or status
                first_data_received = False
                last_chunk_time = None  # Track when we last received a chunk
                no_new_data_timeout = 5  # 5 seconds with no new chunks = assume completion (fallback if /done not called)

                while time.time() - start_time < max_wait_time:
                    # P0 OPTIMIZATION: Get status, chunks, and error in a single Redis round-trip
                    chunks, status, error_message = get_streaming_data_and_status_batch(
                        stream_task_id
                    )

                    # Check if we've received any data (chunks or status)
                    if (chunks and len(chunks) > 0) or status:
                        first_data_received = True

                    # PRIORITY 1: Fast auth failure check (immediate break)
                    auth_failure = get_streaming_metadata(
                        stream_task_id, "auth_failure"
                    )
                    if auth_failure:
                        error_msg = {
                            "object": "error",
                            "message": "Streaming authentication failed: Remote compute endpoint could not authenticate with streaming API. Check INTERNAL_STREAMING_SECRET configuration.",
                            "type": "AuthenticationError",
                            "param": None,
                            "code": 401,
                        }
                        log.error(
                            f"Streaming task {stream_task_id} - authentication failure detected"
                        )
                        set_streaming_status(stream_task_id, "error")
                        set_streaming_error(stream_task_id, error_msg.get("message"))  # type: ignore
                        yield f"data: {json.dumps(error_msg)}\n\n"
                        yield "data: [DONE]\n\n"
                        break

                    # PRIORITY 2: Early timeout check (no data after 30s)
                    elapsed_time = time.time() - start_time
                    if not first_data_received and elapsed_time > first_data_timeout:
                        error_msg = {
                            "object": "error",
                            "message": f"Streaming task timed out: No data received from compute endpoint after {first_data_timeout} seconds. This may indicate network or endpoint configuration issues.",
                            "type": "StreamingTimeoutError",
                            "param": None,
                            "code": 504,
                        }
                        log.error(
                            f"Streaming task {stream_task_id} timed out - no data received after {first_data_timeout}s"
                        )
                        set_streaming_status(stream_task_id, "error")
                        set_streaming_error(stream_task_id, error_msg.get("message"))  # type: ignore
                        yield f"data: {json.dumps(error_msg)}\n\n"
                        yield "data: [DONE]\n\n"
                        break

                    # PRIORITY 3: Handle error status (send error then break)
                    if status == "error":
                        if error_message:
                            # Format and send the error in OpenAI streaming format
                            formatted_error = format_streaming_error_for_openai(
                                error_message
                            )
                            yield formatted_error
                        # Send [DONE] after error to properly terminate the stream
                        yield "data: [DONE]\n\n"
                        break

                    # PRIORITY 4: Process ALL pending chunks FIRST (drain the queue)
                    # This ensures we don't miss chunks that arrived just before /done
                    if chunks and len(chunks) > last_chunk_index:
                        # Send all new chunks at once
                        for i in range(last_chunk_index, len(chunks)):
                            chunk = chunks[i]
                            # Only send actual vLLM content chunks (skip our custom control messages)
                            if chunk.startswith("data: "):
                                # Send the vLLM chunk as-is
                                yield f"{chunk}\n\n"

                            last_chunk_index = i + 1

                        # Update last chunk time
                        last_chunk_time = time.time()

                    # PRIORITY 5: Check completion status AFTER processing chunks
                    # This prevents race condition where /done arrives before final chunks
                    if status == "completed":
                        # One final check for any remaining chunks that arrived during processing
                        final_chunks, _, _ = get_streaming_data_and_status_batch(
                            stream_task_id
                        )
                        if final_chunks and len(final_chunks) > last_chunk_index:
                            for i in range(last_chunk_index, len(final_chunks)):
                                chunk = final_chunks[i]
                                if chunk.startswith("data: "):
                                    yield f"{chunk}\n\n"

                        log.info(
                            f"Streaming task {stream_task_id} - status is completed, sending [DONE]"
                        )
                        yield "data: [DONE]\n\n"
                        break

                    # PRIORITY 6 (FALLBACK): No new data timeout
                    # This handles cases where remote function sent all data but didn't call /done endpoint
                    # Only check this if we haven't seen a "completed" status
                    if (
                        last_chunk_time is not None
                        and (time.time() - last_chunk_time) > no_new_data_timeout
                    ):
                        log.warning(
                            f"Streaming task {stream_task_id} - no new chunks for {no_new_data_timeout}s, assuming completion (done signal was not received)"
                        )
                        yield "data: [DONE]\n\n"
                        # Set completed status for cleanup
                        set_streaming_status(stream_task_id, "completed")
                        break
                    # Fast polling - 25ms
                    await asyncio.sleep(0.025)

            except Exception as e:
                # For exceptions, just end without error message to maintain OpenAI compatibility
                log.error(f"Exception in SSE generator for task {stream_task_id}: {e}")

        # Create streaming response
        response = StreamingHttpResponse(
            streaming_content=sse_generator(),
            content_type="text/event-stream",
        )

        # Set headers for SSE using utility function
        headers = create_streaming_response_headers()
        for key, value in headers.items():
            response[key] = value

        # Return response with StreamingHttpResponse object
        return SubmitStreamingTaskResponse(response=response, task_id=task_uuid)

    # Enable batch support
    @override
    def has_batch_enabled(self) -> bool:
        """Return True if batch can be used for this endpoint, False otherwise."""
        return (self.config.batch_endpoint_uuid is not None) and (
            self.config.batch_function_uuid is not None
        )

    # Submit batch
    @override
    async def submit_batch(
        self, batch_data: BatchSubmit, username: str
    ) -> SubmitBatchResult:
        """Submits a batch job to the compute resource."""

        gce = await self.prepare_executor(for_batch=True)

        gcc = gce.client

        # Prepare input parameter for the compute tasks
        # NOTE: This is already in list format in case we submit multiple tasks per batch
        batch_id = str(uuid.uuid4())
        params_list = [
            {
                "model_params": {
                    "input_file": batch_data.input_file,
                    "model": batch_data.model,
                },
                "batch_id": batch_id,
                "username": username,
            }
        ]
        if batch_data.output_folder_path:
            assert isinstance(params_list[0]["model_params"], dict)
            params_list[0]["model_params"]["output_folder_path"] = (
                batch_data.output_folder_path
            )

        # Prepare the batch job
        batch = gcc.create_batch()
        assert self.config.batch_function_uuid is not None
        for params in params_list:
            batch.add(function_id=self.config.batch_function_uuid, args=(params,))

        # Submit batch to Globus Compute and update batch status if submission is successful
        async with self._client_lock:
            batch_response = cast(
                dict[str, Any],
                await asyncio.to_thread(
                    gcc.batch_run,
                    endpoint_id=self.config.batch_endpoint_uuid,
                    batch=batch,
                ),
            )

        # Extract the Globus batch UUID from submission
        # Temporary: globus_batch_uuid not used
        if "request_id" not in batch_response:
            raise EndpointError("Batch submitted but no batch UUID recovered")

        # Extract the batch and task UUIDs from submission
        tasks: dict[str, Any] = batch_response["tasks"]
        task_uuids: list[str]
        globus_task_uuids = ""
        for task_uuids in tasks.values():
            globus_task_uuids += ",".join(task_uuids) + ","
        globus_task_uuids = globus_task_uuids[:-1]

        # Return success response with batch ID
        return SubmitBatchResult(
            batch_id=batch_id,
            input_file=batch_data.input_file,
            task_ids=globus_task_uuids,
            status=BatchStatus.pending,
            output_folder_path=batch_data.output_folder_path,
        )

    # Get batch status
    @override
    async def get_batch_status(self, batch: BatchLog) -> BatchStatusResult:
        """Get the status and results of a batch job."""

        if not batch.task_ids:
            raise BatchNotFound("Cannot get batch status with missing task_ids")

        task_statuses = globus_utils.get_batch_status(batch.task_ids)

        for task in task_statuses.values():
            if task.get("status") == "failed":
                return BatchStatusResult(
                    status=BatchStatus.failed, result=str(task.get("error"))
                )

        any_pending = any(task["pending"] for task in task_statuses.values())
        all_success = all(
            task["status"] == "success" for task in task_statuses.values()
        )
        batch_result = None

        if any_pending:
            latest_batch_status = BatchStatus.pending
        elif all_success:
            latest_batch_status = BatchStatus.completed
            result_list = [status["result"] for status in task_statuses.values()]
            batch_result = ",".join(map(str, result_list))
        else:
            latest_batch_status = BatchStatus.failed

        return BatchStatusResult(status=latest_batch_status, result=batch_result)

    # Read-only access to the configuration
    @property
    def config(self) -> GlobusComputeEndpointConfig:
        return self.__config
