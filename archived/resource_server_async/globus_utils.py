import ast
import asyncio
import json
import logging
import time
from typing import Any, TypedDict

import globus_sdk
from cachetools import Cache, TTLCache, cached
from django.conf import settings
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.errors import TaskExecutionFailed
from globus_compute_sdk.sdk.asynchronous.compute_future import ComputeFuture
from globus_compute_sdk.sdk.executor import log as EXECUTOR_LOG
from globus_sdk import TransferClient

from resource_server_async.cache import cache_item, get_item_from_cache
from resource_server_async.errors import EndpointError, RequestTimeout
from resource_server_async.schemas.endpoints import SubmitTaskResult

log = logging.getLogger(__name__)


class TaskStatus(TypedDict):
    pending: bool
    status: str
    result: Any
    error: str | None


# Define separate cache object for Globus executor
executor_cache: Cache[str, Executor] = TTLCache(maxsize=1024, ttl=60 * 10)


# Get authenticated Compute Client from endpoint ID
def get_compute_client_from_endpoint_id(endpoint_id: str) -> Client:
    """
    Extract credentials for target endpoint and submit to
    get_compute_client_from_globus_app with the credentials
    to limit the number of cached Globus Clients and Executors.
    Having too many instances of Executors with the same credentials
    have degraded the performance in the past.
    """

    client_id: str | None
    client_secret: str | None
    # Overwrite Globus credentials if needed, or use default credentials otherwise
    if credentials := settings.GLOBUS_ENDPOINT_CREDENTIALS_OVERRIDES.get(endpoint_id):
        client_id = credentials.client_id
        client_secret = credentials.client_secret
    else:
        client_id = settings.SERVICE_ACCOUNT_ID
        client_secret = settings.SERVICE_ACCOUNT_SECRET

    # Create and return the Globus Compute client
    return get_compute_client_from_globus_app(
        client_id=client_id,
        client_secret=client_secret,
    )


# Get authenticated Compute Client using secret
# NOTE: Using in-memory TTLCache since Globus Client objects cannot be serialized to Redis
@cached(cache=TTLCache(maxsize=1024, ttl=60 * 60))
def get_compute_client_from_globus_app(
    client_id: str | None = None,
    client_secret: str | None = None,
) -> Client:
    """
    Create and return an authenticated Compute client using the Globus SDK ClientApp.

    NOTE: This function uses in-memory caching (TTLCache) instead of Redis because
    Globus SDK Client objects are not serializable.

    Returns
    -------
        globus_compute_sdk.Client: Compute client to operate Globus Compute
    """

    # Use default credentials if not provided
    # This is in case the function is called outside of get_compute_client_from_endpoint_id
    if client_id is None or client_secret is None:
        client_id = settings.SERVICE_ACCOUNT_ID
        client_secret = settings.SERVICE_ACCOUNT_SECRET

    # Try to create and return the Compute client
    try:
        return Client(
            app=globus_sdk.ClientApp(
                client_id=client_id,
                client_secret=client_secret,
            )
        )
    except Exception:
        raise EndpointError("Exception in creating Globus Compute Client")


@cached(cache=TTLCache(maxsize=1024, ttl=60 * 60))
def get_transfer_client() -> TransferClient:
    if settings.SERVICE_ACCOUNT_ID is None or settings.SERVICE_ACCOUNT_SECRET is None:
        raise RuntimeError("Missing configuration to create TransferClient")

    confidential_client = globus_sdk.ConfidentialAppAuthClient(
        client_id=settings.SERVICE_ACCOUNT_ID,
        client_secret=settings.SERVICE_ACCOUNT_SECRET,
    )
    cc_authorizer = globus_sdk.ClientCredentialsAuthorizer(
        confidential_client,
        globus_sdk.TransferClient.scopes.all,
    )
    # create a new client
    return TransferClient(authorizer=cc_authorizer)


# Get authenticated Compute Executor using existing client
# NOTE: Using in-memory TTLCache since Globus Executor objects cannot be serialized to Redis
@cached(cache=executor_cache)
def get_compute_executor(
    endpoint_id: str | None = None, client: Client | None = None, amqp_port: int = 443
) -> Executor:
    """
    Create and return an authenticated Compute Executor using using existing client.

    NOTE: This function uses in-memory caching (TTLCache) instead of Redis because
    Globus SDK Executor objects are not serializable.

    Returns
    -------
        globus_compute_sdk.Executor: Compute Executor to operate Globus Compute
    """

    # Set log level
    if settings.GLOBUS_COMPUTE_EXECUTOR_DEBUG:
        EXECUTOR_LOG.setLevel(logging.DEBUG)

    # Try to create and return the Compute executor
    try:
        return Executor(
            endpoint_id=endpoint_id,
            client=client,
            amqp_port=amqp_port,
            batch_size=settings.GLOBUS_EXECUTOR_BATCH_SIZE,
            api_burst_limit=settings.GLOBUS_EXECUTOR_API_BURST_LIMIT,
            api_burst_window_s=settings.GLOBUS_EXECUTOR_API_BURST_WINDOW_S,
        )
    except Exception:
        raise EndpointError("Exception in creating Globus Compute Executor")


def get_endpoint_status(
    endpoint_uuid: str,
    client: Client,
    endpoint_slug: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    """
    Query the status of a Globus Compute endpoint. This version uses Redis cache
    for multi-worker support while keeping Globus objects serializable.

    Returns (endpoint_status, error_message) tuple
    """
    cache_key = f"endpoint_status:{endpoint_uuid}"

    cached_result: tuple[dict[str, Any] | None, str] | None = get_item_from_cache(
        cache_key
    )
    if cached_result is not None:
        return cached_result

    try:
        status_response = client.get_endpoint_status(endpoint_uuid)
        # Convert to serializable dict
        serializable_status = (
            dict(status_response.data)
            if hasattr(status_response, "data")
            else dict(status_response)
        )
    except Exception as e:
        error_result = (
            None,
            f"Error: Cannot access the status of endpoint {endpoint_slug}: {e}",
        )
        cache_item(cache_key, error_result, 10)
        return error_result
    else:
        result = (serializable_status, "")
        cache_item(cache_key, result, 60)
        return result


# Submit function and wait for result
async def submit_and_get_result(
    gce: Executor,
    endpoint_uuid: str,
    function_uuid: str,
    data: dict[str, Any] | None = None,
    timeout: int = 60 * 5,
    endpoint_slug: str | None = None,
) -> SubmitTaskResult:
    """
    Assign endpoint UUID to the executor, submit task to the endpoint,
    wait for the result asynchronously, and return the result or the
    error message. Here we return the error messages instead of rasing
    execptions in order to be able to cache function results if needed.
    """

    # Assign endpoint UUID to the executor
    gce.endpoint_id = endpoint_uuid

    # Submit Globus Compute task and collect the future object
    # NOTE: Do not await here, the submit* function return the future "immediately"
    try:
        if data is None:
            future = gce.submit_to_registered_function(function_uuid)
        else:
            future = gce.submit_to_registered_function(function_uuid, args=(data,))

    # Error message if something goes wrong
    # Clear cache if the Executor is shut down in order for subsequent requests to work
    except Exception as e:
        if "is shutdown" in str(e):
            executor_cache.clear()
            time.sleep(2)
        raise

    # Cache the endpoint slug to tell the application that a user already submitted a request to this endpoint
    if endpoint_slug:
        cache_key = f"endpoint_triggered:{endpoint_slug}"
        ttl = 600  # 10 minutes
        cache_item(cache_key, True, ttl)

    # Wait for the Globus Compute result using asyncio and coroutine
    try:
        asyncio_future: asyncio.Future[Any] = asyncio.wrap_future(future)
        result = await asyncio.wait_for(asyncio_future, timeout=timeout)
    except TimeoutError:
        task_id = get_task_uuid(future)
        raise RequestTimeout(
            "TimeoutError while attempting to access compute resources. Please try again later.",
            info={"task_id": task_id},
        )
    except Exception as exc:
        error_msg = str(exc)
        if "API request" in error_msg:
            raise EndpointError(error_msg)
        raise

    result = unwrap_json(result)

    # Task ID not populated immediately; access after wait_for!
    task_id = get_task_uuid(future)
    return SubmitTaskResult(result=result, task_id=task_id)


def get_task_uuid(future: ComputeFuture) -> str | None:
    try:
        return future.task_id
    except:
        return None


# Get batch status - Redis compatible
def get_batch_status(task_uuids_comma_separated: str) -> dict[str, TaskStatus]:
    """
    Get status and results (if available) of all Globus tasks
    associated with a batch object. Uses Redis cache for multi-worker support.
    """

    cache_key = f"batch_status:{task_uuids_comma_separated}"

    # Try to get from Redis cache first
    result: dict[str, TaskStatus] | None = get_item_from_cache(cache_key)
    if result is not None:
        return result

    task_uuids = task_uuids_comma_separated.split(",")
    gcc = get_compute_client_from_globus_app()

    # TODO: Switch back to this when Globus added a fix for the Exceptions
    # return gcc.get_batch_result(task_uuids), "", 200

    result = {}
    task: TaskStatus
    for task_uuid in task_uuids:
        try:
            task = gcc.get_task(task_uuid)
        except TaskExecutionFailed as e:
            result[task_uuid] = {
                "pending": False,
                "status": "failed",
                "error": str(e),
                "result": None,
            }
        else:
            result[task_uuid] = {
                "pending": task["pending"],
                "status": task["status"],
                "result": unwrap_json(task.get("result", None)),
                "error": None,
            }

    # Cache successful result for 30 seconds
    cache_item(cache_key, result, ttl=30)
    return result


def unwrap_json(raw: Any) -> Any:
    """
    Best effort to deserialize a JSON or python literal expression
    """
    if not isinstance(raw, str):
        return raw

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    try:
        return ast.literal_eval(raw)
    except:
        return raw
