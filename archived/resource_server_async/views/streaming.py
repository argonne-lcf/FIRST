import json
import logging

from django.http import HttpRequest, JsonResponse
from ninja import Router

from ..streaming import (
    decode_request_body,
    set_streaming_error,
    set_streaming_metadata,
    set_streaming_status,
    store_streaming_data,
    validate_streaming_request_security,
)

router = Router()
log = logging.getLogger(__name__)


# Streaming server endpoints (integrated into Django)
@router.post("/api/streaming/data/", auth=None, throttle=[])
async def receive_streaming_data(request: HttpRequest) -> JsonResponse:
    """Receive streaming data from vLLM function - INTERNAL ONLY

    Security layers (optimized with caching):
    1. Content-Length validation (DoS prevention)
    2. Global shared secret validation
    3. Per-task token validation (cached)
    4. Data size validation
    """

    # Validate all security requirements
    is_valid, error_response, status_code = validate_streaming_request_security(
        request, max_content_length=150000
    )
    if not is_valid:
        # Try to extract task_id to record auth failure
        try:
            data = json.loads(decode_request_body(request))
            task_id = data.get("task_id")
            if task_id and status_code in [401, 403]:
                set_streaming_metadata(task_id, "auth_failure", "true", ttl=60)
                log.warning(
                    f"Authentication failure recorded for streaming task {task_id}"
                )
        except Exception:
            pass  # Don't fail the error response if we can't record the failure
        return JsonResponse(error_response, status=status_code)

    try:
        data = json.loads(decode_request_body(request))
        task_id = data.get("task_id")
        chunk_data = data.get("data")

        if chunk_data is None:
            return JsonResponse({"error": "Missing data"}, status=400)

        if "\n" in chunk_data:
            # Split batched chunks and store each one
            chunks = chunk_data.split("\n")
            for individual_chunk in chunks:
                if individual_chunk.strip():
                    store_streaming_data(task_id, individual_chunk.strip())
        else:
            store_streaming_data(task_id, chunk_data)

        set_streaming_status(task_id, "streaming")

        return JsonResponse({"status": "received"})

    except Exception as e:
        log.error(f"Error in streaming data endpoint: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/api/streaming/error/", auth=None, throttle=[])
async def receive_streaming_error(request: HttpRequest) -> JsonResponse:
    """Receive error from vLLM function - INTERNAL ONLY - P0 OPTIMIZED

    Security layers (optimized with caching):
    1. Content-Length validation (DoS prevention)
    2. Global shared secret validation
    3. Per-task token validation (cached)
    """

    # Validate all security requirements
    is_valid, error_response, status_code = validate_streaming_request_security(
        request, max_content_length=15000
    )
    if not is_valid:
        # Try to extract task_id to record auth failure
        try:
            data = json.loads(decode_request_body(request))
            task_id = data.get("task_id")
            if task_id and status_code in [401, 403]:
                set_streaming_metadata(task_id, "auth_failure", "true", ttl=60)
                log.warning(
                    f"Authentication failure recorded for streaming task {task_id}"
                )
        except Exception:
            pass  # Don't fail the error response if we can't record the failure
        return JsonResponse(error_response, status=status_code)

    try:
        data = json.loads(decode_request_body(request))
        task_id = data.get("task_id")
        error = data.get("error")

        if error is None:
            return JsonResponse({"error": "Missing error"}, status=400)

        # Store error with automatic cleanup
        set_streaming_error(task_id, error)
        set_streaming_status(task_id, "error")

        log.error(f"Received error for task {task_id}: {error}")
        return JsonResponse({"status": "ok", "task_id": task_id})

    except Exception as e:
        log.error(f"Error receiving streaming error: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)


@router.post("/api/streaming/done/", auth=None, throttle=[])
async def receive_streaming_done(request: HttpRequest) -> JsonResponse:
    """Receive completion signal from vLLM function - INTERNAL ONLY - P0 OPTIMIZED

    Security layers (optimized with caching):
    1. Content-Length validation (DoS prevention)
    2. Global shared secret validation
    3. Per-task token validation (cached)
    """

    # Validate all security requirements
    is_valid, error_response, status_code = validate_streaming_request_security(
        request, max_content_length=15000
    )
    if not is_valid:
        # Try to extract task_id to record auth failure
        try:
            data = json.loads(decode_request_body(request))
            task_id = data.get("task_id")
            if task_id and status_code in [401, 403]:
                set_streaming_metadata(task_id, "auth_failure", "true", ttl=60)
                log.warning(
                    f"Authentication failure recorded for streaming task {task_id}"
                )
        except Exception:
            pass  # Don't fail the error response if we can't record the failure
        return JsonResponse(error_response, status=status_code)

    try:
        data = json.loads(decode_request_body(request))
        task_id = data.get("task_id")

        # Mark as completed with automatic cleanup
        set_streaming_status(task_id, "completed")

        log.info(f"Completed streaming task: {task_id}")
        return JsonResponse({"status": "ok", "task_id": task_id})

    except Exception as e:
        log.error(f"Error receiving streaming done: {e}")
        return JsonResponse({"error": "Internal server error"}, status=500)
