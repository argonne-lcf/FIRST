import asyncio
import hmac
import json
import re
import secrets
import time
import uuid
from logging import getLogger
from typing import Any

from cachetools import TTLCache
from django.conf import settings
from django.core.cache import cache
from django.http import HttpRequest

from .cache import get_redis_client
from .logging import RequestContext
from .schemas.structured_logs import UsageTokens

logger = getLogger(__name__)

_validation_cache: TTLCache[str, bool] = TTLCache(maxsize=10000, ttl=300)


def extract_status_code_from_error(error_message: str) -> int:
    """Extract status code from error message for database logging"""

    try:
        # Look for explicit status codes in error message
        if "status code:" in error_message:
            match = re.search(r"status code[:\s]+(\d+)", error_message)
            if match:
                return int(match.group(1))

        # Look for status codes in JSON error objects
        if '"code"' in error_message:
            code_match = re.search(r'"code"\s*:\s*(\d+)', error_message)
            if code_match:
                return int(code_match.group(1))

        # Common error patterns
        if (
            "max_tokens must be at least" in error_message
            or "maximum context length" in error_message
        ):
            return 400  # Bad request
        elif (
            "unauthorized" in error_message.lower()
            or "authentication" in error_message.lower()
        ):
            return 401
        elif (
            "forbidden" in error_message.lower()
            or "permission" in error_message.lower()
        ):
            return 403
        elif "not found" in error_message.lower():
            return 404
        elif (
            "rate limit" in error_message.lower()
            or "too many requests" in error_message.lower()
        ):
            return 429

        # Default to 500 for unknown errors
        return 500

    except:
        return 500


def _get_cache_key(key_type: str, task_id: str) -> str:
    """Get cache key for streaming data (Django cache uses Redis in production)"""
    return f"stream:{key_type}:{task_id}"


def _cache_set(task_id: str, key_type: str, value: str, ttl: int = 3600) -> None:
    """Generic cache set - uses Django cache (which is Redis in production)"""
    try:
        key = _get_cache_key(key_type, task_id)
        cache.set(key, value, ttl)
    except Exception as e:
        logger.error(f"Error setting streaming {key_type} for task {task_id}: {e}")


def _cache_get(task_id: str, key_type: str) -> Any:
    """Generic cache get - uses Django cache (which is Redis in production)"""
    try:
        key = _get_cache_key(key_type, task_id)
        return cache.get(key)
    except Exception as e:
        logger.error(f"Error getting streaming {key_type} for task {task_id}: {e}")
        return None


def store_streaming_data(task_id: str, chunk_data: str, ttl: int = 600) -> None:
    """Store streaming chunk using Redis LIST (lpush for ordering)"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key("data", task_id)
            redis_client.lpush(key, chunk_data)
            redis_client.expire(key, ttl)
        else:
            # Fallback: store as regular list in cache (less efficient)
            key = _get_cache_key("data", task_id)
            existing = cache.get(key, [])
            existing.append(chunk_data)
            cache.set(key, existing, ttl)
    except Exception as e:
        logger.error(f"Error storing streaming data for task {task_id}: {e}")


def get_streaming_data(task_id: str) -> list[str]:
    """Get all streaming chunks using Redis LIST (lrange)"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key("data", task_id)
            chunks: list[str | bytes] = redis_client.lrange(key, 0, -1)  # type: ignore
            return [
                chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
                for chunk in reversed(chunks)
            ]
        else:
            # Fallback: retrieve from cache as regular list
            key = _get_cache_key("data", task_id)
            return cache.get(key, [])  # type: ignore[no-any-return]
    except Exception as e:
        logger.error(f"Error getting streaming data for task {task_id}: {e}")
        return []


def set_streaming_metadata(
    task_id: str, metadata_type: str, value: str, ttl: int = 3600
) -> None:
    """Set streaming metadata - use direct Redis for consistency with batch operations"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key(metadata_type, task_id)
            redis_client.setex(key, ttl, value)
        else:
            # Fallback to Django cache
            _cache_set(task_id, metadata_type, value, ttl)
    except Exception as e:
        logger.error(f"Error setting streaming {metadata_type} for task {task_id}: {e}")


def get_streaming_metadata(task_id: str, metadata_type: str) -> Any:
    """Get streaming metadata - use direct Redis for consistency with batch operations"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key(metadata_type, task_id)
            value = redis_client.get(key)
            return value.decode("utf-8") if isinstance(value, bytes) else value
        else:
            # Fallback to Django cache
            return _cache_get(task_id, metadata_type)
    except Exception as e:
        logger.error(f"Error getting streaming {metadata_type} for task {task_id}: {e}")
        return None


def set_streaming_status(task_id: str, status: str, ttl: int = 3600) -> None:
    """Set streaming status"""
    set_streaming_metadata(task_id, "status", status, ttl)


def get_streaming_status(task_id: str) -> Any:
    """Get streaming status"""
    return get_streaming_metadata(task_id, "status")


def set_streaming_error(task_id: str, error: str, ttl: int = 3600) -> None:
    """Set streaming error"""
    set_streaming_metadata(task_id, "error", error, ttl)


def get_streaming_error(task_id: str) -> Any:
    """Get streaming error"""
    return get_streaming_metadata(task_id, "error")


def generate_and_store_streaming_token(task_id: str, ttl: int = 600) -> str:
    """Generate and store authentication token (256 bits entropy) - use direct Redis"""
    token = secrets.token_urlsafe(32)  # 32 bytes = 256 bits
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key("token", task_id)
            redis_client.setex(key, ttl, token)
        else:
            _cache_set(task_id, "token", token, ttl)
    except Exception as e:
        logger.error(f"Error storing token for task {task_id}: {e}")
    logger.debug(f"Generated and stored streaming token for task {task_id}")
    return token


def validate_streaming_task_token(task_id: str, provided_token: str) -> bool:
    """Validate task token (constant-time comparison) - use direct Redis"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key("token", task_id)
            stored_token = redis_client.get(key)
            stored_token = str(
                stored_token.decode("utf-8")
                if isinstance(stored_token, bytes)
                else stored_token
            )
        else:
            stored_token = _cache_get(task_id, "token")

        if stored_token:
            is_valid = hmac.compare_digest(stored_token, provided_token)
            if not is_valid:
                logger.warning(f"Invalid token provided for task {task_id}")
            return is_valid

        logger.warning(f"No stored token found for task {task_id}")
        return False
    except Exception as e:
        logger.error(f"Error validating streaming token for task {task_id}: {e}")
        return False


def validate_streaming_request_optimized(
    task_id: str, provided_token: str
) -> tuple[bool, str | None]:
    """Validate streaming request with caching. Returns (is_valid, error_message)"""
    # Check in-memory cache first
    cache_key = f"{task_id}:{provided_token[:16]}"
    try:
        if cache_key in _validation_cache:
            is_valid = _validation_cache[cache_key]
            return (
                (True, None)
                if is_valid
                else (False, "Invalid or expired task authentication")
            )
    except Exception:
        pass

    # Validate task_id format (UUID)
    try:
        uuid.UUID(task_id)
    except ValueError:
        return False, "Invalid task_id format"

    # Validate token (also checks if task exists)
    try:
        is_valid = validate_streaming_task_token(task_id, provided_token)

        # Cache the result
        try:
            _validation_cache[cache_key] = is_valid
        except Exception as e:
            logger.warning(f"Failed to cache validation result: {e}")

        if is_valid:
            return True, None
        return False, "Invalid task authentication token"

    except Exception as e:
        logger.error(f"Error in validation: {e}")
        return False, f"Validation error: {str(e)}"


def decode_request_body(request: HttpRequest) -> str:
    """
    Safely decode request.body to string, handling both bytes and str.

    Django Ninja can return either bytes or str depending on context.

    Args:
        request: Django request object

    Returns:
        str: Decoded body as string
    """
    body = request.body
    if isinstance(body, bytes):
        return body.decode("utf-8")
    return body  # type: ignore[unreachable]


def validate_streaming_request_security(
    request: HttpRequest, max_content_length: int = 150000
) -> tuple[bool, dict[str, Any] | None, int | None]:
    """
    Validate security requirements for streaming API endpoints.
    Checks Content-Length, X-Internal-Secret, and X-Stream-Task-Token.

    Args:
        request: Django request object
        max_content_length: Maximum allowed content length in bytes

    Returns:
        (is_valid, error_response_dict, status_code) tuple
        - is_valid: True if all checks pass, False otherwise
        - error_response_dict: Dict with error details if validation fails, None if valid
        - status_code: HTTP status code for error response, None if valid
    """

    # SECURITY LAYER 1 - Validate Content-Length BEFORE parsing
    content_length = request.headers.get("Content-Length")
    if content_length:
        try:
            if int(content_length) > max_content_length:
                logger.warning(
                    f"Streaming request exceeded size limit: {content_length} bytes (max: {max_content_length})"
                )
                return False, {"error": "Request too large"}, 413
        except ValueError:
            pass  # Invalid Content-Length, let parsing catch it

    # SECURITY LAYER 2: Validate global internal secret
    internal_secret = request.headers.get("X-Internal-Secret", "")
    expected_secret = getattr(
        settings, "INTERNAL_STREAMING_SECRET", "default-secret-change-me"
    )
    if internal_secret != expected_secret:
        logger.warning("Streaming request with invalid internal secret")
        return False, {"error": "Unauthorized: Invalid internal secret"}, 401

    # SECURITY LAYER 3: Validate per-task token
    task_token = request.headers.get("X-Stream-Task-Token", "")
    if not task_token:
        logger.warning("Streaming request missing task token")
        return False, {"error": "Unauthorized: Missing task token"}, 401

    # Parse request body to get task_id for token validation
    try:
        data = json.loads(decode_request_body(request))
        task_id = data.get("task_id")

        if not task_id:
            return False, {"error": "Missing task_id"}, 400

        # Validate the task token using optimized validation
        is_valid, error_msg = validate_streaming_request_optimized(task_id, task_token)
        if not is_valid:
            logger.warning(
                f"Streaming validation failed for task {task_id}: {error_msg}"
            )
            return False, {"error": error_msg}, 403

        # All validation passed
        return True, None, None

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in streaming request: {e}")
        return False, {"error": "Invalid JSON"}, 400
    except Exception as e:
        logger.error(f"Error validating streaming request: {e}")
        return False, {"error": "Internal server error"}, 500


def get_streaming_data_and_status_batch(
    task_id: str,
) -> tuple[list[str], str | None, str | None]:
    """Get data, status, and error in single Redis pipeline. Returns (chunks, status, error)"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            # Use Redis pipeline for optimal performance
            pipe = redis_client.pipeline()
            pipe.lrange(_get_cache_key("data", task_id), 0, -1)
            pipe.get(_get_cache_key("status", task_id))
            pipe.get(_get_cache_key("error", task_id))

            results = pipe.execute()

            # Process results with byte decoding
            chunks = (
                [
                    chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk
                    for chunk in reversed(results[0])
                ]
                if results[0]
                else []
            )
            status = (
                results[1].decode("utf-8")
                if isinstance(results[1], bytes)
                else results[1]
            )
            error = (
                results[2].decode("utf-8")
                if isinstance(results[2], bytes)
                else results[2]
            )

            return chunks, status, error
        else:
            # Fallback to sequential operations using Django cache
            return (
                get_streaming_data(task_id),
                get_streaming_status(task_id),
                get_streaming_error(task_id),
            )

    except Exception as e:
        logger.error(f"Error in batched streaming retrieval for task {task_id}: {e}")
        return [], None, None


def store_streaming_data_batch(
    task_id: str, chunk_list: list[str], ttl: int = 3600
) -> None:
    """Store multiple chunks in single Redis pipeline"""
    try:
        redis_client = get_redis_client()
        if redis_client:
            key = _get_cache_key("data", task_id)
            pipe = redis_client.pipeline()
            for chunk_data in chunk_list:
                pipe.lpush(key, chunk_data)
            pipe.expire(key, ttl)
            pipe.execute()
        else:
            # Fallback to sequential operations using Django cache
            for chunk_data in chunk_list:
                store_streaming_data(task_id, chunk_data, ttl)
    except Exception as e:
        logger.error(f"Error storing batched streaming data for task {task_id}: {e}")


def prepare_streaming_task_data(
    data: dict[str, Any], stream_task_id: str
) -> dict[str, Any]:
    """Prepare streaming task data with server config and auth token"""
    stream_server_host = getattr(
        settings, "STREAMING_SERVER_HOST", "data-portal-dev.cels.anl.gov"
    )
    stream_server_port = getattr(settings, "STREAMING_SERVER_PORT", 443)
    stream_server_protocol = getattr(settings, "STREAMING_SERVER_PROTOCOL", "https")

    task_token = generate_and_store_streaming_token(stream_task_id)
    data["model_params"].update(
        {
            "streaming_server_host": stream_server_host,
            "streaming_server_port": stream_server_port,
            "streaming_server_protocol": stream_server_protocol,
            "stream_task_id": stream_task_id,
            "stream_task_token": task_token,
        }
    )

    return data


def create_streaming_response_headers() -> dict[str, str]:
    """Create standard headers for SSE streaming responses"""
    return {
        "Cache-Control": "no-cache",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Cache-Control",
    }


def format_streaming_error_for_openai(error_message: str) -> str:
    """Pass through JSON errors as-is, minimal processing for non-JSON errors"""

    try:
        # Try to parse if it's already a JSON error from vLLM
        if error_message.strip().startswith("{") and error_message.strip().endswith(
            "}"
        ):
            try:
                parsed_error = json.loads(error_message)
                if "object" in parsed_error and parsed_error["object"] == "error":
                    # Already in OpenAI error format, return as-is
                    return f"data: {json.dumps(parsed_error)}\n\n"
            except json.JSONDecodeError:
                pass

        # Look for JSON error in "Response text:" sections and extract it as-is
        response_text_match = re.search(
            r"Response text[:\s]*(\{.*?\})", error_message, re.DOTALL
        )
        if response_text_match:
            try:
                json_error = response_text_match.group(1)
                parsed_error = json.loads(json_error)
                if "object" in parsed_error and parsed_error["object"] == "error":
                    # Found a valid JSON error, return it as-is
                    return f"data: {json.dumps(parsed_error)}\n\n"
            except json.JSONDecodeError:
                pass

        # Fallback for non-JSON errors - minimal generic error
        fallback_error = {
            "object": "error",
            "message": "An error occurred during processing",
            "type": "InternalServerError",
            "param": None,
            "code": 500,
        }
        return f"data: {json.dumps(fallback_error)}\n\n"

    except Exception:
        # Ultimate fallback
        fallback_error = {
            "object": "error",
            "message": "An error occurred during processing",
            "type": "InternalServerError",
            "param": None,
            "code": 500,
        }
        return f"data: {json.dumps(fallback_error)}\n\n"


def collect_and_aggregate_streaming_content(
    task_id: str, original_prompt: str | list[str | dict[str, Any]] | None = None
) -> dict[str, Any] | None:
    """Collect all streaming content and create a complete response"""
    chunks = get_streaming_data(task_id)
    if not chunks:
        return None

    try:
        # Reconstruct the complete streaming response
        full_content = ""
        usage_info: dict[str, Any] = {}
        model_info = {}
        finish_reason = None
        content_chunks = 0

        for chunk in chunks:
            if chunk.startswith("data: "):
                chunk_data = chunk[6:]  # Remove "data: " prefix
                if chunk_data.strip() == "[DONE]":
                    continue

                try:
                    parsed_chunk = json.loads(chunk_data)

                    # Collect usage info (usually in the last chunk or special chunks)
                    if "usage" in parsed_chunk and isinstance(
                        parsed_chunk["usage"], dict
                    ):
                        usage_info.update(parsed_chunk["usage"])

                    # Collect model info (from first chunk usually)
                    if "model" in parsed_chunk:
                        model_info["model"] = parsed_chunk["model"]
                    if "id" in parsed_chunk:
                        model_info["id"] = parsed_chunk["id"]
                    if "object" in parsed_chunk:
                        model_info["object"] = parsed_chunk["object"]
                    if "created" in parsed_chunk:
                        model_info["created"] = parsed_chunk["created"]

                    # Collect content from streaming chunks
                    choices = parsed_chunk.get("choices", [])
                    if choices and len(choices) > 0:
                        choice = choices[0]

                        # For streaming responses, content is in delta
                        delta = choice.get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            full_content += content
                            content_chunks += 1

                        # Check for finish reason (in final chunks)
                        if "finish_reason" in choice and choice["finish_reason"]:
                            finish_reason = choice["finish_reason"]

                except json.JSONDecodeError:
                    continue

        # If no usage info was captured from chunks, estimate from content
        if not usage_info or not usage_info.get("total_tokens", 0):
            # Enhanced token estimation using multiple methods
            char_estimate = len(full_content) // 4  # ~4 chars per token
            word_estimate = len(full_content.split()) * 1.3  # ~1.3 tokens per word

            # Use average of methods for better accuracy
            estimated_completion_tokens = int((char_estimate + word_estimate) / 2)
            estimated_completion_tokens = max(1, estimated_completion_tokens)

            # Estimate prompt tokens more accurately if we have the original prompt
            estimated_prompt_tokens = 50  # Conservative default
            if original_prompt:
                try:
                    if isinstance(original_prompt, str):
                        prompt_text = original_prompt
                    elif isinstance(original_prompt, list):
                        # Handle messages format - extract all content
                        prompt_parts: list[str] = []
                        for msg in original_prompt:
                            if isinstance(msg, dict) and msg.get("content"):
                                prompt_parts.append(msg["content"])
                        prompt_text = " ".join(prompt_parts)
                    else:
                        prompt_text = str(original_prompt)  # type: ignore[unreachable]

                    # Better prompt token estimation using same dual method
                    prompt_char_estimate = len(prompt_text) // 4
                    prompt_word_estimate = len(prompt_text.split()) * 1.3
                    estimated_prompt_tokens = int(
                        (prompt_char_estimate + prompt_word_estimate) / 2
                    )
                    estimated_prompt_tokens = max(10, estimated_prompt_tokens)

                    logger.info(
                        f"Prompt token estimation for {task_id}: {estimated_prompt_tokens} tokens from {len(prompt_text)} chars"
                    )
                except Exception as e:
                    logger.warning(f"Error parsing prompt for token estimation: {e}")

            usage_info = {
                "prompt_tokens": estimated_prompt_tokens,
                "completion_tokens": estimated_completion_tokens,
                "total_tokens": estimated_prompt_tokens + estimated_completion_tokens,
                "prompt_tokens_details": None,
            }
            logger.info(
                f"Token estimation for {task_id}: {usage_info['total_tokens']} total ({usage_info['completion_tokens']} completion, {usage_info['prompt_tokens']} prompt)"
            )

        # Ensure we have the correct object type for a complete response (not chunk)
        model_info["object"] = "chat.completion"  # Always set to completion, not chunk

        # Create a complete response in authentic OpenAI/vLLM streaming format
        # Only include fields that are actually provided by vLLM/OpenAI streaming
        complete_response: dict[str, Any] = {
            **model_info,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": full_content},
                    "finish_reason": finish_reason or "stop",
                }
            ],
            "usage": usage_info,
        }

        return complete_response

    except Exception as e:
        logger.error(f"Error aggregating streaming content: {e}")
        return None


async def update_streaming_log_async(
    context: RequestContext,
    final_metrics: dict[str, Any],
    complete_response: dict[str, Any] | None,
    stream_task_id: str | None = None,
) -> None:
    """
    Asynchronously update streaming log entry with final content
    """

    if not context.request_log:
        return

    usage = UsageTokens()
    streaming_error = None
    response_status = 200
    result = None

    try:
        # Check if there was a streaming error
        if final_metrics.get("final_status") == "error" and stream_task_id:
            # Get the actual error message
            streaming_error = get_streaming_error(stream_task_id)
            if streaming_error:
                # Extract status code using the simple utility function
                response_status = extract_status_code_from_error(streaming_error)

        if complete_response and not streaming_error:
            usage.total_tokens = complete_response.get("usage", {}).get(
                "total_tokens", 0
            )
            result = json.dumps(complete_response)

        elif streaming_error:
            # Handle error case - store the full original error message
            error_response = {
                "streaming_response": True,
                "error": True,
                "error_message": streaming_error,  # Store full original error
                "response_time": final_metrics.get("total_processing_time", 0),
                "throughput_tokens_per_second": 0,
                "status": "failed",
            }
            result = json.dumps(error_response, indent=4)
        else:
            # Fallback if we couldn't reconstruct the response
            result = json.dumps(
                {
                    "streaming_response": True,
                    "error": "Could not reconstruct complete response",
                    "metrics": final_metrics,
                    "response_time": final_metrics.get("total_processing_time", 0),
                    "throughput_tokens_per_second": 0,
                },
            )

        context.request_log.emit(result, response_status)
        await context.request_log.emit_metrics(usage)

    except Exception as e:
        logger.error(
            f"Error updating streaming log entry {context.request_log.id}: {e}",
            exc_info=True,
        )


def cleanup_streaming_data(task_id: str) -> None:
    """Clean up all streaming data for a task"""
    try:
        redis_client = get_redis_client()
        key_types = ["data", "status", "error", "token"]

        if redis_client:
            # Batch delete all Redis keys (more efficient than individual deletes)
            keys = [_get_cache_key(kt, task_id) for kt in key_types]
            redis_client.delete(*keys)
        else:
            # Fallback to Django cache delete
            for key_type in key_types:
                cache.delete(_get_cache_key(key_type, task_id))

        logger.debug(f"Cleaned up streaming data for task {task_id}")
    except Exception as e:
        logger.error(f"Error cleaning up streaming data for task {task_id}: {e}")


async def process_streaming_completion_async(
    task_id: str,
    stream_task_id: str,
    context: RequestContext,
    start_time: float,
    original_prompt: str | list[str | dict[str, Any]] | None = None,
) -> None:
    """Background task to process streaming completion and update database"""

    try:
        # Wait a bit for initial streaming data to arrive
        await asyncio.sleep(2)

        # Wait for streaming to complete
        max_wait = 300  # Wait up to 5 minutes for streaming completion
        wait_start = time.time()
        while time.time() - wait_start < max_wait:
            status = get_streaming_status(stream_task_id)
            if status in ["completed", "error"]:
                break
            await asyncio.sleep(0.5)

        # Collect final streaming data
        end_time = time.time()
        complete_response = collect_and_aggregate_streaming_content(
            stream_task_id, original_prompt
        )

        # Simple metrics
        simple_metrics = {
            "total_processing_time": end_time - start_time,
            "final_status": get_streaming_status(stream_task_id) or "completed",
        }

        # Update the database log entry with final data
        await update_streaming_log_async(
            context, simple_metrics, complete_response, stream_task_id
        )

        # Wait a moment before cleanup to ensure SSE generator reads the "completed" status
        # The SSE generator polls every 25ms, so 500ms gives plenty of time
        await asyncio.sleep(0.5)

        # Clean up streaming data from cache
        cleanup_streaming_data(stream_task_id)

        logger.info(f"Completed streaming processing for task {task_id}")

    except Exception as e:
        logger.error(f"Error in streaming completion processing: {e}")
        try:
            # Try to update log with error info
            await update_streaming_log_async(
                context,
                {"error": str(e), "final_status": "error"},
                None,
                stream_task_id,
            )
        except:
            pass
