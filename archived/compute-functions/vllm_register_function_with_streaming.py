import globus_compute_sdk
import requests


def vllm_inference_function(parameters):
    import json
    import os
    import socket
    import time

    from requests.exceptions import RequestException

    # Constants
    DEFAULT_SECRET = "default-secret-change-me"
    STREAMING_DATA_TIMEOUT = 2  # seconds - fast timeout for data chunks
    STREAMING_CONTROL_TIMEOUT = 10  # seconds - for error/done messages
    VLLM_REQUEST_TIMEOUT = 120  # seconds - for health check requests
    BATCH_SIZE = 20  # Number of chunks to batch before sending
    BATCH_TIMEOUT = 0.5  # seconds - send batch if this time elapsed

    def get_proxy_config():
        """Get proxy configuration from environment variables"""
        proxies = {}
        if os.environ.get("http_proxy"):
            proxies["http"] = os.environ.get("http_proxy")
        if os.environ.get("https_proxy"):
            proxies["https"] = os.environ.get("https_proxy")
        return proxies

    def get_streaming_headers(task_token):
        """Generate headers for streaming server requests with authentication"""
        return {
            "Content-Type": "application/json",
            "X-Internal-Secret": os.environ.get(
                "INTERNAL_STREAMING_SECRET", DEFAULT_SECRET
            ),
            "X-Stream-Task-Token": task_token,
        }

    def get_or_create_session():
        """Get or create a shared session for streaming requests"""
        if not hasattr(get_or_create_session, "session"):
            session = requests.Session()
            session.proxies.update(get_proxy_config())
            get_or_create_session.session = session
        return get_or_create_session.session

    def send_to_streaming_server(
        host,
        port,
        protocol,
        endpoint,
        payload,
        task_token,
        timeout,
        use_fresh_session=False,
    ):
        """
        Generic function to send data to streaming server via HTTP.
        Optionally uses a fresh session (to avoid stale keep-alive sockets).
        """
        try:
            url = (
                f"{protocol}://{host}:{port}/resource_server/api/streaming/{endpoint}/"
            )
            headers = get_streaming_headers(task_token)

            # ---- PATCH: choose session strategy ----
            if use_fresh_session:
                # fresh session avoids idle keep-alive reuse
                s = requests.Session()
                s.proxies.update(get_proxy_config())
                session = s
            else:
                # reuse cached session (for frequent /data/ sends)
                session = get_or_create_session()
            # ----------------------------------------

            response = session.post(
                url, json=payload, headers=headers, timeout=timeout, verify=False
            )

            if use_fresh_session:
                session.close()  # close immediately to free socket

            if response.status_code == 200:
                return True, None
            else:
                error_msg = f"{endpoint.upper()} server returned {response.status_code}: {response.text}"
                return False, error_msg

        except requests.exceptions.Timeout as e:
            return False, f"Timeout contacting {endpoint} server: {e}"
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection error to {endpoint} server: {e}"
        except Exception as e:
            return False, f"Unexpected error in {endpoint}: {e}"

    def send_data_to_streaming_server(host, port, protocol, task_id, data, task_token):
        """Send streaming data (batched) to streaming server using shared session."""
        payload = {"task_id": task_id, "data": data, "type": "data"}
        success, error = send_to_streaming_server(
            host,
            port,
            protocol,
            "data",
            payload,
            task_token,
            STREAMING_DATA_TIMEOUT,
            use_fresh_session=False,  # reuse shared connection
        )
        if not success:
            print(f"[STREAMING] Failed to send /data/: {error}")
        return success

    def send_error_to_streaming_server(
        host, port, protocol, task_id, error, task_token
    ):
        """Send error message to streaming server using a fresh session."""
        payload = {"task_id": task_id, "error": error, "type": "error"}
        success, msg = send_to_streaming_server(
            host,
            port,
            protocol,
            "error",
            payload,
            task_token,
            STREAMING_CONTROL_TIMEOUT,
            use_fresh_session=True,  # new session to avoid stale socket
        )
        if not success:
            print(f"[STREAMING] X Failed to send /error/: {msg}")
        return success

    def send_done_to_streaming_server(host, port, protocol, task_id, task_token):
        """Send completion signal using a fresh session (avoids keep-alive issues)."""
        payload = {"task_id": task_id, "type": "done"}
        success, msg = send_to_streaming_server(
            host,
            port,
            protocol,
            "done",
            payload,
            task_token,
            STREAMING_CONTROL_TIMEOUT,
            use_fresh_session=True,
        )
        if success:
            return True
        return False

    def handle_non_streaming_request(
        url, headers, payload, start_time, is_health_check=False
    ):
        """Handle non-streaming requests (original logic)"""
        # For health checks, make GET request instead of POST
        if is_health_check:
            response = requests.get(
                url, headers=headers, verify=False, timeout=VLLM_REQUEST_TIMEOUT
            )
        else:
            # Make the POST request for regular endpoints
            response = requests.post(url, headers=headers, json=payload, verify=False)

        end_time = time.time()
        response_time = end_time - start_time

        # Initialize metrics
        metrics = {"response_time": response_time, "throughput_tokens_per_second": 0}

        # Handle different response scenarios
        if response.status_code == 200:
            try:
                # Try to parse JSON response
                completion = response.json()

                # Extract usage information if available
                usage = completion.get("usage", {})
                total_num_tokens = usage.get("total_tokens", 0)
                metrics["throughput_tokens_per_second"] = (
                    total_num_tokens / response_time if response_time > 0 else 0
                )

                # Return the response even if empty
                output = {**completion, **metrics}
                return json.dumps(output, indent=4)
            except json.JSONDecodeError:
                # If response is not JSON but status is 200, return the raw text
                return json.dumps({"completion": response.text, **metrics}, indent=4)
        else:
            # For non-200 responses, raise an exception with detailed error information
            error_msg = f"API request failed with status code: {response.status_code}\n"
            error_msg += f"Response text: {response.text}\n"
            error_msg += f"Response headers: {dict(response.headers)}"
            raise Exception(error_msg)

    def handle_streaming_request(url, headers, payload, start_time):
        """Handle streaming requests with real-time chunk streaming to streaming server"""
        # Get streaming server details from payload
        stream_server_host = payload.get("streaming_server_host")
        stream_server_port = payload.get("streaming_server_port")
        stream_server_protocol = payload.get("streaming_server_protocol", "https")
        stream_task_id = payload.get("stream_task_id")
        stream_task_token = payload.get("stream_task_token")

        # Validate required streaming parameters
        missing_params = []
        if not stream_server_host:
            missing_params.append("streaming_server_host")
        if not stream_server_port:
            missing_params.append("streaming_server_port")
        if not stream_task_id:
            missing_params.append("stream_task_id")
        if not stream_task_token:
            missing_params.append("stream_task_token")

        if missing_params:
            raise Exception(
                f"Streaming requires the following parameters: {', '.join(missing_params)}"
            )

        # Create clean payload for vLLM (remove streaming-specific parameters)
        vllm_payload = payload.copy()
        streaming_params = [
            "streaming_server_host",
            "streaming_server_port",
            "streaming_server_protocol",
            "stream_task_id",
            "stream_task_token",
        ]
        for param in streaming_params:
            vllm_payload.pop(param, None)

        try:
            # Make streaming request to vLLM with clean payload
            response = requests.post(
                url, headers=headers, json=vllm_payload, stream=True, verify=False
            )

            if response.status_code != 200:
                error_msg = f"API request failed with status code: {response.status_code}\nResponse text: {response.text}"
                # Send error to streaming server
                send_error_to_streaming_server(
                    stream_server_host,
                    stream_server_port,
                    stream_server_protocol,
                    stream_task_id,
                    error_msg,
                    stream_task_token,
                )
                raise Exception(error_msg)

            # Stream chunks in batched mode to streaming server
            streaming_chunks = []
            total_tokens = 0
            chunks_sent = 0
            failed_sends = 0

            # Batching variables
            batch_buffer = []
            last_send_time = time.time()

            # Process chunks as they arrive and send to streaming server
            for chunk in response.iter_lines():
                if chunk:
                    chunk_data = chunk.decode("utf-8")

                    # Handle completion marker
                    if chunk_data.strip() == "data: [DONE]":
                        # Send any remaining batched chunks
                        if batch_buffer:
                            batch_data = "\n".join(batch_buffer)
                            success = send_data_to_streaming_server(
                                stream_server_host,
                                stream_server_port,
                                stream_server_protocol,
                                stream_task_id,
                                batch_data,
                                stream_task_token,
                            )
                            if success:
                                chunks_sent += len(batch_buffer)
                            else:
                                failed_sends += len(batch_buffer)

                        # Send completion to streaming server
                        _ = send_done_to_streaming_server(
                            stream_server_host,
                            stream_server_port,
                            stream_server_protocol,
                            stream_task_id,
                            stream_task_token,
                        )

                        break
                    elif chunk_data.strip():
                        # Store raw chunk for metrics
                        streaming_chunks.append(chunk_data)
                        batch_buffer.append(chunk_data)

                        # Send batch when buffer is full or timeout reached
                        current_time = time.time()
                        should_send = (
                            len(batch_buffer) >= BATCH_SIZE
                            or (current_time - last_send_time) >= BATCH_TIMEOUT
                        )

                        if should_send:
                            batch_data = "\n".join(batch_buffer)
                            success = send_data_to_streaming_server(
                                stream_server_host,
                                stream_server_port,
                                stream_server_protocol,
                                stream_task_id,
                                batch_data,
                                stream_task_token,
                            )
                            if success:
                                chunks_sent += len(batch_buffer)
                            else:
                                failed_sends += len(batch_buffer)
                            batch_buffer = []
                            last_send_time = current_time

                        # Parse for metrics only (not for content extraction)
                        try:
                            # Remove 'data: ' prefix if present
                            json_str = chunk_data
                            if json_str.startswith("data: "):
                                json_str = json_str[6:]

                            parsed_chunk = json.loads(json_str)
                            # Extract token usage if available
                            if "usage" in parsed_chunk:
                                usage = parsed_chunk["usage"]
                                if "total_tokens" in usage:
                                    total_tokens = usage["total_tokens"]
                        except json.JSONDecodeError:
                            pass  # Skip chunks that can't be parsed

            # Calculate metrics
            end_time = time.time()
            response_time = end_time - start_time

            # Calculate throughput (tokens per second)
            throughput_tokens_per_second = (
                total_tokens / response_time if response_time > 0 else 0
            )

            # Return streaming result for Globus Compute
            result = {
                "streaming": True,
                "task_id": stream_task_id,
                "response_time": response_time,
                "throughput_tokens_per_second": throughput_tokens_per_second,
                "total_tokens": total_tokens,
                "status": "completed",
                "total_chunks": len(streaming_chunks),
                "chunks_sent_to_server": chunks_sent,
            }

            # Add warning if there were failed sends
            if failed_sends > 0:
                result["warning"] = (
                    f"{failed_sends} chunks failed to send to streaming server"
                )

            return json.dumps(result)

        except Exception as e:
            # Send error to streaming server
            try:
                send_error_to_streaming_server(
                    stream_server_host,
                    stream_server_port,
                    stream_server_protocol,
                    stream_task_id,
                    str(e),
                    stream_task_token,
                )
            except:
                pass  # Ignore errors when sending error notification

            # Calculate error metrics
            end_time = time.time()
            response_time = end_time - start_time

            # Return error result
            return json.dumps(
                {
                    "streaming": True,
                    "task_id": stream_task_id,
                    "response_time": response_time,
                    "throughput_tokens_per_second": 0,
                    "total_tokens": 0,
                    "status": "error",
                    "error": str(e),
                }
            )

    # Main function logic
    try:
        # Validate required parameters
        if "model_params" not in parameters:
            raise Exception("Missing required parameter: 'model_params'")

        model_params = parameters["model_params"]

        # Validate required model parameters
        if "openai_endpoint" not in model_params:
            raise Exception(
                "Missing required parameter: 'model_params.openai_endpoint'"
            )
        if "api_port" not in model_params:
            raise Exception("Missing required parameter: 'model_params.api_port'")

        # Determine the hostname
        hostname = socket.gethostname()
        os.environ["no_proxy"] = f"localhost,{hostname},127.0.0.1"

        # Get the API key from environment variable
        api_key = os.getenv("OPENAI_API_KEY", "random_api_key")

        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }

        # Determine the endpoint based on the URL parameter
        openai_endpoint = model_params.pop("openai_endpoint")

        # Determine the port based on the URL parameter
        api_port = model_params.pop("api_port")

        # Check if streaming is requested
        stream = model_params.get("stream", False)

        # Check if this is a health check endpoint
        is_health_check = "health" in openai_endpoint.lower()

        # For health checks, use root path without /v1/ prefix
        if is_health_check:
            base_url = f"https://127.0.0.1:{api_port}/"
            # Remove any ../ or v1/ prefixes from the endpoint
            clean_endpoint = openai_endpoint.replace("../", "").replace("v1/", "")
            url = base_url + clean_endpoint
        else:
            base_url = f"https://127.0.0.1:{api_port}/v1/"
            url = base_url + openai_endpoint

        # Prepare the payload
        payload = model_params.copy()

        start_time = time.time()

        if stream:
            # Handle streaming request
            return handle_streaming_request(url, headers, payload, start_time)
        else:
            # Handle non-streaming request (original logic)
            return handle_non_streaming_request(
                url, headers, payload, start_time, is_health_check
            )

    except RequestException as e:
        # Handle network-related errors
        error_msg = f"Network error occurred: {str(e)}"
        if "start_time" in locals():
            error_msg += f"\nResponse time: {time.time() - start_time}"
        raise Exception(error_msg)
    except KeyError as e:
        # Handle missing parameter errors
        error_msg = f"Missing required parameter: {str(e)}"
        raise Exception(error_msg)
    except Exception as e:
        # Handle any other unexpected errors
        error_msg = f"Unexpected error of type {type(e).__name__}: {str(e)}"
        if "start_time" in locals():
            error_msg += f"\nResponse time: {time.time() - start_time}"
        raise Exception(error_msg)


# Creating Globus Compute client
gcc = globus_compute_sdk.Client()

# # Register the function
COMPUTE_FUNCTION_ID = gcc.register_function(vllm_inference_function)

# # Write function UUID in a file
uuid_file_name = "vllm_register_function_sophia_streaming.txt"
with open(uuid_file_name, "w") as file:
    file.write(COMPUTE_FUNCTION_ID)
    file.write("\n")
file.close()

# # End of script
print("Function registered with UUID -", COMPUTE_FUNCTION_ID)
print("The UUID is stored in " + uuid_file_name + ".")
print("")

# Example calls

# List of sample prompts
# prompts = [
#         "Explain the concept of machine learning in simple terms.",
#         "What are the main differences between Python and JavaScript?",
#         "Write a short story about a robot learning to paint.",
#         "Describe the process of photosynthesis.",
#         "What are the key features of a good user interface design?"
# ]


# # # Chat completion example
# chat_out = vllm_inference_function({
#     'model_params': {
#         'openai_endpoint': 'chat/completions',
#         'model': 'meta-llama/Meta-Llama-3-8B-Instruct',
#         'api_port': 8001,
#         "messages": [{"role": "user", "content": random.choice(prompts)}],
#         'logprobs': True
#     }
# })
# print("Chat Completion Output for meta-llama/Meta-Llama-3-8B-Instruct")
# print(chat_out)


# # # Chat completion example
# chat_out = vllm_inference_function({
#     'model_params': {
#         'openai_endpoint': 'chat/completions',
#         'model': 'meta-llama/Meta-Llama-3-70B-Instruct',
#         'api_port': 8000,
#         "messages": [{"role": "user", "content": random.choice(prompts)}],
#         "messages": [{"role": "user", "content": random.choice(prompts)}],
#         'logprobs': True
#     }
# })
# print("Chat Completion Output for meta-llama/Meta-Llama-3-70B-Instruct")
# print(chat_out)

# # # Chat completion example
# chat_out = vllm_inference_function({
#     'model_params': {
#         'openai_endpoint': 'chat/completions',
#     'model': 'mistralai/Mistral-7B-Instruct-v0.3',
#         'api_port': 8002,
#         "messages": [{"role": "user", "content": random.choice(prompts)}],
#         'logprobs': True
#     }
# })
# print("Chat Completion Output for meta-llama/Meta-Llama-3-8B-Instruct")
# print(chat_out)

# # Text completion example
# text_out = vllm_inference_function({
#     'model_params': {
#         'openai_endpoint': 'completions',
#         'model': 'meta-llama/Meta-Llama-3-8B-Instruct',
#         'temperature': 0.2,
#         'max_tokens': 150,
#         'prompt': "List all proteins that interact with RAD51",
#         'logprobs': True
#     }
# })
# print("\\nText Completion Output:")
# print(text_out)
