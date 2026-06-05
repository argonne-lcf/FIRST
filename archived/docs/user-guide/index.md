# Usage Guide

This guide explains how to use the FIRST Inference Gateway to access Large Language Models through an OpenAI-compatible API.

## Authentication

The Gateway uses Globus Auth for authentication. You need a valid access token to make requests.

### Getting Your Access Token

Use the provided authentication script:

#### First-Time Setup

```bash
# Authenticate (opens browser for Globus login)
python inference-auth-token.py authenticate
```

This stores refresh and access tokens locally (typically in `~/.globus/app/...`).

#### Getting an Access Token

```bash
# Retrieve your current valid access token
export MY_TOKEN=$(python inference-auth-token.py get_access_token)
echo "Token stored in MY_TOKEN environment variable."
```

The script automatically refreshes expired tokens using your stored refresh token.

#### Force Re-authentication

If you need to change accounts or encounter permission errors:

```bash
# Log out from Globus
# Visit: https://app.globus.org/logout

# Force re-authentication
python inference-auth-token.py authenticate --force
```

#### Token Validity

- **Access tokens**: Valid for 48 hours
- **Refresh tokens**: Valid for 6 months of inactivity
- Some institutions may enforce more frequent re-authentication (e.g., weekly)

---

## Making Inference Requests

The Gateway provides an OpenAI-compatible API with two main endpoints:

### Chat Completions Endpoint

For conversational interactions:

**Federated endpoint (routes across multiple backends):**

```bash
curl -X POST http://127.0.0.1:8000/resource_server/v1/chat/completions \
  -H "Authorization: Bearer $MY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "facebook/opt-125m",
    "messages": [
      {"role": "user", "content": "Explain the concept of Globus Compute in simple terms."}
    ],
    "max_tokens": 150
  }'
```

**Specific backend (targets a particular cluster/framework):**

```bash
curl -X POST http://127.0.0.1:8000/resource_server/local/vllm/v1/chat/completions \
  -H "Authorization: Bearer $MY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "facebook/opt-125m",
    "messages": [
      {"role": "user", "content": "Explain the concept of Globus Compute in simple terms."}
    ],
    "max_tokens": 150
  }'
```

### Completions Endpoint

For text completion:

```bash
curl -X POST http://127.0.0.1:8000/resource_server/v1/completions \
  -H "Authorization: Bearer $MY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "facebook/opt-125m",
    "prompt": "The future of AI is",
    "max_tokens": 100
  }'
```

### Streaming Responses

Both endpoints support streaming. Set `stream: true` in your request:

**Python example with streaming:**

```python
import openai

client = openai.OpenAI(
    base_url="http://localhost:8000/resource_server/v1",
    api_key=MY_TOKEN  # Your Globus access token
)

stream = client.chat.completions.create(
    model="facebook/opt-125m",
    messages=[{"role": "user", "content": "Explain quantum computing"}],
    stream=True
)

for chunk in stream:
    if chunk.choices[0].delta.content is not None:
        print(chunk.choices[0].delta.content, end="", flush=True)
```

### Testing Streaming with ngrok

For testing streaming with remote endpoints, use ngrok to create a secure tunnel:

1. **Install ngrok**: Visit [ngrok.com](https://ngrok.com/)

2. **Start tunnel**:
```bash
ngrok http 8000
```

3. **Update your test script** to use the ngrok URL:
```python
client = openai.OpenAI(
    base_url="https://your-ngrok-url.ngrok.io/resource_server/v1",
    api_key=access_token
)
```

---

## Using the OpenAI Python SDK

The Gateway is fully compatible with the OpenAI Python SDK:

```python
import openai

# Configure client to point to the Gateway
client = openai.OpenAI(
    base_url="http://localhost:8000/resource_server/v1",
    api_key=MY_TOKEN  # Your Globus access token
)

# Make a request
response = client.chat.completions.create(
    model="meta-llama/Meta-Llama-3-8B-Instruct",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "What is machine learning?"}
    ],
    max_tokens=200
)

print(response.choices[0].message.content)
```

---

## Available Models

To see available models, contact your Gateway administrator or check the deployment's model catalog.

Common model naming conventions:
- `facebook/opt-125m`, `facebook/opt-1.3b`, etc.
- `meta-llama/Meta-Llama-3-8B-Instruct`
- `openai/gpt-4o-mini` (if configured with OpenAI backend)

The exact models available depend on your deployment's configuration.

---

## Batch Processing

For large-scale inference workloads, the Gateway supports batch processing:

```bash
curl -X POST http://127.0.0.1:8000/resource_server/v1/batches \
  -H "Authorization: Bearer $MY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "input_file_path": "/path/to/input.jsonl",
    "output_folder_path": "/path/to/output/",
    "model": "meta-llama/Meta-Llama-3-8B-Instruct"
  }'
```

**Input file format (JSONL):**

```jsonl
{"messages": [{"role": "user", "content": "What is AI?"}], "max_tokens": 100}
{"messages": [{"role": "user", "content": "Explain ML"}], "max_tokens": 100}
```

**Check batch status:**

```bash
curl -X GET http://127.0.0.1:8000/resource_server/v1/batches/<batch_id> \
  -H "Authorization: Bearer $MY_TOKEN"
```

---

## Benchmarking

The repository includes a benchmark script for performance testing:

### Download Dataset

```bash
wget https://huggingface.co/datasets/anon8231489123/ShareGPT_Vicuna_unfiltered/resolve/main/ShareGPT_V3_unfiltered_cleaned_split.json -P examples/load-testing/
```

### Run Benchmark

```bash
python examples/load-testing/benchmark-serving.py \
    --backend vllm \
    --model facebook/opt-125m \
    --base-url http://127.0.0.1:8000/resource_server/v1/chat/completions \
    --dataset-name sharegpt \
    --dataset-path examples/load-testing/ShareGPT_V3_unfiltered_cleaned_split.json \
    --output-file benchmark_results.jsonl \
    --num-prompts 100
```

**Additional options:**
- `--request-rate`: Control request rate (requests per second)
- `--max-concurrency`: Limit concurrent requests
- `--disable-ssl-verification`: For testing with self-signed certificates
- `--disable-stream`: Test non-streaming mode

---

## Request Parameters

### Common Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `model` | string | Model identifier (required) |
| `messages` | array | List of message objects (chat endpoint) |
| `prompt` | string | Text prompt (completions endpoint) |
| `max_tokens` | integer | Maximum tokens to generate |
| `temperature` | float | Sampling temperature (0.0-2.0) |
| `top_p` | float | Nucleus sampling parameter |
| `stream` | boolean | Enable streaming responses |
| `stop` | string/array | Stop sequences |

### Example with Advanced Parameters

```bash
curl -X POST http://127.0.0.1:8000/resource_server/v1/chat/completions \
  -H "Authorization: Bearer $MY_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "meta-llama/Meta-Llama-3-8B-Instruct",
    "messages": [{"role": "user", "content": "Write a story"}],
    "max_tokens": 500,
    "temperature": 0.7,
    "top_p": 0.9,
    "stream": false,
    "stop": ["\n\n"]
  }'
```

---

## Error Handling

The Gateway returns standard HTTP status codes:

| Code | Meaning |
|------|---------|
| 200 | Success |
| 400 | Bad Request (invalid parameters) |
| 401 | Unauthorized (invalid or missing token) |
| 403 | Forbidden (insufficient permissions) |
| 404 | Not Found (model or endpoint not available) |
| 429 | Too Many Requests (rate limited) |
| 500 | Internal Server Error |
| 503 | Service Unavailable (backend offline) |

**Error response format:**

```json
{
  "error": {
    "message": "Model not found",
    "type": "invalid_request_error",
    "code": "model_not_found"
  }
}
```

---

## Best Practices

1. **Token Management**
   - Store tokens securely
   - Refresh tokens before they expire
   - Never commit tokens to version control

2. **Request Optimization**
   - Use appropriate `max_tokens` values
   - Implement retry logic with exponential backoff
   - Use streaming for long responses

3. **Rate Limiting**
   - Respect rate limits
   - Implement client-side throttling
   - Use batch processing for bulk operations

4. **Error Handling**
   - Handle network errors gracefully
   - Implement timeout logic
   - Log errors for debugging

---

## Support

For issues, questions, or feature requests:
- Check the [GitHub repository](https://github.com/auroraGPT-ANL/inference-gateway)
- Contact your Gateway administrator
- Review the [Administrator Setup Guide](../admin-guide/index.md) for deployment issues

