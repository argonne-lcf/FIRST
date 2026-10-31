# API Reference

The FIRST Inference Gateway provides an OpenAI-compatible API.

## Base URL

```
http://your-gateway-domain:8000/resource_server
```

## Authentication

Requests require a Globus access token. Use `Authorization: Bearer` for the
OpenAI-compatible routes. The Anthropic Messages route also accepts the same
Globus token in `x-api-key`:

```http
Authorization: Bearer <globus-access-token>
```

```http
POST /{cluster}/{framework}/v1/messages
x-api-key: <globus-access-token>
anthropic-version: 2023-06-01
```

Both headers use the same Globus token validation and authorization checks.

## Endpoints

### Chat Completions

```http
POST /v1/chat/completions
POST /{cluster}/{framework}/v1/chat/completions
```

### Completions

```http
POST /v1/completions
POST /{cluster}/{framework}/v1/completions
```

### Model Metadata

```http
GET /{cluster}/models
GET /{cluster}/models?model_id={model}
```

The response contains only models the authenticated caller is authorized to
use. Every model includes its identifier, cluster, and framework. Deployments
may also expose explicitly allowlisted public metadata from the endpoint
fixture, such as a display name, description, and a versioned capabilities
object:

```json
[
  {
    "id": "example-model",
    "object": "model",
    "cluster": "example-cluster",
    "framework": "api",
    "display_name": "Example Model",
    "description": "Example model served through FIRST",
    "capabilities": {
      "schema_version": 1,
      "api_protocols": ["chat_completions", "responses"],
      "context_window_tokens": 131072,
      "input_modalities": ["text"],
      "streaming": true,
      "reasoning": {
        "supported": true,
        "separate_output": true,
        "effort_levels": ["low", "medium", "high", "xhigh"],
        "default_effort": "xhigh"
      },
      "tool_calling": {
        "supported": true
      }
    }
  }
]
```

Capability metadata describes the deployed model API. It is intentionally
client-neutral and must not contain backend connection details or secrets.

### Token Counting

For direct API-backed models whose cluster enables `messages` or `responses`,
FIRST forwards token-count requests to the corresponding model-server endpoint:

```http
POST /{cluster}/{framework}/v1/messages/count_tokens
POST /{cluster}/{framework}/v1/responses/input_tokens
```

The model server determines the tokenizer and returned token count. A backend
that does not implement these endpoints returns its own error response.

### Responses

Responses requests with `store: true` are stored by FIRST. Stored responses are
private to the authenticated caller and can be retrieved, continued, or
deleted:

```http
POST /{cluster}/{framework}/v1/responses
GET /{cluster}/{framework}/v1/responses/{response_id}
DELETE /{cluster}/{framework}/v1/responses/{response_id}
```

Pass `previous_response_id` to continue a stored response. FIRST supplies the
stored conversation context to the configured model server.

### Batch Processing

```http
POST /v1/batches
GET /v1/batches/{batch_id}
```

For detailed API documentation, refer to the [OpenAI API Reference](https://platform.openai.com/docs/api-reference) as FIRST follows the same schema.

## Request Parameters

See the [User Guide](../user-guide/index.md#request-parameters) for detailed parameter documentation.
