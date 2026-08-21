# API Reference

The FIRST Inference Gateway provides an OpenAI-compatible API.

## Base URL

```
http://your-gateway-domain:8000/resource_server
```

## Authentication

All requests require a Globus access token in the Authorization header:

```http
Authorization: Bearer <globus-access-token>
```

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
        "separate_output": true
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

### Batch Processing

```http
POST /v1/batches
GET /v1/batches/{batch_id}
```

For detailed API documentation, refer to the [OpenAI API Reference](https://platform.openai.com/docs/api-reference) as FIRST follows the same schema.

## Request Parameters

See the [User Guide](../user-guide/index.md#request-parameters) for detailed parameter documentation.
