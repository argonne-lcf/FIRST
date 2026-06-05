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

### Batch Processing

```http
POST /v1/batches
GET /v1/batches/{batch_id}
```

For detailed API documentation, refer to the [OpenAI API Reference](https://platform.openai.com/docs/api-reference) as FIRST follows the same schema.

## Request Parameters

See the [User Guide](../user-guide/index.md#request-parameters) for detailed parameter documentation.

