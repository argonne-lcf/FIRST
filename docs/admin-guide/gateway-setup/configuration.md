# Configuration Reference

This page documents all environment variables and configuration options for the FIRST Inference Gateway.

## Environment Variables

All configuration is done through environment variables, typically stored in a `.env` file. See [example environment file](https://github.com/argonne-lcf/FIRST/blob/main/env.example) to get started and see definition examples of all variables.

### Core Django Settings

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SECRET_KEY` | Yes | - | Django secret key for cryptographic signing |
| `DEBUG` | No | `False` | Enable debug mode (never use in production) |
| `ALLOWED_HOSTS` | Yes | - | Comma-separated list of allowed hostnames |
| `RUNNING_AUTOMATED_TEST_SUITE` | No | `False` | Set to `True` to skip Globus High Assurance policy checks (development/testing only) |
| `LOG_TO_STDOUT` | No | `False` | Set to `True` to output logs to stdout (useful for Docker) |

!!! danger "Security Warning"
    - Never use `DEBUG=True` in production! This exposes sensitive information.
    - Never use `RUNNING_AUTOMATED_TEST_SUITE=True` in production! This disables important security checks.

### Globus Authentication

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GLOBUS_APPLICATION_ID` | Yes | - | Service API application client UUID |
| `GLOBUS_APPLICATION_SECRET` | Yes | - | Service API application client secret |
| `SERVICE_ACCOUNT_ID` | Yes | - | Service Account application client UUID |
| `SERVICE_ACCOUNT_SECRET` | Yes | - | Service Account application client secret |
| `GLOBUS_GROUPS` | No | - | Space-separated UUIDs of allowed Globus groups |
| `AUTHORIZED_IDP_DOMAINS` | No | - | String field of authorized identity providers |
| `AUTHORIZED_GROUPS_PER_IDP` | No | - | JSON string of groups per IDP |
| `GLOBUS_POLICIES` | No | - | Space-separated policy UUIDs |

### Database Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `USE_SQLITE` | No | `False` | Quick toggle to use SQLite as the database backend (development only) |
| `POSTGRES_DB` | Yes | - | Database name |
| `POSTGRES_USER` | Yes | - | Database user |
| `POSTGRES_PASSWORD` | Yes | - | Database password |
| `PGHOST` | Yes | - | Database host (`postgres` for Docker, `localhost` for bare metal) |
| `PGPORT` | No | `5432` | Database port |
| `PGUSER` | Yes | - | Database user (can be same as POSTGRES_USER) |
| `PGPASSWORD` | Yes | - | Database password |
| `PGDATABASE` | Yes | - | Database name |

!!! tip "Docker Networking"
    When using Docker Compose, set `PGHOST=postgres` to use the container name.
    For bare metal, use `PGHOST=localhost` or the actual hostname.

### Redis Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `REDIS_URL` | No | - | Redis connection URL |
| `USE_REDIS_CACHE` | No | false | Whether Redis cache is enabled |

### Gateway Settings

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MAX_BATCHES_PER_USER` | No | `2` | Maximum concurrent batch jobs per user |
| `STREAMING_SERVER_HOST` | No | - | Internal streaming server host:port |
| `INTERNAL_STREAMING_SECRET` | No | - | Secret for internal streaming authentication |

### Metis (Direct API) Configuration

For direct OpenAI-compatible API connections:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `METIS_STATUS_URL` | No | - | URL to status manifest JSON |
| `METIS_API_TOKENS` | No | - | JSON object of endpoint_id -> API token |

Example:

```dotenv
METIS_STATUS_URL="https://example.com/status.json"
METIS_API_TOKENS='{"openai-prod": "sk-...", "anthropic-prod": "sk-ant-..."}'
```

### Monitoring (Optional)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GF_SECURITY_ADMIN_USER` | No | `admin` | Grafana admin username |
| `GF_SECURITY_ADMIN_PASSWORD` | No | `admin` | Grafana admin password |

### Qstat Endpoints (Optional)

For HPC cluster status monitoring:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOPHIA_QSTAT_ENDPOINT_UUID` | No | - | Endpoint UUID for qstat function |
| `SOPHIA_QSTAT_FUNCTION_UUID` | No | - | Function UUID for qstat |

## Fixture Configuration

Fixtures are JSON files that define available endpoints and models.

### Endpoint Fixture Format

Located at `fixtures/endpoints.json`:

```json
[
    {
        "model": "resource_server.endpoint",
        "pk": 1,
        "fields": {
            "endpoint_slug": "local-vllm-opt-125m",
            "cluster": "local",
            "framework": "vllm",
            "model": "facebook/opt-125m",
            "api_port": 8001,
            "endpoint_uuid": "<globus-compute-endpoint-uuid>",
            "function_uuid": "<globus-compute-function-uuid>",
            "batch_endpoint_uuid": "",
            "batch_function_uuid": "",
            "allowed_globus_groups": ""
        }
    }
]
```

### Federated Endpoint Fixture Format

Located at `fixtures/federated_endpoints.json`:

```json
[
    {
        "model": "resource_server.federatedendpoint",
        "pk": 1,
        "fields": {
            "name": "OPT 125M (Federated)",
            "slug": "federated-opt-125m",
            "target_model_name": "facebook/opt-125m",
            "description": "Federated access point",
            "targets": [
                {
                    "cluster": "local",
                    "framework": "vllm",
                    "model": "facebook/opt-125m",
                    "endpoint_slug": "local-vllm-opt-125m",
                    "endpoint_uuid": "<endpoint-uuid>",
                    "function_uuid": "<function-uuid>",
                    "api_port": 8001
                }
            ]
        }
    }
]
```

## Django Settings

Advanced settings in `inference_gateway/settings.py`:

### CORS Settings

```python
CORS_ALLOW_ALL_ORIGINS = False  # Set True for development only
CORS_ALLOWED_ORIGINS = [
    "https://yourdomain.com",
]
```

### Logging Configuration

Located in `logging_config.py`:

```python
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'logs/django_info.log',
            'maxBytes': 1024 * 1024 * 15,  # 15MB
            'backupCount': 10,
        },
    },
    # ... more configuration
}
```

### Cache Configuration

```python
CACHES = {
    'default': {
        'BACKEND': 'django.core.cache.backends.redis.RedisCache',
        'LOCATION': os.environ.get('REDIS_URL'),
    }
}
```

## Gunicorn Configuration

For production deployments, configure Gunicorn in `gunicorn_asgi.config.py`:

```python
bind = "0.0.0.0:8000"
workers = 4  # Adjust based on CPU cores
worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120
keepalive = 5
```

### Worker Calculation

Recommended formula:

```
workers = (2 * CPU_cores) + 1
```

For a 4-core machine:
```
workers = (2 * 4) + 1 = 9
```

## Nginx Configuration

Example production configuration:

```nginx
upstream inference_gateway {
    server 127.0.0.1:8000 fail_timeout=0;
}

server {
    listen 443 ssl http2;
    server_name yourdomain.com;
    
    # SSL Configuration
    ssl_certificate /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    
    # File upload size limit
    client_max_body_size 100M;
    
    # Timeouts
    proxy_connect_timeout 600s;
    proxy_send_timeout 600s;
    proxy_read_timeout 600s;
    
    location / {
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Host $http_host;
        proxy_redirect off;
        proxy_buffering off;
        proxy_pass http://inference_gateway;
    }
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name yourdomain.com;
    return 301 https://$server_name$request_uri;
}
```

## Environment-Specific Configurations

### Development

```dotenv
DEBUG=True
ALLOWED_HOSTS="localhost,127.0.0.1"
SECRET_KEY="dev-secret-key-not-secure"
PGHOST="localhost"
REDIS_URL="redis://localhost:6379/0"
```

### Staging

```dotenv
DEBUG=False
ALLOWED_HOSTS="staging.yourdomain.com"
SECRET_KEY="<strong-secret-key>"
PGHOST="postgres-staging.internal"
REDIS_URL="redis://redis-staging.internal:6379/0"
```

### Production

```dotenv
DEBUG=False
ALLOWED_HOSTS="yourdomain.com,api.yourdomain.com"
SECRET_KEY="<strong-secret-key>"
PGHOST="postgres-prod.internal"
REDIS_URL="redis://redis-prod.internal:6379/0"

# Enable security features
SECURE_SSL_REDIRECT=True
SESSION_COOKIE_SECURE=True
```

## Secrets Management

### Using Docker Secrets

```yaml
services:
  inference-gateway:
    secrets:
      - globus_secret
      - db_password

secrets:
  globus_secret:
    file: ./secrets/globus_secret.txt
  db_password:
    file: ./secrets/db_password.txt
```

### Using Environment Files

```bash
# .env.local (gitignored)
source .env.production
export POSTGRES_PASSWORD="<secret>"
export GLOBUS_APPLICATION_SECRET="<secret>"
```

## Validation

Check your configuration:

```bash
# Django check
python manage.py check

# Database connectivity
python manage.py dbshell

# Show current settings (dev only!)
python manage.py diffsettings
```

## Next Steps

- [Docker Deployment](docker.md)
- [Bare Metal Setup](bare-metal.md)
- [Production Best Practices](../deployment/production.md)

