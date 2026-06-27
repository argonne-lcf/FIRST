# Developer Guide

## Prerequisites

You will need the following installed:

- uv
- Docker / Compose
- libpq / psql
- NGINX (for running pilot integration tests)

## Setup

### Install the uv workspace

```bash
# Installs all dependencies and pre-commit git hooks:
make install-dev

# Shortcut for:
uv sync --all-groups
pre-commit install
```

### Configure .env files

`.env` is loaded by `alcf-ai` and Docker Compose and should contain:

```ini
# For convenience (don't need to repeat -f flag to docker compose every time):
COMPOSE_FILE=deploy/compose.yaml:deploy/compose.dev.yaml
COMPOSE_PROJECT_NAME=first

# For convenience when using alcf-ai client against local docker compose stack:
# (could also just specify --base-url)
inference_base_url=http://localhost:8000
```

`.env.secret` must be written and contain:

```ini
FIRST_GLOBUS__APP_ID="globus auth app id" # use same as previous
FIRST_GLOBUS__APP_SECRET="globus auth app secret"
FIRST_PILOT_CA_CRT="CA certificate" # can be fake for now
FIRST_PILOT_CA_KEY="CA key" # can be fake for now
```

### Start local services and test
Bring everything up in the Dev Docker Compose stack with:

```bash
make compose-up
```

Run all code quality checks locally:

```
make format
make lint-fix
make mypy
make test
```

Run a declarative apply against the local service:

```bash
alcf-ai auth login
alcf-ai admin apply tests/resource_specs/baseline/
alcf-ai admin audit
```

## More on the Docker Compose setup

- Docker Compose supports combining YAML files. We can take advantage of this by putting the common parts in `deploy/compose.yaml` and the dev specifics in `deploy/compose.dev.yaml`.
- The Dev stack specifically uses a `tmpfs` mount for postgres, which erases all data
on restarts, and runs a `migration` task to recreate the database on startup.  `tmpfs` is memory-backed and enables faster testing without sacrificing the true postgres integration and isolation in each test case.
- The `deploy/compose.prod.yaml` uses a persistent volume instead of tmpfs.

## Env File Structure

The `env_file` block in deploy/compose.yaml shows how environment variables are layered into the Compose containers:

1. `.env.default` contains common environment variables
2. `.env.compose` contains variables specific to services inside compose.  This is mostly concerned with the network (the redis service hostname is `redis`, not `localhost`)
3. `.env.secret` contains the secrets defined above
4. `.env.prod` is OPTIONAL and contains any prod overrides

When running tests on the local host, we want to use the postgres/redis services running in the containers.  However, they bind to ports on `localhost` and the hostnames in `.env.compose` do not exist outside of the Compose network.

Therefore, a `.env.local` file serves the purpose of setting the service endpoints correctly to `localhost:5432` for postgres and `localhost:6379` for redis.  The `.env.local` file is never seen inside the Docker Compose stack, but it makes local testing much more convenient by reusing the compose services.

## Settings loading

The `Settings` class uses Pydantic `BaseSettings` to load and validate all settings at startup.  The settings can come directly from environment variables, which take precedence, or be loaded from the list configured in `env_file` for local development convenience.

Docker Compose works by the former path: Compose `env_files` sets environment variables for each container, and the `Settings` class parses the environment variables without seeing any `.env.*` files.  This is why the .compose.yaml file is never mentioned in the `Settings` class.

Local development uses the `env_file` configured on the Settings class instead.  It discovers and layers the variables in the `.env.default`, `.env.local`, and `.env.secret` files.

Environment variables use the `first_` env prefix and `__` nested delimiter.  So `settings.db_url` is loaded from `FIRST_DB_URL` and `settings.globus.app_id` comes from `FIRST_GLOBUS__APP_ID`.

## Testing the LiteLLM Router against a live cluster

Useful when you need to drive a real cluster's backends from a laptop —
e.g., reproducing a router bug against the actual Sophia replicas. Open a
SOCKSv5 proxy through the login node and point the gateway through it:

```bash
# Create a SOCKSv5 proxy on localhost:8080 that tunnels through sophia
ssh -D 8080 -o "ControlMaster=no" sophia

# In the shell where the gateway (or a one-off Router) will run:
export ALL_PROXY=socks5://localhost:8080

# The aiohttp transport is the default for prod, but it doesn't honor
# ALL_PROXY. Disable it in DEV ONLY to fall back to plain httpx, which
# does:
export DISABLE_AIOHTTP_TRANSPORT=True
```

The router has to be run with the `httpx[socks]` extra:

```bash
uv run --with litellm,httpx[socks] python
```

You can now hit backend nodes by IP — internal DNS will not resolve from
the laptop:

```bash
curl http://10.140.49.238:8000/v1/chat/completions
```

And the router works end-to-end against the real backend:

```python
from litellm import Router

router = Router(
    model_list=[
        {
            "model_name": "gemma-4",
            "litellm_params": {
                "model": "hosted_vllm/google/gemma-4-E4B-it",
                "api_base": "http://10.140.49.238:8000/v1",
                "api_key": "dummy",
            },
        }
    ],
)
```