# Use Python 3.12 as base image
FROM python:3.12-slim@sha256:dd29372629eeba2dd003fd9e9d35a5b8236c44727875a0364254b5127af88e65 AS build
COPY --from=ghcr.io/astral-sh/uv:latest@sha256:d0a6eca6c669dc7e9c51218707b8438a3d30402733d739dcc00adb3e213e8f5c /uv /uvx /bin/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV UV_NO_DEV=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

COPY uv.lock pyproject.toml ./
COPY packages/common/pyproject.toml  packages/common/
COPY packages/gateway/pyproject.toml packages/gateway/


# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --package first-gateway --frozen --no-install-workspace --no-dev

# Copy project files
COPY packages packages

# Sync project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --package first-gateway --frozen --no-dev

RUN useradd -m app
USER app
EXPOSE 7000
CMD [".venv/bin/gunicorn", \
    "first_gateway.apiserver.api:app", \
    "-k", "first_gateway.apiserver.uvicorn_worker.UvicornWorker", \
    "-b", "0.0.0.0:7000", \
    "--workers", "4", \
    "--timeout", "30"]
