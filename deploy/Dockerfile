# Use Python 3.12 as base image
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set work directory
WORKDIR /app
RUN mkdir -p /app/alcf_ai/src/alcf_ai/
RUN touch /app/alcf_ai/src/alcf_ai/__init__.py
RUN touch /app/alcf_ai/README.md

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=alcf_ai/pyproject.toml,target=alcf_ai/pyproject.toml \
    apt-get update \
    && apt-get install -y --no-install-recommends gcc python3-dev \
    && uv sync --locked --no-install-workspace --no-dev \
    && apt-get purge -y gcc python3-dev \
    && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*


# Sync project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Expose port
EXPOSE 7000

# Run the application with Uvicorn worker
CMD ["uv", "run", "--", "gunicorn", \
     "first.apiserver.api:app", \
     "-k", "first.apiserver.uvicorn_worker.UvicornWorker", \
     "-b", "0.0.0.0:7000", \
     "--workers", "4", \
     "--timeout", "120", \
     "--log-level", "debug", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "--capture-output"]