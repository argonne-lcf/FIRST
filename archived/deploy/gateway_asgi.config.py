import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

"""Inference Gateway Gunicorn ASGI server configuration."""

# Determine if we're in production or development
environment = os.getenv("ENV", "production")

# Localhost port to communicate between Nginx and Gunicorn
bind = "0.0.0.0:7000"

# Maximum response time above which Gunicorn sends a timeout error
timeout = 60

# Graceful timeout for worker shutdown
graceful_timeout = 30

# Keep-alive setting
keepalive = 5

# Number of requests before workers automatically restart
max_requests = 3000

# Randomize worker restarts
max_requests_jitter = 300

# Maximum number of pending connections
backlog = 2048

# Type of workers
worker_class = "resource_server_async.uvicorn_workers.InferenceUvicornWorker"

# Worker configuration
workers = 5
threads = 1
worker_connections = 1000  # Maximum number of simultaneous clients per worker

# Worker lifecycle settings
preload_app = False  # Do not preload so that you can keep main process when reloading
daemon = False  # Run in foreground (managed by systemd)

if environment == "development":
    bind = "127.0.0.1:8000"


class _GunicornJsonFormatter(logging.Formatter):
    """Minimal JSON formatter for the gunicorn master process.
    Workers get the full GatewayJsonFormatter once Django loads."""

    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "stream": (
                "gunicorn.error" if "error" in record.name else "gunicorn.access"
            ),
            "logger": record.name,
            "message": record.getMessage(),
            "pid": record.process,
        }
        if record.exc_info and record.exc_info[1] is not None:
            doc["traceback"] = self.formatException(record.exc_info)
        return json.dumps(doc)


class _TracebackOnly(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.exc_info is not None and record.exc_info[1] is not None


logconfig_dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "json": {"()": _GunicornJsonFormatter},
        "plain": {"format": "\n %(message)s \n"},
    },
    "filters": {
        "traceback_only": {"()": _TracebackOnly},
    },
    "handlers": {
        "stdout": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        },
        "stderr_crash": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "formatter": "plain",
            "filters": ["traceback_only"],
        },
    },
    "loggers": {
        "gunicorn.error": {
            "handlers": ["stdout", "stderr_crash"],
            "level": "INFO",
            "propagate": False,
        },
        "gunicorn.access": {
            "handlers": ["stdout"],
            "level": "INFO",
            "propagate": False,
        },
    },
    "root": {
        "level": "WARNING",
        "handlers": ["stdout", "stderr_crash"],
    },
}

# StatsD metrics (if you have StatsD configured)
# statsd_host = 'localhost:8125'
# statsd_prefix = 'gunicorn'

# Process naming for better monitoring
proc_name = "inference-gateway"

# Error handling
max_retries = 3
