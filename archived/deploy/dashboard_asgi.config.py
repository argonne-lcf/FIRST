import os

"""Dashboard Gunicorn ASGI server configuration."""

# Determine if we're in production or development
environment = os.getenv("ENV", "production")

# Localhost port to communicate between Nginx and Gunicorn
bind = "0.0.0.0:7001"

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
workers = 1
threads = 1
worker_connections = 1000  # Maximum number of simultaneous clients per worker

# Worker lifecycle settings
preload_app = False  # Do not preload so that you can keep main process when reloading
daemon = False  # Run in foreground (managed by systemd)

# Log directory based on environment
if environment == "development":
    # Development log files in the current directory
    accesslog = "./logs/backend_dashboard.access.log"
    errorlog = "./logs/backend_dashboard.error.log"
    bind = "127.0.0.1:8001"
    # More verbose logging in development
    loglevel = "debug"
else:
    # Production log files in a local directory
    accesslog = "/var/log/inference-service/backend_dashboard.access.log"
    errorlog = "/var/log/inference-service/backend_dashboard.error.log"
    # Less verbose logging in production
    loglevel = "debug"

# Whether to send Django output to the error log
capture_output = True

# Enable stdio inheritance for proper logging
enable_stdio_inheritance = True

# StatsD metrics (if you have StatsD configured)
# statsd_host = 'localhost:8125'
# statsd_prefix = 'gunicorn'

# Process naming for better monitoring
proc_name = "inference-dashboard"

# Error handling
max_retries = 3
