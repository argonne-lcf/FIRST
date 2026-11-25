"""Prometheus counters for the apiserver, served on a separate internal port.

Each uvicorn replica keeps its own counters; Prometheus scrapes every replica
(DNS service discovery, deploy/prometheus.yml) and queries sum them, e.g.
``sum(rate(gateway_http_responses_total{code="500"}[5m]))``. The health
alerter's gateway-errors check (controllers/workers/health_alerter) alerts on
them.

Kept deliberately cheap: one counter increment (~0.5 us) per response and per
upstream attempt, and labels of small, bounded cardinality (no user, model or
path labels).  The metrics port is never published or proxied by nginx.
"""

import logging
import threading
from wsgiref.simple_server import WSGIServer

from prometheus_client import Counter, disable_created_metrics, start_http_server

logger = logging.getLogger(__name__)

HTTP_RESPONSES = Counter(
    "gateway_http_responses",
    "HTTP responses sent by the apiserver, by status code "
    "(503: no backend tried; 502/504: a backend was tried and failed; "
    "500: gateway bug). /health probes are not counted.",
    ["code"],
)

INFERENCE_ATTEMPTS = Counter(
    "gateway_inference_attempts",
    "Inference attempts by outcome (as in InferenceLog) and deployment. "
    "admission_rejected attempts have deployment='' unless the request was "
    "pinned to one.",
    ["outcome", "deployment"],
)

# increase() takes a series' first sample as its baseline, so a 500 that
# creates the series would never be counted. Start the alerted-on codes at 0.
# (A deployment's first failed attempt on each replica is missed the same way;
# negligible against the upstream alert's minimum count.)
for _code in ("500", "502", "503", "504"):
    HTTP_RESPONSES.labels(_code)


def start_metrics_server(port: int) -> tuple[WSGIServer, threading.Thread]:
    """Serve /metrics from a daemon thread, off the event loop."""
    # The *_created series double the exposition and nothing reads them.
    disable_created_metrics()  # type: ignore[no-untyped-call]
    server, thread = start_http_server(port)
    logger.info(f"Serving Prometheus metrics on port {port}")
    return server, thread
