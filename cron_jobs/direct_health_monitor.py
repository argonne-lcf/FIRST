#!/usr/bin/env python3
"""Internal health monitor for inference endpoints.

This script is intended to be executed from a trusted VM (cron job).
It performs the following tasks:

1. Load Django context to access endpoint metadata.
2. Query Globus Compute (Sophia) qstat to find running models.
3. For each running Sophia model, directly invoke the vLLM health check via
   Globus Compute without going through the public API.
4. Fetch Metis status directly and call the model /health endpoint using the
   model-specific API token.
5. Flag slow (>5s) or failing health checks and highlight endpoints that are
   online but have no running jobs.
6. Post a concise summary to Slack using the incoming webhook URL stored in
   the WEBHOOK_URL environment variable.

The script exits after a single run; the cron scheduler is responsible for
periodic execution.
"""

from __future__ import annotations

import ast
import asyncio
import json
import logging
import os
import sys
import time
from argparse import ArgumentParser
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum, auto
from typing import Any

import httpx
from asgiref.sync import sync_to_async
from django.core.cache import cache
from django.db import connection
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Django setup
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

_ = load_dotenv(override=True)

_ = os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inference_gateway.settings")

import django  # noqa: E402  (import after setting DJANGO_SETTINGS_MODULE)

django.setup()


# ---------------------------------------------------------------------------
# Imports that require Django to be configured
# ---------------------------------------------------------------------------

from resource_server_async import globus_utils
from resource_server_async.clusters import (
    BaseCluster,  # noqa: E402
    MetisCluster,
)
from resource_server_async.endpoints import MetisEndpoint
from resource_server_async.errors import BaseError
from resource_server_async.models import (
    AuthService,
    Endpoint,  # noqa: E402
)
from resource_server_async.schemas.structured_logs import UserPydantic

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("HEALTH_MONITOR_LOG_LEVEL", "INFO").upper()
LOG_FILE_DEFAULT = os.path.join(SCRIPT_DIR, "direct_health_monitor_run.log")


def configure_logging(log_file: str | None = None) -> logging.Logger:
    """Configure console + file logging for the monitor."""

    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)

    log_level = getattr(logging, LOG_LEVEL, logging.INFO)
    root.setLevel(log_level)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    file_path = log_file or LOG_FILE_DEFAULT
    try:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except OSError as exc:
        root.warning("Failed to create log file %s (%s)", file_path, exc)

    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("globus_sdk").setLevel(logging.INFO)

    return logging.getLogger(__name__)


log = configure_logging()
LAST_FULL_MARKER = os.path.join(SCRIPT_DIR, "direct_health_monitor_last_full.txt")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
APPLICATION_URL = os.getenv("STREAMING_SERVER_HOST", "localhost:8000")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

SLOW_THRESHOLD_SECONDS = float(os.getenv("HEALTH_MONITOR_SLOW_THRESHOLD", 5.0))
QSTAT_TIMEOUT_SECONDS = int(os.getenv("HEALTH_MONITOR_QSTAT_TIMEOUT", 60))
GATEWAY_HEALTH_TIMEOUT = int(os.getenv("HEALTH_MONITOR_GATEWAY_TIMEOUT", 5))
GLOBUS_HEALTH_TIMEOUT = int(os.getenv("HEALTH_MONITOR_GLOBUS_TIMEOUT", 30))
METIS_HEALTH_TIMEOUT = int(os.getenv("HEALTH_MONITOR_METIS_TIMEOUT", 15))

FULL_REPORT_FREQUENCY_HOURS = int(os.getenv("HEALTH_MONITOR_FULL_REPORT_HOURS", 24))


@dataclass
class EndpointInfo:
    """Minimal metadata required to run a health check."""

    model: str
    endpoint_uuid: str
    function_uuid: str
    api_port: int
    endpoint_slug: str
    allowed_globus_groups: str | None

    @property
    def has_mock_group(self) -> bool:
        groups = self.allowed_globus_groups or ""
        return "MockGroup" in groups


@dataclass
class HealthRecord:
    """Result of a single health check."""

    component: str
    cluster: str
    status: HealthStatus
    detail: str
    response_time: float | None = None
    elapsed: float | None = None


@dataclass
class EndpointStatus:
    url: httpx.URL
    status: HealthStatus
    detail: str
    elapsed: float | None = None

    def to_health_record(
        self,
        cluster: str,
        component: str | None = None,
        response_time: float | None = None,
    ) -> HealthRecord:
        return HealthRecord(
            component=component or self.url.path,
            cluster=cluster,
            status=self.status,
            detail=self.detail,
            response_time=response_time,
            elapsed=self.elapsed,
        )


@dataclass
class HealthStatus(StrEnum):
    HEALTHY = auto()
    SLOW = auto()
    FAILED = auto()
    OFFLINE = auto()
    IDLE = auto()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
async def check_endpoint(
    request: httpx.Request, timeout: int
) -> httpx.Response | EndpointStatus:
    """Check if an endpoint responds (and return the response)"""

    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.send(request)
            return response.raise_for_status()
    except httpx.HTTPStatusError as e:
        return EndpointStatus(
            url=e.response.url,
            status=HealthStatus.FAILED,
            detail=f"HTTP {e.response.status_code}: {e.response.text.strip()}",
            elapsed=e.response.elapsed.total_seconds(),
        )
    except httpx.ConnectError as e:
        return EndpointStatus(
            url=request.url, status=HealthStatus.OFFLINE, detail=str(e)
        )
    except httpx.RequestError as e:
        return EndpointStatus(
            url=request.url, status=HealthStatus.FAILED, detail=str(e)
        )


def format_duration(value: float | None) -> str:
    return f"{value:.2f}s" if value is not None else "?"


def normalize_model_name(name: str) -> str:
    return name.strip()


async def gather_endpoints(cluster: str) -> dict[str, EndpointInfo]:
    """Load Sophia endpoints that should be monitored (non-mock)."""

    result: dict[str, EndpointInfo] = {}
    async for endpoint in Endpoint.objects.filter(cluster=cluster):
        # Extract config parameters
        endpoint_config = ast.literal_eval(endpoint.config)
        endpoint_uuid = endpoint_config.get("endpoint_uuid", None)
        function_uuid = endpoint_config.get("function_uuid", None)
        api_port = endpoint_config.get("api_port", None)

        # Skip if not in production
        if (
            "removed" in endpoint.model
            or "aaaaaaaa" in endpoint_uuid
            or "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa" in endpoint.allowed_globus_groups
        ):
            continue

        # Add endpoint if in production
        else:
            info = EndpointInfo(
                model=endpoint.model,
                endpoint_uuid=endpoint_uuid,
                function_uuid=function_uuid,
                api_port=api_port,
                endpoint_slug=endpoint.endpoint_slug,
                allowed_globus_groups=endpoint.allowed_globus_groups,
            )
            if info.has_mock_group:
                continue
            result[normalize_model_name(info.model)] = info

    return result


async def fetch_qstat_running_models(
    cluster: str,
) -> dict[str, dict[str, Any]] | HealthRecord:
    """Return mapping of running model name -> qstat entry."""

    # Create mock User object to run get_jobs()
    mock_auth = UserPydantic(
        id="ALCF-monitor-tool-id",
        name="ALCF-monitor-tool-name",
        username="ALCF-monitor-tool-username",
        idp_id="ALCF-monitor-tool-idp-id",
        idp_name="ALCF-monitor-tool-idp-name",
        user_group_uuids=[],
        auth_service=AuthService.GLOBUS.value,
    )

    # Get the jobs response from the cluster adapter
    try:
        adapter = await BaseCluster.load_adapter(cluster)
        jobs_response = await adapter.get_jobs(mock_auth)
    except BaseError as e:
        return HealthRecord(
            component="qstat",
            cluster=cluster,
            status=HealthStatus.FAILED,
            detail=f"Failed to fetch details (code {e.status_code}): {str(e)}",
        )
    except Exception as e:
        return HealthRecord(
            component="qstat",
            cluster=cluster,
            status=HealthStatus.FAILED,
            detail=f"Failed to fetch details: {str(e)}",
        )

    result = {}
    for entry in jobs_response.running:
        models_field = entry.Models
        model_status = entry.model_dump().get("Model Status", "")
        if not models_field:
            continue
        for model_name in models_field.split(","):
            model = normalize_model_name(model_name)
            if model:
                result[model] = {**entry.model_dump(), "Model Status": model_status}
    return result


def parse_health_payload(result: Any) -> tuple[float | None, str | None]:
    """Return response_time (float) and optional status string."""

    payload = result
    if isinstance(result, bytes):
        result = result.decode()
    if isinstance(result, str):
        try:
            payload = json.loads(result)
        except json.JSONDecodeError:
            return None, None
    if isinstance(payload, dict):
        resp_time = payload.get("response_time")
        status = payload.get("status") or payload.get("result")
        try:
            resp_time = float(resp_time) if resp_time is not None else None
        except (TypeError, ValueError):
            resp_time = None
        return resp_time, status
    return None, None


async def check_sophia_models() -> list[HealthRecord]:
    """Run health checks against running Sophia models."""

    records: list[HealthRecord] = []
    endpoints = await gather_endpoints("sophia")

    if not endpoints:
        log.warning("No Sophia endpoints found for monitoring.")
        return records

    try:
        gcc = globus_utils.get_compute_client_from_globus_app()
        gce = globus_utils.get_compute_executor(client=gcc)
    except Exception as e:
        return [
            HealthRecord(
                component="Globus Compute",
                cluster="sophia",
                status=HealthStatus.FAILED,
                detail=f"Initialization failed: {str(e)}",
            )
        ]

    running_models = await fetch_qstat_running_models("sophia")
    if isinstance(running_models, HealthRecord):
        return [running_models]  # error while trying to qstat

    endpoint_status_cache: dict[str, tuple[dict[Any, Any] | None, str | None]] = {}

    def get_endpoint_status_cached(
        info: EndpointInfo,
    ) -> tuple[dict[Any, Any] | None, str | None]:
        cached = endpoint_status_cache.get(info.endpoint_slug)
        if cached is not None:
            return cached
        status, err = globus_utils.get_endpoint_status(
            endpoint_uuid=info.endpoint_uuid,
            client=gcc,
            endpoint_slug=info.endpoint_slug,
        )
        endpoint_status_cache[info.endpoint_slug] = (status, err)
        return status, err

    for model_name, info in endpoints.items():
        status_payload, status_error = get_endpoint_status_cached(info)
        running_entry = running_models.get(model_name)

        if status_error:
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.FAILED,
                    detail=f"Endpoint status error: {status_error}",
                )
            )
            continue

        endpoint_state = (status_payload or {}).get("status", "unknown")
        managers = 0
        details = (status_payload or {}).get("details", {}) or {}
        try:
            managers = int(details.get("managers", 0))
        except (TypeError, ValueError):
            managers = 0

        last_result_raw = details.get("last_result")
        last_result = {}
        if isinstance(last_result_raw, str):
            try:
                last_result = json.loads(last_result_raw)
            except json.JSONDecodeError:
                last_result = {}
        elif isinstance(last_result_raw, dict):
            last_result = last_result_raw

        last_status = (last_result or {}).get("status")

        if endpoint_state != "online":
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.OFFLINE,
                    detail=f"Endpoint state={endpoint_state}",
                )
            )
            continue

        if running_entry is None:
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.IDLE,
                    detail="Endpoint online but no running job",
                )
            )
            continue

        if managers <= 0:
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.FAILED,
                    detail="Endpoint online but no active managers",
                )
            )
            continue

        params = {
            "model_params": {
                "openai_endpoint": "health",
                "api_port": info.api_port,
                "model": model_name,
            }
        }

        log.info(
            "Submitting health check for Sofia model=%s endpoint=%s port=%s",
            model_name,
            info.endpoint_uuid,
            info.api_port,
        )
        start = time.monotonic()
        try:
            result = await globus_utils.submit_and_get_result(
                gce,
                info.endpoint_uuid,
                info.function_uuid,
                data=params,
                timeout=GLOBUS_HEALTH_TIMEOUT,
            )
        except Exception as e:
            error_message, error_code = str(e), 500
            task_uuid = None
        else:
            error_message, error_code = None, None
            result, task_uuid = result.result, result.task_id

        elapsed = time.monotonic() - start

        log.info(
            "Health check submitted for model=%s task_uuid=%s elapsed=%s error=%s",
            model_name,
            task_uuid,
            format_duration(elapsed),
            bool(error_message),
        )

        if error_message:
            detail = f"{error_message} (code={error_code})"
            if last_status and last_status != "ok":
                detail += f" | Last health: {last_status}"
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.FAILED,
                    detail=detail,
                    elapsed=elapsed,
                )
            )
            continue

        response_time, status_text = parse_health_payload(result)
        detail = status_text or "ok"

        record_status = HealthStatus.HEALTHY
        if response_time is not None and response_time > SLOW_THRESHOLD_SECONDS:
            record_status = HealthStatus.SLOW
        if elapsed > GLOBUS_HEALTH_TIMEOUT:
            record_status = HealthStatus.FAILED

        addon = []
        addon.append(f"resp={format_duration(response_time)}")
        addon.append(f"elapsed={format_duration(elapsed)}")

        detail = f"{detail} ({', '.join(addon)})"

        records.append(
            HealthRecord(
                component=model_name,
                cluster="sophia",
                status=record_status,
                detail=detail,
                response_time=response_time,
                elapsed=elapsed,
            )
        )

    # Handle running models that do not map to known endpoints
    for model_name in running_models.keys():
        if model_name not in endpoints:
            records.append(
                HealthRecord(
                    component=model_name,
                    cluster="sophia",
                    status=HealthStatus.FAILED,
                    detail="Running job has no matching endpoint configuration",
                )
            )

    return records


async def extract_metis_models() -> list[str]:
    """Flatten Metis status structure into a list of live model names"""

    metis = await MetisCluster.load_adapter("metis")
    jobs = await metis.get_jobs(None)

    return [model.strip() for j in jobs.running for model in j.Models.split(",")]


async def check_metis_models() -> list[HealthRecord]:
    """Run health checks for active Metis models."""

    records: list[HealthRecord] = []

    try:
        models = await extract_metis_models()
    except Exception as e:
        records.append(
            HealthRecord(
                component="Metis",
                cluster="metis",
                status=HealthStatus.FAILED,
                detail=str(e),
            )
        )
        return records

    if not models:
        records.append(
            HealthRecord(
                component="Metis",
                cluster="metis",
                status=HealthStatus.IDLE,
                detail="No live models returned by Metis status",
            )
        )
        return records

    url = "https://metis.alcf.anl.gov/v1/health"

    for model_name in models:
        endpoint = await MetisEndpoint.load_adapter("metis", "api", model_name)
        headers = endpoint.httpx_client.headers

        payload = {"model": model_name}

        log.info("Calling Metis health: model=%s url=%s", model_name, url)

        request = httpx.Request("POST", url, json=payload, headers=headers)
        response = await check_endpoint(request, METIS_HEALTH_TIMEOUT)

        response_time = None
        if isinstance(response, httpx.Response):
            response_time, status_text = parse_health_payload(response.text)

            detail = status_text or "ok"
            status = HealthStatus.HEALTHY
            elapsed = response.elapsed.total_seconds()

            if (
                response_time is not None and response_time > SLOW_THRESHOLD_SECONDS
            ) or elapsed > SLOW_THRESHOLD_SECONDS:
                status = HealthStatus.SLOW
                detail += f" (slow: resp={format_duration(response_time)}, elapsed={format_duration(elapsed)})"
            else:
                detail += f" (resp={format_duration(response_time)}, elapsed={format_duration(elapsed)})"

            response = EndpointStatus(
                url=response.url,
                status=status,
                detail=detail,
                elapsed=elapsed,
            )

            log.info(
                "Metis health succeeded model=%s status=%s resp=%s elapsed=%s",
                model_name,
                status,
                format_duration(response_time),
                format_duration(elapsed),
            )

        records.append(
            response.to_health_record(
                component=model_name, cluster="metis", response_time=response_time
            )
        )

    return records


async def check_gateway_health() -> HealthRecord:
    """Check resource_server /health"""

    log.info("Checking Application /health endpoint...")

    request = httpx.Request("GET", f"http://{APPLICATION_URL}/resource_server/health")
    response = await check_endpoint(
        request,
        timeout=GATEWAY_HEALTH_TIMEOUT,
    )

    if isinstance(response, httpx.Response):
        data = response.json()
        if data.get("status") == "ok":
            response = EndpointStatus(
                url=request.url,
                status=HealthStatus.HEALTHY,
                detail="Application sucessfully responded",
                elapsed=response.elapsed.total_seconds(),
            )
        else:
            response = EndpointStatus(
                url=request.url,
                status=HealthStatus.FAILED,
                detail=f"Unexpected response: {data}",
            )

    return response.to_health_record(
        component="Application /health endpoint", cluster="vm"
    )


async def check_redis_health() -> HealthRecord:
    """Check Redis connectivity"""

    log.info("Checking Redis...")

    test_key = "health_check_test"
    test_value = f"test_{datetime.now().timestamp()}"

    try:
        # Try to set and get a test value
        await cache.aset(test_key, test_value, 60)
        retrieved_value = await cache.aget(test_key)
        _ = await cache.adelete(test_key)
    except Exception as e:
        return HealthRecord(
            component="Redis",
            cluster="vm",
            status=HealthStatus.FAILED,
            detail=f"Failure while testing: {str(e)}.",
        )

    if retrieved_value == test_value:
        return HealthRecord(
            component="Redis",
            cluster="vm",
            status=HealthStatus.HEALTHY,
            detail="Get/set test succeeded",
        )
    else:
        return HealthRecord(
            component="Redis",
            cluster="vm",
            status=HealthStatus.FAILED,
            detail="Get/set test failed: values do not match",
        )


async def check_postgres_health() -> HealthRecord:
    """Check PostgreSQL connectivity"""

    log.info("Checking PostgreSQL...")

    # Try a simple database query
    @sync_to_async
    def query() -> Any:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            return cursor.fetchone()

    try:
        result = await query()
        if result and result[0] == 1:
            # Also check if we can query a table
            endpoint_count = await Endpoint.objects.acount()
            return HealthRecord(
                component="PostgreSQL",
                cluster="vm",
                status=HealthStatus.HEALTHY,
                detail=f"Connection successful (found {endpoint_count} endpoints)",
            )
        else:
            return HealthRecord(
                component="PostgreSQL",
                cluster="vm",
                status=HealthStatus.FAILED,
                detail="Query returned unexpected result",
            )
    except Exception as e:
        return HealthRecord(
            component="PostgreSQL",
            cluster="vm",
            status=HealthStatus.FAILED,
            detail=f"Connection failed: {str(e)}",
        )


async def check_globus_compute() -> HealthRecord:
    """Check Globus Compute connectivity"""

    log.info("Checking Globus Compute...")

    @sync_to_async
    def check() -> None:
        # Try to create a Globus Compute client
        gcc = globus_utils.get_compute_client_from_globus_app()

        # Try to get executor
        _ = globus_utils.get_compute_executor(client=gcc)

    try:
        await check()
    except Exception as e:
        return HealthRecord(
            component="Globus Compute",
            cluster="vm",
            status=HealthStatus.FAILED,
            detail=f"Initialization failed: {str(e)}",
        )

    return HealthRecord(
        component="Globus Compute",
        cluster="vm",
        status=HealthStatus.HEALTHY,
        detail="Client and Executor initialized successfully",
    )


def group_records(records: Iterable[HealthRecord]) -> dict[str, list[HealthRecord]]:
    grouped: dict[str, list[HealthRecord]] = {}
    for record in records:
        grouped.setdefault(record.status.value, []).append(record)
    return grouped


def format_records(
    records: list[HealthRecord], *, full: bool = False
) -> tuple[str, bool]:
    lines: list[str] = []
    grouped = group_records(records)

    order = (
        [
            HealthStatus.FAILED,
            HealthStatus.OFFLINE,
            HealthStatus.SLOW,
            HealthStatus.IDLE,
            HealthStatus.HEALTHY,
        ]
        if full
        else [HealthStatus.FAILED, HealthStatus.OFFLINE, HealthStatus.SLOW]
    )

    icons = {
        "failed": "❌",
        "offline": "⛔",
        "slow": "⚠️",
        "idle": "💤",
        "healthy": "✅",
    }

    has_entries = False
    for status in order:
        entries = grouped.get(status.value, [])
        if not entries:
            continue
        has_entries = True
        header = (
            f"{icons.get(status.value, '')} {status.value.upper()} ({len(entries)})"
        )
        lines.append(header)
        for record in sorted(entries, key=lambda r: (r.cluster, r.component)):
            lines.append(f"• [{record.cluster}] {record.component}: {record.detail}")

    if not lines:
        return "No records", has_entries
    return "\n".join(lines), has_entries


def format_summary(
    records: list[HealthRecord], *, full: bool = False
) -> tuple[str, bool]:
    total = len(records)
    grouped = group_records(records)
    summary_parts = [f"Total checked: {total}"]
    for status in ["failed", "offline", "slow", "idle", "healthy"]:
        if status in grouped:
            summary_parts.append(f"{status}: {len(grouped[status])}")
    summary = " | ".join(summary_parts)
    details, has_entries = format_records(records, full=full)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"Health Monitor @ {timestamp}\n{summary}\n\n{details}", has_entries


async def run_monitor() -> list[HealthRecord]:
    results = await asyncio.gather(
        check_sophia_models(),
        check_metis_models(),
        asyncio.gather(
            check_gateway_health(),
            check_redis_health(),
            check_postgres_health(),
            check_globus_compute(),
        ),
        return_exceptions=True,
    )

    cluster_labels = ["sophia", "metis", "vm"]
    records: list[HealthRecord] = []
    for cluster_name, result in zip(cluster_labels, results):
        if isinstance(result, Exception):
            log.error("Health check for %s failed", cluster_name, exc_info=result)
            records.append(
                HealthRecord(
                    component=f"{cluster_name} monitor",
                    cluster=cluster_name,
                    status=HealthStatus.FAILED,
                    detail=str(result),
                )
            )
        elif isinstance(result, list):
            records.extend(result)
        else:
            log.error("Unexpected result for %s monitor: %r", cluster_name, result)
            records.append(
                HealthRecord(
                    component=f"{cluster_name} monitor",
                    cluster=cluster_name,
                    status=HealthStatus.FAILED,
                    detail="Unexpected result type",
                )
            )

    return records


def should_send_full_report(force: bool = False) -> bool:
    if force:
        return True
    try:
        import pathlib

        marker = pathlib.Path(LAST_FULL_MARKER)
        if not marker.exists():
            return True
        mtime = marker.stat().st_mtime
        elapsed_hours = (time.time() - mtime) / 3600.0
        return elapsed_hours >= FULL_REPORT_FREQUENCY_HOURS
    except Exception as exc:
        log.warning("Failed to check full report marker: %s", exc)
        return True


def update_full_marker() -> None:
    try:
        with open(LAST_FULL_MARKER, "w", encoding="utf-8") as fh:
            _ = fh.write(datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        log.warning("Failed to update full report marker: %s", exc)


async def post_to_slack(message: str) -> None:
    if not WEBHOOK_URL:
        log.warning("WEBHOOK_URL not set; skipping Slack notification")
        return

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            _ = await client.post(WEBHOOK_URL, json={"text": message})
    except httpx.HTTPStatusError as e:
        log.error(
            "Failed to post to Slack: HTTP %s %s",
            e.response.status_code,
            e.response.text,
        )
    except Exception as e:
        log.error("Error posting to Slack: %s", e)


async def main() -> None:
    parser = ArgumentParser(description="Internal health monitor")
    _ = parser.add_argument(
        "--full", action="store_true", help="send full report without truncation"
    )
    _ = parser.add_argument("--log-file", help="override log file destination")
    _ = parser.add_argument(
        "--summary",
        action="store_true",
        help="print summary without sending Slack notification",
    )
    args = parser.parse_args()

    if args.log_file:
        _ = configure_logging(args.log_file)

    records = await run_monitor()
    full_report = should_send_full_report(force=args.full)
    message, has_entries = format_summary(records, full=full_report)
    print(message)

    if not args.summary and (full_report or has_entries):
        await post_to_slack(message)

    if full_report:
        update_full_marker()


if __name__ == "__main__":
    asyncio.run(main())
