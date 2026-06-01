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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import httpx
import requests
from asgiref.sync import sync_to_async
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Django setup
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

load_dotenv(override=True)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "inference_gateway.settings")

import django  # noqa: E402  (import after setting DJANGO_SETTINGS_MODULE)

django.setup()


# ---------------------------------------------------------------------------
# Imports that require Django to be configured
# ---------------------------------------------------------------------------

from cron_jobs.check_application_health import ApplicationHealthChecker  # noqa: E402
from resource_server_async import globus_utils
from resource_server_async.clusters import (
    BaseCluster,  # noqa: E402
    MetisCluster,
)
from resource_server_async.endpoints import MetisEndpoint
from resource_server_async.errors import BaseError
from resource_server_async.models import (
    Endpoint,  # noqa: E402
    User,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("HEALTH_MONITOR_LOG_LEVEL", "INFO").upper()
LOG_FILE_DEFAULT = os.path.join(SCRIPT_DIR, "direct_health_monitor_run.log")


def configure_logging(log_file: Optional[str] = None) -> logging.Logger:
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
SLOW_THRESHOLD_SECONDS = float(os.getenv("HEALTH_MONITOR_SLOW_THRESHOLD", 5.0))
QSTAT_TIMEOUT_SECONDS = int(os.getenv("HEALTH_MONITOR_QSTAT_TIMEOUT", 60))
GLOBUS_HEALTH_TIMEOUT = int(os.getenv("HEALTH_MONITOR_HEALTH_TIMEOUT", 30))
METIS_HEALTH_TIMEOUT = float(os.getenv("HEALTH_MONITOR_METIS_TIMEOUT", 15.0))

FULL_REPORT_FREQUENCY_HOURS = int(os.getenv("HEALTH_MONITOR_FULL_REPORT_HOURS", 24))


@dataclass
class EndpointInfo:
    """Minimal metadata required to run a health check."""

    model: str
    endpoint_uuid: str
    function_uuid: str
    api_port: int
    endpoint_slug: str
    allowed_globus_groups: Optional[str]

    @property
    def has_mock_group(self) -> bool:
        groups = self.allowed_globus_groups or ""
        return "MockGroup" in groups


@dataclass
class HealthRecord:
    """Result of a single health check."""

    model: str
    cluster: str
    status: str  # healthy | slow | failed | offline | idle
    detail: str
    response_time: Optional[float] = None
    elapsed: Optional[float] = None


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
def format_duration(value: Optional[float]) -> str:
    if value is None:
        return "?"
    return f"{value:.2f}s"


def normalize_model_name(name: str) -> str:
    return name.strip()


async def gather_endpoints() -> Dict[str, EndpointInfo]:
    """Load Sophia endpoints that should be monitored (non-mock)."""

    result: Dict[str, EndpointInfo] = {}
    async for endpoint in Endpoint.objects.filter(cluster="sophia"):
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


async def fetch_qstat_running_models() -> Tuple[Dict[str, Dict], Optional[str]]:
    """Return mapping of running model name -> qstat entry."""

    # Create mock User object to run get_jobs()
    mock_auth_data = {
        "id": "ALCF-monitor-tool-id",
        "name": "ALCF-monitor-tool-name",
        "username": "ALCF-monitor-tool-username",
        "idp_id": "ALCF-monitor-tool-idp-id",
        "idp_name": "ALCF-monitor-tool-idp-name",
    }
    mock_auth = User(**mock_auth_data)

    # Get the jobs response from the cluster adapter
    jobs_response = None
    error_message = None
    error_code = None
    try:
        cluster = await BaseCluster.load_adapter("sophia")
        jobs_response = await cluster.get_jobs(mock_auth)
    except BaseError as exc:
        error_message = str(exc)
        error_code = exc.status_code
    except Exception as exc:
        error_message = str(exc)
        error_code = 500

    if error_message:
        log.error(
            "Failed to fetch qstat details (code %s): %s",
            error_code,
            error_message,
        )
        return {}, error_message
    else:
        assert jobs_response is not None

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
    return result, None


def parse_health_payload(result) -> Tuple[Optional[float], Optional[str]]:
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


async def check_sophia_models() -> List[HealthRecord]:
    """Run health checks against running Sophia models."""

    records: List[HealthRecord] = []
    endpoints = await gather_endpoints()

    if not endpoints:
        log.warning("No Sophia endpoints found for monitoring.")
        return records

    gcc = globus_utils.get_compute_client_from_globus_app()
    gce = globus_utils.get_compute_executor(client=gcc)

    endpoint_status_cache: Dict[str, Tuple[Optional[dict], Optional[str]]] = {}

    def get_endpoint_status_cached(
        info: EndpointInfo,
    ) -> Tuple[Optional[dict], Optional[str]]:
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

    running_models, qstat_error = await fetch_qstat_running_models()

    if qstat_error:
        records.append(
            HealthRecord(
                model="Sophia qstat",
                cluster="sophia",
                status="failed",
                detail=qstat_error,
            )
        )

    for model_name, info in endpoints.items():
        status_payload, status_error = get_endpoint_status_cached(info)
        running_entry = running_models.get(model_name)

        if status_error:
            records.append(
                HealthRecord(
                    model=model_name,
                    cluster="sophia",
                    status="failed",
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
                    model=model_name,
                    cluster="sophia",
                    status="offline",
                    detail=f"Endpoint state={endpoint_state}",
                )
            )
            continue

        if running_entry is None:
            records.append(
                HealthRecord(
                    model=model_name,
                    cluster="sophia",
                    status="idle",
                    detail="Endpoint online but no running job",
                )
            )
            continue

        if managers <= 0:
            records.append(
                HealthRecord(
                    model=model_name,
                    cluster="sophia",
                    status="failed",
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
                    model=model_name,
                    cluster="sophia",
                    status="failed",
                    detail=detail,
                    elapsed=elapsed,
                )
            )
            continue

        response_time, status_text = parse_health_payload(result)
        detail = status_text or "ok"

        record_status = "healthy"
        if response_time is not None and response_time > SLOW_THRESHOLD_SECONDS:
            record_status = "slow"
        if elapsed > GLOBUS_HEALTH_TIMEOUT:
            record_status = "failed"

        addon = []
        addon.append(f"resp={format_duration(response_time)}")
        addon.append(f"elapsed={format_duration(elapsed)}")

        detail = f"{detail} ({', '.join(addon)})"

        records.append(
            HealthRecord(
                model=model_name,
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
                    model=model_name,
                    cluster="sophia",
                    status="failed",
                    detail="Running job has no matching endpoint configuration",
                )
            )

    return records


def extract_metis_models(status_data: Dict) -> List[Dict]:
    """Flatten Metis status structure into a list of live models."""

    models: List[Dict] = []
    for model_key, model_info in status_data.items():
        if model_info.get("status") != "Live":
            continue
        # experts = model_info.get("experts", [])
        endpoint_id = model_info.get("endpoint_id", "")
        model_name = model_info.get("model", "")
        health_path = model_info.get("health_path", "health")
        # for expert in experts or []:
        #    models.append(
        #        {
        #            "model": normalize_model_name(expert),
        #            "endpoint_id": endpoint_id,
        #            "model_info": model_info,
        #            "health_path": health_path,
        #        }
        #    )
        models.append(
            {
                "model": normalize_model_name(model_name),
                "endpoint_id": endpoint_id,
                "model_info": model_info,
                "health_path": health_path,
            }
        )
    return models


async def check_metis_models() -> List[HealthRecord]:
    """Run health checks for active Metis models."""

    records: List[HealthRecord] = []
    metis = await MetisCluster.load_adapter("metis")
    try:
        jobs = await metis.get_jobs(None)
    except Exception as e:
        records.append(
            HealthRecord(
                model="Metis status",
                cluster="metis",
                status="failed",
                detail=str(e),
            )
        )
        return records

    if not jobs:
        log.warning("Metis status returned no data.")
        return records

    models = jobs.running

    if not models:
        records.append(
            HealthRecord(
                model="Metis",
                cluster="metis",
                status="idle",
                detail="No live models returned by Metis status",
            )
        )
        return records

    url = "https://metis.alcf.anl.gov/v1/health"

    for model_entry in models:
        for model_name in model_entry.Models.split(","):
            model_name = model_name.strip()

            endpoint = await MetisEndpoint.load_adapter("metis", "api", model_name)
            headers = endpoint.httpx_client.headers

            payload = {"model": model_name}

            log.info("Calling Metis health: model=%s url=%s", model_name, url)
            start = time.monotonic()
            try:
                async with httpx.AsyncClient(timeout=METIS_HEALTH_TIMEOUT) as client:
                    response = await client.post(url, json=payload, headers=headers)
                    elapsed = time.monotonic() - start

                    if response.status_code >= 400:
                        detail = response.text.strip()
                        records.append(
                            HealthRecord(
                                model=model_name,
                                cluster="metis",
                                status="failed",
                                detail=f"HTTP {response.status_code}: {detail}",
                                elapsed=elapsed,
                            )
                        )
                        continue

                    resp_time, status_text = parse_health_payload(response.text)
                    detail_text = status_text or "ok"

                    record_status = "healthy"
                    if (
                        resp_time is not None and resp_time > SLOW_THRESHOLD_SECONDS
                    ) or elapsed > SLOW_THRESHOLD_SECONDS:
                        record_status = "slow"
                        detail_text += f" (slow: resp={format_duration(resp_time)}, elapsed={format_duration(elapsed)})"
                    else:
                        detail_text += f" (resp={format_duration(resp_time)}, elapsed={format_duration(elapsed)})"

                    records.append(
                        HealthRecord(
                            model=model_name,
                            cluster="metis",
                            status=record_status,
                            detail=detail_text,
                            response_time=resp_time,
                            elapsed=elapsed,
                        )
                    )
                    log.info(
                        "Metis health succeeded model=%s status=%s resp=%s elapsed=%s",
                        model_name,
                        record_status,
                        format_duration(resp_time),
                        format_duration(elapsed),
                    )
            except httpx.TimeoutException:
                elapsed = time.monotonic() - start
                records.append(
                    HealthRecord(
                        model=model_name,
                        cluster="metis",
                        status="failed",
                        detail=f"Timeout after {METIS_HEALTH_TIMEOUT}s",
                        elapsed=elapsed,
                    )
                )
            except httpx.HTTPError as exc:
                elapsed = time.monotonic() - start
                records.append(
                    HealthRecord(
                        model=model_name,
                        cluster="metis",
                        status="failed",
                        detail=f"HTTP error: {exc}",
                        elapsed=elapsed,
                    )
                )
            except Exception as exc:
                elapsed = time.monotonic() - start
                records.append(
                    HealthRecord(
                        model=model_name,
                        cluster="metis",
                        status="failed",
                        detail=f"Unexpected error: {exc}",
                        elapsed=elapsed,
                    )
                )

    return records


async def check_vm_health() -> List[HealthRecord]:
    """Run VM/application health checks (Redis, Postgres, App /health, Globus)."""

    def _run_checks() -> dict:
        checker = ApplicationHealthChecker()
        checker.application_url = os.getenv(
            "STREAMING_SERVER_HOST", "http://localhost:8000"
        )

        # Override the checker health endpoint to the main service URL
        def _health_override() -> dict:
            try:
                url = f"https://{checker.application_url}/resource_server/health"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("status") == "ok":
                        return {
                            "component": "Application /health Endpoint",
                            "status": "healthy",
                            "message": f"Application responding successfully at {url}",
                        }
                    return {
                        "component": "Application /health Endpoint",
                        "status": "failed",
                        "error": f"Unexpected response from {url}: {data}",
                    }
                return {
                    "component": "Application /health Endpoint",
                    "status": "failed",
                    "error": f"Health endpoint returned status code {response.status_code}",
                }
            except Exception as exc:
                return {
                    "component": "Application /health Endpoint",
                    "status": "failed",
                    "error": f"Health endpoint check failed: {exc}",
                }

        checker.check_application_health_endpoint = _health_override
        return checker.check_all_components()

    results = await sync_to_async(_run_checks, thread_sensitive=True)()
    records: List[HealthRecord] = []

    for component in results.get("components", []):
        name = component.get("component", "unknown")
        status = component.get("status", "unhealthy")
        detail = component.get("message") or component.get("error", "")

        record_status = "healthy" if status == "healthy" else "failed"
        records.append(
            HealthRecord(
                model=name,
                cluster="vm",
                status=record_status,
                detail=detail,
            )
        )

    return records


def group_records(records: Iterable[HealthRecord]) -> Dict[str, List[HealthRecord]]:
    grouped: Dict[str, List[HealthRecord]] = {}
    for record in records:
        grouped.setdefault(record.status, []).append(record)
    return grouped


def format_records(
    records: List[HealthRecord], *, full: bool = False
) -> Tuple[str, bool]:
    lines: List[str] = []
    grouped = group_records(records)

    order = (
        ["failed", "offline", "slow", "idle", "healthy"]
        if full
        else ["failed", "offline", "slow"]
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
        entries = grouped.get(status, [])
        if not entries:
            continue
        has_entries = True
        header = f"{icons.get(status, '')} {status.upper()} ({len(entries)})"
        lines.append(header)
        for record in sorted(entries, key=lambda r: (r.cluster, r.model)):
            lines.append(f"• [{record.cluster}] {record.model}: {record.detail}")

    if not lines:
        return "No records", has_entries
    return "\n".join(lines), has_entries


def format_summary(
    records: List[HealthRecord], *, full: bool = False
) -> Tuple[str, bool]:
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


async def run_monitor() -> List[HealthRecord]:
    results = await asyncio.gather(
        check_sophia_models(),
        check_metis_models(),
        check_vm_health(),
        return_exceptions=True,
    )

    cluster_labels = ["sophia", "metis", "vm"]
    records: List[HealthRecord] = []
    for cluster_name, result in zip(cluster_labels, results):
        if isinstance(result, Exception):
            log.error("Health check for %s failed", cluster_name, exc_info=result)
            records.append(
                HealthRecord(
                    model=f"{cluster_name} monitor",
                    cluster=cluster_name,
                    status="failed",
                    detail=str(result),
                )
            )
        elif isinstance(result, list):
            records.extend(result)
        else:
            log.error("Unexpected result for %s monitor: %r", cluster_name, result)
            records.append(
                HealthRecord(
                    model=f"{cluster_name} monitor",
                    cluster=cluster_name,
                    status="failed",
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
            fh.write(datetime.now(timezone.utc).isoformat())
    except Exception as exc:
        log.warning("Failed to update full report marker: %s", exc)


async def post_to_slack(message: str) -> None:
    webhook_url = os.getenv("WEBHOOK_URL")
    if not webhook_url:
        log.warning("WEBHOOK_URL not set; skipping Slack notification")
        return

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(webhook_url, json={"text": message})
    except Exception as e:
        log.error("Error posting to Slack: %s", e)
        return

    if response.status_code >= 400:
        log.error(
            "Failed to post to Slack: HTTP %s %s",
            response.status_code,
            response.text,
        )


def main(argv: Optional[List[str]] = None) -> None:
    parser = ArgumentParser(description="Internal health monitor")
    parser.add_argument(
        "--full", action="store_true", help="send full report without truncation"
    )
    parser.add_argument("--log-file", help="override log file destination")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="print summary without sending Slack notification",
    )
    args = parser.parse_args(argv)

    if args.log_file:
        configure_logging(args.log_file)

    records = asyncio.run(run_monitor())
    full_report = should_send_full_report(force=args.full)
    message, has_entries = format_summary(records, full=full_report)
    print(message)

    if not args.summary and (full_report or has_entries):
        post_to_slack(message)

    if full_report:
        update_full_marker()


if __name__ == "__main__":
    main()
