"""
check_log_shipping: are the gateway's logs leaving the VM?

Local disk is only a buffer for structured logs and large prompt bodies (see
docs/deployment/log-shipping.md). This check warns while there is still time to
fix a stalled shipper, before the buffer overflows or old data is deleted.

It reads two things, and judges them with the rules in this file:

* Vector (structured logs): its own metrics, queried from Prometheus.
* Each prompt shipper (large bodies): the status file it writes every cycle.
"""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from httpx import AsyncClient

from first_common.schema.resources.runtime import Severity
from first_gateway.controllers.workers.health_alerter.types import Observation
from first_gateway.settings import ClientState

_LOGS_HINT = "see `docker compose logs {service}`"


# ---------------------------------------------------------------------------
# Vector
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _VectorRule:
    """A PromQL query; every series it returns is one alert.

    `summary` is formatted with the series' labels and its `value`. Paste
    `query` into the Prometheus UI to see what a rule sees.
    """

    name: str
    query: str
    severity: Severity
    summary: str


_VECTOR_UP = 'up{job="vector"}'

_VECTOR_RULES = [
    # A sink retries forever without counting an error, so a stuck sink shows
    # only as a disk buffer that never empties while nothing is sent. The
    # windows are longer than the S3 sink's 5-minute batch.
    _VectorRule(
        name="sink_stuck",
        query=(
            'vector_buffer_size_events{job="vector",buffer_type="disk"} > 0'
            " and on (component_id)"
            ' min_over_time(vector_buffer_size_events{job="vector",buffer_type="disk"}[10m]) > 0'
            " unless on (component_id)"
            ' increase(vector_component_sent_events_total{job="vector"}[15m]) > 0'
        ),
        severity="crit",
        summary=(
            "Vector sink `{component_id}` has sent nothing for 15m; "
            "{value:.0f} events are waiting in its buffer"
        ),
    ),
    # A full buffer stops Vector reading. From then on the only copy of new
    # lines is the log files on the host, which rotate away.
    _VectorRule(
        name="buffer_filling",
        query=(
            'vector_buffer_size_bytes{job="vector",buffer_type="disk"}'
            " / vector_buffer_max_size_bytes > 0.5"
        ),
        severity="crit",
        summary="Vector sink `{component_id}`: disk buffer is {value:.0%} full",
    ),
    _VectorRule(
        name="events_dropped",
        query=(
            "sum by (component_id) (increase("
            'vector_component_discarded_events_total{job="vector",intentional="false"}[15m]'
            ")) > 0"
        ),
        severity="crit",
        summary="Vector `{component_id}` dropped {value:.0f} log events in 15m",
    ),
    _VectorRule(
        name="errors",
        query=(
            "sum by (component_id, error_type) (increase("
            'vector_component_errors_total{job="vector"}[15m]'
            ")) > 0"
        ),
        severity="warn",
        summary="Vector `{component_id}`: {value:.0f} `{error_type}` errors in 15m",
    ),
    # The V2 gateway logs every request, health probes included, so silence
    # means Vector is not reading its log files (or the gateway is down).
    _VectorRule(
        name="v2_silent",
        query=(
            "(sum(increase("
            'vector_component_sent_events_total{job="vector",component_id="lines_v2"}[30m]'
            ")) or vector(0)) == 0"
        ),
        severity="crit",
        summary="Vector has read no V2 gateway log lines for 30m",
    ),
]


async def _query(
    http: AsyncClient, prometheus_url: str, promql: str
) -> list[tuple[dict[str, str], float]]:
    """Run an instant query; return (labels, value) per series."""
    resp = await http.get(f"{prometheus_url}/api/v1/query", params={"query": promql})
    resp.raise_for_status()
    body = resp.json()
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {body.get('error', body)}")
    return [
        (series["metric"], float(series["value"][1]))
        for series in body["data"]["result"]
    ]


def _vector_observation(
    name: str, component: str, severity: Severity, summary: str
) -> Observation:
    hint = _LOGS_HINT.format(service="vector")
    return Observation(
        key=f"log_shipping/vector/{name}/{component}",
        status=name,
        summary=f"{summary} ({hint})",
        display_name=f"Vector {component}".strip(),
        severity=severity,
    )


async def _check_vector(prometheus_url: str) -> list[Observation]:
    async with AsyncClient(timeout=10.0) as http:
        up = await _query(http, prometheus_url, _VECTOR_UP)
        if not up or up[0][1] != 1:
            # Its metrics are gone or stale: the other rules have nothing to judge.
            return [
                _vector_observation(
                    "down",
                    "",
                    "crit",
                    "Vector is down: structured logs are not being shipped"
                    if up
                    else "Prometheus has no `vector` target: structured logs "
                    "are not being shipped, or not monitored",
                )
            ]

        observations = []
        for rule in _VECTOR_RULES:
            for labels, value in await _query(http, prometheus_url, rule.query):
                fields: dict[str, Any] = {"component_id": "", "error_type": ""}
                fields.update(labels)
                observations.append(
                    _vector_observation(
                        rule.name,
                        fields["component_id"],
                        rule.severity,
                        rule.summary.format(value=value, **fields),
                    )
                )
        return observations


# ---------------------------------------------------------------------------
# Prompt shippers
# ---------------------------------------------------------------------------

# A cycle is 5 minutes plus at most 20 of uploading (ship-prompts.sh).
_STATUS_STALE_S = 45 * 60
_FAILED_CYCLES = 3
_BACKLOG_WARN_S = 3600
# Deliberately far below the shipper's hard limit (MAX_AGE_DAYS: 14 for V2),
# past which it deletes files that never uploaded.
_BACKLOG_CRIT_S = 86400


def _age(seconds: float) -> str:
    if seconds >= 2 * 86400:
        return f"{seconds / 86400:.0f}d"
    if seconds >= 2 * 3600:
        return f"{seconds / 3600:.0f}h"
    return f"{seconds / 60:.0f}m"


def _check_prompt_shipper(name: str, status_dir: Path, now: float) -> list[Observation]:
    """Judge one shipper by its status file (written by ship-prompts.sh)."""
    display = f"Prompt shipper {name}"
    key = f"log_shipping/prompts/{name}"
    service = "prompt-shipper" if name == "v2" else f"prompt-shipper-{name}"
    hint = _LOGS_HINT.format(service=service)

    def shipping(status: str, severity: Severity, summary: str) -> Observation:
        return Observation(
            key=key,
            status=status,
            summary=f"{display}: {summary} ({hint})",
            display_name=display,
            severity=severity,
        )

    path = status_dir / f"prompt-shipper-{name}.json"
    try:
        status = json.loads(path.read_text())
    except (OSError, ValueError):
        return [shipping("not_running", "crit", f"no status at {path}")]

    silent_s = now - status["updated_at"]
    if silent_s > _STATUS_STALE_S:
        return [shipping("not_running", "crit", f"no status for {_age(silent_s)}")]

    observations = []
    pending = status["pending_files"]
    oldest_s = status["oldest_pending_age_secs"]
    failures = status["consecutive_failures"]
    backlog = f"{pending} files waiting, the oldest for {_age(oldest_s)}"
    if oldest_s > _BACKLOG_CRIT_S:
        observations.append(
            shipping(
                "backlog",
                "crit",
                f"{backlog}. Files that reach the local age limit are "
                "deleted without being uploaded",
            )
        )
    elif failures >= _FAILED_CYCLES:
        observations.append(
            shipping(
                "failing",
                "warn",
                f"upload failed {failures} times in a row "
                f"(rclone exit {status['exit_code']}); {backlog}",
            )
        )
    elif oldest_s > _BACKLOG_WARN_S:
        observations.append(shipping("failing", "warn", backlog))

    # Cumulative since the shipper started, so the alert stays up until
    # someone restarts it.
    if deleted := status["expired_deleted_total"]:
        observations.append(
            Observation(
                key=f"{key}/expired",
                status="expired",
                summary=(
                    f"{display}: deleted {deleted} prompt files that were never "
                    f"uploaded (past the local age limit). Restart `{service}` "
                    "to acknowledge"
                ),
                display_name=f"{display} local age limit",
                severity="crit",
            )
        )
    return observations


async def check_log_shipping(client_state: ClientState) -> list[Observation]:
    """Alert when Vector or a prompt shipper stops moving data off the VM.

    Runs only where log shipping is deployed (`Settings.log_shipping` is set).
    """
    config = client_state.settings.log_shipping
    if config is None:
        return []

    observations = await _check_vector(config.prometheus_url)
    now = time.time()
    for name in config.prompt_shippers:
        observations += _check_prompt_shipper(name, config.status_dir, now)
    return observations
