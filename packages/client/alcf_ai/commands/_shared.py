"""
Helpers shared by the CLI command modules: context access, JSON output, and
rich rendering of resource state and cold-start countdowns.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

import typer
from pydantic import BaseModel
from rich.console import Console
from rich.status import Status

from first_common.errors import ServiceUnavailable
from first_common.schema.resources.read import ModelSummary
from first_common.schema.types import HealthCheckResult, PilotDeploymentState

from ..client import InferenceClient
from ..inference import LLM_ENDPOINTS, StartupStatus

T = TypeVar("T")

console = Console()
err_console = Console(stderr=True)

JsonFlag = typer.Option(False, "--json", help="Print raw JSON instead of a table.")


@dataclass
class CliContext:
    client: InferenceClient


def get_client(ctx: typer.Context) -> InferenceClient:
    """Return the InferenceClient stashed on the Typer context by the root callback."""
    obj: CliContext = ctx.obj
    return obj.client


def print_json(obj: Any) -> None:
    """Print pydantic models (or lists of them) as indented JSON on stdout."""
    if isinstance(obj, BaseModel):
        data: Any = obj.model_dump(mode="json")
    elif isinstance(obj, list):
        data = [
            o.model_dump(mode="json") if isinstance(o, BaseModel) else o for o in obj
        ]
    else:
        data = obj
    console.print_json(json.dumps(data))


# --- Formatting -----------------------------------------------------------------

_STATE_STYLES = {
    "healthy": "green",
    "ready": "green",
    "degraded": "yellow",
    "starting": "yellow",
    "launching": "yellow",
    "placed": "yellow",
    "pending": "yellow",
    "awaiting_capacity": "yellow",
    "stopping": "magenta",
    "terminating": "magenta",
    "terminated": "dim",
    "offline": "dim",
    "unknown": "dim",
    "unhealthy": "red",
    "failed": "red",
    "error": "red",
    "start_timeout": "red",
}


def styled_state(state: str | None) -> str:
    """Colorize a resource state/health string for rich."""
    if not state:
        return "[dim]—[/]"
    style = _STATE_STYLES.get(state, "")
    return f"[{style}]{state}[/]" if style else state


def fmt_seconds(sec: float | None) -> str:
    if sec is None:
        return "—"
    if sec < 90:
        return f"{sec:.0f}s"
    return f"{sec / 60:.1f}m"


def fmt_int(n: int | None) -> str:
    return f"{n:,}" if n is not None else "—"


def fmt_age(dt: datetime | None) -> str:
    """Compact 'how long ago' for timestamps."""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = (datetime.now(timezone.utc) - dt).total_seconds()
    if delta < 0:
        return "now"
    if delta < 3600:
        return f"{delta / 60:.0f}m ago"
    if delta < 86400:
        return f"{delta / 3600:.1f}h ago"
    return f"{delta / 86400:.1f}d ago"


def short_endpoint(endpoint: str) -> str:
    return "chat" if endpoint == "chat/completions" else endpoint


def endpoint_list(endpoints: list[str]) -> str:
    return ", ".join(short_endpoint(e) for e in endpoints) or "—"


def llm_endpoints(model: ModelSummary) -> list[str]:
    return [e for e in model.supported_endpoints if e in LLM_ENDPOINTS]


def task_endpoints(model: ModelSummary) -> list[str]:
    return [e for e in model.supported_endpoints if e not in LLM_ENDPOINTS]


def model_state(model: ModelSummary) -> str:
    """
    Aggregate one state for a Model from its deployments: ready when any
    deployment can serve, starting when a pilot deployment is on its way up.
    """
    pilot_states = {d.state for d in model.pilot_deployments}
    static_health = {d.health for d in model.static_deployments}
    if (
        pilot_states & {PilotDeploymentState.healthy, PilotDeploymentState.degraded}
        or HealthCheckResult.healthy in static_health
    ):
        return "ready"
    if pilot_states & {
        PilotDeploymentState.starting,
        PilotDeploymentState.awaiting_capacity,
    }:
        return "starting"
    if not model.pilot_deployments and not model.static_deployments:
        return "unknown"
    return "offline"


def ready_deployments(model: ModelSummary) -> tuple[int, int]:
    """(ready, total) deployment counts."""
    ready = sum(
        d.state in (PilotDeploymentState.healthy, PilotDeploymentState.degraded)
        for d in model.pilot_deployments
    ) + sum(d.health == HealthCheckResult.healthy for d in model.static_deployments)
    return ready, len(model.pilot_deployments) + len(model.static_deployments)


# --- Cold-start countdown ---------------------------------------------------------


def describe_startup(model: str, status: StartupStatus) -> str:
    """One-line rich description of a startup status with a live countdown."""
    if not status.cold_start:
        return f"[yellow]{status.message}[/] [dim]retrying…[/]"
    eta = status.eta_remaining_sec
    if eta is not None and eta > 0:
        return f"[bold]{model}[/] is starting · ready in [bold cyan]~{eta}s[/]"
    if eta is not None:
        return f"[bold]{model}[/] is starting · [cyan]should be ready shortly[/]"
    if status.typical_startup_sec is not None:
        return (
            f"[bold]{model}[/] is starting · "
            f"[dim]typically takes ~{status.typical_startup_sec}s[/]"
        )
    return f"[bold]{model}[/]: {status.message}"


def countdown(live: Status, model: str, status: StartupStatus, delay: float) -> None:
    """Sleep ``delay`` seconds while ticking the spinner text once per second."""
    end = time.monotonic() + delay
    while (now := time.monotonic()) < end:
        live.update(describe_startup(model, status))
        time.sleep(min(1.0, end - now))


def with_startup_wait(
    request: Callable[[], T],
    *,
    model: str,
    wait: bool,
    timeout: float,
) -> T:
    """
    Run ``request``; on a cold-start 503, show the startup ETA countdown and
    retry (until ``timeout``) instead of failing.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            return request()
        except ServiceUnavailable as exc:
            status = StartupStatus.from_error(exc)
            if status is None or not wait:
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"{model!r} was not ready after {timeout:.0f}s"
                ) from exc
            delay = min(status.retry_after_sec or 10, 30)
            with err_console.status("") as live:
                countdown(live, model, status, delay)
