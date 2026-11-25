"""
check_gateway_errors: is the gateway failing requests itself, or is an upstream
failing the requests we send it?

It judges the apiserver's own counters (apiserver/metrics.py), summed over
every replica Prometheus scrapes. See the status-code comment at the top of
apiserver/inference.py for what each 5xx means. Two signals:

* HTTP 500s: the gateway's own bugs.
* Per deployment, the share of dispatched attempts that failed
  (InferenceLog outcome ``upstream_error``). Admission rejections (our 503s:
  cold starts, saturation) are not dispatched, so they never count. A failure
  that a retry on another backend hid from the user still counts: the
  upstream is unhealthy either way.

Each alert must hold for ``_DEBOUNCE_S`` (a few polls) before it is posted, and
its recovery likewise, on top of the rolling window: a burst shorter than that
never reaches Slack, and an alert near its threshold does not flap.
"""

from first_common.schema.resources.runtime import Severity
from first_gateway.controllers.workers.health_alerter.prometheus import (
    prometheus_client,
    query,
)
from first_gateway.controllers.workers.health_alerter.types import Observation
from first_gateway.settings import ClientState, GatewayErrorAlertSettings

_DEBOUNCE_S = 300.0
_JOB = 'job="inference-gateway"'
# An upstream failing at least this share of its attempts is critical.
_CRIT_RATIO = 0.5


def _metrics_missing(up: list[tuple[dict[str, str], float]]) -> Observation:
    return Observation(
        key="gateway_errors/metrics",
        status="missing",
        summary=(
            "Prometheus scrapes no gateway replica"
            if not up
            else "No gateway replica's metrics are up"
        )
        + ": alerts on gateway 500s and unhealthy upstreams are blind",
        display_name="Gateway metrics",
        recovery_hint="metrics returned",
        detail=f"PromQL: up{{{_JOB}}}\nSee deploy/prometheus.yml (job inference-gateway).",
        severity="warn",
        group="Infrastructure",
        debounce_s=_DEBOUNCE_S,
    )


def _http_500s(n: float, total: float, cfg: GatewayErrorAlertSettings) -> Observation:
    share = f" ({n / total:.1%} of responses)" if total else ""
    return Observation(
        key="gateway_errors/http_500",
        status="elevated",
        summary=(
            f"Gateway returned {n:.0f} HTTP 500s in the last {cfg.window}{share}: "
            "the gateway itself is failing (a bug, not an upstream)"
        ),
        display_name="Gateway HTTP 500s",
        recovery_hint="500s subsided",
        detail=(
            "Each 500 logs its traceback with an error_id:\n"
            "  docker compose logs inference-gateway --since=15m "
            "| grep 'Uncaught Exception'\n"
            "Inference attempts that hit one are logged with "
            "outcome=gateway_error.\n"
            f'PromQL: sum(increase(gateway_http_responses_total{{code="500"}}[{cfg.window}]))'
        ),
        severity="crit",
        group="Infrastructure",
        debounce_s=_DEBOUNCE_S,
    )


def _upstream_unhealthy(
    deployment: str, errors: float, attempts: float, cfg: GatewayErrorAlertSettings
) -> Observation:
    ratio = errors / attempts
    severity: Severity = "crit" if ratio >= _CRIT_RATIO else "warn"
    return Observation(
        key=f"gateway_errors/upstream/{deployment}",
        status="unhealthy",
        summary=(
            f"Deployment `{deployment}`: {errors:.0f} of {attempts:.0f} upstream "
            f"attempts failed in the last {cfg.window} ({ratio:.0%})"
        ),
        display_name=f"Deployment {deployment}",
        recovery_hint="upstream errors subsided",
        detail=(
            "The gateway admitted requests and sent them to this deployment, and "
            "they failed (5xx, connection error, timeout, or a stream broken "
            "midway). Users see 502/504 unless a retry on another backend "
            "succeeded. The failure summaries are in the InferenceLog `error` "
            f"field (outcome=upstream_error, deployment={deployment}).\n"
            "PromQL: sum by (deployment) (increase(gateway_inference_attempts_total"
            f'{{outcome="upstream_error"}}[{cfg.window}]))'
        ),
        severity=severity,
        group="Deployments",
        debounce_s=_DEBOUNCE_S,
    )


async def check_gateway_errors(client_state: ClientState) -> list[Observation]:
    """Alert on gateway 500s and on deployments failing their upstream attempts.

    Runs only where configured (`Settings.gateway_error_alerts` is set).
    """
    cfg = client_state.settings.gateway_error_alerts
    if cfg is None:
        return []

    w = cfg.window
    url = cfg.prometheus_url
    async with prometheus_client(cfg.prometheus_ca_crt_file) as http:
        up = await query(http, url, f"up{{{_JOB}}}")
        if not any(value == 1 for _, value in up):
            return [_metrics_missing(up)]

        [(_, n500)] = await query(
            http,
            url,
            f'sum(increase(gateway_http_responses_total{{code="500"}}[{w}])) or vector(0)',
        )
        [(_, total)] = await query(
            http, url, f"sum(increase(gateway_http_responses_total[{w}])) or vector(0)"
        )
        by = "sum by (deployment) (increase(gateway_inference_attempts_total{%s}[%s]))"
        errors = await query(http, url, by % ('outcome="upstream_error"', w))
        dispatched = await query(http, url, by % ('outcome!="admission_rejected"', w))

    observations = []
    if n500 >= cfg.min_500s:
        observations.append(_http_500s(n500, total, cfg))

    attempts = {labels.get("deployment", ""): v for labels, v in dispatched}
    for labels, n in errors:
        deployment = labels.get("deployment", "")
        total_attempts = attempts.get(deployment, 0.0)
        if (
            n >= cfg.upstream_min_errors
            and total_attempts > 0
            and n / total_attempts >= cfg.upstream_error_ratio
        ):
            observations.append(_upstream_unhealthy(deployment, n, total_attempts, cfg))
    return observations
