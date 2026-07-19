import os
from enum import Enum
from http import HTTPMethod
from pathlib import Path
from typing import Any, Callable, ClassVar, Literal, NewType, TypedDict

from jinja2 import Environment, TemplateSyntaxError, meta
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    GetCoreSchemaHandler,
    ImportString,
    SecretStr,
    field_validator,
)
from pydantic_core import core_schema

from .base_scheduler import SchedulerAdapter

ResourceName = NewType("ResourceName", str)


class HealthCheckParams(BaseModel):
    """
    Inputs to first_common.health.perform_health_check.

    If url is an empty string, health check is disabled.
    """

    url: str
    connect_timeout: float = 3.1
    read_timeout: float = 12
    http_method: HTTPMethod = HTTPMethod.GET
    json_body: Any | None = None
    status_range: tuple[int, int] = (200, 299)
    match_pattern: str | None = None
    attempts_per_check: int = 2
    attempt_delay: float = 0.1
    debounce: int = 3


class HealthCheckResult(str, Enum):
    """
    Result from /health API check
    """

    healthy = "healthy"
    unhealthy = "unhealthy"
    unknown = "unknown"


class PilotDeploymentState(str, Enum):
    """
    Aggregated state of a PilotDeployment.

    Refer to aggregate_state() in pilot_replica_observer.py for the procedure to
    calculate this state from a PilotDeployment and its Replica children.
    """

    healthy = "healthy"
    degraded = "degraded"
    starting = "starting"
    stopping = "stopping"
    failed = "failed"
    awaiting_capacity = "awaiting_capacity"
    offline = "offline"


class ReplicaState(str, Enum):
    """
    Lifecycle of a single PilotReplica model instance.
    """

    # initial states
    pending = "pending"  # desired; awaiting placement (no GPUs claimed)
    placed = "placed"  # GPUs claimed on a PilotJob; not yet launched
    launching = "launching"  # control plane Popen'd it; weights loading

    # poll until ready, timeout, or exited
    ready = "ready"  # serving; registered with the router
    unhealthy = "unhealthy"  # was READY, now failing /health

    # Terminal states:
    error = "error"  # Process exited with nonzero status code
    start_timeout = "start_timeout"  # Did not become healthy within max_startup_sec
    terminating = "terminating"  # being torn down
    terminated = "terminated"  # finished tear down


class UsageLimits(BaseModel):
    """
    Usage rate limits.

    Tokens and requests are metered using a GCRA (leaky bucket) algorithm to
    enable immediate bursts with smooth refill:
    - tpm: tokens/minute steady state usage
    - burst_tokens: Token bucket depth (max burst usage)
    - rpm: requests/minute steady state usage
    - burst_requests: Request bucket depth
    """

    tpm: int = 100_000
    burst_tokens: int = 200_000
    rpm: int = 120
    burst_requests: int = 10
    max_user_concurrency: int = 8

    @property
    def tokens_per_sec(self) -> float:
        return self.tpm / 60.0

    @property
    def requests_per_sec(self) -> float:
        return self.rpm / 60.0


class UsagePolicy(BaseModel):
    """
    Default usage rate limits, applied per-model x per-user.
    Allows overriding usage limits for specific user or group IDs.
    """

    default: UsageLimits = UsageLimits()
    overrides: dict[str, UsageLimits] = {}


class OverloadPolicy(BaseModel):
    """
    Retry-After parameters for overloaded models.
    """

    short_retry_sec: int = 15  # micro-contention Retry-After base
    retry_jitter_percent: int = 30  # server-side jitter to break retry herds


class RouterParams(BaseModel):
    """
    Desired deployment routing configuration.
    """

    weight: int = 1
    max_backend_concurrency: int = 16
    cooldown_threshold: int = 3
    cooldown_window_sec: int = 30
    cooldown_bench_sec: int = 60


class PilotConfig(BaseModel):
    """
    HPC Cluster-wide configuration for the pilot system.

    Controls how pilots are started and configured.
    """

    scheduler_adapter: ImportString[type[SchedulerAdapter]]
    scheduler_config: dict[str, Any] = {}

    job_walltime_min: int
    pilot_max_idle_time_min: int = 60
    pilot_max_unhealthy_time_min: int = 5
    max_concurrent_jobs: int = 100
    max_num_nodes: int = 64
    gpus_per_node: int = 8
    queue: str
    account: str
    scheduler_flags: str = ""
    workdir: Path
    external_port: int
    nginx_path: Path
    ip_allowlist: list[str]
    node_file_env: str
    submit_script_preamble: str
    pilot_version: str
    job_name_prefix: str = "__FIRST_PILOT_"


class DemandSignalConfig(BaseModel):
    """Model-level config for the shared demand signal all of a model's
    pilot deployments scale from.

    The demand signal combines in-flight request count with an estimate of
    rejected-but-would-be-inflight demand to produce a single demand metric.

    - `reject_window_sec`: rejection rate is obtained from the rise in total
    model rejections over this window
    - `avg_request_duration_sec`: rejections/sec is multiplied by this duration
    to obtain the expected number of requests that would be running if the
    rejected traffic had been admitted.
    - `ewma_alpha`: the autoscaler samples demand on a fixed ~10s clock and
    smooths it into an exponentially weighted moving average
    (`ewma = alpha*sample + (1-alpha)*ewma`).
    """

    reject_window_sec: int = 60
    avg_request_duration_sec: int = 30
    ewma_alpha: float = Field(0.5, ge=0.01, le=1.0)

    def calculate_demand(self, inflight: float, reject_rate: float) -> float:
        return inflight + reject_rate * self.avg_request_duration_sec


class DemandThresholdStrategy(BaseModel):
    """
    A per-deployment policy for automatically scaling a PilotDeployment from the
    shared per-model demand signal, setting the desired number of replicas by a
    ladder of thresholds.

    The demand signal itself (EWMA, reject window) is configured on the parent
    Model via `DemandSignalConfig`; this strategy only expresses how a single
    deployment reacts to that signal.
    """

    strategy: Literal["DemandThresholdStrategy"] = "DemandThresholdStrategy"

    # Act immediately on first nonzero sample:
    immediate_cold_start: bool = True

    # --- Scale-down policy ---
    # EWMA demand must be at or below the threshold for this duration before
    # scaling down.
    scale_down_sustain_sec: int = 2 * 60 * 60

    # Ordered (demand_lower_bound_exclusive, num_replicas)
    scaling_thresholds: list[tuple[float, int]] = [
        (0.0, 1),
        (10.0, 2),
    ]

    @field_validator("scaling_thresholds")
    @classmethod
    def _check_thresholds(cls, v: list[tuple[float, int]]) -> list[tuple[float, int]]:
        demands = [rung[0] for rung in v]
        targets = [rung[1] for rung in v]
        if demands != sorted(demands):
            raise ValueError("scaling_thresholds demands must be in sorted order")
        if targets != sorted(targets):
            raise ValueError("scaling_thresholds targets must be in sorted order")
        if len(demands) != len(set(demands)):
            raise ValueError("scaling_thresholds demands must be unique")
        if len(targets) != len(set(targets)):
            raise ValueError("scaling_thresholds targets must be unique")
        return v


class ScriptTemplateContext(TypedDict):
    """
    Variables made available to `PilotLaunchSpec.serve_script_template` when it
    is rendered.

    The single source of truth for what a Jinja template author may reference.
    """

    replica_name: str
    """Unique name of this replica (generated by pilot system)"""

    served_model_name: str
    """`served_model_name` from the launch spec."""

    port: int
    """TCP port the replica must listen on."""

    gpus_per_node: int
    """`gpus_per_node` from the launch spec."""

    num_nodes: int
    """`num_nodes` from the launch spec."""

    gpus_by_host: dict[str, list[str]]
    """
    GPU ids allocated to this replica, grouped by hostname. Order within each
    list is preserved from the scheduler's claim.
    """

    venv_path: str
    """`venv_path` from the launch spec"""

    weights_path: str
    """`weights_path` from the launch spec"""

    weights_cache_path: str
    """`weights_cache_path` from the launch spec"""

    env: dict[str, str]
    """
    `env` from the launch spec. Also injected into the subprocess environment,
    so most templates do not need to reference it directly.
    """

    quote: Callable[[str], str]
    """`shlex.quote`, for safely interpolating any of the above into a shell command."""


SCRIPT_TEMPLATE_VARIABLES: frozenset[str] = frozenset(
    ScriptTemplateContext.__annotations__
)


class PilotLaunchSpec(BaseModel):
    """
    Specification for launching a model replica inside of a pilot job.

    The Spec author retains full flexibility to write a bash model startup
    script in `serve_script_template`. The template must respect the port,
    served model name, and GPU resources provided as context to the script
    template.
    """

    model_config = ConfigDict(use_attribute_docstrings=True)
    served_model_name: str

    gpus_per_node: int
    num_nodes: int

    venv_path: Path
    weights_path: Path
    weights_cache_path: Path

    env: dict[str, str]

    serve_script_template: str

    max_startup_sec: int
    health_check: HealthCheckParams

    @field_validator("serve_script_template")
    @classmethod
    def _check_template_variables(cls, v: str) -> str:
        try:
            ast = Environment().parse(v)
        except TemplateSyntaxError as e:
            raise ValueError(f"serve_script_template is not valid Jinja2: {e}") from e
        used = meta.find_undeclared_variables(ast)
        unknown = used - SCRIPT_TEMPLATE_VARIABLES
        if unknown:
            raise ValueError(
                f"serve_script_template references unknown variables: "
                f"{sorted(unknown)}. Allowed: {sorted(SCRIPT_TEMPLATE_VARIABLES)}"
            )
        return v


class GpuClaim(BaseModel):
    """
    An reservation of GPUs on a host.

    Tracked by the Pilot system when placing replicas onto pilot jobs.  Included
    in each ReplicaStartRequest to start the replica on the desired GPUs.
    """

    hostname: str
    gpu_ids: list[str]


class SecretRef(str):
    """
    A pydantic field that allows secrets to be included as references to a
    remote resource in the declarative configuration.

    For example: the value SecretRef("env_var://METIS_API_KEY") is used so that the
    declarative configuration can be applied on a machine without access to the secret
    values. The gateway will resolve() the secret value just-in-time.

    Extend `_resolvers` to add support for additional Secret Vaulting strategies.
    """

    @staticmethod
    def _from_env_var(name: str) -> str:
        try:
            return os.environ[name]
        except KeyError:
            raise ValueError(f"environment variable {name!r} is not set")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(cls._validate),
            ]
        )

    @classmethod
    def _validate(cls, value: str) -> "SecretRef":
        if not any(value.startswith(pfx) for pfx in cls._prefixes):
            raise ValueError(f"Secret Ref must be prefixed by one of: {cls._prefixes}")
        return cls(value)

    def resolve(self) -> SecretStr:
        scheme, sep, payload = self.partition("://")
        if sep and scheme in self._resolvers:
            return SecretStr(self._resolvers[scheme](payload))
        raise AssertionError(f"No secret resolver registered for {self}")

    _resolvers: ClassVar[dict[str, Callable[[str], str]]] = {
        "env_var": _from_env_var,
    }

    _prefixes = sorted(f"{k}://" for k in _resolvers)
