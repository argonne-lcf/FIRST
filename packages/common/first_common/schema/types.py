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
    Aggregated state of a PilotDeployment
    """

    offline = "offline"  # No replicas exist / all pending
    starting = "starting"  # At least one replica is placed or launching; none are healthy or unhealthy
    healthy = "healthy"  # All are healthy
    partial_capacity = "partial_capacity"  # At least one is healthy
    unhealthy = "unhealthy"  # None are healthy, at least one is unhealthy


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
    max_concurrent_jobs: int = 100
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


class DemandEstimate(BaseModel):
    """
    Combines in-flight request count with an estimate of
    rejected-but-would-be-inflight demand to produce a single demand metric.

    - `reject_window_sec`: rejection rate is obtained from the rise in total
    model rejections over this window
    - `avg_request_duration_sec`: rejections/sec is multiplied by this duration
    to obtain the expected number of requests that would be running if the
    rejected traffic had been admitted.
    """

    reject_window_sec: int = 60
    avg_request_duration_sec: int = 30

    def calculate_demand(self, inflight: float, reject_rate: float) -> float:
        return inflight + reject_rate * self.avg_request_duration_sec


class DemandThresholdStrategy(BaseModel):
    """
    A method for automatically scaling a PilotDeployment by tracking the average
    demand (in-flight requests and rejections of would-be inflight requests) and
    setting the desired number of replicas by a ladder of thresholds.
    """

    strategy: Literal["DemandThresholdStrategy"] = "DemandThresholdStrategy"
    demand: DemandEstimate = DemandEstimate()

    # --- Scale-up policy ---
    # Autoscaler samples demand every ~10s.
    # Exponentially weighted moving average demand must exceed the threshold
    # before scaling up (ewma = alpha*sample + (1-alpha)*ewma)
    ewma_alpha: float = Field(0.5, ge=0.01, le=1.0)

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
