import os
from enum import Enum
from pathlib import Path
from typing import Any, Callable, ClassVar, Literal, NewType

from pydantic import (
    BaseModel,
    GetCoreSchemaHandler,
    ImportString,
    SecretStr,
)
from pydantic_core import core_schema

from .base_scheduler import SchedulerAdapter

ResourceName = NewType("ResourceName", str)


class HealthEndpointStatus(str, Enum):
    """
    Result from /health API check
    """

    healthy = "healthy"
    unhealthy = "unhealthy"
    unknown = "unknown"


class DeploymentHealth(str, Enum):
    """
    Aggregated deployment health
    """

    offline = "offline"  # No replicas exist / all pending
    starting = "starting"  # All replicas are placed or launching
    healthy = "healthy"  # Up at full capacity
    degraded = "degraded"  # Up at partial capacity
    unhealthy = "unhealthy"  # Replicas exist, but none are ready


class ClusterStatus(str, Enum):
    """
    Overall status of an HPC / Inference Cluster
    """

    up = "up"
    down = "down"
    degraded = "degraded"
    maintenance = "maintenance"
    unknown = "unknown"


class ReplicaPhase(str, Enum):
    """
    Lifecycle of a single AI model instance (replica).
    """

    pending = "pending"  # desired; awaiting placement (no GPUs claimed)
    placed = "placed"  # GPUs claimed on a PilotJob; not yet launched
    launching = "launching"  # control plane Popen'd it; weights loading
    # poll until ready, timeout, or exited
    ready = "ready"  # serving; registered with the router
    unhealthy = "unhealthy"  # was READY, now failing /health
    error = "error"  # Process exited with nonzero status code
    start_timeout = "start_timeout"  # Did not become healthy within max_startup_time
    terminating = "terminating"  # being torn down
    terminated = "terminated"  # finished tear down


class RouterParams(BaseModel):
    weight: int = 1
    rpm: int | None = None
    tpm: int | None = None
    max_parallel_requests: int | None = None
    order: int | None = None


class PilotConfig(BaseModel):
    scheduler_adapter: ImportString[type[SchedulerAdapter]]
    scheduler_interface_config: dict[str, Any] = {}

    job_walltime: int
    queue: str
    account: str
    scheduler_flags: str = ""


class LoadThresholdStrategy(BaseModel):
    strategy: Literal["LoadThresholdStrategy"] = "LoadThresholdStrategy"
    scale_up_interval: int = 120  # scale up at most once / 2 min
    min_scale_down_age: int = 7200  # a started replica lives 2 hr before scale-down
    check_interval: int = 30  # evaluate scaling every 30 s

    # Ordered (load_lower_bound_exclusive, num_replicas). At/below the lowest
    # threshold, scale to min_replicas.
    scaling_thresholds: list[tuple[float, int]] = [
        (0.0, 1),  # load > 0 (not idle) → 1 replica
        (10.0, 2),  # load > 120          → 2 replicas
    ]


class PilotLaunchSpec(BaseModel):
    served_model_name: str

    num_gpus: int
    num_nodes: int

    venv_path: Path
    weights_path: Path
    weights_cache_path: Path

    env: dict[str, str]
    serve_script_template: str

    max_startup_time: int
    health_path: str | None = "/health"


class GpuClaim(BaseModel):
    hostname: str
    gpu_ids: list[str]


class SecretRef(str):
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
