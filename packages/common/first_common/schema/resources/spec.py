from typing import Any, Callable, ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    ImportString,
)

from ..types import (
    ClusterStatus,
    HealthEndpointStatus,
    LoadThresholdStrategy,
    PilotConfig,
    PilotLaunchSpec,
    ResourceName,
    RouterParams,
    SecretRef,
)


class ResourceSpec(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    registry: ClassVar[dict[str, type["ResourceSpec"]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Only direct subclasses are registered:
        if cls.__bases__[0] is ResourceSpec:
            if cls.__name__.endswith("Spec"):
                kind = cls.__name__[:-4]
                ResourceSpec.registry[kind] = cls
            else:
                raise RuntimeError(
                    "Direct subclass of ResourceSpec: name must end in 'Spec'"
                )


class AccessGroupSpec(ResourceSpec):
    allowed_groups: list[str] = []
    allowed_domains: list[str] = []


class ModelSpec(ResourceSpec):
    access_group_name: ResourceName
    supported_endpoints: list[str]


class ClusterSpec(ResourceSpec):
    status_method: ImportString[Callable[[dict[str, Any]], ClusterStatus]]
    status_kwargs: dict[str, Any]

    maintenance_notice: str | None = None
    pilot_system: PilotConfig | None = None


class StaticDeploymentSpec(ResourceSpec):
    cluster_name: ResourceName
    model_name: ResourceName

    api_url: str
    api_key: SecretRef | None = None
    upstream_model_name: str

    router_params: RouterParams = RouterParams()

    health_check_method: ImportString[Callable[[dict[str, Any]], HealthEndpointStatus]]
    health_check_kwargs: dict[str, Any]

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval: int = 15


class PilotDeploymentSpec(ResourceSpec):
    cluster_name: ResourceName
    model_name: ResourceName

    router_params: RouterParams = RouterParams()

    health_check_method: ImportString[Callable[[dict[str, Any]], HealthEndpointStatus]]
    health_check_kwargs: dict[str, Any]

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval: int = 15

    scaling_strategy: LoadThresholdStrategy | None = None
    min_replicas: int = 0
    max_replicas: int = 1

    launch_spec: PilotLaunchSpec
