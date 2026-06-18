from datetime import datetime
from typing import Any, Callable, ClassVar, NamedTuple

from pydantic import (
    BaseModel,
    ConfigDict,
    ImportString,
    SerializeAsAny,
    model_validator,
)

from .types import (
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
            ResourceSpec.registry[cls.__name__] = cls


class ResourceApply(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str
    name: ResourceName
    spec: SerializeAsAny[ResourceSpec]

    @model_validator(mode="before")
    @classmethod
    def _resolve(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        kind = data.get("kind")
        if not kind:
            raise ValueError("resource kind must be specified")

        spec_cls = ResourceSpec.registry.get(kind)
        if spec_cls is None:
            raise ValueError(
                f"unknown resource kind {kind!r}; "
                f"known: {sorted(ResourceSpec.registry)}"
            )

        return {**data, "spec": spec_cls.model_validate(data.get("spec"))}


class AccessGroup(ResourceSpec):
    allowed_groups: list[str] = []
    allowed_domains: list[str] = []


class Model(ResourceSpec):
    access_group_name: ResourceName
    supported_endpoints: list[str]


class Cluster(ResourceSpec):
    status_method: ImportString[Callable[[dict[str, Any]], ClusterStatus]]
    status_kwargs: dict[str, Any]

    maintenance_notice: str | None = None
    pilot_system: PilotConfig | None = None


class StaticDeployment(ResourceSpec):
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


class PilotDeployment(ResourceSpec):
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


class ResourceIdentifier(BaseModel):
    kind: str
    name: str


class FieldChange(NamedTuple):
    old: Any
    new: Any


class ResourcePatch(BaseModel):
    kind: str
    name: str
    patch: dict[str, FieldChange]


class ResourceChangePlan(BaseModel):
    previous_version: int
    no_change: list[ResourceIdentifier]
    to_delete: list[ResourceIdentifier]
    to_add: SerializeAsAny[list[ResourceApply]]
    to_update: list[ResourcePatch]


class ConfigVersion(BaseModel):
    uid: int
    applied_at: datetime
    applied_by: str
    changes: dict[str, Any]
