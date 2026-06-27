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
    """
    Base class for registering specs that can be referenced in a
    `ResourceManifest`.
    """

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
    """
    Specifies model access permissions by user group/domain membership.
    """

    allowed_groups: list[str] = []
    allowed_domains: list[str] = []


class ModelSpec(ResourceSpec):
    """
    The top-level Model resource, which may be backed by multiple child
    deployments.

    The model resource specifies access permissions and what gateway API
    endpoints the model supports.
    """

    access_group_name: ResourceName
    supported_endpoints: list[str]


class ClusterSpec(ResourceSpec):
    """
    An HPC cluster to which deployments are tied.

    If pilot_system is not None, this cluster is understood to support Pilot Job
    submissions.  Otherwise, it assumed that the cluster is used for
    StaticDeployments where model launching is handled externally.
    """

    status_method: ImportString[Callable[[dict[str, Any]], ClusterStatus]]
    status_kwargs: dict[str, Any]

    maintenance_notice: str | None = None
    pilot_system: PilotConfig | None = None


class StaticDeploymentSpec(ResourceSpec):
    """
    Static Deployments of a Model should be used when the model lifecycle is
    externally-managed and FIRST merely proxies to a given URL.

    The deployment is "static" in the sense that we do nothing to start or scale
    the model.
    """

    cluster_name: ResourceName
    model_name: ResourceName

    api_url: str
    api_key: SecretRef | None = None
    upstream_model_name: str

    router_params: RouterParams = RouterParams()

    health_check_method: ImportString[Callable[[dict[str, Any]], HealthEndpointStatus]]
    health_check_kwargs: dict[str, Any]

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval_sec: int = 15


class PilotDeploymentSpec(ResourceSpec):
    """
    Pilot Deployments of a Model should be used when the model is launched
    inside of an HPC job allocation. The `first-pilot` package and command line
    entrypoint provides an mTLS-secured control plane and replica process
    manager to spawn model replicas dynamically.

    Pilot deployments are auto-scaled when `scaling_strategy` is set.
    Otherwise, use the `set_desired_pilot_replicas` API to manually scale the
    deployment.
    """

    cluster_name: ResourceName
    model_name: ResourceName

    router_params: RouterParams = RouterParams()

    health_check_method: ImportString[Callable[[dict[str, Any]], HealthEndpointStatus]]
    health_check_kwargs: dict[str, Any]

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval_sec: int = 15

    scaling_strategy: LoadThresholdStrategy | None = None
    min_replicas: int = 0
    max_replicas: int = 1

    launch_spec: PilotLaunchSpec
