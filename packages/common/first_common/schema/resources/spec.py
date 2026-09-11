import shlex
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from ..types import (
    DemandSignalConfig,
    DemandThresholdStrategy,
    HealthCheckParams,
    OverloadPolicy,
    PilotConfig,
    ResolvedLaunchSpec,
    ResourceName,
    RouterParams,
    RuntimeScriptContext,
    ScriptTemplateContext,
    SecretRef,
    UsagePolicy,
    render_script,
)

ParameterValue = StrictStr | StrictInt | StrictFloat | None
PreStopTimeout = Annotated[float, Field(gt=0, le=25.0)]
PostStopTimeout = Annotated[float, Field(gt=0, le=50.0)]


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

    max_model_len specifies the maximum context window for LLM models.  It is shared
    across all deployments of the model.  The same model at different context lengths
    is represented by multiple entries (e.g. claude-opus-4-8 vs. claude-opus-4-8-1m).
    """

    access_group_name: ResourceName
    supported_endpoints: list[str]
    aliases: list[str] = []
    usage_limits: UsagePolicy = UsagePolicy()
    overload: OverloadPolicy = OverloadPolicy()
    demand_signal: DemandSignalConfig = DemandSignalConfig()
    max_model_len: int | None = Field(None, ge=1)
    display_name: str | None = None
    capabilities: dict[str, Any] = {}

    @field_validator("supported_endpoints")
    @classmethod
    def normalize_endpoints(cls, v: list[str]) -> list[str]:
        return [e.strip().strip("/") for e in v]


class ClusterSpec(ResourceSpec):
    """
    An HPC cluster to which deployments are tied.

    If pilot_system is not None, this cluster is understood to support Pilot Job
    submissions.  Otherwise, it assumed that the cluster is used for
    StaticDeployments where model launching is handled externally.
    """

    health_check: HealthCheckParams
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

    health_check: HealthCheckParams

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval_sec: int = 15


class ScriptParameter(BaseModel):
    """A typed input that a `LaunchTemplate` requires from each deployment."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["str", "int", "float"]
    required: bool = False
    default: ParameterValue = None
    minimum: float | None = None
    maximum: float | None = None

    def coerce(self, name: str, value: ParameterValue) -> ParameterValue:
        """Validate a supplied value; null (or absent) inherits the default."""
        if value is None:
            value = self.default
        if value is None:
            if self.required:
                raise ValueError(f"parameter {name!r} is required")
            return None
        if self.type == "str":
            if not isinstance(value, str):
                raise ValueError(f"parameter {name!r} must be str")
            return value
        if isinstance(value, str) or (
            self.type == "int" and not isinstance(value, int)
        ):
            raise ValueError(f"parameter {name!r} must be {self.type}")
        if self.minimum is not None and value < self.minimum:
            raise ValueError(f"parameter {name!r} must be >= {self.minimum}")
        if self.maximum is not None and value > self.maximum:
            raise ValueError(f"parameter {name!r} must be <= {self.maximum}")
        return value

    @model_validator(mode="after")
    def check_default(self) -> Self:
        if self.default is not None:
            self.coerce("default", self.default)
        return self


class LaunchSpec(BaseModel):
    """
    Per-deployment inputs to a `LaunchTemplate`. Null overrides inherit the
    template's value.
    """

    model_config = ConfigDict(extra="forbid")

    served_model_name: str
    gpus_per_node: int = Field(gt=0)
    num_nodes: int = Field(gt=0)
    parameters: dict[str, ParameterValue] = {}
    env: dict[str, str] = {}

    max_startup_sec: int | None = Field(default=None, gt=0)
    max_unhealthy_sec: int | None = Field(default=None, gt=0)
    pre_stop_timeout_sec: PreStopTimeout | None = None
    post_stop_timeout_sec: PostStopTimeout | None = None
    health_check: HealthCheckParams | None = None


_SAMPLE_VALUES: dict[str, ParameterValue] = {"str": "x", "int": 1, "float": 1.0}
_SAMPLE_RUNTIME = RuntimeScriptContext(
    replica_name="replica",
    served_model_name="model",
    uds="/tmp/replica.sock",
    gpus_per_node=1,
    num_nodes=1,
    gpus_by_host={"host": ["0"]},
    env={},
    max_model_len=4096,
)


class LaunchTemplateSpec(ResourceSpec):
    """
    Reusable serve and stop scripts for a family of PilotDeployments, with the
    typed `parameters` each deployment must supply and lifecycle defaults a
    deployment may override.

    Templates are Jinja2 and may reference `runtime.*`, `parameters.*`, and the
    `quote` filter (see `ScriptTemplateContext`).
    """

    parameters: dict[str, ScriptParameter] = {}
    env: dict[str, str] = {}

    serve_script_template: str
    pre_stop_script_template: str | None = None
    post_stop_script_template: str | None = None

    max_startup_sec: int = Field(gt=0)
    max_unhealthy_sec: int | None = Field(default=None, gt=0)
    pre_stop_timeout_sec: PreStopTimeout = 20.0
    post_stop_timeout_sec: PostStopTimeout = 50.0
    health_check: HealthCheckParams

    @model_validator(mode="after")
    def check_templates(self) -> Self:
        reserved = [name for name in self.parameters if hasattr(dict, name)]
        if reserved:
            raise ValueError(f"reserved parameter names: {reserved}")
        # Render once against a fully populated context so typos, bad filters
        # and syntax errors fail at apply time rather than on the pilot.
        context = ScriptTemplateContext(
            runtime=_SAMPLE_RUNTIME,
            parameters={
                name: p.default if p.default is not None else _SAMPLE_VALUES[p.type]
                for name, p in self.parameters.items()
            },
            quote=shlex.quote,
        )
        for field in (
            "serve_script_template",
            "pre_stop_script_template",
            "post_stop_script_template",
        ):
            template = getattr(self, field)
            if template is not None:
                try:
                    render_script(template, context)
                except ValueError as exc:
                    raise ValueError(f"{field}: {exc}") from exc
        return self

    def resolve(
        self, launch: LaunchSpec, max_model_len: int | None
    ) -> ResolvedLaunchSpec:
        unknown = launch.parameters.keys() - self.parameters.keys()
        if unknown:
            raise ValueError(f"unknown parameters: {sorted(unknown)}")
        values = self.model_dump(exclude={"parameters", "env"})
        values.update(
            launch.model_dump(exclude_none=True, exclude={"parameters", "env"})
        )
        return ResolvedLaunchSpec(
            **values,
            max_model_len=max_model_len,
            env=self.env | launch.env,
            parameters={
                name: p.coerce(name, launch.parameters.get(name))
                for name, p in self.parameters.items()
            },
        )


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

    prometheus_metrics_path: str | None = "/metrics"
    prometheus_scrape_interval_sec: int = 15

    scaling_strategy: DemandThresholdStrategy | None = None
    min_replicas: int = 0
    max_replicas: int = 1

    launch_template_name: ResourceName
    launch_spec: LaunchSpec
    max_consecutive_launch_failures: int = 3
