from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, computed_field

from ..base_scheduler import SchedulerJobState
from ..pilot import PilotResources
from ..types import (
    RESOURCE_NAME_PATTERN,
    GpuClaim,
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
    ResourceName,
    RouterParams,
)
from . import spec
from .runtime import BackendRuntime, ModelRuntime, PilotJobRuntime


class _Overlay:
    """Attribute view: overrides first, then delegate to the wrapped object."""

    __slots__ = ("_obj", "_overrides")

    def __init__(self, obj: Any, **overrides: Any) -> None:
        self._obj = obj
        self._overrides = overrides

    def __getattr__(self, name: str) -> Any:
        try:
            return self._overrides[name]
        except KeyError:
            return getattr(self._obj, name)


class ResourceMeta(BaseModel):
    """
    Metadata common to all resource types.

    - kind identifies the database table in models.py
    - name is unique per `kind`
    - uid is the surrogate PK (rarely used, but can distinguish resource
    that was deleted and re-created with same name)
    """

    model_config = ConfigDict(from_attributes=True)

    kind: str
    name: str = Field(
        min_length=1,
        max_length=320,  # larger than spec to accomodate generated names
        pattern=RESOURCE_NAME_PATTERN,
        examples=["meta-llama/Meta-Llama-3.1-8B"],
    )
    uid: int
    created_at: datetime
    reconcile_failures: int = 0
    reconcile_last_error: str | None = None
    reconcile_last_traceback: str | None = None
    reconcile_retry_at: datetime | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def slug(self) -> str:
        """
        Case-sensitive slug, safe for use in a URL path segment.

        '/' is allowed in name but unsuitable for slugs; replace it by '~' to
        obtain a bijective mapping (~ is disallowed in name but slug-safe).
        """
        return self.name.replace("/", "~")

    @classmethod
    def merge(cls, obj: Any, **overrides: Any) -> Self:
        """
        Validate `obj` with additional attributes defined in overrides.
        """
        return cls.model_validate(_Overlay(obj, **overrides), from_attributes=True)


class AccessGroup(ResourceMeta, spec.AccessGroupSpec):
    """
    Specifies model access permissions by user group/domain membership.
    """

    kind: Literal["AccessGroup"] = "AccessGroup"

    pass


class LaunchTemplate(ResourceMeta, spec.LaunchTemplateSpec):
    """
    Reusable, typed launch scripts shared by a family of PilotDeployments.
    """

    kind: Literal["LaunchTemplate"] = "LaunchTemplate"


class PilotDeploymentSummary(ResourceMeta):
    """
    Concise information about pilot job-based deployments, omitting any replicas
    that may be running on the deployment currently.
    """

    kind: Literal["PilotDeployment"] = "PilotDeployment"

    cluster_name: ResourceName
    model_name: ResourceName
    router_params: RouterParams

    desired_replicas: int
    state: PilotDeploymentState
    consecutive_launch_failures: int
    last_startup_sec: float | None = None


class StaticDeploymentSummary(ResourceMeta, spec.StaticDeploymentSpec):
    """
    Concise information about StaticDeployments, where the model lifecycle is
    externally-managed and FIRST merely proxies to a given URL.
    """

    kind: Literal["StaticDeployment"] = "StaticDeployment"
    health: HealthCheckResult


class StaticDeploymentDetail(StaticDeploymentSummary):
    """
    Static Deployment with backend runtime information.
    """

    runtime: BackendRuntime = BackendRuntime()


class PilotReplica(ResourceMeta):
    """
    A single model instance spawned by the parent PilotDeployment.

    Replicas are eventually placed onto pilot jobs where they begin executing
    and expose a `model_url` which the gateway can reach.
    """

    kind: Literal["PilotReplica"] = "PilotReplica"

    pilot_deployment_name: str
    pilot_job_name: str | None
    claimed_gpu_ids: list[tuple[int, int]]
    resources: list[GpuClaim]
    model_url: str | None
    observed_served_name: str | None = None

    state: ReplicaState
    state_message: str
    started_at: datetime | None = None
    stopped_at: datetime | None = None
    deleted_at: datetime | None = None

    runtime: BackendRuntime = BackendRuntime()


class PilotDeploymentDetail(PilotDeploymentSummary, spec.PilotDeploymentSpec):
    """
    Pilot Deployment information, including nested replicas
    """

    replicas: list[PilotReplica]


class ModelSummary(ResourceMeta, spec.ModelSpec):
    """
    Top-level Model, which may be backed by multiple deployments.

    The model resource specifies access permissions and what gateway API
    endpoints the model supports.

    Embeds a summary of deployments currently specified for this model.
    `maintenance_notices` maps the name of each cluster hosting this model that
    is under maintenance to its notice.
    """

    kind: Literal["Model"] = "Model"
    pilot_deployments: list[PilotDeploymentSummary]
    static_deployments: list[StaticDeploymentSummary]
    maintenance_notices: dict[str, str] = {}
    runtime: ModelRuntime = ModelRuntime()


class PilotJob(ResourceMeta):
    """
    An HPC scheduler (e.g. PBS Pro) managed run of `first-pilot`.

    Submitted using the parent cluster's `pilot_system`.

    Eventually, when scheduler_state="running", the job exposes a `manager_url` which
    provides the control API to place and manage Replicas.
    """

    kind: Literal["PilotJob"] = "PilotJob"
    scheduler_job_id: str | None
    cluster_name: str
    scheduler_state: SchedulerJobState
    manager_url: str | None
    manager_health: HealthCheckResult
    resources: PilotResources
    claimed_gpu_ids: list[tuple[int, int]]
    assigned_replicas: list[PilotReplica]
    time_started: datetime | None = None
    idle_since: datetime | None = None
    walltime_min: int
    num_nodes: int
    gpus_per_node: int
    deleted_at: datetime | None = None
    failure_reason: str | None = None

    runtime: PilotJobRuntime | None = None


class ClusterSummary(ResourceMeta, spec.ClusterSpec):
    """
    An HPC cluster to which deployments are tied.  All StaticDeployments and
    PilotDeployments refer to a cluster for the sake of tracking cluster
    availability/maintenance status.
    """

    kind: Literal["Cluster"] = "Cluster"
    health: HealthCheckResult
    # New pilot submissions halt at pilot_system's
    # max_consecutive_pilot_start_failures.
    pilot_start_failures: int = 0


class ClusterDetail(ClusterSummary):
    """
    HPC Cluster information with embedded details of pilot jobs currently
    associated with the cluster.
    """

    kind: Literal["Cluster"] = "Cluster"
    pilot_jobs: list[PilotJob]


class ResourceHealth(BaseModel):
    """
    A single resource's health/state plus any reconcile error from the control
    plane.  `status` carries the resource's health or state enum value verbatim
    (e.g. HealthCheckResult, PilotDeploymentState, or ReplicaState); callers
    interpret criticality per resource type.
    """

    name: str
    uid: int
    status: str
    reconcile_failures: int = 0
    reconcile_last_error: str | None = None
    reconcile_last_traceback: str | None = None
    reconcile_retry_at: datetime | None = None


class ClusterHealth(ResourceHealth):
    """
    A cluster's ResourceHealth plus its pilot start failure streak.  New pilot
    submissions halt once `pilot_start_failures` reaches
    `max_consecutive_pilot_start_failures` (None without a pilot system).
    """

    pilot_start_failures: int = 0
    max_consecutive_pilot_start_failures: int | None = None


class PilotJobFailure(BaseModel):
    """A pilot job that died before its manager came up."""

    name: str
    uid: int
    cluster_name: str
    scheduler_state: SchedulerJobState
    failure_reason: str
    deleted_at: datetime | None = None


class SystemHealth(BaseModel):
    """
    One-glance operational snapshot: the health/state of every operational
    resource, grouped by type, with reconcile errors bundled in.

    `failed_pilot_jobs` lists pilot jobs that died before their manager came up:
    those ended in the last day, plus each cluster's current start failure
    streak; newest first.
    """

    clusters: list[ClusterHealth]
    static_deployments: list[ResourceHealth]
    pilot_deployments: list[ResourceHealth]
    pilot_jobs: list[ResourceHealth]
    pilot_replicas: list[ResourceHealth]
    failed_pilot_jobs: list[PilotJobFailure] = []
