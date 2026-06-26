from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ..base_scheduler import JobPhase
from ..pilot import PilotResources
from ..types import (
    ClusterStatus,
    DeploymentHealth,
    GpuClaim,
    HealthEndpointStatus,
    ReplicaPhase,
    ResourceName,
    RouterParams,
)
from . import spec


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
    name: str = Field(min_length=2, max_length=128)
    uid: int
    created_at: datetime


class AccessGroup(ResourceMeta, spec.AccessGroupSpec):
    """
    Specifies model access permissions by user group/domain membership.
    """

    kind: Literal["AccessGroup"] = "AccessGroup"

    pass


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
    health: DeploymentHealth
    last_health_check: datetime | None = None
    consecutive_launch_failures: int


class StaticDeployment(ResourceMeta, spec.StaticDeploymentSpec):
    """
    Concise information about StaticDeployments, where the model lifecycle is
    externally-managed and FIRST merely proxies to a given URL.
    """

    kind: Literal["StaticDeployment"] = "StaticDeployment"

    health: DeploymentHealth
    last_health_check: datetime | None = None
    # TODO: add load averages from redis


class PilotReplica(ResourceMeta):
    """
    A single model instance spawned by the parent PilotDeployment.

    Replicas are eventually placed onto pilot jobs where they begin executing
    and expose a `model_url` which the gateway can reach.
    """

    kind: Literal["PilotReplica"] = "PilotReplica"

    pilot_deployment_name: str
    pilot_job_name: str | None
    used_resources: list[GpuClaim]
    model_url: str | None
    observed_served_name: str

    phase: ReplicaPhase
    status_info: str
    last_health_check: datetime | None = None
    started_at: datetime | None = None


class PilotDeploymentDetail(PilotDeploymentSummary, spec.PilotDeploymentSpec):
    """
    Pilot Deployment information, including nested replicas
    """

    replicas: list[PilotReplica]
    # TODO: add load averages from redis


class ModelSummary(ResourceMeta, spec.ModelSpec):
    """
    Top-level Model, which may be backed by multiple deployments.

    The model resource specifies access permissions and what gateway API
    endpoints the model supports.

    Embeds a summary of deployments currently specified for this model.
    """

    kind: Literal["Model"] = "Model"
    pilot_deployments: list[PilotDeploymentSummary]
    static_deployments: list[StaticDeployment]


class PilotJob(ResourceMeta):
    """
    An HPC scheduler (e.g. PBS Pro) managed run of `first-pilot`.

    Submitted using the parent cluster's `pilot_system`.

    Eventually, when phase="running", the job exposes a `manager_url` which
    provides the control API to place and manage Replicas.
    """

    kind: Literal["PilotJob"] = "PilotJob"
    scheduler_job_id: str
    cluster_name: str
    phase: JobPhase
    manager_url: str | None
    manager_health: HealthEndpointStatus
    resources: PilotResources
    assigned_replicas: list[PilotReplica]
    time_started: datetime | None = None
    walltime_min: int
    num_nodes: int
    gpus_per_node: int


class ClusterSummary(ResourceMeta, spec.ClusterSpec):
    """
    An HPC cluster to which deployments are tied.  All StaticDeployments and
    PilotDeployments refer to a cluster for the sake of tracking cluster
    availability/maintenance status.
    """

    kind: Literal["Cluster"] = "Cluster"
    status: ClusterStatus
    last_status_check: datetime | None


class ClusterDetail(ClusterSummary):
    """
    HPC Cluster information with embedded details of pilot jobs currently
    associated with the cluster.
    """

    kind: Literal["Cluster"] = "Cluster"
    pilot_jobs: list[PilotJob]
