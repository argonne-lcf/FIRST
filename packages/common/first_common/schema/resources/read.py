from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from ..types import (
    ClusterStatus,
    DeploymentHealth,
    GpuClaim,
    HealthEndpointStatus,
    PilotJobPhase,
    ReplicaPhase,
    ResourceName,
    RouterParams,
)
from . import spec


class ResourceMeta(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    kind: str
    name: str = Field(min_length=2, max_length=128)
    uid: int
    created_at: datetime


class AccessGroup(ResourceMeta, spec.AccessGroupSpec):
    pass


class PilotDeploymentSummary(ResourceMeta):
    cluster_name: ResourceName
    model_name: ResourceName
    router_params: RouterParams

    desired_replicas: int
    health: DeploymentHealth
    last_health_check: datetime | None = None
    consecutive_launch_failures: int


class StaticDeployment(ResourceMeta, spec.StaticDeploymentSpec):
    health: DeploymentHealth
    last_health_check: datetime | None = None


class PilotReplica(ResourceMeta):
    pilot_deployment_name: str
    pilot_job_name: str | None
    used_resources: list[GpuClaim]
    model_url: str | None
    observed_served_name: str

    phase: ReplicaPhase
    health: HealthEndpointStatus
    status_info: dict[str, str]
    last_health_check: datetime | None = None


class PilotDeploymentDetail(PilotDeploymentSummary, spec.PilotDeploymentSpec):
    replicas: list[PilotReplica]


class ModelSummary(ResourceMeta, spec.ModelSpec):
    pilot_deployments: list[PilotDeploymentSummary]
    static_deployments: list[StaticDeployment]


class PilotJob(ResourceMeta):
    scheduler_job_id: str
    cluster_name: str
    phase: PilotJobPhase
    manager_url: str
    manager_health: HealthEndpointStatus
    resources: list[GpuClaim]
    assigned_replicas: list[PilotReplica]
    time_started: datetime | None = None
    walltime_sec: int


class ClusterSummary(ResourceMeta, spec.ClusterSpec):
    status: ClusterStatus
    last_status_check: datetime | None


class ClusterDetail(ClusterSummary):
    pilot_jobs: list[PilotJob]
