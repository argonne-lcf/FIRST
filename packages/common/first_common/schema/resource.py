from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from first_common.schema import resource_specs as spec

from .types import (
    ClusterStatus,
    DeploymentHealth,
    GpuClaim,
    HealthEndpointStatus,
    PilotJobPhase,
    ReplicaPhase,
    ResourceName,
    RouterParams,
)


class ResourceBase(BaseModel):
    kind: str
    name: str = Field(min_length=2, max_length=128)
    uid: int
    created_at: datetime


class AccessGroup(ResourceBase, spec.AccessGroup):
    pass


class PilotDeploymentSummary(ResourceBase):
    cluster_name: ResourceName
    model_name: ResourceName
    router_params: RouterParams

    desired_replicas: int
    health: DeploymentHealth
    last_health_check: datetime | None = None
    consecutive_launch_failures: int


class StaticDeployment(ResourceBase, spec.StaticDeployment):
    health: DeploymentHealth
    last_health_check: datetime | None = None


class PilotReplica(ResourceBase):
    pilot_deployment_name: str
    pilot_job_name: str | None
    used_resources: list[GpuClaim]
    model_url: str | None
    observed_served_name: str

    phase: ReplicaPhase
    health: HealthEndpointStatus
    status_info: dict[str, str]
    last_health_check: datetime | None = None


class PilotDeploymentDetail(PilotDeploymentSummary, spec.PilotDeployment):
    replicas: list[PilotReplica]


class ModelSummary(ResourceBase, spec.Model):
    pilot_deployments: list[PilotDeploymentSummary]
    static_deployments: list[StaticDeployment]


class PilotJob(ResourceBase):
    scheduler_job_id: str
    cluster_uid: int
    phase: PilotJobPhase
    manager_url: str
    manager_health: HealthEndpointStatus
    resources: list[GpuClaim]
    assigned_replicas: list[PilotReplica]
    time_started: datetime | None = None
    walltime_sec: int


class ClusterSummary(ResourceBase, spec.Cluster):
    model_config = ConfigDict(from_attributes=True)
    status: ClusterStatus
    last_status_check: datetime | None


class ClusterDetail(ClusterSummary):
    pilot_jobs: list[PilotJob]
