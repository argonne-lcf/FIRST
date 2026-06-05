from typing import Any, TypedDict

from pydantic import BaseModel

from resource_server_async.errors import ClusterUnderMaintenance


class ClusterStatus(TypedDict, total=False):
    status: str
    cluster: str
    message: str


class CheckMaintenanceResult(BaseModel):
    is_under_maintenance: bool
    message: str

    def raise_if_down(self) -> None:
        """
        Raise `ClusterUnderMaintenance` if the cluster maintenance status is currently down.
        """
        if self.is_under_maintenance:
            raise ClusterUnderMaintenance(self.message)


class JobInfo(BaseModel):
    Models: str
    Framework: str
    Cluster: str
    model_config = {"extra": "allow"}  # Open dictionary that allow more fields


class JobsByStatus(BaseModel):
    running: list[JobInfo] = []
    queued: list[JobInfo] = []
    stopped: list[JobInfo] = []
    others: list[JobInfo] = []
    private_batch_running: list[JobInfo] = []
    private_batch_queued: list[JobInfo] = []

    cluster_status: dict[str, Any] = {}
