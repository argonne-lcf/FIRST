from pydantic import BaseModel

from .types import GpuClaim, PilotLaunchSpec


class ReplicaStartRequest(BaseModel):
    name: str
    launch_spec: PilotLaunchSpec
    resources: list[GpuClaim]
