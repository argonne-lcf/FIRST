from first_common.schema.base_scheduler import SchedulerInterface
from first_common.schema.types import PilotConfig

from ...settings import ClientState
from .globus_compute_pbs import GlobusComputePBSWrapper


async def build_scheduler(
    pilot: PilotConfig, client_state: ClientState
) -> SchedulerInterface:
    """
    Construct a SchedulerInterface from a PilotConfig and the process's shared
    client resources.
    """
    return await pilot.scheduler_interface.build(
        client_state, pilot.scheduler_interface_config
    )


__all__ = ["build_scheduler", "GlobusComputePBSWrapper"]
