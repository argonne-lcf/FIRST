from first_common.schema.base_scheduler import SchedulerAdapter
from first_common.schema.types import PilotConfig

from ...settings import ClientState
from .globus_compute_pbs import GlobusComputePBSAdapter


async def build_scheduler(
    pilot: PilotConfig, client_state: ClientState
) -> SchedulerAdapter:
    """
    Construct a SchedulerAdapter from a PilotConfig and the process's shared
    client resources.
    """
    return await pilot.scheduler_adapter.build(client_state, pilot.scheduler_config)


__all__ = ["build_scheduler", "GlobusComputePBSAdapter"]
