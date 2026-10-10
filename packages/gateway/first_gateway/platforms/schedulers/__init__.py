from first_common.schema.base_scheduler import SchedulerAdapter
from first_common.schema.types import PilotConfig

from ...settings import ClientState
from .globus_compute_pbs import GlobusComputePBSAdapter
from .graphql_pbs import GraphQLPBSAdapter


async def build_scheduler(
    pilot: PilotConfig, client_state: ClientState
) -> SchedulerAdapter:
    """
    Construct a SchedulerAdapter from a PilotConfig and the process's shared
    client resources.
    """
    config = dict(pilot.scheduler_config)
    if "quarantine_environment" in config or "_quarantine_environment" in config:
        raise ValueError(
            "configure node quarantine through pilot_system.quarantine, not legacy scheduler flags"
        )
    if pilot.quarantine is not None:
        config["_quarantine_environment"] = pilot.quarantine.environment
    return await pilot.scheduler_adapter.build(client_state, config)


__all__ = ["build_scheduler", "GlobusComputePBSAdapter", "GraphQLPBSAdapter"]
