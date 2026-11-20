import asyncio
from collections.abc import Awaitable
from typing import TypeVar

from first_common.schema.base_scheduler import SchedulerAdapter
from first_common.schema.types import PilotConfig

from ...settings import ClientState
from .globus_compute_pbs import GlobusComputePBSAdapter
from .graphql_pbs import GraphQLPBSAdapter

# Upper bound on one scheduler call from a controller or observer. They work
# through jobs one at a time, so a hung scheduler stalls everything behind it.
SCHEDULER_RPC_TIMEOUT = 30.0

_T = TypeVar("_T")


async def build_scheduler(
    pilot: PilotConfig, client_state: ClientState
) -> SchedulerAdapter:
    """
    Construct a SchedulerAdapter from a PilotConfig and the process's shared
    client resources.
    """
    return await pilot.scheduler_adapter.build(client_state, pilot.scheduler_config)


async def scheduler_rpc(
    awaitable: Awaitable[_T], timeout: float = SCHEDULER_RPC_TIMEOUT
) -> _T:
    """Await a scheduler RPC with a hard timeout that names the call."""
    name = getattr(awaitable, "__qualname__", None) or repr(awaitable)
    try:
        async with asyncio.timeout(timeout) as cm:
            return await awaitable
    except TimeoutError as e:
        if not cm.expired():
            # The adapter raised its own (more specific) TimeoutError before
            # our deadline; don't hide its message behind ours.
            raise
        # asyncio.timeout raises a bare TimeoutError with an empty message;
        # name the RPC so record_failure() stores something actionable.
        raise TimeoutError(f"Scheduler {name} RPC timed out after {timeout:g}s") from e


__all__ = [
    "build_scheduler",
    "scheduler_rpc",
    "SCHEDULER_RPC_TIMEOUT",
    "GlobusComputePBSAdapter",
    "GraphQLPBSAdapter",
]
