from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from first_common.schema.types import PilotConfig
from first_gateway.platforms.schedulers import build_scheduler
from first_gateway.platforms.schedulers.globus_compute_pbs import (
    GlobusComputePBSWrapper,
)
from first_gateway.settings import ClientState


@pytest.mark.asyncio
async def test_build_scheduler_dispatches_globus_compute_pbs() -> None:
    compute_client = MagicMock()
    # register_function is called once per command (6 total); each returns a
    # distinct function ID string.
    compute_client.register_function.side_effect = [f"fid-{i}" for i in range(6)]

    deps = cast(ClientState, SimpleNamespace(compute_client=compute_client))

    pilot = PilotConfig.model_validate(
        {
            "scheduler_interface": "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSWrapper",
            "scheduler_interface_config": {"endpoint_id": "endpoint-uuid-xyz"},
            "job_walltime": 60,
            "queue": "debug",
            "account": "ALCFTest",
        }
    )

    scheduler = await build_scheduler(pilot, deps)

    assert isinstance(scheduler, GlobusComputePBSWrapper)
    assert scheduler.endpoint_id == "endpoint-uuid-xyz"
    assert scheduler.client is compute_client
    assert compute_client.register_function.call_count == 6
