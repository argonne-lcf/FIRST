from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import pytest

from first_common.schema.types import PilotConfig
from first_gateway.platforms.schedulers import build_scheduler
from first_gateway.platforms.schedulers.globus_compute_pbs import (
    GlobusComputePBSAdapter,
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
            "scheduler_adapter": "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter",
            "scheduler_config": {"endpoint_id": "endpoint-uuid-xyz"},
            "job_walltime": 60,
            "queue": "debug",
            "account": "ALCFTest",
            "workdir": "/tmp/pilot-workdir",
            "external_port": 8443,
            "nginx_path": "/usr/bin/nginx",
            "ip_allowlist": ["10.0.0.0/8"],
            "node_file_env": "PBS_NODEFILE",
            "submit_script_preamble": "#!/bin/bash",
            "pilot_version": "0.1.0",
        }
    )

    scheduler = await build_scheduler(pilot, deps)

    assert isinstance(scheduler, GlobusComputePBSAdapter)
    assert scheduler.endpoint_id == "endpoint-uuid-xyz"
    assert scheduler.client is compute_client
    assert compute_client.register_function.call_count == 6
