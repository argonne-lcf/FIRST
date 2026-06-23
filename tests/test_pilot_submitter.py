from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self

import pytest
import yaml

from first_common.schema.base_scheduler import (
    JobPhase,
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
)
from first_common.schema.pilot import AddressInfo
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import HealthEndpointStatus, PilotConfig
from first_gateway.certmanager import gen_ca_pem
from first_gateway.platforms.pilot_submitter import (
    PILOT_NAME_PREFIX,
    PilotSubmitter,
)


class FakeSchedulerAdapter(SchedulerAdapter):
    """In-memory adapter that records submissions and stages files."""

    def __init__(self) -> None:
        self.files: dict[str, tuple[str, int]] = {}
        self.directories: dict[str, list[str]] = {}
        self.submitted: list[JobSubmitPayload] = []
        self.statuses: list[JobStatusInfo] = []

    @classmethod
    async def build(cls, _client_state: Any, _config: dict[str, Any]) -> Self:
        return cls()

    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult:
        self.submitted.append(job_spec)
        return JobSubmitResult(job_name=job_spec.name, scheduler_id="42.fake")

    async def get_job_statuses(self) -> list[JobStatusInfo]:
        return list(self.statuses)

    async def terminate_job(self, _job_id: str) -> None:
        raise NotImplementedError

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        self.files[str(path)] = (content, mode)

    async def list_files(self, directory: Path) -> list[str]:
        return list(self.directories.get(str(directory), []))

    async def read_file(self, path: Path) -> str:
        return self.files[str(path)][0]


@pytest.fixture
def ca_pair() -> tuple[str, str]:
    return gen_ca_pem(name="test-ca")


@pytest.fixture
def pilot_config(tmp_path: Path) -> PilotConfig:
    nginx_path = tmp_path / "nginx"
    nginx_path.write_text("#!/bin/sh\n")
    return PilotConfig.model_validate(
        {
            "scheduler_adapter": "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter",
            "scheduler_interface_config": {},
            "job_walltime": 60,
            "queue": "debug",
            "account": "TestAcct",
            "scheduler_flags": "-l filesystems=home",
            "workdir": str(tmp_path / "pilot_workdir"),
            "external_port": 8443,
            "nginx_path": str(nginx_path),
            "ip_allowlist": ["10.0.0.0/8"],
            "node_file_env": "PBS_NODEFILE",
            "submit_script_preamble": "#!/bin/bash\nset -eu\nmodule load python",
            "pilot_version": "0.1.2",
        }
    )


def _make_pilot_job(name: str) -> PilotJob:
    return PilotJob(
        kind="PilotJob",
        name=name,
        uid=1,
        created_at=datetime.now(timezone.utc),
        scheduler_job_id="",
        cluster_name="testcluster",
        phase=JobPhase.pending_submit,
        manager_url="",
        manager_health=HealthEndpointStatus.unknown,
        resources=[],
        assigned_replicas=[],
        walltime_min=120,
        num_nodes=2,
        gpus_per_node=4,
    )


async def test_submit_renders_config_and_script(
    pilot_config: PilotConfig, ca_pair: tuple[str, str]
) -> None:
    ca_crt, ca_key = ca_pair
    adapter = FakeSchedulerAdapter()
    submitter = PilotSubmitter(pilot_config, adapter, ca_crt, ca_key)

    pilot_job = _make_pilot_job("alpha-7")
    result = await submitter.submit(pilot_job)

    config_path = pilot_config.workdir / "submit_scripts" / "alpha-7.config.yaml"
    script_path = pilot_config.workdir / "submit_scripts" / "alpha-7.sh"

    config_content, config_mode = adapter.files[str(config_path)]
    script_content, script_mode = adapter.files[str(script_path)]

    assert config_mode == 0o600
    assert script_mode == 0o755

    parsed = yaml.safe_load(config_content)
    assert parsed["job_name"] == "alpha-7"
    assert parsed["ca_crt"] == ca_crt
    assert parsed["external_port"] == 8443
    assert "BEGIN CERTIFICATE" in parsed["server_crt"]
    assert "BEGIN" in parsed["server_key"]

    assert script_content.startswith(pilot_config.submit_script_preamble)
    assert f"PILOT_CONFIG_FILE={config_path} uvx first-pilot@0.1.2" in script_content

    assert len(adapter.submitted) == 1
    payload = adapter.submitted[0]
    assert payload.name == f"{PILOT_NAME_PREFIX}alpha-7"
    assert payload.queue == "debug"
    assert payload.account == "TestAcct"
    assert payload.scheduler_flags == "-l filesystems=home"
    assert payload.num_nodes == 2
    assert payload.gpus_per_node == 4
    assert payload.walltime_min == 120
    assert payload.script_path == script_path
    assert payload.log_path == pilot_config.workdir / "submit_scripts" / "alpha-7.log"

    assert result.job_name == f"{PILOT_NAME_PREFIX}alpha-7"
    assert result.scheduler_id == "42.fake"


async def test_get_statuses_filters_by_prefix(
    pilot_config: PilotConfig, ca_pair: tuple[str, str]
) -> None:
    adapter = FakeSchedulerAdapter()
    now = datetime.now(timezone.utc)
    adapter.statuses = [
        JobStatusInfo(
            id="1",
            name=f"{PILOT_NAME_PREFIX}mine",
            state=JobPhase.running,
            created_at=now,
            started_at=now,
            walltime_minutes=60,
        ),
        JobStatusInfo(
            id="2",
            name="someone-else",
            state=JobPhase.running,
            created_at=now,
            started_at=now,
            walltime_minutes=60,
        ),
    ]
    submitter = PilotSubmitter(pilot_config, adapter, *ca_pair)

    statuses = await submitter.get_statuses()
    assert [s.name for s in statuses] == [f"{PILOT_NAME_PREFIX}mine"]


async def test_list_ready_endpoints_strips_suffix(
    pilot_config: PilotConfig, ca_pair: tuple[str, str]
) -> None:
    adapter = FakeSchedulerAdapter()
    adapter.directories[str(pilot_config.workdir / "readyfiles")] = [
        "alpha.ready.json",
        "beta.ready.json",
        "ignore.txt",
    ]
    submitter = PilotSubmitter(pilot_config, adapter, *ca_pair)

    assert sorted(await submitter.list_ready_endpoints()) == ["alpha", "beta"]


async def test_get_endpoint_roundtrips_address_info(
    pilot_config: PilotConfig, ca_pair: tuple[str, str]
) -> None:
    addr = AddressInfo(
        hostname="x3001",
        ip="10.1.2.3",
        external_port=8443,
        control_path="/control",
    )
    adapter = FakeSchedulerAdapter()
    path = pilot_config.workdir / "readyfiles" / "alpha.ready.json"
    adapter.files[str(path)] = (addr.model_dump_json(), 0o644)
    submitter = PilotSubmitter(pilot_config, adapter, *ca_pair)

    got = await submitter.get_endpoint("alpha")
    assert got.hostname == addr.hostname
    assert got.ip == addr.ip
    assert got.external_port == addr.external_port
    assert got.control_path == addr.control_path
