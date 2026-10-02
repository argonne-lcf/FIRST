import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Self

import pytest
from pydantic import ValidationError

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.pilot import AddressInfo, PilotResources
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import (
    HealthCheckResult,
    PalsDiscovery,
    PilotConfig,
    SSHDiscovery,
)
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.services.pilot_submitter import PilotSubmitter


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

    async def get_exact_job_status(self, job_id: str) -> JobStatusInfo | None:
        return None

    async def terminate_job(self, _job_id: str) -> None:
        raise NotImplementedError

    async def put_file(self, content: str, path: Path, mode: int) -> None:
        self.files[str(path)] = (content, mode)

    async def list_files(self, directory: Path) -> list[str]:
        return list(self.directories.get(str(directory), []))

    async def read_file(self, path: Path) -> str:
        return self.files[str(path)][0]


class FakeGraphQLSchedulerAdapter(GraphQLPBSAdapter):
    """GraphQL marker adapter that only records the rendered submit payload."""

    def __init__(self) -> None:
        self.submitted: list[JobSubmitPayload] = []

    async def submit_job(self, job_spec: JobSubmitPayload) -> JobSubmitResult:
        self.submitted.append(job_spec)
        return JobSubmitResult(job_name=job_spec.name, scheduler_id="42.fake")


@pytest.fixture
def pilot_config(tmp_path: Path) -> PilotConfig:
    nginx_path = tmp_path / "nginx"
    nginx_path.write_text("#!/bin/sh\n")
    return PilotConfig.model_validate(
        {
            "scheduler_adapter": "first_gateway.platforms.schedulers.globus_compute_pbs.GlobusComputePBSAdapter",
            "scheduler_config": {},
            "job_walltime_min": 60,
            "queue": "debug",
            "account": "TestAcct",
            "max_num_nodes": 10,
            "gpus_per_node": 8,
            "scheduler_flags": "-l filesystems=home",
            "workdir": str(tmp_path / "pilot_workdir"),
            "external_port": 8443,
            "nginx_path": str(nginx_path),
            "ip_allowlist": ["10.0.0.0/8"],
            "node_file_env": "PBS_NODEFILE",
            "gpu_discovery": {
                "method": "pals",
                "launcher_path": "/opt/test/mpiexec",
            },
            "submit_script_preamble": "#!/bin/bash\nset -eu\nmodule load python",
            "pilot_path": "/test/first-pilot",
            "pilot_config_path": "/opt/test/pilot-config.yaml",
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
        scheduler_state=SchedulerJobState.pending_submit,
        manager_url="",
        manager_health=HealthCheckResult.unknown,
        resources=PilotResources(hosts=[]),
        assigned_replicas=[],
        claimed_gpu_ids=[],
        walltime_min=120,
        num_nodes=2,
        gpus_per_node=4,
    )


async def test_submit_places_script_and_submits_by_path(
    pilot_config: PilotConfig,
) -> None:
    adapter = FakeSchedulerAdapter()
    submitter = PilotSubmitter(pilot_config, adapter)

    pilot_job = _make_pilot_job("alpha-7")
    result = await submitter.submit(pilot_job)

    # Only the script is placed: certs live in the pre-staged runtime config.
    script_path = pilot_config.workdir / "submit_scripts" / "alpha-7.sh"
    assert list(adapter.files) == [str(script_path)]
    script_content, script_mode = adapter.files[str(script_path)]
    assert script_mode == 0o755

    assert script_content.startswith(pilot_config.submit_script_preamble)
    for assignment in (
        "PILOT_CONFIG_FILE=/opt/test/pilot-config.yaml",
        "PILOT_JOB_NAME=alpha-7",
        "PILOT_EXTERNAL_PORT=8443",
        "PILOT_NUM_NODES=2",
        "PILOT_GPUS_PER_NODE=4",
        "PILOT_WALLTIME_MIN=120",
    ):
        assert assignment in script_content
    assert script_content.endswith(" exec /test/first-pilot\n")

    assert len(adapter.submitted) == 1
    payload = adapter.submitted[0]
    assert payload.name == f"{pilot_config.job_name_prefix}alpha-7"
    assert payload.queue == "debug"
    assert payload.account == "TestAcct"
    assert payload.scheduler_flags == "-l filesystems=home"
    assert payload.num_nodes == 2
    assert payload.gpus_per_node == 4
    assert payload.walltime_min == 120
    assert payload.script is None
    assert payload.script_path == script_path
    assert payload.log_path == pilot_config.workdir / "submit_scripts" / "alpha-7.log"

    assert result.job_name == f"{pilot_config.job_name_prefix}alpha-7"
    assert result.scheduler_id == "42.fake"


async def test_submit_script_is_identical_across_adapters(
    pilot_config: PilotConfig,
) -> None:
    by_path = FakeSchedulerAdapter()
    inline = FakeGraphQLSchedulerAdapter()
    for adapter in (by_path, inline):
        await PilotSubmitter(pilot_config, adapter).submit(_make_pilot_job("same"))

    script_path = pilot_config.workdir / "submit_scripts" / "same.sh"
    assert inline.submitted[0].script_path is None
    assert inline.submitted[0].script == by_path.files[str(script_path)][0]


async def test_submit_accepts_multi_node_ssh_discovery(
    pilot_config: PilotConfig,
) -> None:
    ssh_config = pilot_config.model_copy(update={"gpu_discovery": SSHDiscovery()})
    adapter = FakeSchedulerAdapter()
    submitter = PilotSubmitter(ssh_config, adapter)

    await submitter.submit(_make_pilot_job("portable-ssh"))

    assert len(adapter.submitted) == 1
    script_path = ssh_config.workdir / "submit_scripts" / "portable-ssh.sh"
    script = adapter.files[str(script_path)][0]
    assert """PILOT_GPU_DISCOVERY='{"method":"ssh","timeout_sec":5.0}'""" in script


def test_pals_discovery_requires_launcher_path(pilot_config: PilotConfig) -> None:
    raw_config = pilot_config.model_dump()
    raw_config["gpu_discovery"] = {"method": "pals"}

    with pytest.raises(ValidationError, match="launcher_path"):
        PilotConfig.model_validate(raw_config)


def test_gpu_discovery_defaults_to_ssh(pilot_config: PilotConfig) -> None:
    raw_config = pilot_config.model_dump(exclude={"gpu_discovery"})

    config = PilotConfig.model_validate(raw_config)

    assert config.gpu_discovery == SSHDiscovery()


async def test_graphql_submit_serializes_and_quotes_discovery_environment(
    pilot_config: PilotConfig,
) -> None:
    config = pilot_config.model_copy(
        update={
            "gpu_discovery": PalsDiscovery(launcher_path=Path("/opt/test/mpiexec")),
            "ip_allowlist": ["192.0.2.10/32"],
            "pilot_config_path": Path("/opt/test/pilot config.yaml"),
            "pilot_path": Path("/opt/test/first pilot"),
        }
    )
    adapter = FakeGraphQLSchedulerAdapter()

    await PilotSubmitter(config, adapter).submit(_make_pilot_job("graphql-pilot"))

    assert len(adapter.submitted) == 1
    script = adapter.submitted[0].script
    assert script is not None
    assert "PILOT_CONFIG_FILE='/opt/test/pilot config.yaml'" in script
    assert "PILOT_IP_ALLOWLIST='[\"192.0.2.10/32\"]'" in script
    assert (
        "PILOT_GPU_DISCOVERY="
        '\'{"method":"pals","launcher_path":"/opt/test/mpiexec",'
        '"timeout_sec":35.0}\''
    ) in script
    assert "PILOT_PALS_PATH" not in script
    assert "PILOT_IP_ALLOWLIST_JSON" not in script
    assert script.endswith("exec '/opt/test/first pilot'\n")


@pytest.mark.parametrize("graphql", [False, True])
@pytest.mark.parametrize("exit_code", [0, 7])
async def test_submit_exec_preserves_batch_pid_environment_and_exit_status(
    pilot_config: PilotConfig,
    tmp_path: Path,
    graphql: bool,
    exit_code: int,
) -> None:
    """Run the real rendered shell, with only an owned, short CPU stand-in."""
    pilot = tmp_path / "pilot with 'quoted' spaces"
    pilot.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "print(json.dumps({'pid': os.getpid(), 'config': "
        "os.environ['PILOT_CONFIG_FILE'], 'job': "
        "os.environ.get('PILOT_JOB_NAME')}), flush=True)\n"
        f"sys.exit({exit_code})\n"
    )
    pilot.chmod(0o700)
    config = pilot_config.model_copy(
        update={
            "pilot_path": pilot,
            "workdir": tmp_path / "literal 'quotes' and $dollar",
            "pilot_config_path": tmp_path / "prebaked 'config' $literal.yaml",
            "submit_script_preamble": (
                '#!/bin/bash\nset -euo pipefail\nprintf "%s\\n" "$$"'
            ),
        }
    )
    name = "exec-cpu-only"
    adapter: FakeGraphQLSchedulerAdapter | FakeSchedulerAdapter
    adapter = FakeGraphQLSchedulerAdapter() if graphql else FakeSchedulerAdapter()
    await PilotSubmitter(config, adapter).submit(_make_pilot_job(name))
    if isinstance(adapter, FakeGraphQLSchedulerAdapter):
        script = adapter.submitted[0].script
    else:
        script_path = config.workdir / "submit_scripts" / f"{name}.sh"
        script = adapter.files[str(script_path)][0]
    assert script is not None
    completed = subprocess.run(
        ["/bin/bash", "-s"],
        input=script,
        text=True,
        capture_output=True,
        timeout=5,
        check=False,
        env={"PATH": "/usr/bin:/bin"},
    )
    assert completed.returncode == exit_code, completed.stderr
    shell_pid, child_json = completed.stdout.splitlines()
    child = json.loads(child_json)
    assert child["pid"] == int(shell_pid)
    assert child["config"] == str(config.pilot_config_path)
    assert child["job"] == name


async def test_get_statuses_filters_by_prefix(
    pilot_config: PilotConfig,
) -> None:
    adapter = FakeSchedulerAdapter()
    now = datetime.now(timezone.utc)
    adapter.statuses = [
        JobStatusInfo(
            id="1",
            name=f"{pilot_config.job_name_prefix}mine",
            state=SchedulerJobState.running,
            created_at=now,
            started_at=now,
            walltime_minutes=60,
        ),
        JobStatusInfo(
            id="2",
            name="someone-else",
            state=SchedulerJobState.running,
            created_at=now,
            started_at=now,
            walltime_minutes=60,
        ),
    ]
    submitter = PilotSubmitter(pilot_config, adapter)

    statuses = await submitter.get_statuses()
    assert [s.name for s in statuses] == ["mine"]


async def test_list_ready_endpoints_strips_suffix(
    pilot_config: PilotConfig,
) -> None:
    adapter = FakeSchedulerAdapter()
    adapter.directories[str(pilot_config.workdir / "readyfiles")] = [
        "alpha.ready.json",
        "beta.ready.json",
        "ignore.txt",
    ]
    submitter = PilotSubmitter(pilot_config, adapter)

    assert sorted(await submitter.list_ready_endpoints()) == ["alpha", "beta"]


async def test_get_endpoint_roundtrips_address_info(
    pilot_config: PilotConfig,
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
    submitter = PilotSubmitter(pilot_config, adapter)

    got = await submitter.get_endpoint("alpha")
    assert got.hostname == addr.hostname
    assert got.ip == addr.ip
    assert got.external_port == addr.external_port
    assert got.control_path == addr.control_path
