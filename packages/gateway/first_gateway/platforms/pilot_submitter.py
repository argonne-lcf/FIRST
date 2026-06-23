from pathlib import Path

import yaml

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
)
from first_common.schema.pilot import AddressInfo, PilotRuntimeConfig
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import PilotConfig

from ..certmanager import generate_server_cert

PILOT_NAME_PREFIX = "__FIRST_PILOT_"
_READY_SUFFIX = ".ready.json"


class PilotSubmitter:
    """
    Manages PilotJob lifecycles on top of a SchedulerAdapter.

    One instance is bound to one PilotConfig (one cluster). The adapter
    handles the raw HPC scheduler + filesystem RPC; this class layers
    pilot-specific concerns (script rendering, cert injection, name
    namespacing, readyfile discovery) on top of it.
    """

    def __init__(
        self,
        pilot_config: PilotConfig,
        adapter: SchedulerAdapter,
        ca_crt: str,
        ca_key: str,
    ) -> None:
        self.pilot_config = pilot_config
        self.adapter = adapter
        self.ca_crt = ca_crt
        self.ca_key = ca_key

    async def submit(self, pilot_job: PilotJob) -> JobSubmitResult:
        pc = self.pilot_config
        name = pilot_job.name
        scheduler_name = f"{PILOT_NAME_PREFIX}{name}"

        server_crt, server_key = generate_server_cert(
            cn=name,
            ca_cert_pem=self.ca_crt,
            ca_key_pem=self.ca_key,
        )

        runtime_cfg = PilotRuntimeConfig(
            ca_crt=self.ca_crt,
            server_crt=server_crt,
            server_key=server_key,
            external_port=pc.external_port,
            nginx_path=pc.nginx_path,
            ip_allowlist=pc.ip_allowlist,
            workdir=pc.workdir,
            node_file_env=pc.node_file_env,
            job_name=name,
        )
        config_yaml = yaml.safe_dump(runtime_cfg.model_dump(mode="json"))

        config_path = pc.workdir / "submit_scripts" / f"{name}.config.yaml"
        script_path = pc.workdir / "submit_scripts" / f"{name}.sh"
        log_path = pc.workdir / "submit_scripts" / f"{name}.log"

        script = (
            f"{pc.submit_script_preamble}\n"
            f"PILOT_CONFIG_FILE={config_path} uvx first-pilot@{pc.pilot_version}\n"
        )

        await self.adapter.put_file(config_yaml, config_path, mode=0o600)
        await self.adapter.put_file(script, script_path, mode=0o755)

        payload = JobSubmitPayload(
            name=scheduler_name,
            queue=pc.queue,
            account=pc.account,
            scheduler_flags=pc.scheduler_flags,
            num_nodes=pilot_job.num_nodes,
            gpus_per_node=pilot_job.gpus_per_node,
            walltime_min=pilot_job.walltime_min,
            script_path=script_path,
            log_path=log_path,
        )
        return await self.adapter.submit_job(payload)

    async def get_statuses(self) -> list[JobStatusInfo]:
        all_jobs = await self.adapter.get_job_statuses()
        return [j for j in all_jobs if j.name.startswith(PILOT_NAME_PREFIX)]

    async def list_ready_endpoints(self) -> list[str]:
        files = await self.adapter.list_files(self._readyfile_dir)
        return [f[: -len(_READY_SUFFIX)] for f in files if f.endswith(_READY_SUFFIX)]

    async def get_endpoint(self, job_name: str) -> AddressInfo:
        path = self._readyfile_dir / f"{job_name}{_READY_SUFFIX}"
        content = await self.adapter.read_file(path)
        return AddressInfo.model_validate_json(content)

    @property
    def _readyfile_dir(self) -> Path:
        return self.pilot_config.workdir / "readyfiles"
