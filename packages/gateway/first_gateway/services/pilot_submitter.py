from dataclasses import replace
from math import ceil
from pathlib import Path

import yaml

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.pilot import AddressInfo, PilotRuntimeConfig
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import PilotConfig

from ..database import models as db
from ..platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from .certmanager import generate_server_cert

_READY_SUFFIX = ".ready.json"


class _BlockStringDumper(yaml.SafeDumper):
    """SafeDumper that emits multi-line strings as block literals (|)."""


def _str_representer(dumper: _BlockStringDumper, data: str) -> yaml.ScalarNode:
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


_BlockStringDumper.add_representer(str, _str_representer)


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

    async def submit(self, pilot_job: PilotJob | db.PilotJob) -> JobSubmitResult:
        pc = self.pilot_config
        name = pilot_job.name
        scheduler_name = f"{pc.job_name_prefix}{name}"
        log_path = pc.workdir / "submit_scripts" / f"{name}.log"

        script: str | None = None
        script_path: Path | None = None

        if isinstance(self.adapter, GraphQLPBSAdapter):
            # No filesystem access: the runtime config is a pre-baked YAML already
            # on the target system, referenced by path. The script is submitted
            # inline (no put_file).
            if pc.pilot_config_path is None:
                raise ValueError(
                    "GraphQLPBSAdapter requires PilotConfig.pilot_config_path"
                )
            script = (
                f"{pc.submit_script_preamble}\n"
                f'PILOT_CONFIG_FILE={pc.pilot_config_path} PILOT_JOB_NAME="{name}" '
                f"{pc.pilot_path}\n"
            )
        else:
            # Filesystem-backed: render the runtime config and submit script
            # just-in-time, then place them on the target system.
            server_crt, server_key = generate_server_cert(
                cn=name,
                ca_cert_pem=self.ca_crt,
                ca_key_pem=self.ca_key,
                days=ceil(self.pilot_config.job_walltime_min / 60 / 24),
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
            config_yaml = yaml.dump(
                runtime_cfg.model_dump(mode="json"), Dumper=_BlockStringDumper
            )

            config_path = pc.workdir / "submit_scripts" / f"{name}.config.yaml"
            script_path = pc.workdir / "submit_scripts" / f"{name}.sh"
            body = (
                f"{pc.submit_script_preamble}\n"
                f"PILOT_CONFIG_FILE={config_path} {pc.pilot_path}\n"
            )

            await self.adapter.put_file(config_yaml, config_path, mode=0o600)
            await self.adapter.put_file(body, script_path, mode=0o755)

        payload = JobSubmitPayload(
            name=scheduler_name,
            queue=pc.queue,
            account=pc.account,
            scheduler_flags=pc.scheduler_flags,
            num_nodes=pilot_job.num_nodes,
            gpus_per_node=pilot_job.gpus_per_node,
            walltime_min=pilot_job.walltime_min,
            log_path=log_path,
            script=script,
            script_path=script_path,
        )
        return await self.adapter.submit_job(payload)

    async def get_statuses(self) -> list[JobStatusInfo]:
        all_jobs = await self.adapter.get_job_statuses()
        result = []
        for job in all_jobs:
            if job.name.startswith(self.pilot_config.job_name_prefix):
                job = replace(
                    job, name=job.name.removeprefix(self.pilot_config.job_name_prefix)
                )
                result.append(job)
        return result

    async def list_ready_endpoints(self) -> list[str]:
        if isinstance(self.adapter, GraphQLPBSAdapter):
            # No filesystem access: a job is ready once it is running and the
            # scheduler reports its head node's IP.
            return [
                s.name
                for s in await self.get_statuses()
                if s.state == SchedulerJobState.running and s.head_node_ip_address
            ]
        files = await self.adapter.list_files(self._readyfile_dir)
        return [f[: -len(_READY_SUFFIX)] for f in files if f.endswith(_READY_SUFFIX)]

    async def get_endpoint(self, job_name: str) -> AddressInfo:
        if isinstance(self.adapter, GraphQLPBSAdapter):
            for s in await self.get_statuses():
                if (
                    s.name == job_name
                    and s.state == SchedulerJobState.running
                    and s.head_node_ip_address
                ):
                    ip = s.head_node_ip_address
                    return AddressInfo(
                        hostname=s.head_node_hostname or ip,
                        ip=ip,
                        external_port=self.pilot_config.external_port,
                        control_path="/control/",
                    )
            raise ValueError(f"No ready endpoint for job {job_name!r}")
        path = self._readyfile_dir / f"{job_name}{_READY_SUFFIX}"
        content = await self.adapter.read_file(path)
        return AddressInfo.model_validate_json(content)

    @property
    def _readyfile_dir(self) -> Path:
        return self.pilot_config.workdir / "readyfiles"
