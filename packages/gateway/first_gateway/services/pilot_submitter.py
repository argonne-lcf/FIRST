import json
from dataclasses import replace
from pathlib import Path
from shlex import quote

from first_common.schema.base_scheduler import (
    JobStatusInfo,
    JobSubmitPayload,
    JobSubmitResult,
    SchedulerAdapter,
    SchedulerJobState,
)
from first_common.schema.pilot import AddressInfo
from first_common.schema.resources.read import PilotJob
from first_common.schema.types import PilotConfig

from ..database import models as db
from ..platforms.schedulers.graphql_pbs import GraphQLPBSAdapter

_READY_SUFFIX = ".ready.json"


class PilotSubmitter:
    """
    Manages PilotJob lifecycles on top of a SchedulerAdapter.

    One instance is bound to one PilotConfig (one cluster). The adapter
    handles the raw HPC scheduler + filesystem RPC; this class layers
    pilot-specific concerns (script rendering, name namespacing, readyfile
    discovery) on top of it.
    """

    def __init__(self, pilot_config: PilotConfig, adapter: SchedulerAdapter) -> None:
        self.pilot_config = pilot_config
        self.adapter = adapter

    def _render_script(self, pilot_job: PilotJob | db.PilotJob) -> str:
        """
        The job body: exec the pilot against the pre-staged runtime config
        (CA + server cert), specialized for this job via PILOT_* overrides.
        """
        pc = self.pilot_config
        runtime_env = {
            "PILOT_CONFIG_FILE": str(pc.pilot_config_path),
            "PILOT_JOB_NAME": pilot_job.name,
            "PILOT_EXTERNAL_PORT": str(pc.external_port),
            "PILOT_NGINX_PATH": str(pc.nginx_path),
            "PILOT_IP_ALLOWLIST": json.dumps(pc.ip_allowlist, separators=(",", ":")),
            "PILOT_WORKDIR": str(pc.workdir),
            "PILOT_NODE_FILE_ENV": pc.node_file_env,
            "PILOT_GPU_DISCOVERY": json.dumps(
                pc.gpu_discovery.model_dump(mode="json"), separators=(",", ":")
            ),
            "PILOT_NUM_NODES": str(pilot_job.num_nodes),
            "PILOT_GPUS_PER_NODE": str(pilot_job.gpus_per_node),
            "PILOT_WALLTIME_MIN": str(pilot_job.walltime_min),
        }
        assignments = " ".join(f"{k}={quote(v)}" for k, v in runtime_env.items())
        return (
            f"{pc.submit_script_preamble}\n"
            # Keep the pilot at the batch-shell PID for scheduler signals.
            f"{assignments} exec {quote(str(pc.pilot_path))}\n"
        )

    async def submit(self, pilot_job: PilotJob | db.PilotJob) -> JobSubmitResult:
        pc = self.pilot_config
        submit_dir = pc.workdir / "submit_scripts"
        script = self._render_script(pilot_job)
        script_path: Path | None = None
        if not isinstance(self.adapter, GraphQLPBSAdapter):
            # GraphQL-PBS takes the script inline; Globus Compute needs a path.
            script_path = submit_dir / f"{pilot_job.name}.sh"
            await self.adapter.put_file(script, script_path, mode=0o755)

        payload = JobSubmitPayload(
            name=f"{pc.job_name_prefix}{pilot_job.name}",
            queue=pc.queue,
            account=pc.account,
            scheduler_flags=pc.scheduler_flags,
            num_nodes=pilot_job.num_nodes,
            gpus_per_node=pilot_job.gpus_per_node,
            walltime_min=pilot_job.walltime_min,
            log_path=submit_dir / f"{pilot_job.name}.log",
            script=None if script_path else script,
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
