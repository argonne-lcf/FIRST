import asyncio
import logging
import ssl
import tempfile
from datetime import datetime, timezone
from typing import Any

import httpx
import sqlalchemy as sa

from first_common.schema.pilot import PilotJobStatus, ReplicaInfo
from first_common.schema.types import HealthCheckResult, ReplicaState

from ...database.models import PilotDeployment, PilotJob, PilotReplica
from ...services.certmanager import generate_client_cert
from ...settings import ClientState
from ..worker import Worker

logger = logging.getLogger(__name__)


class PilotReplicaObserver(Worker):
    """
    Polls GET /status on each running pilot manager and syncs replica state
    back to Postgres.

    Writes: PilotJob.{resources, manager_health, manager_unhealthy_since,
    idle_since}; PilotReplica.{state, state_message, model_url,
    observed_served_name, started_at}; PilotDeployment.consecutive_launch_failures.

    Reaps orphan replicas reported by a pilot manager that have no matching
    PilotReplica row (or a row pointing at a different PilotJob).
    """

    poll_interval: float = 10.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 120.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
            restart_backoff=restart_backoff,
            max_backoff=max_backoff,
            heartbeat_timeout=heartbeat_timeout,
        )
        self._http = self._build_http_client(client_state)

    @staticmethod
    def _build_http_client(client_state: ClientState) -> httpx.AsyncClient:
        settings = client_state.settings
        ca_crt = settings.pilot_ca_crt
        ca_key = settings.pilot_ca_key.get_secret_value()

        client_crt_pem, client_key_pem = generate_client_cert(
            cn="pilot-replica-observer",
            ca_cert_pem=ca_crt,
            ca_key_pem=ca_key,
        )

        tmpdir = tempfile.mkdtemp(prefix="pilot_obs_")
        ca_path = f"{tmpdir}/ca.crt"
        crt_path = f"{tmpdir}/client.crt"
        key_path = f"{tmpdir}/client.key"

        for path, content in [
            (ca_path, ca_crt),
            (crt_path, client_crt_pem),
            (key_path, client_key_pem),
        ]:
            with open(path, "w") as f:
                f.write(content)

        ctx = ssl.create_default_context(cafile=ca_path)
        ctx.check_hostname = False
        ctx.load_cert_chain(crt_path, key_path)

        return httpx.AsyncClient(verify=ctx, timeout=10.0)

    async def run(self) -> None:
        hb = self.register_heartbeat("poll")
        while True:
            hb.beat()
            try:
                await self._poll()
            except Exception:
                logger.exception("%s: poll failed", self.name)
            await asyncio.sleep(self.poll_interval)

    async def _poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            jobs = list(
                await sess.scalars(
                    sa.select(PilotJob).where(
                        PilotJob.scheduler_state == "running",
                        PilotJob.manager_url.is_not(None),
                    )
                )
            )

        deployment_results: dict[str, list[str]] = {}

        for job in jobs:
            try:
                assert job.manager_url is not None
                status = await self._fetch_status(job.manager_url)
            except Exception:
                logger.exception(
                    "%s: failed to fetch /status from job %s at %s",
                    self.name,
                    job.name,
                    job.manager_url,
                )
                await self._record_unhealthy(job)
                continue

            await self._update_job_status(job, status)
            await self._sync_replicas(job, status.replicas, deployment_results)
            await self._reap_orphans(job, status.replicas)

        await self._update_consecutive_launch_failures(deployment_results)

    async def _fetch_status(self, manager_url: str) -> PilotJobStatus:
        resp = await self._http.get(f"{manager_url}/status")
        resp.raise_for_status()
        return PilotJobStatus.model_validate(resp.json())

    async def _stop_replica(self, manager_url: str, replica_name: str) -> None:
        resp = await self._http.post(f"{manager_url}/stop-replica/{replica_name}")
        resp.raise_for_status()

    async def _record_unhealthy(self, job: PilotJob) -> None:
        now = datetime.now(timezone.utc)
        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(PilotJob)
                .where(
                    PilotJob.uid == job.uid,
                    PilotJob.manager_health.is_distinct_from(
                        HealthCheckResult.unhealthy.value
                    ),
                )
                .values(
                    manager_health=HealthCheckResult.unhealthy.value,
                    manager_unhealthy_since=now,
                )
            )

    async def _update_job_status(self, job: PilotJob, status: PilotJobStatus) -> None:
        resources_dict = status.resources.model_dump(mode="json")
        has_running = any(
            r.state
            in (
                ReplicaState.ready,
                ReplicaState.launching,
                ReplicaState.placed,
                ReplicaState.unhealthy,
            )
            for r in status.replicas
        )

        values: dict[str, Any] = {}
        wheres: list[Any] = [PilotJob.uid == job.uid]

        if resources_dict != job.resources:
            values["resources"] = resources_dict
            wheres.append(PilotJob.resources.is_distinct_from(resources_dict))

        if job.manager_health != HealthCheckResult.healthy.value:
            values["manager_health"] = HealthCheckResult.healthy.value
            values["manager_unhealthy_since"] = None
            wheres.append(
                PilotJob.manager_health.is_distinct_from(
                    HealthCheckResult.healthy.value
                )
            )

        if has_running and job.idle_since is not None:
            values["idle_since"] = None
            wheres.append(PilotJob.idle_since.is_not(None))
        elif not has_running and job.idle_since is None:
            values["idle_since"] = datetime.now(timezone.utc)
            wheres.append(PilotJob.idle_since.is_(None))

        if not values:
            return

        async with self.client_state.db_sessionmaker.begin() as sess:
            await sess.execute(sa.update(PilotJob).where(*wheres).values(**values))

    async def _sync_replicas(
        self,
        job: PilotJob,
        remote_replicas: list[ReplicaInfo],
        deployment_results: dict[str, list[str]],
    ) -> None:
        for ri in remote_replicas:
            async with self.client_state.db_sessionmaker() as sess:
                replica = await sess.scalar(
                    sa.select(PilotReplica).where(
                        PilotReplica.name == ri.name,
                        PilotReplica.pilot_job_name == job.name,
                    )
                )

            if replica is None:
                continue

            deployment_name = replica.pilot_deployment_name
            deployment_results.setdefault(deployment_name, []).append(ri.state.value)

            values: dict[str, Any] = {}
            wheres: list[Any] = [PilotReplica.uid == replica.uid]

            if ri.state.value != replica.state:
                values["state"] = ri.state.value
                wheres.append(PilotReplica.state.is_distinct_from(ri.state.value))
            if ri.state_message != replica.state_message:
                values["state_message"] = ri.state_message
                wheres.append(
                    PilotReplica.state_message.is_distinct_from(ri.state_message)
                )
            if ri.url != replica.model_url:
                values["model_url"] = ri.url
                wheres.append(PilotReplica.model_url.is_distinct_from(ri.url))
            if ri.served_model_name != replica.observed_served_name:
                values["observed_served_name"] = ri.served_model_name
                wheres.append(
                    PilotReplica.observed_served_name.is_distinct_from(
                        ri.served_model_name
                    )
                )
            if ri.started_at != replica.started_at:
                values["started_at"] = ri.started_at
                wheres.append(PilotReplica.started_at.is_distinct_from(ri.started_at))

            if not values:
                continue

            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotReplica).where(*wheres).values(**values)
                )

    async def _reap_orphans(
        self, job: PilotJob, remote_replicas: list[ReplicaInfo]
    ) -> None:
        for ri in remote_replicas:
            async with self.client_state.db_sessionmaker() as sess:
                exists = await sess.scalar(
                    sa.select(
                        sa.exists().where(
                            PilotReplica.name == ri.name,
                            PilotReplica.pilot_job_name == job.name,
                        )
                    )
                )

            if exists:
                continue

            logger.info("Reaping orphan replica %s on job %s", ri.name, job.name)
            try:
                await self._stop_replica(job.manager_url, ri.name)  # type: ignore[arg-type]
            except Exception:
                logger.exception(
                    "Failed to stop orphan replica %s on job %s",
                    ri.name,
                    job.name,
                )

    async def _update_consecutive_launch_failures(
        self, deployment_results: dict[str, list[str]]
    ) -> None:
        for deployment_name, states in sorted(deployment_results.items()):
            has_success = ReplicaState.ready.value in states
            failure_states = {
                ReplicaState.error.value,
                ReplicaState.start_timeout.value,
            }
            failure_count = sum(1 for s in states if s in failure_states)

            if has_success:
                async with self.client_state.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(PilotDeployment)
                        .where(
                            PilotDeployment.name == deployment_name,
                            PilotDeployment.consecutive_launch_failures != 0,
                        )
                        .values(consecutive_launch_failures=0)
                    )
            elif failure_count > 0:
                async with self.client_state.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(PilotDeployment)
                        .where(
                            PilotDeployment.name == deployment_name,
                        )
                        .values(
                            consecutive_launch_failures=(
                                PilotDeployment.consecutive_launch_failures
                                + failure_count
                            )
                        )
                    )
