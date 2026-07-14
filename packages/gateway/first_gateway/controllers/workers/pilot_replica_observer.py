import asyncio
import logging
import ssl
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import sqlalchemy as sa

from first_common.schema.pilot import PilotJobStatus, ReplicaInfo
from first_common.schema.resources.runtime import PilotJobRuntime
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

    Writes:
    - PilotJob.{resources, manager_health, manager_unhealthy_since, idle_since}
    - PilotReplica.{state, state_message, model_url, observed_served_name, started_at}
    - PilotDeployment.consecutive_launch_failures

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

        ctx = ssl.create_default_context(cadata=settings.pilot_ca_crt)
        ctx.check_hostname = False

        client_crt_pem, client_key_pem = generate_client_cert(
            cn="pilot-replica-observer",
            ca_cert_pem=settings.pilot_ca_crt,
            ca_key_pem=settings.pilot_ca_key.get_secret_value(),
        )

        with tempfile.TemporaryDirectory(delete=True) as tmpdir:
            crt_path = Path(tmpdir) / "client.crt"
            key_path = Path(tmpdir) / "client.key"
            crt_path.write_text(client_crt_pem)
            key_path.write_text(client_key_pem)
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

        successful_deployments: set[str] = set()
        deploy_fail_counts: dict[str, int] = defaultdict(int)

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

            success, fail = await self._sync_replicas(job, status.replicas)
            successful_deployments |= success
            for key, count in fail.items():
                deploy_fail_counts[key] += count

            await self._reap_orphans(job, status.replicas)

        await self._update_consecutive_launch_failures(
            successful_deployments, deploy_fail_counts
        )

    async def _fetch_status(self, manager_url: str) -> PilotJobStatus:
        resp = await self._http.get(f"{manager_url}/status")
        resp.raise_for_status()
        return PilotJobStatus.model_validate(resp.json())

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
        id_clause = PilotJob.uid == job.uid

        # If we have manager status response, it's healthy:
        if job.manager_health != HealthCheckResult.healthy.value:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(
                        id_clause,
                        PilotJob.manager_health.is_distinct_from(
                            HealthCheckResult.healthy.value
                        ),
                    )
                    .values(
                        manager_health=HealthCheckResult.healthy.value,
                        manager_unhealthy_since=None,
                    )
                )

        # Update Resources in DB on startup only (default empty dict):
        resources_dict = status.resources.model_dump(mode="json")
        if not job.resources:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause)
                    .values(resources=resources_dict)
                )

        # Update in Redis unconditionally (tracking GPU memory usage # continuously)
        await self.client_state.redis_repo.set_pilot_job_runtime(
            job.uid, PilotJobRuntime(resources=status.resources)
        )

        has_running = any(
            replica.state
            in (
                ReplicaState.placed,
                ReplicaState.launching,
                ReplicaState.ready,
                ReplicaState.unhealthy,
            )
            for replica in status.replicas
        )

        # Mark or clear idle_since timestamp
        if has_running and job.idle_since is not None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause, PilotJob.idle_since.is_not(None))
                    .values(idle_since=None)
                )
        elif not has_running and job.idle_since is None:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotJob)
                    .where(id_clause, PilotJob.idle_since.is_(None))
                    .values(idle_since=datetime.now(timezone.utc))
                )

    async def _sync_replicas(
        self,
        job: PilotJob,
        remote_replicas: list[ReplicaInfo],
    ) -> tuple[set[str], dict[str, int]]:
        """
        Update PilotReplica DB state and return (success_deployments, fail_counts).
        """
        success_deployments: set[str] = set()
        fail_counts: defaultdict[str, int] = defaultdict(int)

        for ri in remote_replicas:
            async with self.client_state.db_sessionmaker() as sess:
                db_replica = await sess.scalar(
                    sa.select(PilotReplica).where(
                        PilotReplica.name == ri.name,
                        PilotReplica.pilot_job_name == job.name,
                    )
                )

            if db_replica is None:
                continue

            values: dict[str, Any] = {}

            if ri.state.value != db_replica.state:
                values["state"] = ri.state.value

                # Only on state transition, track successes and failures:
                if ri.state == ReplicaState.ready:
                    success_deployments.add(db_replica.pilot_deployment_name)
                elif ri.state in (ReplicaState.error, ReplicaState.start_timeout):
                    fail_counts[db_replica.pilot_deployment_name] += 1

            if ri.state_message != db_replica.state_message:
                values["state_message"] = ri.state_message

            if ri.url != db_replica.model_url:
                values["model_url"] = ri.url

            if ri.served_model_name != db_replica.observed_served_name:
                values["observed_served_name"] = ri.served_model_name

            if ri.started_at != db_replica.started_at:
                values["started_at"] = ri.started_at

            if not values:
                continue

            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotReplica)
                    .where(PilotReplica.uid == db_replica.uid)
                    .values(**values)
                )

        return success_deployments, fail_counts

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

            logger.warning("Reaping orphan replica %s on job %s", ri.name, job.name)
            try:
                await self._stop_replica(job.manager_url, ri.name)  # type: ignore[arg-type]
            except Exception:
                logger.exception(
                    "Failed to stop orphan replica %s on job %s",
                    ri.name,
                    job.name,
                )

    async def _stop_replica(self, manager_url: str, replica_name: str) -> None:
        resp = await self._http.post(f"{manager_url}/stop-replica/{replica_name}")
        resp.raise_for_status()

    async def _update_consecutive_launch_failures(
        self, successful_deployments: set[str], fail_counts: dict[str, int]
    ) -> None:
        for deployment_name in successful_deployments:
            async with self.client_state.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(PilotDeployment)
                    .where(
                        PilotDeployment.name == deployment_name,
                        PilotDeployment.consecutive_launch_failures != 0,
                    )
                    .values(consecutive_launch_failures=0)
                )

        for deployment_name, fail_count in fail_counts.items():
            if fail_count > 0 and deployment_name not in successful_deployments:
                async with self.client_state.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(PilotDeployment)
                        .where(PilotDeployment.name == deployment_name)
                        .values(
                            consecutive_launch_failures=(
                                PilotDeployment.consecutive_launch_failures + fail_count
                            )
                        )
                    )
