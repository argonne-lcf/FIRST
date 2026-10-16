"""Opt-in pilot start accounting races on the same isolated PostgreSQL fixture.

Inherits the five insertion-fence regressions as a combined safety check.
PilotControlClient is mocked before construction: no certificates, RPC, Redis,
scheduler, pilot, or model resources are accessed.
"""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from unittest import main
from unittest.mock import MagicMock, patch

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import JobStatusInfo, SchedulerJobState
from first_common.schema.types import ReplicaState, ResourceName
from first_gateway.controllers.workers.pilot_job_observer import PilotJobObserver
from first_gateway.database.models import (
    Cluster,
    PilotDeployment,
    PilotJob,
    PilotReplica,
)

from . import test_replica_reconciler_fence_postgres as fence_tests

NOW = datetime(2026, 9, 13, tzinfo=timezone.utc)


class PilotFailureAccountingPostgresTests(fence_tests.ReplicaInsertionPostgresTests):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        with patch(
            "first_gateway.controllers.workers.pilot_job_observer.PilotControlClient"
        ):
            self.observer = PilotJobObserver(
                "accounting-test", self.controller.client_state, MagicMock()
            )

    async def job(
        self,
        name: str = "test-job",
        *,
        manager_ready: bool = False,
        intentional_deletion: bool = False,
        replica_draining: bool = False,
    ) -> PilotJob:
        async with self.db.begin() as session:
            job = PilotJob(
                name=name,
                cluster_name="test-cluster",
                scheduler_job_id=f"{name}.offline",
                scheduler_state=SchedulerJobState.running.value,
                manager_url="https://offline.invalid/control"
                if manager_ready
                else None,
                scheduled_deletion_at=NOW if intentional_deletion else None,
                time_started=NOW,
                walltime_min=15,
                num_nodes=1,
                gpus_per_node=4,
            )
            session.add(job)
            await session.flush()
            session.add(
                PilotReplica(
                    name=f"test-deployment/replica/{name}",
                    pilot_deployment_name=self.stale.name,
                    pilot_job_name=name,
                    state=ReplicaState.placed.value,
                    scheduled_deletion_at=NOW if replica_draining else None,
                )
            )
        return job

    @staticmethod
    def exiting(job: PilotJob) -> JobStatusInfo:
        assert job.scheduler_job_id is not None
        return JobStatusInfo(
            id=job.scheduler_job_id,
            name=job.name,
            state=SchedulerJobState.exiting,
            created_at=NOW,
            started_at=NOW,
            walltime_minutes=15,
        )

    async def failures(self) -> int:
        """The cluster's pilot start failure count."""
        async with self.db() as session:
            result = await session.scalar(
                sa.select(Cluster.pilot_start_failures).where(
                    Cluster.name == "test-cluster"
                )
            )
        assert result is not None
        return result

    async def deployment_failures(self) -> list[int]:
        async with self.db() as session:
            return list(
                await session.scalars(
                    sa.select(PilotDeployment.consecutive_launch_failures)
                )
            )

    async def test_exiting_accounts_before_drainer_erases_assignment_then_gone(
        self,
    ) -> None:
        job = await self.job()
        await self.observer._update_job(job, self.exiting(job))
        self.assertEqual(await self.failures(), 1)
        async with self.db.begin() as session:
            await session.execute(
                sa.update(PilotReplica)
                .where(PilotReplica.pilot_job_name == job.name)
                .values(
                    pilot_job_name=None,
                    scheduled_deletion_at=NOW,
                    deleted_at=NOW,
                )
            )
            await session.execute(
                sa.update(PilotJob)
                .where(PilotJob.uid == job.uid)
                .values(scheduled_deletion_at=NOW)
            )
        await self.observer._update_job(job, None)
        self.assertEqual(await self.failures(), 1)

    async def test_duplicate_concurrent_terminal_observers_count_once(self) -> None:
        job = await self.job()
        async with self.db.begin() as blocker:
            await blocker.scalar(
                sa.select(PilotJob).where(PilotJob.uid == job.uid).with_for_update()
            )
            first = self.task(self.observer._update_job(job, self.exiting(job)))
            second = self.task(self.observer._update_job(job, self.exiting(job)))
            await self.wait_for_lock_waiters(2)
        await asyncio.wait_for(asyncio.gather(first, second), timeout=5)
        await self.observer._update_job(job, None)
        self.assertEqual(await self.failures(), 1)

    async def test_direct_disappearance_counts_once(self) -> None:
        job = await self.job()
        await self.observer._update_job(job, None)
        await self.observer._update_job(job, None)
        self.assertEqual(await self.failures(), 1)

    async def test_terminal_states_reject_stale_observations_without_recounting(
        self,
    ) -> None:
        job = await self.job()
        exiting = self.exiting(job)
        await self.observer._update_job(job, exiting)
        for state in (
            SchedulerJobState.queued,
            SchedulerJobState.starting,
            SchedulerJobState.running,
        ):
            with self.subTest(after="exiting", observed=state):
                await self.observer._update_job(job, replace(exiting, state=state))
                async with self.db() as session:
                    current = await session.get(PilotJob, job.uid)
                assert current is not None
                self.assertEqual(
                    current.scheduler_state, SchedulerJobState.exiting.value
                )
        await self.observer._update_job(job, None)
        for state in (
            SchedulerJobState.exiting,
            SchedulerJobState.queued,
            SchedulerJobState.starting,
            SchedulerJobState.running,
        ):
            with self.subTest(after="gone", observed=state):
                await self.observer._update_job(job, replace(exiting, state=state))
                async with self.db() as session:
                    current = await session.get(PilotJob, job.uid)
                assert current is not None
                self.assertEqual(current.scheduler_state, SchedulerJobState.gone.value)
        await self.observer._update_job(job, None)
        self.assertEqual(await self.failures(), 1)

    async def test_nonterminal_scheduler_transitions_including_requeue_remain_allowed(
        self,
    ) -> None:
        job = await self.job()
        for state in (
            SchedulerJobState.queued,
            SchedulerJobState.starting,
            SchedulerJobState.running,
        ):
            with self.subTest(observed=state):
                await self.observer._update_job(
                    job, replace(self.exiting(job), state=state)
                )
                async with self.db() as session:
                    current = await session.get(PilotJob, job.uid)
                assert current is not None
                self.assertEqual(current.scheduler_state, state.value)
                self.assertEqual(await self.failures(), 0)

    async def test_counts_once_per_pilot_without_charging_deployments(self) -> None:
        """Replicas on the job, draining or not, don't change the accounting."""
        job = await self.job(replica_draining=True)
        async with self.db.begin() as session:
            other = self.new_deployment()
            other.name = ResourceName("other-deployment")
            session.add(other)
            await session.flush()
            session.add_all(
                [
                    PilotReplica(
                        name="test-deployment/replica/second",
                        pilot_deployment_name=self.stale.name,
                        pilot_job_name=job.name,
                        state=ReplicaState.placed.value,
                    ),
                    PilotReplica(
                        name="other-deployment/replica/one",
                        pilot_deployment_name=other.name,
                        pilot_job_name=job.name,
                        state=ReplicaState.placed.value,
                    ),
                ]
            )
        await self.observer._update_job(job, self.exiting(job))
        self.assertEqual(await self.failures(), 1)
        self.assertEqual(await self.deployment_failures(), [0, 0])

    async def test_intentional_deletion_and_manager_ready_are_not_counted(self) -> None:
        for name, options in (
            ("intentional", {"intentional_deletion": True}),
            ("ready", {"manager_ready": True}),
        ):
            with self.subTest(job=name):
                job = await self.job(name, **options)
                await self.observer._update_job(job, self.exiting(job))
                await self.observer._update_job(job, None)
                self.assertEqual(await self.failures(), 0)

    async def test_job_lost_in_maintenance_is_not_counted(self) -> None:
        job = await self.job()
        await self.observer._update_job(job, self.exiting(job), in_maintenance=True)
        await self.observer._update_job(job, None, in_maintenance=True)
        self.assertEqual(await self.failures(), 0)

    async def test_state_and_counter_roll_back_together_on_accounting_failure(
        self,
    ) -> None:
        job = await self.job()
        record = self.observer._record_pilot_start_failure

        async def fail_after_count(session: AsyncSession, current: PilotJob) -> None:
            await record(session, current)
            raise RuntimeError("injected precommit failure")

        with patch.object(
            self.observer,
            "_record_pilot_start_failure",
            side_effect=fail_after_count,
        ):
            with self.assertRaisesRegex(RuntimeError, "precommit"):
                await self.observer._update_job(job, self.exiting(job))
        self.assertEqual(await self.failures(), 0)
        async with self.db() as session:
            state = await session.scalar(
                sa.select(PilotJob.scheduler_state).where(PilotJob.uid == job.uid)
            )
        self.assertEqual(state, SchedulerJobState.running.value)
        async with self.db() as session:
            reason = await session.scalar(
                sa.select(PilotJob.failure_reason).where(PilotJob.uid == job.uid)
            )
        self.assertIsNone(reason)
        await self.observer._update_job(job, self.exiting(job))
        self.assertEqual(await self.failures(), 1)


if __name__ == "__main__":
    main()
