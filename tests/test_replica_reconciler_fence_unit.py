"""Insertion-fence unit tests; no database, Redis, RPC, or scheduler access."""

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any
from unittest import IsolatedAsyncioTestCase, main
from unittest.mock import AsyncMock, MagicMock

from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.types import ReplicaState
from first_gateway.controllers.workers.replica_reconciler import ReplicaReconciler
from first_gateway.database.models import PilotDeployment, PilotJob, PilotReplica
from first_gateway.database.redis.pubsub import Channel


def deployment(
    *, desired: int = 1, failures: int = 0, limit: int = 1
) -> PilotDeployment:
    return PilotDeployment(
        uid=11,
        name="test-deployment",
        desired_replicas=desired,
        consecutive_launch_failures=failures,
        max_consecutive_launch_failures=limit,
        replicas=[],
    )


def replica(
    state: ReplicaState = ReplicaState.pending,
    *,
    draining: bool = False,
    deleted: bool = False,
    job_state: SchedulerJobState | None = None,
) -> PilotReplica:
    now = datetime.now(timezone.utc)
    job = (
        PilotJob(name="test-job", scheduler_state=job_state.value)
        if job_state is not None
        else None
    )
    return PilotReplica(
        state=state.value,
        scheduled_deletion_at=now if draining else None,
        deleted_at=now if deleted else None,
        pilot_job=job,
    )


class ReplicaInsertionFenceTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.stale = deployment()
        self.current = deployment()
        self.inserted: list[PilotReplica] = []
        self.events: list[str] = []
        self.sess = MagicMock(spec=AsyncSession)
        self.sess.scalar = AsyncMock(return_value=self.current)

        def add_all(rows: Iterable[PilotReplica]) -> None:
            self.events.append("insert")
            self.inserted.extend(rows)

        async def enter() -> Any:
            self.events.append("begin")
            return self.sess

        async def leave(*_args: Any) -> bool:
            self.events.append("commit")
            return False

        async def publish(*_args: Any) -> None:
            self.events.append("publish")

        self.sess.add_all.side_effect = add_all
        transaction = MagicMock()
        transaction.__aenter__ = AsyncMock(side_effect=enter)
        transaction.__aexit__ = AsyncMock(side_effect=leave)
        self.transaction = transaction
        clients = MagicMock()
        clients.db_sessionmaker.begin.return_value = transaction
        clients.redis_pubsub.publish = AsyncMock(side_effect=publish)
        self.clients = clients
        self.controller = ReplicaReconciler("fence-test", clients, MagicMock())

    async def test_scale_zero_after_snapshot_prevents_insert_and_notification(
        self,
    ) -> None:
        self.current.desired_replicas = 0
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(self.inserted, [])
        self.clients.redis_pubsub.publish.assert_not_awaited()

    async def test_failure_budget_is_rechecked_and_equality_still_retries(self) -> None:
        self.current.consecutive_launch_failures = 2
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(self.inserted, [])
        self.clients.redis_pubsub.publish.assert_not_awaited()
        self.current.consecutive_launch_failures = 1
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(len(self.inserted), 1)

    async def test_missing_exact_uid_never_inserts_by_stale_name(self) -> None:
        self.sess.scalar.return_value = None
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(self.inserted, [])
        self.clients.redis_pubsub.publish.assert_not_awaited()

    async def test_fresh_capacity_catches_a_previous_inserter(self) -> None:
        self.current.replicas = [replica()]
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(self.inserted, [])
        self.clients.redis_pubsub.publish.assert_not_awaited()

    async def test_current_deficit_replaces_stale_requested_count(self) -> None:
        self.current.desired_replicas = 3
        self.current.replicas = [replica(ReplicaState.ready)]
        await self.controller._insert_replicas(self.stale, 9)
        self.assertEqual(len(self.inserted), 2)
        self.assertTrue(
            all(row.state == ReplicaState.pending.value for row in self.inserted)
        )
        self.assertTrue(
            all(row.pilot_deployment_name == self.current.name for row in self.inserted)
        )
        self.assertEqual(self.events, ["begin", "insert", "commit", "publish"])
        self.clients.redis_pubsub.publish.assert_awaited_once_with(
            Channel.replica_created, self.current.name
        )

    async def test_terminal_draining_and_dying_parent_capacity_remains_replaceable(
        self,
    ) -> None:
        retiring_parent = replica(
            ReplicaState.placed, job_state=SchedulerJobState.running
        )
        assert retiring_parent.pilot_job is not None
        retiring_parent.pilot_job.scheduled_deletion_at = datetime.now(timezone.utc)
        self.current.replicas = [
            retiring_parent,
            replica(ReplicaState.error),
            replica(ReplicaState.start_timeout),
            replica(ReplicaState.terminated),
            replica(ReplicaState.ready, draining=True),
            replica(ReplicaState.ready, deleted=True),
            replica(ReplicaState.placed, job_state=SchedulerJobState.exiting),
            replica(ReplicaState.placed, job_state=SchedulerJobState.gone),
        ]
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(len(self.inserted), 1)

    async def test_pending_placed_launching_ready_unhealthy_all_count(self) -> None:
        self.current.desired_replicas = 5
        self.current.replicas = [
            replica(state)
            for state in (
                ReplicaState.pending,
                ReplicaState.placed,
                ReplicaState.launching,
                ReplicaState.ready,
                ReplicaState.unhealthy,
            )
        ]
        await self.controller._insert_replicas(self.stale, 5)
        self.assertEqual(self.inserted, [])

    async def test_query_locks_exact_deployment_before_capacity_decision(self) -> None:
        await self.controller._insert_replicas(self.stale, 1)
        statement = self.sess.scalar.call_args.args[0]
        sql = str(
            statement.compile(
                dialect=postgresql.dialect(),  # type: ignore[no-untyped-call]
                compile_kwargs={"literal_binds": True},
            )
        )
        self.assertIn("pilot_deployment.uid = 11", sql)
        self.assertTrue(sql.endswith("FOR UPDATE"), sql)

    async def test_failed_commit_never_publishes_created_event(self) -> None:
        self.transaction.__aexit__.side_effect = RuntimeError("test commit failure")
        with self.assertRaisesRegex(RuntimeError, "commit failure"):
            await self.controller._insert_replicas(self.stale, 1)
        self.clients.redis_pubsub.publish.assert_not_awaited()


if __name__ == "__main__":
    main()
