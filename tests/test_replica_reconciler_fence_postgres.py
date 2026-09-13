"""Opt-in, isolated PostgreSQL row-lock tests; never use application DB settings.

Only FIRST_RECONCILE_TEST_DATABASE_URL enables these tests. The URL must name
the dedicated first-test-reconcile-pg-20260913 container and first_reconcile_test
user/database. No shared DB fixtures, Redis, scheduler, or model runtime is used.
"""

import asyncio
import os
from collections.abc import Coroutine
from typing import Any
from unittest import IsolatedAsyncioTestCase, main, skipUnless
from unittest.mock import AsyncMock, MagicMock

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from first_common.schema.types import ReplicaState
from first_gateway.controllers.workers.replica_reconciler import ReplicaReconciler
from first_gateway.database.models import (
    AccessGroup,
    Base,
    Cluster,
    LaunchTemplate,
    Model,
    PilotDeployment,
    PilotReplica,
)

DATABASE_ENV = "FIRST_RECONCILE_TEST_DATABASE_URL"
APPLICATION_NAME = "first-reconcile-qualification-20260913"


@skipUnless(os.environ.get(DATABASE_ENV), "requires dedicated opt-in PostgreSQL")
class ReplicaInsertionPostgresTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        url = make_url(os.environ[DATABASE_ENV])
        if (
            url.drivername != "postgresql+psycopg"
            or url.host != "first-test-reconcile-pg-20260913"
            or url.username != "first_reconcile_test"
            or url.database != "first_reconcile_test"
            or url.port not in {None, 5432}
            or url.query
        ):
            raise ValueError("test URL is not the dedicated isolated PostgreSQL")
        self.engine = create_async_engine(
            url,
            connect_args={
                "application_name": APPLICATION_NAME,
                "options": "-c statement_timeout=10000 -c lock_timeout=5000",
            },
        )
        self.addAsyncCleanup(self.engine.dispose)
        self.db = async_sessionmaker(self.engine, expire_on_commit=False)
        async with self.engine.begin() as connection:
            await connection.execute(sa.text("CREATE SCHEMA first"))
            await connection.run_sync(Base.metadata.create_all)
        self.addAsyncCleanup(self.drop_test_schema)
        clients = MagicMock()
        clients.db_sessionmaker = self.db
        self.publish = AsyncMock()
        clients.redis_pubsub.publish = self.publish
        self.controller = ReplicaReconciler("postgres-fence-test", clients, MagicMock())
        async with self.db.begin() as session:
            session.add_all(
                [
                    Cluster(name="test-cluster", health_check={}),
                    AccessGroup(
                        name="test-access", allowed_groups=[], allowed_domains=[]
                    ),
                    LaunchTemplate(
                        name="test-template",
                        parameters={},
                        env={},
                        serve_script_template="echo offline",
                        max_startup_sec=60,
                        pre_stop_timeout_sec=20.0,
                        post_stop_timeout_sec=50.0,
                        health_check={},
                    ),
                ]
            )
            await session.flush()
            session.add(
                Model(
                    name="test-model",
                    access_group_name="test-access",
                    supported_endpoints=[],
                )
            )
            await session.flush()
            self.stale = self.new_deployment()
            session.add(self.stale)

    async def drop_test_schema(self) -> None:
        # The constructor rejects every URL except this task's isolated DB.
        async with self.engine.begin() as connection:
            await connection.execute(sa.text("DROP SCHEMA first CASCADE"))

    @staticmethod
    def new_deployment() -> PilotDeployment:
        return PilotDeployment(
            name="test-deployment",
            cluster_name="test-cluster",
            model_name="test-model",
            launch_template_name="test-template",
            launch_spec={"num_nodes": 1, "gpus_per_node": 4},
            router_params={},
            prometheus_scrape_interval_sec=30,
            min_replicas=0,
            max_replicas=5,
            desired_replicas=1,
            max_consecutive_launch_failures=1,
            consecutive_launch_failures=0,
        )

    def task(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task = asyncio.create_task(coroutine)

        async def stop_task() -> None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.addAsyncCleanup(stop_task)
        return task

    async def wait_for_lock_waiters(self, number: int) -> None:
        async with asyncio.timeout(3):
            while True:
                async with self.engine.connect() as connection:
                    count = await connection.scalar(
                        sa.text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE application_name = :name AND wait_event_type = 'Lock'"
                        ),
                        {"name": APPLICATION_NAME},
                    )
                if count is not None and count >= number:
                    return
                await asyncio.sleep(0.01)

    async def count_replicas(self) -> int:
        async with self.db() as session:
            return int(
                await session.scalar(
                    sa.select(sa.func.count()).select_from(PilotReplica)
                )
                or 0
            )

    async def test_concurrent_inserters_create_only_current_deficit(self) -> None:
        async with self.db.begin() as blocker:
            await blocker.scalar(
                sa.select(PilotDeployment)
                .where(PilotDeployment.uid == self.stale.uid)
                .with_for_update()
            )
            first = self.task(self.controller._insert_replicas(self.stale, 1))
            second = self.task(self.controller._insert_replicas(self.stale, 1))
            await self.wait_for_lock_waiters(2)
        await asyncio.wait_for(asyncio.gather(first, second), timeout=5)
        self.assertEqual(await self.count_replicas(), 1)
        self.publish.assert_awaited_once()

    async def test_scale_zero_committed_while_inserter_waits_is_observed(self) -> None:
        async with self.db.begin() as blocker:
            await blocker.execute(
                sa.update(PilotDeployment)
                .where(PilotDeployment.uid == self.stale.uid)
                .values(desired_replicas=0)
            )
            insertion = self.task(self.controller._insert_replicas(self.stale, 1))
            await self.wait_for_lock_waiters(1)
        await asyncio.wait_for(insertion, timeout=5)
        self.assertEqual(await self.count_replicas(), 0)

    async def test_failure_budget_committed_while_waiting_then_equality_retry(
        self,
    ) -> None:
        async with self.db.begin() as blocker:
            await blocker.execute(
                sa.update(PilotDeployment)
                .where(PilotDeployment.uid == self.stale.uid)
                .values(consecutive_launch_failures=2)
            )
            insertion = self.task(self.controller._insert_replicas(self.stale, 1))
            await self.wait_for_lock_waiters(1)
        await asyncio.wait_for(insertion, timeout=5)
        self.assertEqual(await self.count_replicas(), 0)
        async with self.db.begin() as session:
            await session.execute(
                sa.update(PilotDeployment)
                .where(PilotDeployment.uid == self.stale.uid)
                .values(consecutive_launch_failures=1)
            )
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(await self.count_replicas(), 1)

    async def test_deleted_recreated_name_does_not_receive_stale_uid_insertion(
        self,
    ) -> None:
        async with self.db.begin() as session:
            await session.execute(
                sa.delete(PilotDeployment).where(PilotDeployment.uid == self.stale.uid)
            )
            replacement = self.new_deployment()
            session.add(replacement)
            await session.flush()
            self.assertNotEqual(replacement.uid, self.stale.uid)
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(await self.count_replicas(), 0)

    async def test_terminal_replica_does_not_block_immediate_replacement(self) -> None:
        async with self.db.begin() as session:
            session.add(
                PilotReplica(
                    name="test-deployment/replica/terminal",
                    pilot_deployment_name=self.stale.name,
                    state=ReplicaState.error.value,
                )
            )
        await self.controller._insert_replicas(self.stale, 1)
        self.assertEqual(await self.count_replicas(), 2)


if __name__ == "__main__":
    main()
