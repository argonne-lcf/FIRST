"""Opt-in policy-fence races against a dedicated disposable PostgreSQL only.

Uses the same isolated qualification runner as the replica insertion tests,
serially, with no application settings, Redis, scheduler, or live database.
"""

import asyncio
import copy
import os
from unittest import IsolatedAsyncioTestCase, main, skipUnless
from unittest.mock import AsyncMock, MagicMock

import sqlalchemy as sa
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from first_gateway.controllers.workers.autoscaler import PilotAutoscaler
from first_gateway.database.models import (
    AccessGroup,
    Base,
    Cluster,
    LaunchTemplate,
    Model,
    PilotDeployment,
)

DATABASE_ENV = "FIRST_RECONCILE_TEST_DATABASE_URL"
APPLICATION_NAME = "first-autoscaler-policy-qualification-20260913"
STRATEGY = {
    "strategy": "DemandThresholdStrategy",
    "immediate_cold_start": True,
    "scale_down_sustain_sec": 60,
    "scaling_thresholds": [[0.0, 1]],
}


@skipUnless(os.environ.get(DATABASE_ENV), "requires dedicated opt-in PostgreSQL")
class AutoscalerPolicyPostgresTests(IsolatedAsyncioTestCase):
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
        self.controller = PilotAutoscaler("policy-postgres-test", clients, MagicMock())
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
                        serve_script_template="exit 78",
                        max_startup_sec=30,
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
            self.stale = PilotDeployment(
                name="test-deployment",
                cluster_name="test-cluster",
                model_name="test-model",
                launch_template_name="test-template",
                launch_spec={"num_nodes": 1, "gpus_per_node": 4},
                router_params={},
                prometheus_scrape_interval_sec=30,
                min_replicas=0,
                max_replicas=4,
                desired_replicas=0,
                max_consecutive_launch_failures=3,
                consecutive_launch_failures=0,
                scaling_strategy=copy.deepcopy(STRATEGY),
            )
            session.add(self.stale)

    async def drop_test_schema(self) -> None:
        # Constructor rejects all URLs except this task's disposable database.
        async with self.engine.begin() as connection:
            await connection.execute(sa.text("DROP SCHEMA first CASCADE"))

    async def desired(self) -> int:
        async with self.db() as session:
            value = await session.scalar(
                sa.select(PilotDeployment.desired_replicas).where(
                    PilotDeployment.uid == self.stale.uid
                )
            )
        assert value is not None
        return value

    async def test_unchanged_auto_policy_still_writes(self) -> None:
        await self.controller._write_desired(self.stale, 1)
        self.assertEqual(await self.desired(), 1)
        self.publish.assert_awaited_once()

    async def test_jsonb_key_order_does_not_make_policy_stale(self) -> None:
        reordered = dict(reversed(list(STRATEGY.items())))
        async with self.db.begin() as session:
            await session.execute(
                sa.update(PilotDeployment).values(scaling_strategy=reordered)
            )
        await self.controller._write_desired(self.stale, 1)
        self.assertEqual(await self.desired(), 1)

    async def test_manual_json_null_and_sql_null_allow_failure_latch(self) -> None:
        for value in (sa.JSON.NULL, sa.null()):
            with self.subTest(value=str(value)):
                async with self.db.begin() as session:
                    await session.execute(
                        sa.update(PilotDeployment).values(
                            scaling_strategy=value,
                            desired_replicas=1,
                            consecutive_launch_failures=4,
                        )
                    )
                async with self.db() as session:
                    current = await session.get(PilotDeployment, self.stale.uid)
                assert current is not None
                self.assertIsNone(current.scaling_strategy)
                await self.controller._write_desired(current, 0)
                self.assertEqual(await self.desired(), 0)

    async def test_policy_change_rechecks_after_actual_row_lock_wait(self) -> None:
        async with self.db.begin() as blocker:
            await blocker.execute(
                sa.update(PilotDeployment).values(scaling_strategy=None)
            )
            task = asyncio.create_task(self.controller._write_desired(self.stale, 1))

            async def stop_task() -> None:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            self.addAsyncCleanup(stop_task)
            async with asyncio.timeout(3):
                while True:
                    async with self.engine.connect() as connection:
                        waiters = await connection.scalar(
                            sa.text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE application_name = :name "
                                "AND wait_event_type = 'Lock'"
                            ),
                            {"name": APPLICATION_NAME},
                        )
                    if waiters:
                        break
                    await asyncio.sleep(0.01)
        await asyncio.wait_for(task, timeout=5)
        self.assertEqual(await self.desired(), 0)
        self.publish.assert_not_awaited()

    async def test_each_changed_policy_input_blocks_stale_decision(self) -> None:
        changes = {
            "scaling_strategy": {**STRATEGY, "immediate_cold_start": False},
            "min_replicas": 1,
            "max_replicas": 0,
            "max_consecutive_launch_failures": 0,
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                async with self.db.begin() as session:
                    await session.execute(
                        sa.update(PilotDeployment).values({field: value})
                    )
                await self.controller._write_desired(self.stale, 1)
                self.assertEqual(await self.desired(), 0)
                async with self.db.begin() as session:
                    await session.execute(
                        sa.update(PilotDeployment).values(
                            {field: getattr(self.stale, field)}
                        )
                    )
        self.publish.assert_not_awaited()

    async def test_manual_snapshot_cannot_overwrite_new_auto_policy(self) -> None:
        self.stale.scaling_strategy = None
        await self.controller._write_desired(self.stale, 1)
        self.assertEqual(await self.desired(), 0)
        self.publish.assert_not_awaited()

    async def test_existing_desired_and_failure_guards_are_preserved(self) -> None:
        for field, value in (
            ("desired_replicas", 2),
            ("consecutive_launch_failures", 1),
        ):
            with self.subTest(field=field):
                async with self.db.begin() as session:
                    await session.execute(
                        sa.update(PilotDeployment).values({field: value})
                    )
                await self.controller._write_desired(self.stale, 1)
                self.assertEqual(
                    await self.desired(), 2 if field == "desired_replicas" else 0
                )
                async with self.db.begin() as session:
                    await session.execute(sa.update(PilotDeployment).values({field: 0}))
        self.publish.assert_not_awaited()


if __name__ == "__main__":
    main()
