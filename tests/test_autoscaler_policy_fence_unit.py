"""CPU-only autoscaler write-fence contracts; no DB, Redis, or scheduler."""

from typing import Any
from unittest import IsolatedAsyncioTestCase, main
from unittest.mock import AsyncMock, MagicMock

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from first_gateway.controllers.workers.autoscaler import PilotAutoscaler
from first_gateway.database.models import PilotDeployment
from first_gateway.database.redis.pubsub import Channel


class AutoscalerPolicyFenceTests(IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.dep = PilotDeployment(
            uid=11,
            name="test-deployment",
            desired_replicas=0,
            consecutive_launch_failures=0,
            max_consecutive_launch_failures=3,
            min_replicas=0,
            max_replicas=4,
            scaling_strategy={"strategy": "DemandThresholdStrategy"},
        )
        self.events: list[str] = []
        self.session = MagicMock(spec=AsyncSession)
        self.session.execute = AsyncMock(return_value=MagicMock(rowcount=1))

        async def enter() -> Any:
            self.events.append("begin")
            return self.session

        async def leave(*_args: Any) -> bool:
            self.events.append("commit")
            return False

        async def publish(*_args: Any) -> None:
            self.events.append("publish")

        self.transaction = MagicMock()
        self.transaction.__aenter__ = AsyncMock(side_effect=enter)
        self.transaction.__aexit__ = AsyncMock(side_effect=leave)
        self.clients = MagicMock()
        self.clients.db_sessionmaker.begin.return_value = self.transaction
        self.clients.redis_pubsub.publish = AsyncMock(side_effect=publish)
        self.controller = PilotAutoscaler(
            "policy-fence-test", self.clients, MagicMock()
        )

    async def test_write_premises_every_deployment_policy_input(self) -> None:
        await self.controller._write_desired(self.dep, 1)
        statement = self.session.execute.call_args.args[0]
        compiled = statement.compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
        where = str(compiled).split(" WHERE ")[1]
        for field in (
            "uid",
            "desired_replicas",
            "consecutive_launch_failures",
            "scaling_strategy",
            "min_replicas",
            "max_replicas",
            "max_consecutive_launch_failures",
        ):
            self.assertIn(f"pilot_deployment.{field} = ", where)
        self.assertEqual(
            compiled.params["scaling_strategy_1"], self.dep.scaling_strategy
        )
        self.assertEqual(compiled.params["desired_replicas"], 1)
        self.assertEqual(self.events, ["begin", "commit", "publish"])
        self.clients.redis_pubsub.publish.assert_awaited_once_with(
            Channel.desired_replicas_changed, self.dep.name
        )

    async def test_manual_policy_accepts_sql_null_or_json_null_only(self) -> None:
        self.dep.scaling_strategy = None
        await self.controller._write_desired(self.dep, 0)
        compiled = self.session.execute.call_args.args[0].compile(
            dialect=postgresql.dialect()  # type: ignore[no-untyped-call]
        )
        self.assertIn(
            "(first.pilot_deployment.scaling_strategy IS NULL OR "
            "first.pilot_deployment.scaling_strategy = ",
            str(compiled),
        )
        self.assertIs(compiled.params["scaling_strategy_1"], sa.JSON.NULL)

    async def test_stale_policy_skips_event_without_logging_policy_contents(
        self,
    ) -> None:
        self.dep.scaling_strategy = {"private-test-marker": "must-not-be-logged"}
        self.session.execute.return_value = MagicMock(rowcount=0)
        with self.assertLogs(
            "first_gateway.controllers.workers.autoscaler", level="WARNING"
        ) as logs:
            await self.controller._write_desired(self.dep, 1)
        self.clients.redis_pubsub.publish.assert_not_awaited()
        self.assertIn("policy premise stale", " ".join(logs.output))
        self.assertNotIn("must-not-be-logged", " ".join(logs.output))

    async def test_failed_commit_never_publishes(self) -> None:
        self.transaction.__aexit__.side_effect = RuntimeError("test commit failure")
        with self.assertRaisesRegex(RuntimeError, "commit failure"):
            await self.controller._write_desired(self.dep, 1)
        self.clients.redis_pubsub.publish.assert_not_awaited()


if __name__ == "__main__":
    main()
