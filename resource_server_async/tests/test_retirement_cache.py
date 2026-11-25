"""raise_if_retired is on the inference hot path and must not query per request."""

import asyncio
from unittest.mock import AsyncMock, patch

from django.db import OperationalError

from resource_server_async import retirements
from resource_server_async.models import Cluster
from resource_server_async.retirements import raise_if_retired, retired_model_names

from . import ResourceServerTestCase
from .test_openai_responsibility_separation import request_context

CLUSTER = "your-cluster"


class RetirementSnapshotCacheTests(ResourceServerTestCase):
    def setUp(self):
        super().setUp()
        self.real_load = retirements._load_clusters
        self.load = AsyncMock(side_effect=self.real_load)
        self.enterContext(patch.object(retirements, "_load_clusters", self.load))
        self.now = 1000.0
        self.enterContext(
            patch.object(retirements.time, "monotonic", side_effect=lambda: self.now)
        )

    async def check(self, cluster: str = CLUSTER) -> None:
        await raise_if_retired(cluster, "api", "some-model", request_context().user)

    async def test_repeated_requests_load_cluster_table_once(self):
        for _ in range(20):
            await self.check()
            await self.check("no-such-cluster")
            self.assertEqual(await retired_model_names(CLUSTER), set())
        self.assertEqual(self.load.await_count, 1)

    async def test_concurrent_misses_share_one_load(self):
        await asyncio.gather(*(self.check() for _ in range(50)))
        self.assertEqual(self.load.await_count, 1)

    async def test_snapshot_refreshes_after_ttl(self):
        await self.check()
        self.now += retirements._SNAPSHOT_TTL_SECONDS + 1
        await self.check()
        self.assertEqual(self.load.await_count, 2)

    async def test_failed_refresh_serves_stale_snapshot_and_backs_off(self):
        await self.check()
        self.load.side_effect = OperationalError("connection timeout expired")
        self.now += retirements._SNAPSHOT_TTL_SECONDS + 1
        for _ in range(10):
            await self.check()  # Served from the stale snapshot, no 500.
        self.assertEqual(self.load.await_count, 2)
        self.load.side_effect = self.real_load
        self.now += retirements._SNAPSHOT_RETRY_SECONDS + 1
        await self.check()
        self.assertEqual(self.load.await_count, 3)

    async def test_failed_first_load_raises_and_is_not_cached(self):
        self.load.side_effect = OperationalError("connection timeout expired")
        with self.assertRaises(OperationalError):
            await self.check()
        self.load.side_effect = self.real_load
        await self.check()
        self.assertEqual(self.load.await_count, 2)

    async def test_cluster_save_in_process_invalidates_snapshot(self):
        await self.check()
        cluster = await Cluster.objects.aget(cluster_name=CLUSTER)
        await cluster.asave()
        await self.check()
        self.assertEqual(self.load.await_count, 2)
