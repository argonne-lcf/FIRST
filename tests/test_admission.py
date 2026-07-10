"""Tests for the admission controller Lua scripts and Python facade."""

import asyncio
from typing import AsyncGenerator

import pytest
from redis.asyncio import Redis

from first_common.schema.types import RouterParams, UsageLimits
from first_gateway import Settings
from first_gateway.database.redis.admission import (
    AdmissionController,
    AdmitResult,
    AdmitStatus,
    CandidateBackend,
    CapacityReason,
    QuotaReason,
)
from first_gateway.database.redis.keys import Keys


@pytest.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    url = Settings().redis_url
    r: Redis = Redis.from_url(url)
    await r.flushdb()
    try:
        yield r
    finally:
        await r.aclose()


@pytest.fixture
def ac(redis: Redis) -> AdmissionController:
    return AdmissionController(redis, lease_sec=30, max_request_sec=900)


QUOTA = UsageLimits(
    tpm=60_000, burst_tokens=120_000, rpm=60, burst_requests=5, max_user_concurrency=3
)
MODEL = "llama-3"
USER = "alice"


def _candidates(
    *uids: str, concurrency: int = 10, cooldown_threshold: int = 3
) -> list[CandidateBackend]:
    return [
        CandidateBackend(
            uid=uid,
            max_backend_concurrency=concurrency,
            cooldown_threshold=cooldown_threshold,
        )
        for uid in uids
    ]


async def _admit(ac: AdmissionController, request_id: str, **kw: object) -> AdmitResult:
    defaults: dict[str, object] = dict(
        request_id=request_id,
        model_name=MODEL,
        user_id=USER,
        candidates=_candidates("r1", "r2"),
        estimated_tokens=100,
        quota=QUOTA,
    )
    defaults.update(kw)
    return await ac.admit(**defaults)  # type: ignore[arg-type]


async def _hget_int(redis: Redis, key: str, field: str) -> int:
    raw = await redis.hget(key, field)
    return int(raw) if raw is not None else 0


# ---------------------------------------------------------------------------
# 1. Admit / settle happy path
# ---------------------------------------------------------------------------


class TestAdmitSettleHappyPath:
    async def test_admit_returns_chosen_backend(self, ac: AdmissionController) -> None:
        result = await _admit(ac, "req-1")
        assert result.admitted
        assert result.backend_id in ("r1", "r2")

    async def test_settle_returns_true_on_first_call(
        self, ac: AdmissionController
    ) -> None:
        await _admit(ac, "req-1")
        applied = await ac.settle(
            "req-1", actual_tokens=50, model_name=MODEL, user_id=USER
        )
        assert applied is True

    async def test_admit_increments_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None
        inflight = await _hget_int(redis, Keys.model_inflight(MODEL), result.backend_id)
        assert inflight == 1

    async def test_settle_decrements_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        inflight = await _hget_int(redis, Keys.model_inflight(MODEL), result.backend_id)
        assert inflight == 0

    async def test_settle_cleans_up_reservation_and_deadline(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        assert await redis.get(Keys.reservation("req-1")) is None
        assert await redis.zscore(Keys.deadlines(), "req-1") is None

    async def test_settle_cleans_user_inflight_at_zero(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        assert await redis.get(Keys.quota(MODEL, USER, "inflight")) is None

    async def test_settle_without_caller_context_pre_reads(
        self, ac: AdmissionController
    ) -> None:
        """Sweeper path: settle without model_name/user_id still works."""
        await _admit(ac, "req-1")
        applied = await ac.settle("req-1", actual_tokens=50)
        assert applied is True

    async def test_demand_gauge_incremented_and_decremented(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        assert await _hget_int(redis, Keys.model_demand(MODEL), "inflight") == 1

        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        assert await _hget_int(redis, Keys.model_demand(MODEL), "inflight") == 0


# ---------------------------------------------------------------------------
# 2. Quota rejects
# ---------------------------------------------------------------------------


class TestQuotaRejects:
    async def test_user_concurrency_reject(self, ac: AdmissionController) -> None:
        for i in range(QUOTA.max_user_concurrency):
            result = await _admit(ac, f"req-{i}")
            assert result.admitted, f"request {i} should admit"

        result = await _admit(ac, "req-over")
        assert result.status is AdmitStatus.REJECT_QUOTA
        assert result.quota_reason is QuotaReason.USER_CONCURRENCY
        assert result.retry_after_sec is None

    async def test_rpm_reject(self, ac: AdmissionController) -> None:
        tight_quota = UsageLimits(
            tpm=1_000_000,
            burst_tokens=1_000_000,
            rpm=60,
            burst_requests=2,
            max_user_concurrency=100,
        )
        for i in range(2):
            result = await _admit(ac, f"req-{i}", quota=tight_quota)
            assert result.admitted

        result = await _admit(ac, "req-over", quota=tight_quota)
        assert result.status is AdmitStatus.REJECT_QUOTA
        assert result.quota_reason is QuotaReason.USER_RPM
        assert result.retry_after_sec is not None
        assert result.retry_after_sec >= 0

    async def test_tpm_reject(self, ac: AdmissionController) -> None:
        tight_quota = UsageLimits(
            tpm=600,
            burst_tokens=150,
            rpm=6000,
            burst_requests=1000,
            max_user_concurrency=100,
        )
        result = await _admit(ac, "req-1", estimated_tokens=100, quota=tight_quota)
        assert result.admitted

        result = await _admit(ac, "req-2", estimated_tokens=100, quota=tight_quota)
        assert result.status is AdmitStatus.REJECT_QUOTA
        assert result.quota_reason is QuotaReason.USER_TPM
        assert result.retry_after_sec is not None
        assert result.retry_after_sec > 0

    async def test_quota_reject_does_not_increment_demand(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        for i in range(QUOTA.max_user_concurrency):
            await _admit(ac, f"req-{i}")

        await _admit(ac, "req-over")
        rejects = await redis.hget(Keys.model_demand(MODEL), "capacity_rejects_total")
        assert rejects is None or int(rejects) == 0


# ---------------------------------------------------------------------------
# 3. Capacity rejects
# ---------------------------------------------------------------------------


class TestCapacityRejects:
    async def test_saturated_when_all_backends_full(
        self, ac: AdmissionController
    ) -> None:
        candidates = _candidates("r1", concurrency=1)
        quota = UsageLimits(max_user_concurrency=100)
        r1 = await _admit(ac, "req-1", candidates=candidates, quota=quota)
        assert r1.admitted

        r2 = await _admit(ac, "req-2", candidates=candidates, quota=quota)
        assert r2.status is AdmitStatus.REJECT_CAPACITY
        assert r2.capacity_reason is CapacityReason.SATURATED

    async def test_no_candidates(self, ac: AdmissionController) -> None:
        result = await _admit(ac, "req-1", candidates=[])
        assert result.status is AdmitStatus.REJECT_CAPACITY
        assert result.capacity_reason is CapacityReason.NO_CANDIDATES

    async def test_all_benched(self, ac: AdmissionController) -> None:
        params = RouterParams(cooldown_threshold=1, cooldown_bench_sec=60)
        candidates = _candidates("r1", cooldown_threshold=1)

        await ac.record_error("r1", params)

        result = await _admit(ac, "req-1", candidates=candidates)
        assert result.status is AdmitStatus.REJECT_CAPACITY
        assert result.capacity_reason is CapacityReason.ALL_BENCHED

    async def test_capacity_reject_increments_demand_counter(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1", candidates=[])

        rejects = await redis.hget(Keys.model_demand(MODEL), "capacity_rejects_total")
        assert rejects is not None
        assert int(rejects) == 1

    async def test_skips_benched_picks_healthy(self, ac: AdmissionController) -> None:
        params = RouterParams(cooldown_threshold=1, cooldown_bench_sec=60)
        await ac.record_error("r1", params)

        candidates = [
            CandidateBackend(
                uid="r1", max_backend_concurrency=10, cooldown_threshold=1
            ),
            CandidateBackend(
                uid="r2", max_backend_concurrency=10, cooldown_threshold=1
            ),
        ]
        result = await _admit(ac, "req-1", candidates=candidates)
        assert result.admitted
        assert result.backend_id == "r2"


# ---------------------------------------------------------------------------
# 4. Double-settle safety
# ---------------------------------------------------------------------------


class TestDoubleSettleSafety:
    async def test_second_settle_returns_false(self, ac: AdmissionController) -> None:
        await _admit(ac, "req-1")
        first = await ac.settle(
            "req-1", actual_tokens=50, model_name=MODEL, user_id=USER
        )
        second = await ac.settle(
            "req-1", actual_tokens=50, model_name=MODEL, user_id=USER
        )
        assert first is True
        assert second is False

    async def test_settle_never_existed(self, ac: AdmissionController) -> None:
        result = await ac.settle("never-existed", actual_tokens=0)
        assert result is False

    async def test_double_settle_does_not_underflow_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)

        inflight = await _hget_int(redis, Keys.model_inflight(MODEL), result.backend_id)
        assert inflight >= 0

    async def test_concurrent_settle_paths(self, ac: AdmissionController) -> None:
        """Hot-path settle (with context) racing sweeper settle (without context)."""
        await _admit(ac, "req-1")
        hot = await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)
        sweep = await ac.settle("req-1", actual_tokens=None)
        assert (hot, sweep) == (True, False)


# ---------------------------------------------------------------------------
# 5. Record errors / cooldown / bench
# ---------------------------------------------------------------------------


class TestRecordErrorCooldown:
    async def test_errors_below_threshold(self, ac: AdmissionController) -> None:
        params = RouterParams(cooldown_threshold=3, cooldown_window_sec=30)
        count, benched = await ac.record_error("r1", params)
        assert count == 1
        assert benched is False

    async def test_bench_at_threshold(self, ac: AdmissionController) -> None:
        params = RouterParams(
            cooldown_threshold=3, cooldown_window_sec=30, cooldown_bench_sec=60
        )
        for _ in range(2):
            await ac.record_error("r1", params)
        count, benched = await ac.record_error("r1", params)
        assert count == 3
        assert benched is True

    async def test_benched_backend_rejected_by_admit(
        self, ac: AdmissionController
    ) -> None:
        params = RouterParams(cooldown_threshold=2, cooldown_bench_sec=60)
        for _ in range(2):
            await ac.record_error("r1", params)

        candidates = _candidates("r1", cooldown_threshold=2)
        result = await _admit(ac, "req-1", candidates=candidates)
        assert result.status is AdmitStatus.REJECT_CAPACITY

    async def test_error_key_has_ttl(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        params = RouterParams(cooldown_threshold=3, cooldown_window_sec=30)
        await ac.record_error("r1", params)
        ttl = await redis.ttl(Keys.backend_errors("r1"))
        assert 0 < ttl <= 30

    async def test_bench_extends_ttl(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        params = RouterParams(
            cooldown_threshold=2, cooldown_window_sec=10, cooldown_bench_sec=120
        )
        for _ in range(2):
            await ac.record_error("r1", params)
        ttl = await redis.ttl(Keys.backend_errors("r1"))
        assert ttl > 10


# ---------------------------------------------------------------------------
# 6. Lease renewal
# ---------------------------------------------------------------------------


class TestLeaseRenewal:
    WIDE_QUOTA = UsageLimits(max_user_concurrency=100)

    async def test_renew_extends_deadline(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        before = await redis.zscore(Keys.deadlines(), "req-1")

        renewed = await ac.renew(["req-1"])
        assert renewed == 1

        after = await redis.zscore(Keys.deadlines(), "req-1")
        assert after is not None
        assert before is not None
        assert after >= before

    async def test_renew_skips_settled_reservation(
        self, ac: AdmissionController
    ) -> None:
        await _admit(ac, "req-1")
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)

        renewed = await ac.renew(["req-1"])
        assert renewed == 0

    async def test_renew_batch(self, ac: AdmissionController) -> None:
        for i in range(5):
            await _admit(ac, f"req-{i}", quota=self.WIDE_QUOTA)
        renewed = await ac.renew([f"req-{i}" for i in range(5)])
        assert renewed == 5

    async def test_renew_chunks_large_batches(self, ac: AdmissionController) -> None:
        ac_small_chunk = AdmissionController(ac.client, renew_chunk=2)
        for i in range(5):
            await _admit(ac, f"req-{i}", quota=self.WIDE_QUOTA)
        renewed = await ac_small_chunk.renew([f"req-{i}" for i in range(5)])
        assert renewed == 5

    async def test_renew_respects_max_stream_cap(self, ac: AdmissionController) -> None:
        short_ac = AdmissionController(ac.client, lease_sec=30, max_request_sec=0.1)
        await _admit(short_ac, "req-1")

        await asyncio.sleep(0.2)

        renewed = await short_ac.renew(["req-1"])
        assert renewed == 0


# ---------------------------------------------------------------------------
# 7. Sweeper settles stale, not fresh
# ---------------------------------------------------------------------------


class TestSweeper:
    async def test_sweeper_ignores_fresh_reservations(
        self, ac: AdmissionController
    ) -> None:
        await _admit(ac, "req-fresh")
        settled = await ac.sweep()
        assert settled == 0

    async def test_sweeper_settles_expired_reservations(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        short_lease = AdmissionController(ac.client, lease_sec=0.1, max_request_sec=900)
        await _admit(short_lease, "req-stale")

        await asyncio.sleep(0.2)

        settled = await short_lease.sweep()
        assert settled == 1
        assert await redis.get(Keys.reservation("req-stale")) is None

    async def test_sweeper_is_idempotent(self, ac: AdmissionController) -> None:
        short_lease = AdmissionController(ac.client, lease_sec=0.1, max_request_sec=900)
        await _admit(short_lease, "req-stale")

        await asyncio.sleep(0.2)

        first = await short_lease.sweep()
        second = await short_lease.sweep()
        assert first == 1
        assert second == 0

    async def test_sweeper_restores_counters(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        short_lease = AdmissionController(ac.client, lease_sec=0.1, max_request_sec=900)
        result = await _admit(short_lease, "req-stale")
        assert result.backend_id is not None

        await asyncio.sleep(0.2)

        await short_lease.sweep()
        inflight = await _hget_int(redis, Keys.model_inflight(MODEL), result.backend_id)
        assert inflight == 0


# ---------------------------------------------------------------------------
# 8. Rebuild inflight from ledger
# ---------------------------------------------------------------------------


class TestRebuildInflight:
    async def test_empty_ledger(self, ac: AdmissionController) -> None:
        counts = await ac.rebuild_inflight_from_ledger()
        assert counts.by_backend == {}

    async def test_counts_match_live_reservations(
        self, ac: AdmissionController
    ) -> None:
        await _admit(ac, "req-1")
        await _admit(ac, "req-2")

        counts = await ac.rebuild_inflight_from_ledger()
        assert MODEL in counts.by_backend
        total = sum(counts.by_backend[MODEL].values())
        assert total == 2

    async def test_settled_not_counted(self, ac: AdmissionController) -> None:
        await _admit(ac, "req-1")
        await _admit(ac, "req-2")
        await ac.settle("req-1", actual_tokens=50, model_name=MODEL, user_id=USER)

        counts = await ac.rebuild_inflight_from_ledger()
        total = sum(counts.by_backend[MODEL].values())
        assert total == 1

    async def test_multiple_models(self, ac: AdmissionController) -> None:
        candidates = _candidates("r1", concurrency=100)
        await _admit(ac, "req-1", model_name="model-a", candidates=candidates)
        await _admit(ac, "req-2", model_name="model-b", candidates=candidates)

        counts = await ac.rebuild_inflight_from_ledger()
        assert "model-a" in counts.by_backend
        assert "model-b" in counts.by_backend
        assert sum(counts.by_backend["model-a"].values()) == 1
        assert sum(counts.by_backend["model-b"].values()) == 1

    async def test_per_backend_breakdown(self, ac: AdmissionController) -> None:
        candidates_r1_only = _candidates("r1", concurrency=100)
        candidates_r2_only = _candidates("r2", concurrency=100)
        await _admit(ac, "req-1", candidates=candidates_r1_only)
        await _admit(ac, "req-2", candidates=candidates_r1_only)
        await _admit(ac, "req-3", candidates=candidates_r2_only)

        counts = await ac.rebuild_inflight_from_ledger()
        assert counts.by_backend[MODEL]["r1"] == 2
        assert counts.by_backend[MODEL]["r2"] == 1
