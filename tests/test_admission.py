"""Tests for the admission controller Lua scripts and Python facade."""

import asyncio
import json
from typing import AsyncGenerator

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from first_common.schema.types import RouterParams, UsageLimits
from first_gateway import Settings
from first_gateway.database.redis.admission import (
    USAGE_EWMA_ALPHA,
    AdmissionController,
    AdmitResult,
    AdmitStatus,
    CandidateBackend,
    CapacityReason,
    QuotaReason,
    StickyOutcome,
    TokenStats,
)
from first_gateway.database.redis.keys import Keys


@pytest.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    url = Settings().redis_url
    r: Redis = Redis.from_url(url, decode_responses=True)
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
        result = await _admit(ac, "req-1")
        applied = await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        assert applied is True

    async def test_admit_increments_backend_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None
        inflight = await redis.zcard(Keys.backend_inflight(MODEL, result.backend_id))
        assert inflight == 1

    async def test_settle_decrements_backend_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None
        await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        inflight = await redis.zcard(Keys.backend_inflight(MODEL, result.backend_id))
        assert inflight == 0

    async def test_settle_cleans_up_reservation_and_deadline(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        assert await redis.get(Keys.reservation("req-1")) is None
        assert await redis.zscore(Keys.deadlines(), "req-1") is None

    async def test_settle_cleans_user_inflight_at_zero(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        assert await redis.zcard(Keys.user_inflight(MODEL, USER)) == 0

    async def test_settle_without_caller_context_pre_reads(
        self, ac: AdmissionController
    ) -> None:
        """Sweeper path: settle without model_name/user_id/backend_id still works."""
        await _admit(ac, "req-1")
        applied = await ac.settle("req-1", actual_tokens=50)
        assert applied is True

    async def test_model_reservations_incremented_and_decremented(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        assert await redis.zcard(Keys.model_inflight(MODEL)) == 1

        result = await _admit(ac, "req-1-lookup")
        await ac.settle(
            "req-1", actual_tokens=50, model_name=MODEL, user_id=USER, backend_id="r1"
        )
        # Settle only removes one; the second is still there
        assert await redis.zcard(Keys.model_inflight(MODEL)) == 1
        await ac.settle(
            "req-1-lookup",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        assert await redis.zcard(Keys.model_inflight(MODEL)) == 0

    async def test_user_inflight_zset_membership(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        assert await redis.zcard(Keys.user_inflight(MODEL, USER)) == 1

        await _admit(ac, "req-2")
        assert await redis.zcard(Keys.user_inflight(MODEL, USER)) == 2


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

    async def test_failover_retry_charges_quota_once(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        tight_quota = UsageLimits(
            tpm=600,
            burst_tokens=150,
            rpm=60,
            burst_requests=1,
            max_user_concurrency=100,
        )
        rpm_key = Keys.user_rate_limit(MODEL, USER, "rpm")
        tokens_key = Keys.user_rate_limit(MODEL, USER, "tokens")

        result = await _admit(ac, "req-1", estimated_tokens=100, quota=tight_quota)
        assert result.admitted
        rpm_tat = await redis.get(rpm_key)
        tokens_tat = await redis.get(tokens_key)

        # The backend fails; the same request fails over to another one.
        await ac.settle("req-1", actual_tokens=0)
        result = await _admit(
            ac,
            "req-1",
            estimated_tokens=100,
            quota=tight_quota,
            charge_request=False,
        )
        assert result.admitted
        assert await redis.get(rpm_key) == rpm_tat
        tokens_tat_after = await redis.get(tokens_key)
        assert tokens_tat is not None and tokens_tat_after is not None
        assert float(tokens_tat_after) == pytest.approx(float(tokens_tat), abs=0.5)

    async def test_quota_reject_does_not_increment_demand(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        for i in range(QUOTA.max_user_concurrency):
            await _admit(ac, f"req-{i}")

        await _admit(ac, "req-over")
        rejects = await redis.hget(Keys.model_rejects(MODEL), "capacity_rejects_total")
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

        rejects = await redis.hget(Keys.model_rejects(MODEL), "capacity_rejects_total")
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
        result = await _admit(ac, "req-1")
        first = await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        second = await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
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
        await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
        inflight = await redis.zcard(Keys.backend_inflight(MODEL, result.backend_id))
        assert inflight >= 0

    async def test_concurrent_settle_paths(self, ac: AdmissionController) -> None:
        """Hot-path settle (with context) racing sweeper settle (without context)."""
        result = await _admit(ac, "req-1")
        hot = await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
        )
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
        ac_small_chunk = AdmissionController(ac.client, chunk_size=2)
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

    async def test_sweeper_restores_inflight(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        short_lease = AdmissionController(ac.client, lease_sec=0.1, max_request_sec=900)
        result = await _admit(short_lease, "req-stale")
        assert result.backend_id is not None

        await asyncio.sleep(0.2)

        await short_lease.sweep()
        inflight = await redis.zcard(Keys.backend_inflight(MODEL, result.backend_id))
        assert inflight == 0

    async def test_sweeper_survives_malformed_blob(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        """A blob missing backend_id doesn't wedge the sweeper."""
        short_lease = AdmissionController(ac.client, lease_sec=0.1, max_request_sec=900)
        result = await _admit(short_lease, "req-good")
        assert result.backend_id is not None

        # Inject a malformed reservation: missing backend_id
        poison_key = Keys.reservation("req-poison")
        await redis.set(poison_key, json.dumps({"model_name": MODEL, "user_id": USER}))
        await redis.zadd(Keys.deadlines(), {"req-poison": 0.0})

        await asyncio.sleep(0.2)

        settled = await short_lease.sweep()
        # req-good is settled normally; req-poison is cleaned up without crashing
        assert settled >= 1
        assert await redis.get(poison_key) is None
        assert await redis.zscore(Keys.deadlines(), "req-poison") is None


# ---------------------------------------------------------------------------
# 8. Orphan repair
# ---------------------------------------------------------------------------


class TestOrphanRepair:
    async def test_repair_removes_orphaned_zset_members(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        """Orphan: ZSET member exists but reservation blob is gone."""
        result = await _admit(ac, "req-1")
        assert result.backend_id is not None

        await redis.delete(Keys.reservation("req-1"))

        removed = await ac.repair_orphaned_zsets()
        assert removed == 3  # backend_inflight + user_inflight + model_reservations

        assert await redis.zcard(Keys.backend_inflight(MODEL, result.backend_id)) == 0
        assert await redis.zcard(Keys.user_inflight(MODEL, USER)) == 0
        assert await redis.zcard(Keys.model_inflight(MODEL)) == 0

    async def test_repair_leaves_live_reservations_alone(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        """Live reservation with blob intact should not be touched."""
        await _admit(ac, "req-1")

        removed = await ac.repair_orphaned_zsets()
        assert removed == 0

        assert await redis.zcard(Keys.model_inflight(MODEL)) == 1

    async def test_repair_is_idempotent(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _admit(ac, "req-1")
        await redis.delete(Keys.reservation("req-1"))

        first = await ac.repair_orphaned_zsets()
        second = await ac.repair_orphaned_zsets()
        assert first == 3
        assert second == 0

    async def test_repair_chunks_large_zsets(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        """Exercise chunk boundary with >chunk_size orphans in one ZSET."""
        n = ac.chunk_size + 50
        zset_key = Keys.model_inflight(MODEL)
        now = 1700000000.0
        await redis.zadd(zset_key, {f"orphan-{i}": now for i in range(n)})

        removed = await ac.repair_orphaned_zsets()
        assert removed == n
        assert await redis.zcard(zset_key) == 0


# ---------------------------------------------------------------------------
# 9. Script-level guards
# ---------------------------------------------------------------------------


class TestScriptGuards:
    async def test_admit_rejects_odd_candidate_key_count(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        """Passing an odd number of trailing keys triggers a loud error."""
        keys = [
            Keys.user_rate_limit(MODEL, USER, "tokens"),
            Keys.user_rate_limit(MODEL, USER, "rpm"),
            Keys.user_inflight(MODEL, USER),
            Keys.model_inflight(MODEL),
            Keys.model_rejects(MODEL),
            Keys.deadlines(),
            Keys.reservation("req-odd"),
            "rt:sticky:off",
            Keys.backend_errors("r1"),
            # missing backend_inflight key — odd count
        ]
        args: list[str | int | float] = [
            "req-odd",
            MODEL,
            USER,
            100,
            3,
            1000.0,
            200000,
            1.0,
            5,
            30.0,
            0,
            "r1",
            10,
            3,
        ]
        with pytest.raises(ResponseError, match="odd number of candidate keys"):
            await ac._admit(keys=keys, args=args)


# ---------------------------------------------------------------------------
# Token-estimation EWMA calibration
# ---------------------------------------------------------------------------


async def _record(
    ac: AdmissionController,
    *,
    chars_per_token_sample: float | None,
    output_tokens_sample: float | None,
    user: str = USER,
) -> None:
    """Settle a request (none is reserved: only the samples matter here)."""
    await ac.settle(
        "stats-req",
        actual_tokens=0,
        model_name=MODEL,
        user_id=user,
        backend_id="b1",
        chars_per_token_sample=chars_per_token_sample,
        output_tokens_sample=output_tokens_sample,
    )


async def _stored_stats(redis: Redis, user: str = USER) -> TokenStats:
    """What another worker (with nothing cached) would read."""
    return await AdmissionController(redis).get_token_stats(MODEL, user)


class TestTokenStats:
    async def test_cold_read_returns_empty_stats(self, ac: AdmissionController) -> None:
        stats = await ac.get_token_stats(MODEL, USER)
        assert stats == TokenStats(chars_per_token=None, output_tokens=None)

    async def test_first_sample_seeds_ewma(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _record(ac, chars_per_token_sample=3.5, output_tokens_sample=500.0)
        for stats in (
            await ac.get_token_stats(MODEL, USER),
            await _stored_stats(redis),
        ):
            assert stats.chars_per_token == 3.5
            assert stats.output_tokens == 500.0

    async def test_second_sample_blends_with_alpha(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _record(ac, chars_per_token_sample=4.0, output_tokens_sample=100.0)
        await _record(ac, chars_per_token_sample=2.0, output_tokens_sample=300.0)
        a = USAGE_EWMA_ALPHA
        for stats in (
            await ac.get_token_stats(MODEL, USER),
            await _stored_stats(redis),
        ):
            assert stats.chars_per_token == pytest.approx(a * 2.0 + (1 - a) * 4.0)
            assert stats.output_tokens == pytest.approx(a * 300.0 + (1 - a) * 100.0)

    async def test_none_sample_leaves_that_metric_untouched(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _record(ac, chars_per_token_sample=4.0, output_tokens_sample=100.0)
        # Update only output; chars-per-token must keep its prior value.
        await _record(ac, chars_per_token_sample=None, output_tokens_sample=200.0)
        a = USAGE_EWMA_ALPHA
        for stats in (
            await ac.get_token_stats(MODEL, USER),
            await _stored_stats(redis),
        ):
            assert stats.chars_per_token == 4.0
            assert stats.output_tokens == pytest.approx(a * 200.0 + (1 - a) * 100.0)

    async def test_all_none_samples_write_nothing(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _record(ac, chars_per_token_sample=None, output_tokens_sample=None)
        assert (
            await redis.exists(
                Keys.usage_ewma(MODEL, USER, "input_token_chars"),
                Keys.usage_ewma(MODEL, USER, "output_token"),
            )
            == 0
        )

    async def test_stats_scoped_per_user_and_model(
        self, ac: AdmissionController
    ) -> None:
        await _record(ac, chars_per_token_sample=2.0, output_tokens_sample=10.0)
        assert await ac.get_token_stats(MODEL, "bob") == TokenStats()
        assert await ac.get_token_stats("other-model", USER) == TokenStats()

    async def test_record_sets_ttl(self, ac: AdmissionController, redis: Redis) -> None:
        await _record(ac, chars_per_token_sample=4.0, output_tokens_sample=100.0)
        ttl = await redis.ttl(Keys.usage_ewma(MODEL, USER, "input_token_chars"))
        assert ttl > 0

    async def test_samples_ride_on_the_settle_of_a_reservation(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1")
        applied = await ac.settle(
            "req-1",
            actual_tokens=50,
            model_name=MODEL,
            user_id=USER,
            backend_id=result.backend_id,
            output_tokens_sample=40.0,
        )
        assert applied
        assert (await _stored_stats(redis)).output_tokens == 40.0

    async def test_read_is_served_from_the_worker_cache(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await _record(ac, chars_per_token_sample=4.0, output_tokens_sample=100.0)
        # Another worker moves the EWMA; this one keeps its recent value...
        other = AdmissionController(redis)
        await _record(other, chars_per_token_sample=None, output_tokens_sample=300.0)
        assert (await ac.get_token_stats(MODEL, USER)).output_tokens == 100.0
        # ...until it expires, or its own next settle reports the shared value.
        ac._token_stats.clear()
        a = USAGE_EWMA_ALPHA
        shared = pytest.approx(a * 300.0 + (1 - a) * 100.0)
        assert (await ac.get_token_stats(MODEL, USER)).output_tokens == shared


# ---------------------------------------------------------------------------
# Sticky sessions
# ---------------------------------------------------------------------------

STICKY_KEY = Keys.sticky_session(MODEL, None, USER, "sess-1")


async def _sticky_admit(
    ac: AdmissionController, request_id: str, *uids: str, **kw: object
) -> AdmitResult:
    kw.setdefault("candidates", _candidates(*(uids or ("r1", "r2"))))
    return await _admit(ac, request_id, sticky_key=STICKY_KEY, sticky_ttl_sec=600, **kw)


class TestStickySessions:
    async def test_off_by_default(self, ac: AdmissionController, redis: Redis) -> None:
        result = await _admit(ac, "req-1")
        assert result.sticky is StickyOutcome.OFF
        assert await redis.keys("rt:sticky:*") == []

    async def test_zero_ttl_disables(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _admit(ac, "req-1", sticky_key=STICKY_KEY, sticky_ttl_sec=0)
        assert result.sticky is StickyOutcome.OFF
        assert await redis.get(STICKY_KEY) is None

    async def test_new_session_pins_chosen_backend(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        result = await _sticky_admit(ac, "req-1", "r1", "r2")
        assert result.sticky is StickyOutcome.NEW
        assert result.backend_id == "r1"
        assert await redis.get(STICKY_KEY) == "r1"
        assert 590 < await redis.ttl(STICKY_KEY) <= 600

    async def test_hit_overrides_router_order(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r2", ex=5)
        result = await _sticky_admit(ac, "req-1", "r1", "r2")
        assert result.sticky is StickyOutcome.HIT
        assert result.backend_id == "r2"
        # Each hit renews the session TTL.
        assert await redis.ttl(STICKY_KEY) > 5

    async def test_full_sticky_backend_spills_and_repins(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r2")
        candidates = _candidates("r1", "r2", concurrency=1)
        await _admit(ac, "other", candidates=_candidates("r2", concurrency=1))
        result = await _sticky_admit(ac, "req-1", candidates=candidates)
        assert result.sticky is StickyOutcome.REPINNED
        assert result.backend_id == "r1"
        assert await redis.get(STICKY_KEY) == "r1"

    async def test_benched_sticky_backend_spills_and_repins(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r1")
        await ac.record_error("r1", RouterParams(cooldown_threshold=1))
        candidates = _candidates("r1", "r2", cooldown_threshold=1)
        result = await _sticky_admit(ac, "req-1", candidates=candidates)
        assert result.sticky is StickyOutcome.REPINNED
        assert result.backend_id == "r2"
        assert await redis.get(STICKY_KEY) == "r2"

    async def test_gone_sticky_backend_repins(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r-retired")
        result = await _sticky_admit(ac, "req-1", "r1", "r2")
        assert result.sticky is StickyOutcome.REPINNED
        assert result.backend_id == "r1"
        assert await redis.get(STICKY_KEY) == "r1"

    async def test_benched_sticky_counts_once_toward_all_benched(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r2")
        for uid in ("r1", "r2"):
            await ac.record_error(uid, RouterParams(cooldown_threshold=1))
        candidates = _candidates("r1", "r2", cooldown_threshold=1)
        result = await _sticky_admit(ac, "req-1", candidates=candidates)
        assert result.capacity_reason is CapacityReason.ALL_BENCHED

    async def test_capacity_reject_keeps_session(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r1")
        await _admit(ac, "other", candidates=_candidates("r1", concurrency=1))
        result = await _sticky_admit(
            ac, "req-1", candidates=_candidates("r1", concurrency=1)
        )
        assert result.capacity_reason is CapacityReason.SATURATED
        assert await redis.get(STICKY_KEY) == "r1"

    async def test_sticky_admit_reserves_like_normal(
        self, ac: AdmissionController, redis: Redis
    ) -> None:
        await redis.set(STICKY_KEY, "r2")
        await _sticky_admit(ac, "req-1", "r1", "r2")
        assert await redis.zcard(Keys.backend_inflight(MODEL, "r2")) == 1
        assert await redis.zcard(Keys.backend_inflight(MODEL, "r1")) == 0
        assert await ac.settle(
            "req-1", 0, model_name=MODEL, user_id=USER, backend_id="r2"
        )
        assert await redis.zcard(Keys.backend_inflight(MODEL, "r2")) == 0
