import asyncio
import json
import logging
import time
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Sequence

from pydantic import BaseModel
from redis.asyncio import Redis

from first_common.schema.types import RouterParams, UsageLimits

from .keys import Keys

logger = logging.getLogger(__name__)

LUA_DIR = Path(__file__).parent / "lua"
_ADMIT_LUA = (LUA_DIR / "admit.lua").read_text()
_SETTLE_LUA = (LUA_DIR / "settle.lua").read_text()
_RENEW_LUA = (LUA_DIR / "renew.lua").read_text()
_RECORD_ERROR_LUA = (LUA_DIR / "record_error.lua").read_text()


def to_str(v: Any) -> str:
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)


class AdmitStatus(int, Enum):
    """
    Return status of admit() Redis Lua script.
    """

    ADMITTED = 1  # proceed
    REJECT_QUOTA = 2  # user is over their usage limit; retry-after
    REJECT_CAPACITY = 3  # model does not have headroom (or cold start; 0 free capacity)


class QuotaReason(str, Enum):
    USER_CONCURRENCY = "user_concurrency"  # user over max inflight limit
    USER_RPM = "user_rpm"  # user over requests/minute rate
    USER_TPM = "user_tpm"  # user over tokens/minute rate


class CapacityReason(str, Enum):
    SATURATED = "saturated"  # there are live backends; all slots are full
    ALL_BENCHED = "all_benched"  # all backends are in error cooldown
    NO_CANDIDATES = "no_candidates"  # no live backends (cold start)


class AdmitResult(BaseModel):
    """Decoded return of the admit.lua script."""

    status: AdmitStatus
    backend_id: str | None = None
    quota_reason: QuotaReason | None = None
    capacity_reason: CapacityReason | None = None
    retry_after_sec: float | None = None

    @property
    def admitted(self) -> bool:
        return self.status is AdmitStatus.ADMITTED

    @classmethod
    def from_lua(cls, raw: Sequence[Any]) -> "AdmitResult":
        code = int(raw[0])
        if code == AdmitStatus.ADMITTED:
            return cls(status=AdmitStatus.ADMITTED, backend_id=to_str(raw[1]))
        if code == AdmitStatus.REJECT_QUOTA:
            retry_after = float(raw[2])
            return cls(
                status=AdmitStatus.REJECT_QUOTA,
                quota_reason=QuotaReason(to_str(raw[1])),
                retry_after_sec=retry_after if retry_after >= 0 else None,
            )
        return cls(
            status=AdmitStatus.REJECT_CAPACITY,
            capacity_reason=CapacityReason(to_str(raw[1])),
        )


class CandidateBackend(BaseModel):
    """One entry of the ordered candidate list, to be checked for capacity."""

    uid: str
    max_backend_concurrency: int
    cooldown_threshold: int


class AdmissionController:
    """
    Controller for distributed request admission: performs quota bookkeeping
    (RPM, TPM, Concurrency) per user/model and backend capacity bookkeeping (
    cooldown state, per-backend concurrency).

    Simple INT counters cannot be reliably maintained in a distributed system
    with crashing workers and retries. The controller manages a ledger of
    reservations (one per admitted unique request_id) and uses ZSETs (sorted
    sets keyed on request_id) for idempotent request accounting.  Atomic sets
    of operations are executed on the Redis server as serializable Lua scripts.

    Owns the Lua script inventory (admit, settle, renew, record_error) and the
    sweep loop.
    """

    def __init__(
        self,
        client: Redis,
        *,
        lease_sec: float = 30.0,
        max_request_sec: float = 3600.0,
        chunk_size: int = 500,
        renew_interval_sec: float = 10.0,
    ) -> None:
        """
        - lease_sec: default reservation duration
        - max_request_sec: lease can be renewed for up to this long (backstop
          for stuck requests that never stop renewing the lease)
        - chunk_size: batch size for lease renewal and repair ops
        - renew_interval_sec: period of the background lease-renewal loop
        """
        self.client = client
        self.lease_sec = lease_sec
        self.max_request_sec = max_request_sec
        self.chunk_size = chunk_size
        self.renew_interval_sec = renew_interval_sec
        self._admit = client.register_script(_ADMIT_LUA)
        self._settle = client.register_script(_SETTLE_LUA)
        self._renew = client.register_script(_RENEW_LUA)
        self._record_error = client.register_script(_RECORD_ERROR_LUA)
        # request_ids admitted by this worker and not yet settled; the renew
        # loop keeps their leases alive so healthy workers hold their slots.
        self._inflight: set[str] = set()
        self._renew_task: asyncio.Task[None] | None = None

    async def admit(
        self,
        *,
        request_id: str,
        model_name: str,
        user_id: str,
        candidates: Sequence[CandidateBackend],
        estimated_tokens: int,
        quota: UsageLimits,
    ) -> AdmitResult:
        """
        Attempt to admit a request.  Performs one atomic round trip to Redis:
        check all quotas once, then iterate candidates and assign the first one
        with available capacity.

        A reservation is created if admitted.

        The reservation has an expiring lease (see `lease_sec` above) that must
        be maintained during the lifetime of the request, using renew().

        All reservations must be cleared used settle().  Reservations cannot use
        a TTL, because related state must be rolled back atomically.

        - `request_id`: unique per-request identifier
        - `model_name`: unique model name
        - `user_id`: unique user ID
        - `candidates`: ordered list of backends.  The router must select
           healthy backends in scope for the current request and sort them into the
           desired weighted random sampling order.
        - `estimated_tokens`:  estimated heuristic prompt_tokens+max_tokens for
           this request to be reserved.  Set to 0 for non-LLM requests.
        - `quota`: the configured usage rate limits for this model_name.
        """
        keys: list[str] = [
            Keys.user_rate_limit(model_name, user_id, "tokens"),
            Keys.user_rate_limit(model_name, user_id, "rpm"),
            Keys.user_inflight(model_name, user_id),
            Keys.model_inflight(model_name),
            Keys.model_rejects(model_name),
            Keys.deadlines(),
            Keys.reservation(request_id),
        ]
        for c in candidates:
            keys.append(Keys.backend_errors(c.uid))
            keys.append(Keys.backend_inflight(model_name, c.uid))

        args: list[str | int | float] = [
            request_id,
            model_name,
            user_id,
            estimated_tokens,
            quota.max_user_concurrency,
            quota.tokens_per_sec,
            quota.burst_tokens,
            quota.requests_per_sec,
            quota.burst_requests,
            self.lease_sec,
        ]
        for c in candidates:
            args.extend([c.uid, c.max_backend_concurrency, c.cooldown_threshold])

        raw = await self._admit(keys=keys, args=args)
        result = AdmitResult.from_lua(raw)
        if result.admitted:
            self._inflight.add(request_id)
        return result

    async def settle(
        self,
        request_id: str,
        actual_tokens: Optional[int] = None,
        *,
        model_name: str | None = None,
        user_id: str | None = None,
        backend_id: str | None = None,
    ) -> bool:
        """
        Reverse one reservation's effects idempotently.

        Safe to call from the request `finally`, the sweeper, and retries
        simultaneously; returns True iff this call was the one that applied.

        Pass model_name, user_id, and backend_id from the request context to
        skip the pre-read round trip on the hot path.  The sweeper omits them
        and pays one extra GET to discover the reservation's identity.
        """
        self._inflight.discard(request_id)
        reservation_key = Keys.reservation(request_id)

        if not model_name or not user_id or not backend_id:
            raw_reservation = await self.client.get(reservation_key)
            if raw_reservation is None:
                await self.client.zrem(Keys.deadlines(), request_id)
                return False
            try:
                row = json.loads(raw_reservation)
            except json.JSONDecodeError:
                row = {}
            model_name = row.get("model_name")
            user_id = row.get("user_id")
            backend_id = row.get("backend_id")
            if not model_name or not user_id or not backend_id:
                logger.error(
                    "settle: malformed reservation blob for %s, "
                    "force-removing from deadlines",
                    request_id,
                )
                await self.client.zrem(Keys.deadlines(), request_id)
                await self.client.delete(reservation_key)
                return False

        raw = await self._settle(
            keys=[
                reservation_key,
                Keys.deadlines(),
                Keys.backend_inflight(model_name, backend_id),
                Keys.user_inflight(model_name, user_id),
                Keys.user_rate_limit(model_name, user_id, "tokens"),
                Keys.model_inflight(model_name),
            ],
            args=[
                "" if actual_tokens is None else int(actual_tokens),
                request_id,
            ],
        )
        code = int(raw[0])
        return bool(code)

    async def renew(self, request_ids: Sequence[str]) -> int:
        """
        Batched, chunked lease renewal.  Each apiserver worker MUST call this
        method with all live request_ids every ~lease_sec/3 seconds to maintain
        the lease on its requests.

        Crashed workers stop renewing the lease, and another sweeper cleans up
        the stale reservations within ~lease_sec.
        """
        renewed = 0
        for i in range(0, len(request_ids), self.chunk_size):
            chunk = list(request_ids[i : i + self.chunk_size])
            keys = [Keys.deadlines()] + [Keys.reservation(rid) for rid in chunk]
            renewed += int(
                await self._renew(
                    keys=keys,
                    args=[self.lease_sec, self.max_request_sec, *chunk],
                )
            )
        return renewed

    async def start(self) -> None:
        """Start the background loop renewing this worker's in-flight leases.

        Call once from the app lifespan; pair with stop() on shutdown.
        """
        self._renew_task = asyncio.create_task(
            self._renew_loop(), name="admission-renew"
        )

    async def stop(self) -> None:
        if self._renew_task is None:
            return
        self._renew_task.cancel()
        try:
            await self._renew_task
        except asyncio.CancelledError:
            pass
        self._renew_task = None

    async def _renew_loop(self) -> None:
        while True:
            await asyncio.sleep(self.renew_interval_sec)
            if not self._inflight:
                continue
            try:
                await self.renew(list(self._inflight))
            except Exception:
                logger.warning("Inflight lease renewal failed", exc_info=True)

    async def sweep(self, batch: int = 100) -> int:
        """
        Settle reservations whose lease lapsed (crashed worker, stuck
        handler past max_request_sec).

        Lock-free: settle's idempotency makes concurrent sweeps merely wasteful,
        never wrong.  Any worker may run this opportunistically on a timer.
        """
        now = time.time()
        expired = await self.client.zrangebyscore(
            Keys.deadlines(), "-inf", now, start=0, num=batch
        )
        settled = 0
        for rid in expired:
            if await self.settle(to_str(rid), actual_tokens=None):
                settled += 1

        if settled > 0:
            logger.warning(
                f"Reservation sweeper settled {settled} expired reservations"
            )

        return settled

    async def record_error(
        self, backend_id: str, router_params: RouterParams
    ) -> tuple[int, bool]:
        """
        Register an upstream failure.  Returns (error_count, is_benched).

        admit() treats error_count >= router_params.cooldown_threshold as benched: the backend
        is removed from the pool for router_params.cooldown_bench_sec.

        For example: given cooldown threshold=2, window=30s, bench=120s: any
        backend that experiences 2 errors within a 30second window is benched
        for 2 minutes.

        The Lua script correctly re-arms the TTL under concurrent failures.
        """
        raw = await self._record_error(
            keys=[Keys.backend_errors(backend_id)],
            args=[
                router_params.cooldown_window_sec,
                router_params.cooldown_threshold,
                router_params.cooldown_bench_sec,
            ],
        )
        return int(raw[0]), bool(int(raw[1]))

    async def repair_orphaned_zsets(self) -> int:
        """
        Remove orphans from the backend/user/model inflight sorted sets.

        An orphan is a ZSET member whose reservation blob no longer exists
        (e.g. eviction or partial admit failure).

        SCAN membership zsets, then ZSCAN each set in chunks and batch-EXISTS
        the reservations.  ZREM the dead ones.  Race-safe without any
        transaction: if a concurrent settle deletes the blob mid-check, both
        parties ZREM safely.
        """
        patterns = [
            Keys.backend_inflight_scan_pattern(),
            Keys.user_inflight_scan_pattern(),
            Keys.model_inflight_scan_pattern(),
        ]
        removed = 0
        for pattern in patterns:
            async for key in self.client.scan_iter(match=pattern, count=100):
                key_str = to_str(key)
                batch: list[str] = []
                async for member in self.client.zscan_iter(
                    key_str, count=self.chunk_size
                ):
                    batch.append(to_str(member[0]))
                    if len(batch) >= self.chunk_size:
                        removed += await self._zrem_dead(key_str, batch)
                        batch = []
                if batch:
                    removed += await self._zrem_dead(key_str, batch)
        return removed

    async def _zrem_dead(self, key: str, request_ids: list[str]) -> int:
        async with self.client.pipeline(transaction=False) as pipe:
            for rid in request_ids:
                pipe.exists(Keys.reservation(rid))
            alive = await pipe.execute()
        dead = [rid for rid, ok in zip(request_ids, alive) if not ok]
        if dead:
            return int(await self.client.zrem(key, *dead))
        return 0
