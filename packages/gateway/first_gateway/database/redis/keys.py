from typing import Literal

CONFIG_CHANNEL = "cfg:changed"


class Keys:
    """Redis key builders — the single source of truth for all key patterns.

    Every Redis key used by the gateway MUST be constructed here.
    Lua scripts receive pre-built keys via KEYS[] and never assemble keys
    from prefix strings.
    """

    @staticmethod
    def config() -> str:
        return "router-cfg"

    # -- router state (rt:*) -------------------------------------------------

    @staticmethod
    def model_inflight(model: str) -> str:
        """HASH keyed by backend_id → concurrent request count."""
        return f"rt:model:{model}:inflight"

    @staticmethod
    def model_demand(model: str) -> str:
        """HASH {inflight, capacity_rejects_total, last_reject_ts}."""
        return f"rt:model:{model}:demand"

    @staticmethod
    def backend_errors(backend_id: str) -> str:
        """INT counter with TTL; count >= threshold IS the cooldown bench."""
        return f"rt:backend:{backend_id}:errors"

    @staticmethod
    def reservation(request_id: str) -> str:
        """JSON blob written by admit, read/deleted by settle."""
        return f"rt:reserve:{request_id}"

    @staticmethod
    def reservation_scan_pattern() -> str:
        """SCAN match pattern that covers all reservation keys."""
        return "rt:reserve:*"

    @staticmethod
    def deadlines() -> str:
        """ZSET of request_ids scored by deadline timestamp."""
        return "rt:deadlines"

    # -- quota state (quota:*) -----------------------------------------------

    @staticmethod
    def quota(
        model: str, user: str, resource: Literal["tokens", "rpm", "inflight"]
    ) -> str:
        return f"quota:{model}:{user}:{resource}"
