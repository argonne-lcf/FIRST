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

    @staticmethod
    def quota(
        model: str, user: str, resource: Literal["tokens", "rpm", "inflight"]
    ) -> str:
        return f"quota:{model}:{user}:{resource}"

    @staticmethod
    def token_introspect(token_hash: str) -> str:
        """Cached token introspection result, keyed by SHA-256 of the bearer token."""
        return f"auth:token_introspect:{token_hash}"

    @staticmethod
    def authed_user(user_id: str) -> str:
        """NX-guarded flag to emit a UserAuthEvent once per TTL window."""
        return f"auth:user:{user_id}"

    @staticmethod
    def log_dedup_5xx(user: str, status_code: int) -> str:
        """NX-guarded flag to suppress repeated 5xx log lines."""
        return f"logdedup:{user}:{status_code}"

    @staticmethod
    def log_dedup_4xx(user: str, fingerprint: str, status_code: int) -> str:
        """NX-guarded flag to suppress repeated 4xx log lines."""
        return f"logdedup:{user}:{fingerprint}:{status_code}"
