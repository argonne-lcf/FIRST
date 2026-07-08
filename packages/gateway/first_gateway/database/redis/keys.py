from typing import Literal

CONFIG_CHANNEL = "cfg:changed"
RT_PREFIX = "rt:"
QUOTA_PREFIX = "quota:"
RESERVE_PREFIX = "rt:reserve:"


class Keys:
    """Redis Key builders"""

    @staticmethod
    def config() -> str:
        return "router-cfg"

    @staticmethod
    def inflight(model: str) -> str:
        return f"{RT_PREFIX}{model}:inflight"

    @staticmethod
    def demand(model: str) -> str:
        return f"{RT_PREFIX}{model}:demand"

    @staticmethod
    def replica_errors(replica_id: str) -> str:
        return f"{RT_PREFIX}replica:{replica_id}:errors"

    @staticmethod
    def reservation(request_id: str) -> str:
        return f"{RESERVE_PREFIX}{request_id}"

    @staticmethod
    def deadlines() -> str:
        return f"{RESERVE_PREFIX}deadlines"

    @staticmethod
    def quota(
        model: str, user: str, resource: Literal["tokens", "rpm", "inflight"]
    ) -> str:
        return f"{QUOTA_PREFIX}{model}:{user}:{resource}"
