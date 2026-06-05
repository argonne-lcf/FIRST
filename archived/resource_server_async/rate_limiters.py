import logging
from typing import NamedTuple

from redis import Redis

logger = logging.getLogger(__name__)


class TokenLimiterCheck(NamedTuple):
    allow: bool
    usage_model: int
    usage_user: int
    limit_model: int
    limit_user: int


class TokenRateLimiter:
    def __init__(
        self, redis_client: Redis, prefix: str, *, tpm_model: int, tpm_user: int
    ) -> None:
        """
        Create a rate limiter with a unique `prefix` for the model endpoint with
        desired Tokens per Minute for the model (`tpm_model`) and per-user
        (`tpm_user`).

        Setting tpm limits to 0 disables rate limiting.
        """
        self.redis = redis_client
        self.prefix = f"tpm:{prefix}"
        self.tpm_model = tpm_model
        self.tpm_user = tpm_user

    def _keys(self, user_id: str | None) -> tuple[str, str]:
        model_key = self.prefix
        user_key = f"{self.prefix}:{user_id}"
        return model_key, user_key

    def check(self, user_id: str) -> TokenLimiterCheck:
        """
        Check if rate limiter should allow call and return current usage.
        """
        model_key, user_key = self._keys(user_id)
        try:
            current_model = int(self.redis.get(model_key) or 0)  # type: ignore
            current_user = int(self.redis.get(user_key) or 0)  # type: ignore
        except ValueError:
            current_model, current_user = 0, 0

        allow = True
        if self.tpm_model > 0:
            allow = allow and (current_model < self.tpm_model)
        if self.tpm_user > 0:
            allow = allow and (current_user < self.tpm_user)

        logger.debug(
            f"{model_key=} {user_key=} {current_model=}/{self.tpm_model=} {current_user=}/{self.tpm_user=}"
        )
        return TokenLimiterCheck(
            allow, current_model, current_user, self.tpm_model, self.tpm_user
        )

    def record(self, user_id: str | None, tokens: int) -> None:
        """
        Call after LLM response with actual token counts to record the usage of the model.
        """
        model_key, user_key = self._keys(user_id)
        pipe = self.redis.pipeline(transaction=True)

        pipe.set(model_key, 0, nx=True, ex=60)
        pipe.incrby(model_key, tokens)

        # Guard in case of anonymous usage:
        if user_id:
            pipe.set(user_key, 0, nx=True, ex=60)
            pipe.incrby(user_key, tokens)

        pipe.execute()
