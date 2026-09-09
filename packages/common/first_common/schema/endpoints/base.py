from typing import Any, ClassVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class BaseModelAllowExtra(BaseModel):
    model_config = ConfigDict(extra="allow")


class BasePayload(BaseModelAllowExtra):
    endpoint: ClassVar[str]
    model: str = Field(..., min_length=1)

    def __init_subclass__(
        cls, *, dynamic_endpoint: bool = False, **kwargs: Any
    ) -> None:
        super().__init_subclass__(**kwargs)
        # `dynamic_endpoint` subclasses carry `endpoint` as a per-instance field
        # rather than a fixed class variable.
        if not dynamic_endpoint and not getattr(cls, "endpoint", None):
            raise TypeError(
                f"{cls.__name__} must set a non-empty `endpoint` class variable."
            )

    def estimate_tokens(
        self,
        _max_context: int | None,
        *,
        chars_per_token: float | None = None,  # noqa: ARG002
        output_estimate: int | None = None,  # noqa: ARG002
    ) -> int:
        """
        Estimate the total tokens from the payload and the models's maximum
        context.  Defaults to always 0.  Override for LLM request payloads.

        ``chars_per_token`` and ``output_estimate`` are optional per-user+model
        values learned from recent traffic; ``None`` uses the static defaults.
        """
        return 0

    def input_basis(self) -> tuple[int, int]:
        """
        Return ``(text_chars, image_count)`` for this request's input
        """
        return 0, 0
