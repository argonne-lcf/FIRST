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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not getattr(cls, "endpoint", None):
            raise TypeError(
                f"{cls.__name__} must set a non-empty `endpoint` class variable."
            )
