from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)


class BaseModelAllowExtra(BaseModel):
    model_config = ConfigDict(extra="allow")


class BasePayload(BaseModelAllowExtra):
    model: str = Field(..., min_length=1)
