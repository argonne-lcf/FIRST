from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


# OpenAI/Anthropic-compatible model listing (SDK compat).
# https://docs.claude.com/en/api/models-list
class ModelInfo(BaseModel):
    id: str
    type: Literal["model"] = "model"
    created_at: datetime | None = None
    display_name: str
    capabilities: dict[str, Any] = {}
    max_tokens: int | None = None


class ModelListResponse(BaseModel):
    data: list[ModelInfo]
