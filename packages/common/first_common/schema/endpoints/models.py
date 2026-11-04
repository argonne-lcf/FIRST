from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, model_validator


def api_protocols(supported_endpoints: list[str]) -> list[str]:
    """
    Gateway endpoint paths as API protocol names, as harness configs and the V1
    catalog name them ("chat/completions" -> "chat_completions").
    """
    return [e.replace("/", "_") for e in supported_endpoints]


# Model listing compatible with both the OpenAI and Anthropic SDKs (a superset
# of the two shapes), plus the vLLM-style `max_model_len` many harnesses read.
# https://platform.openai.com/docs/api-reference/models/list
# https://docs.claude.com/en/api/models-list
class ModelInfo(BaseModel):
    id: str
    # OpenAI: object/created/owned_by.  Anthropic: type/created_at/display_name.
    object: Literal["model"] = "model"
    type: Literal["model"] = "model"
    created: int = 0  # Unix seconds; derived from created_at when unset
    created_at: datetime | None = None
    owned_by: str = "alcf"
    display_name: str
    # Includes `api_protocols` (e.g. ["chat_completions", "responses"]) so a
    # harness can pick a wire protocol the model actually serves.
    capabilities: dict[str, Any] = {}
    max_tokens: int | None = None
    max_model_len: int | None = None  # vLLM name for the context window

    @model_validator(mode="after")
    def _derive_created(self) -> "ModelInfo":
        if not self.created and self.created_at is not None:
            self.created = int(self.created_at.timestamp())
        return self


class ModelListResponse(BaseModel):
    object: Literal["list"] = "list"
    data: list[ModelInfo]
    # Anthropic pagination fields: the gateway always returns a single page.
    has_more: bool = False
    first_id: str | None = None
    last_id: str | None = None

    @model_validator(mode="after")
    def _derive_page_ids(self) -> "ModelListResponse":
        if self.data:
            self.first_id = self.first_id or self.data[0].id
            self.last_id = self.last_id or self.data[-1].id
        return self
