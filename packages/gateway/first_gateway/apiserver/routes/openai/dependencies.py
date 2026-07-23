from typing import Annotated

from fastapi import Depends

from first_common.errors import InvalidSpecError, NotFound
from first_common.schema.endpoints.openai import BasePayload, OpenAIEndpoints

from ....database.redis.router_config import ModelConfig, RouterConfig
from ...auth import enforce_permission
from ...dependencies import AuthUser


async def resolve_model(
    *, router_config: RouterConfig, user: AuthUser, model_name: str, endpoint: str
) -> ModelConfig:
    model = router_config.models_by_name.get(model_name)
    if model is None:
        raise NotFound(f"Model {model_name} does not exist.")

    enforce_permission(user, model)

    if endpoint not in model.supported_endpoints:
        raise InvalidSpecError(f"Endpoint {endpoint} not supported for {model.name}.")
    return model


def model_dependency(endpoint: str):
    async def dependency(
        router_config: RouterConfig,
        user: AuthUser,
        payload: BasePayload,
    ) -> ModelConfig:
        return await resolve_model(
            router_config=router_config,
            user=user,
            model=payload.model,
            endpoint=endpoint,
        )

    return dependency


ChatCompletionsModel = Annotated[
    ModelConfig,
    Depends(model_dependency(OpenAIEndpoints.chat_completions.value)),
]

ResponsesModel = Annotated[
    ModelConfig,
    Depends(model_dependency(OpenAIEndpoints.responses.value)),
]

EmbeddingsModel = Annotated[
    ModelConfig,
    Depends(model_dependency(OpenAIEndpoints.embeddings.value)),
]
