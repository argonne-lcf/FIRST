from typing import Annotated

from fastapi import Depends, Request

from first_common.errors import InvalidSpecError, NotFound
from first_common.schema.endpoints.openai import BasePayload

from ....database.redis.router_config import ModelConfig
from ...auth import enforce_permission
from ...dependencies import AuthUser, RouterConfigDep


async def resolve_model(
    request: Request,
    router_config: RouterConfigDep,
    user: AuthUser,
    payload: BasePayload,
) -> ModelConfig:
    model = router_config.models_by_name.get(payload.model)
    if model is None:
        model = router_config.models_by_alias.get(payload.model)
        if model is None:
            raise NotFound(f"Model {payload.model} does not exist.")

    enforce_permission(user, model)

    endpoint = request.url.path.partition("/v1/")[2]
    if endpoint not in model.supported_endpoints:
        raise InvalidSpecError(f"Endpoint {endpoint} not supported for {model.name}.")
    return model


AuthorizedModel = Annotated[ModelConfig, Depends(resolve_model)]
