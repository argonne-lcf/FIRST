from typing import Any, NamedTuple

from pydantic import (
    BaseModel,
    ConfigDict,
    SerializeAsAny,
    model_validator,
)

from ..types import (
    ResourceName,
)
from .spec import ResourceSpec


class ResourceManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: str
    name: ResourceName
    spec: SerializeAsAny[ResourceSpec]

    @model_validator(mode="before")
    @classmethod
    def _resolve(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        kind = data.get("kind")
        if not kind:
            raise ValueError("resource kind must be specified")

        spec_cls = ResourceSpec.registry.get(kind)
        if spec_cls is None:
            raise ValueError(
                f"unknown resource kind {kind!r}; "
                f"known: {sorted(ResourceSpec.registry)}"
            )

        return {**data, "spec": spec_cls.model_validate(data.get("spec"))}


class ResourceRef(BaseModel):
    kind: str
    name: str


class FieldChange(NamedTuple):
    old: Any
    new: Any


class ResourcePatch(BaseModel):
    kind: str
    name: str
    patch: dict[str, FieldChange]


class ResourceChangePlan(BaseModel):
    previous_version: int
    no_change: list[ResourceRef]
    to_delete: list[ResourceRef]
    to_add: SerializeAsAny[list[ResourceManifest]]
    to_update: list[ResourcePatch]
