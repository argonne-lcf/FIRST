from typing import Any, NamedTuple

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializeAsAny,
    model_validator,
)

from ..types import (
    ResourceName,
)
from .spec import ResourceSpec


class ResourceManifest(BaseModel):
    """
    Validator of declarative YAML resource specs.

    `kind` identifies a specific ResourceSpec subclass which is used to validate
    the content of `spec` dynamically.

    `name` is the unique identifier of each resource.  Names may contain
    alphanumerics (a-z, A-Z, 0-9) and ._-/.  The special characters are
    deliberately chosen so that name:slug mapping is 1:1 (bijective).
    """

    model_config = ConfigDict(extra="forbid")
    kind: str
    name: ResourceName = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[a-zA-Z0-9._\-/]+$",
        examples=["meta-llama/Meta-Llama-3.1-8B"],
    )
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
    """
    Unique Resource Identifier
    """

    kind: str
    name: str


class FieldChange(NamedTuple):
    """
    Represents a diff (old) -> (new)
    """

    old: Any
    new: Any


class ResourcePatch(ResourceRef):
    """
    A resource update action: a specific (kind, name) has attributes in `patch`
    updated.
    """

    patch: dict[str, FieldChange]


class ResourceChangePlan(BaseModel):
    """
    A complete terraform-style change plan: what resources are being added,
    updated, deleted relative to the previous declarative version.
    """

    previous_version: int
    no_change: list[ResourceRef]
    to_delete: list[ResourceRef]
    to_add: SerializeAsAny[list[ResourceManifest]]
    to_update: list[ResourcePatch]
