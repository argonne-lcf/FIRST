"""Reusable launch templates; resolved before a request reaches a pilot."""

import math
import shlex
from typing import Literal, Self, TypeAlias

from jinja2 import Environment, TemplateSyntaxError, meta, nodes
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)

from .types import HealthCheckParams, PilotLaunchSpec, RuntimeScriptContext

ParameterValue: TypeAlias = StrictStr | StrictInt | StrictFloat | None


class ScriptParameter(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["str", "int", "float", "string", "integer", "number", "path"]
    required: bool = False
    default: ParameterValue = None
    capability: str | None = Field(default=None, min_length=1)
    minimum: float | None = Field(default=None, allow_inf_nan=False)
    maximum: float | None = Field(default=None, allow_inf_nan=False)
    min_length: int | None = Field(default=None, ge=0)
    max_length: int | None = Field(default=None, ge=0)

    def validate_value(self, value: ParameterValue) -> ParameterValue:
        if value is None:
            if self.required:
                raise ValueError("required parameter cannot be null")
            return None
        if self.type in {"str", "string", "path"}:
            if not isinstance(value, str) or "\x00" in value:
                raise ValueError("expected a string without NUL characters")
            if self.type == "path" and not value:
                raise ValueError("path cannot be empty")
            if self.min_length is not None and len(value) < self.min_length:
                raise ValueError(f"length must be >= {self.min_length}")
            if self.max_length is not None and len(value) > self.max_length:
                raise ValueError(f"length must be <= {self.max_length}")
        else:
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or (self.type in {"int", "integer"} and not isinstance(value, int))
                or (isinstance(value, float) and not math.isfinite(value))
            ):
                raise ValueError(f"expected a finite {self.type}")
            if self.minimum is not None and value < self.minimum:
                raise ValueError(f"value must be >= {self.minimum}")
            if self.maximum is not None and value > self.maximum:
                raise ValueError(f"value must be <= {self.maximum}")
        return value

    @model_validator(mode="after")
    def check_definition(self) -> Self:
        string = self.type in {"str", "string", "path"}
        if string and (self.minimum is not None or self.maximum is not None):
            raise ValueError("numeric bounds require a numeric parameter")
        if not string and (self.min_length is not None or self.max_length is not None):
            raise ValueError("length bounds require a string parameter")
        for lower, upper in (
            (self.minimum, self.maximum),
            (self.min_length, self.max_length),
        ):
            if lower is not None and upper is not None and lower > upper:
                raise ValueError("minimum cannot exceed maximum")
        if self.default is not None:
            self.validate_value(self.default)
        return self


class ProfileLaunchSpec(BaseModel):
    """Per-deployment inputs; null overrides inherit the profile's defaults."""

    model_config = ConfigDict(extra="forbid")
    served_model_name: str
    gpus_per_node: int = Field(gt=0)
    num_nodes: int = Field(gt=0)
    parameters: dict[str, ParameterValue] = {}
    env: dict[str, str] = {}
    max_startup_sec: int | None = Field(default=None, gt=0)
    pre_stop_timeout_sec: float | None = Field(default=None, gt=0, le=25.0)
    post_stop_timeout_sec: float | None = Field(default=None, gt=0, le=50.0)
    max_unhealthy_sec: int | None = Field(default=None, gt=0)
    health_check: HealthCheckParams | None = None


class LaunchProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)

    parameters: dict[str, ScriptParameter] = {}
    env: dict[str, str] = {}
    serve_script_template: str = Field(min_length=1)
    pre_stop_script_template: str | None = None
    post_stop_script_template: str | None = None
    max_startup_sec: int = Field(gt=0)
    pre_stop_timeout_sec: float = Field(default=20.0, gt=0, le=25.0)
    post_stop_timeout_sec: float = Field(default=50.0, gt=0, le=50.0)
    max_unhealthy_sec: int | None = Field(default=None, gt=0)
    health_check: HealthCheckParams

    @model_validator(mode="after")
    def check_templates(self) -> Self:
        capabilities = [p.capability for p in self.parameters.values() if p.capability]
        if len(capabilities) != len(set(capabilities)):
            raise ValueError("a capability can only map to one parameter")
        env = Environment()
        env.filters["quote"] = shlex.quote
        for template in (
            self.serve_script_template,
            self.pre_stop_script_template,
            self.post_stop_script_template,
        ):
            if template is None:
                continue
            try:
                ast = env.parse(template)
                unknown = meta.find_undeclared_variables(ast) - {
                    "runtime",
                    "parameters",
                    "quote",
                }
                if unknown:
                    raise ValueError(f"unknown template variables: {sorted(unknown)}")
                for node in ast.find_all((nodes.Getattr, nodes.Getitem)):
                    assert isinstance(node, (nodes.Getattr, nodes.Getitem))
                    if not isinstance(node.node, nodes.Name):
                        continue
                    key = (
                        node.attr
                        if isinstance(node, nodes.Getattr)
                        else (
                            node.arg.value
                            if isinstance(node.arg, nodes.Const)
                            else None
                        )
                    )
                    allowed = {
                        "parameters": self.parameters,
                        "runtime": RuntimeScriptContext.__annotations__,
                    }.get(node.node.name)
                    if allowed is not None and key is not None and key not in allowed:
                        raise ValueError(f"unknown {node.node.name} field: {key!r}")
            except TemplateSyntaxError as exc:
                raise ValueError(f"script template is not valid Jinja2: {exc}") from exc
        return self

    def resolve_parameters(
        self, supplied: dict[str, ParameterValue]
    ) -> dict[str, ParameterValue]:
        unknown = supplied.keys() - self.parameters.keys()
        if unknown:
            raise ValueError(f"unknown launch parameters: {sorted(unknown)}")
        result = {}
        for name, definition in self.parameters.items():
            try:
                result[name] = definition.validate_value(
                    supplied.get(name, definition.default)
                )
            except ValueError as exc:
                raise ValueError(f"parameter {name!r}: {exc}") from exc
        return result

    def resolve(self, launch: ProfileLaunchSpec) -> PilotLaunchSpec:
        values = self.model_dump(exclude={"parameters"})
        values.update(
            launch.model_dump(exclude_none=True, exclude={"parameters", "env"})
        )
        values["env"] = self.env | launch.env
        values["parameters"] = self.resolve_parameters(launch.parameters)
        return PilotLaunchSpec.model_validate(values)

    def get_capabilities(self, launch: ProfileLaunchSpec) -> dict[str, ParameterValue]:
        values = self.resolve_parameters(launch.parameters)
        return {
            parameter.capability: values[name]
            for name, parameter in self.parameters.items()
            if parameter.capability is not None and values[name] is not None
        }
