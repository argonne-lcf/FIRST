"""Native manifest configuration, encoded only at the existing script boundary."""

import json
import shlex
from copy import deepcopy
from typing import Any, Literal

import pytest
import yaml
from pydantic import ValidationError

from first_common.schema.resources.spec import (
    LaunchSpec,
    LaunchTemplateSpec,
    ScriptParameter,
)
from first_common.schema.types import (
    ResolvedLaunchSpec,
    RuntimeScriptContext,
    render_script,
)


def template(**kwargs: Any) -> LaunchTemplateSpec:
    return LaunchTemplateSpec.model_validate(
        {
            "parameters": {"configuration": {"type": "json", "required": True}},
            "serve_script_template": "serve --configuration {{ parameters.configuration | quote }}",
            "max_startup_sec": 600,
            "health_check": {"url": "/health"},
            **kwargs,
        }
    )


def launch(configuration: Any) -> LaunchSpec:
    return LaunchSpec(
        served_model_name="org/model",
        num_nodes=1,
        gpus_per_node=4,
        parameters={"configuration": configuration},
    )


def test_native_yaml_roundtrip_encodes_once_without_mutation() -> None:
    configuration = yaml.safe_load("""
parallelism: {tensor: 4, expert: false}
arguments: [--name, "Unicode ⌈x⌉; $(not-a-command)", "it's a value"]
optional: null
""")
    before = deepcopy(configuration)
    spec = launch(configuration)
    restored = LaunchSpec.model_validate_json(spec.model_dump_json())
    resolved = template().resolve(restored, 131072)
    encoded = resolved.parameters["configuration"]
    assert isinstance(encoded, str)
    assert json.loads(encoded) == configuration
    assert encoded == json.dumps(configuration, sort_keys=True, separators=(",", ":"))
    assert configuration == before
    # No pilot DTO expansion: the resolved request still contains scalar values.
    pilot = ResolvedLaunchSpec.model_validate_json(resolved.model_dump_json())
    runtime = RuntimeScriptContext(
        replica_name="tara/org/model/replica/one",
        served_model_name="org/model",
        uds="/tmp/replica.sock",
        gpus_per_node=4,
        num_nodes=1,
        gpus_by_host={"host": ["0", "1", "2", "3"]},
        env={},
        max_model_len=131072,
    )
    script = render_script(
        pilot.serve_script_template,
        {
            "runtime": runtime,
            "parameters": pilot.parameters,
            "quote": shlex.quote,
        },
    )
    assert shlex.split(script)[2] == encoded


def test_json_default_is_validated_and_resolved_not_mutated() -> None:
    default = {"enabled": True, "nested": [None, 3, "a\nb"]}
    value = deepcopy(default)
    t = template(parameters={"configuration": {"type": "json", "default": default}})
    encoded = t.resolve(launch(None), None).parameters["configuration"]
    assert isinstance(encoded, str)
    assert json.loads(encoded) == value
    assert default == value
    assert t.parameters["configuration"].default == value
    assert ScriptParameter(type="json").coerce("x", [True, None]) == "[true,null]"


@pytest.mark.parametrize("value", ["{}", 42, 1.5])
def test_json_parameters_require_structured_inputs(value: Any) -> None:
    with pytest.raises(ValueError, match="JSON object or array"):
        template().resolve(launch(value), None)


@pytest.mark.parametrize("value", [{"bad": float("nan")}, [float("inf")]])
def test_nonfinite_json_is_rejected(value: Any) -> None:
    with pytest.raises(ValueError, match="valid JSON"):
        template().resolve(launch(value), None)


@pytest.mark.parametrize("kind", ["str", "int", "float"])
@pytest.mark.parametrize("value", [{"x": 1}, [1]])
def test_structured_inputs_do_not_weaken_scalar_parameter_types(
    kind: Literal["str", "int", "float"], value: Any
) -> None:
    with pytest.raises(ValueError, match=f"must be {kind}"):
        ScriptParameter(type=kind).coerce("x", value)


def test_existing_scalar_validation_and_defaults() -> None:
    assert ScriptParameter(type="int", default=8).coerce("x", None) == 8
    assert ScriptParameter(type="float").coerce("x", 2) == 2
    assert ScriptParameter(type="str").coerce("x", "value") == "value"
    with pytest.raises(ValidationError):
        launch(True)
    with pytest.raises(ValidationError):
        ScriptParameter.model_validate(
            {"type": "json", "default": {"invalid": object()}}
        )
    with pytest.raises(ValueError, match="finite"):
        ScriptParameter(type="float").coerce("x", float("inf"))
