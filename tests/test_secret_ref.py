"""SecretRef may only resolve upstream-key variables; keys require https."""

from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from first_common.schema.resources.spec import StaticDeploymentSpec
from first_common.schema.types import SecretRef

_ALLOWED = "FIRST_UPSTREAM_KEY_TEST_SECRET_REF"
_adapter = TypeAdapter(SecretRef)


def test_allowed_prefix_validates_and_resolves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(_ALLOWED, "sentinel")
    ref = _adapter.validate_python(f"env_var://{_ALLOWED}")
    assert ref.resolve().get_secret_value() == "sentinel"


@pytest.mark.parametrize(
    "name",
    [
        "FIRST_DB_URL",
        "FIRST_GLOBUS__APP_SECRET",
        "METIS_API_KEY",
        "FIRST_UPSTREAM_KEY_",
        "first_upstream_key_lower",
        "FIRST_UPSTREAM_KEY_X\nFIRST_DB_URL",
    ],
)
def test_other_names_fail_validation_and_resolution(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv("FIRST_DB_URL", "postgresql://sentinel")
    monkeypatch.setenv("METIS_API_KEY", "sentinel")
    with pytest.raises(ValidationError, match="FIRST_UPSTREAM_KEY_"):
        _adapter.validate_python(f"env_var://{name}")
    # A stored ref bypasses validation; resolving it must still refuse
    with pytest.raises(ValueError, match="FIRST_UPSTREAM_KEY_"):
        SecretRef(f"env_var://{name}").resolve()


def _static(**overrides: Any) -> dict[str, Any]:
    return {
        "cluster_name": "metis",
        "model_name": "m",
        "api_url": "https://metis.example/v1",
        "api_key": f"env_var://{_ALLOWED}",
        "upstream_model_name": "m",
        "health_check": {
            "url": "https://metis.example/health",
            "api_key": f"env_var://{_ALLOWED}",
        },
        **overrides,
    }


def test_static_deployment_keyed_https_ok() -> None:
    StaticDeploymentSpec.model_validate(_static())


def test_static_deployment_rejects_key_over_http() -> None:
    with pytest.raises(ValidationError, match="api_url must use https"):
        StaticDeploymentSpec.model_validate(_static(api_url="http://metis.example/v1"))


def test_static_deployment_rejects_health_key_over_http() -> None:
    health = {"url": "http://metis.example/health", "api_key": f"env_var://{_ALLOWED}"}
    with pytest.raises(ValidationError, match="health_check.url must use https"):
        StaticDeploymentSpec.model_validate(_static(health_check=health))


def test_static_deployment_http_without_key_ok() -> None:
    StaticDeploymentSpec.model_validate(
        _static(
            api_url="http://localhost:8080/v1",
            api_key=None,
            health_check={"url": "http://localhost:8080/health"},
        )
    )
