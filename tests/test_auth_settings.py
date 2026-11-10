import logging
from typing import Any

import pytest
from pydantic import ValidationError

from first_common.schema.resources.spec import AccessGroupSpec
from first_gateway.settings import GlobusAuthSettings


@pytest.mark.parametrize(
    "spec,missing",
    [
        ({}, {"allowed_groups", "allowed_domains"}),
        ({"allowed_groups": []}, {"allowed_domains"}),
        ({"allowed_domains": []}, {"allowed_groups"}),
    ],
)
def test_access_group_requires_explicit_gates(
    spec: dict[str, Any], missing: set[str]
) -> None:
    with pytest.raises(ValidationError) as exc:
        AccessGroupSpec.model_validate(spec)
    assert {error["loc"][0] for error in exc.value.errors()} == missing
    assert all(error["type"] == "missing" for error in exc.value.errors())


@pytest.mark.parametrize("field", ["allowed_groups", "allowed_domains"])
def test_access_group_rejects_null(field: str) -> None:
    spec: dict[str, Any] = {"allowed_groups": [], "allowed_domains": []}
    spec[field] = None
    with pytest.raises(ValidationError):
        AccessGroupSpec.model_validate(spec)


@pytest.mark.parametrize("groups,domains", [([], []), (["group"], ["anl.gov"])])
def test_access_group_accepts_explicit_lists(
    groups: list[str], domains: list[str]
) -> None:
    spec = AccessGroupSpec(allowed_groups=groups, allowed_domains=domains)
    assert spec.allowed_groups == groups
    assert spec.allowed_domains == domains


def _globus_settings() -> dict[str, Any]:
    return {
        "app_id": "test-app",
        "app_secret": "test-secret",
        "compute_client_id": "test-compute",
        "compute_client_secret": "test-compute-secret",
        "admin_group": "test-admin",
        "policies": ["test-policy"],
        "authorized_idp_domains": ["anl.gov"],
        "user_groups": [],
        "authorized_groups_per_idp": {},
        "authorized_service_usernames": [],
    }


@pytest.mark.parametrize("field", ["policies", "authorized_idp_domains"])
@pytest.mark.parametrize("omitted", [False, True])
def test_global_gates_reject_empty_or_missing(field: str, omitted: bool) -> None:
    settings = _globus_settings()
    if omitted:
        del settings[field]
    else:
        settings[field] = []
    with pytest.raises(ValidationError, match=f"{field} must not be empty"):
        GlobusAuthSettings.model_validate(settings)


def test_valid_global_gates_allow_empty_optional_restrictions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = GlobusAuthSettings.model_validate(_globus_settings())
    assert settings.policies == ["test-policy"]
    assert settings.authorized_idp_domains == ["anl.gov"]
    assert settings.user_groups == []
    assert settings.authorized_service_usernames == []
    assert not caplog.records


def test_unknown_idp_group_keys_warn_without_mutation(
    gateway_caplog: pytest.LogCaptureFixture,
) -> None:
    data = _globus_settings()
    groups = {"outside.edu": ["private-group-id"], "anl.gov": ["another-group"]}
    data["authorized_groups_per_idp"] = groups
    with gateway_caplog.at_level(logging.WARNING, logger="first_gateway.settings"):
        settings = GlobusAuthSettings.model_validate(data)
    assert settings.authorized_groups_per_idp == groups
    assert settings.authorized_idp_domains == ["anl.gov"]
    assert "outside.edu" in gateway_caplog.text
    assert "private-group-id" not in gateway_caplog.text
    assert "test-secret" not in gateway_caplog.text


def test_known_idp_group_keys_do_not_warn(
    gateway_caplog: pytest.LogCaptureFixture,
) -> None:
    data = _globus_settings()
    data["authorized_groups_per_idp"] = {"anl.gov": ["group"]}
    GlobusAuthSettings.model_validate(data)
    assert not gateway_caplog.records
