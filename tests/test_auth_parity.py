import io
import json
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/check-auth-parity.py"
parity = runpy.run_path(str(SCRIPT))

V1 = {
    "GLOBUS_POLICIES": "policy-b; policy-a",
    "GLOBUS_GROUPS": "",
    "AUTHORIZED_IDP_DOMAINS": "anl.gov\nuchicago.edu",
    "AUTHORIZED_GROUPS_PER_IDP": '{"uchicago.edu": "group-b, group-a"}',
    "AUTHORIZED_GLOBUS_SERVICE_USERNAMES": '["client-b", "client-a"]',
}
V2 = {
    "FIRST_GLOBUS__POLICIES": '["policy-a", "policy-b"]',
    "FIRST_GLOBUS__USER_GROUPS": "[]",
    "FIRST_GLOBUS__AUTHORIZED_IDP_DOMAINS": '["uchicago.edu", "anl.gov"]',
    "FIRST_GLOBUS__AUTHORIZED_GROUPS_PER_IDP": (
        '{"uchicago.edu": ["group-a", "group-b"]}'
    ),
    "FIRST_GLOBUS__AUTHORIZED_SERVICE_USERNAMES": '["client-a", "client-b"]',
}


def _compose(env: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "services": {"inference-gateway": {"environment": V2 if env is None else env}}
    }


def test_equivalent_v1_v2_encodings() -> None:
    assert parity["v1_gates"](V1) == parity["v2_gates"](_compose(), "inference-gateway")


def test_defaults_match_for_optional_gates() -> None:
    old = {
        k: v
        for k, v in V1.items()
        if k in {"GLOBUS_POLICIES", "AUTHORIZED_IDP_DOMAINS"}
    }
    new = {
        k: v
        for k, v in V2.items()
        if k.endswith(("__POLICIES", "__AUTHORIZED_IDP_DOMAINS"))
    }
    assert parity["v1_gates"](old) == parity["v2_gates"](
        _compose(new), "inference-gateway"
    )


def test_parent_json_and_nested_overrides() -> None:
    parent = {
        key.removeprefix("FIRST_GLOBUS__").lower(): json.loads(value)
        for key, value in V2.items()
    }
    env = {
        "FIRST_GLOBUS": json.dumps(parent),
        "first_globus__user_groups": '["nested"]',
    }
    gates = parity["v2_gates"](_compose(env), "inference-gateway")
    assert gates["user_groups"] == ["nested"]
    assert gates["policies"] == ["policy-a", "policy-b"]


def test_deeper_nested_overrides_fail_instead_of_false_parity(tmp_path: Path) -> None:
    env = dict(V2)
    env["FIRST_GLOBUS__AUTHORIZED_GROUPS_PER_IDP__uchicago.edu"] = '["private-group"]'
    result = _run(tmp_path, env)
    assert result.returncode == 2
    assert result.stderr.strip() == (
        "error: invalid V2 gate FIRST_GLOBUS__AUTHORIZED_GROUPS_PER_IDP"
    )


def test_dotenv_parsing_is_not_shell_execution_or_interpolation() -> None:
    stream = io.StringIO(
        "export KEY='first\nsecond'\nSECRET=$(touch /never-execute)\nOTHER=${SECRET}\n"
    )
    assert parity["read_dotenv"](stream) == {
        "KEY": "first\nsecond",
        "SECRET": "$(touch /never-execute)",
        "OTHER": "${SECRET}",
    }


def _run(
    tmp_path: Path, env: dict[str, Any], *, stdin: bool = False
) -> subprocess.CompletedProcess[str]:
    old = tmp_path / "v1.env"
    old.write_text("\n".join(f"{key}='{value}'" for key, value in V1.items()))
    new = tmp_path / "compose.json"
    raw = json.dumps(_compose(env))
    new.write_text(raw)
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--v1-env",
            str(old),
            "--v2-compose",
            "-" if stdin else str(new),
        ],
        input=raw if stdin else None,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("stdin", [False, True])
def test_cli_success(tmp_path: Path, stdin: bool) -> None:
    result = _run(tmp_path, V2, stdin=stdin)
    assert result.returncode == 0
    assert "No differences" in result.stdout
    assert not result.stderr


@pytest.mark.parametrize("key", list(V2))
def test_cli_reports_each_gate_difference_without_values(
    tmp_path: Path, key: str
) -> None:
    env = dict(V2)
    value: Any = (
        {"private.edu": ["private-group"]}
        if key.endswith("GROUPS_PER_IDP")
        else ["private-value"]
    )
    env[key] = json.dumps(value)
    env["FIRST_GLOBUS__APP_SECRET"] = "never-print-secret"
    result = _run(tmp_path, env)
    assert result.returncode == 1
    assert (
        result.stdout.strip() == f"DIFF: {key.removeprefix('FIRST_GLOBUS__').lower()}"
    )
    assert "private" not in result.stdout + result.stderr
    assert "never-print-secret" not in result.stdout + result.stderr


@pytest.mark.parametrize("value", ["bad-private-json", "null", "42", '["ok", 1]', "[]"])
def test_invalid_v2_gate_is_safe_error(tmp_path: Path, value: str) -> None:
    env = dict(V2, FIRST_GLOBUS__POLICIES=value)
    result = _run(tmp_path, env)
    assert result.returncode == 2
    assert result.stderr.strip() == "error: invalid V2 gate FIRST_GLOBUS__POLICIES"
    assert "bad-private-json" not in result.stderr


@pytest.mark.parametrize("key", list(V1))
def test_invalid_v1_gate_is_safe_error(key: str) -> None:
    env = dict(V1)
    env[key] = "${UNRESOLVED_SECRET}"
    with pytest.raises(ValueError, match=f"^invalid V1 gate {key}$"):
        parity["v1_gates"](env)


@pytest.mark.parametrize(
    "compose",
    [{}, {"services": {}}, {"services": {"inference-gateway": {"environment": []}}}],
)
def test_missing_or_invalid_service(compose: Any) -> None:
    with pytest.raises(
        ValueError, match="missing or invalid Compose service environment"
    ):
        parity["v2_gates"](compose, "inference-gateway")


def test_invalid_dotenv_is_safe_error() -> None:
    with pytest.raises(ValueError, match="^invalid V1 dotenv syntax$"):
        parity["read_dotenv"](io.StringIO("SECRET='unclosed-private-value"))


def test_cli_invalid_json_and_missing_input(tmp_path: Path) -> None:
    old = tmp_path / "v1.env"
    old.write_text("\n".join(f"{key}='{value}'" for key, value in V1.items()))
    args = [sys.executable, str(SCRIPT), "--v1-env", str(old), "--v2-compose", "-"]
    result = subprocess.run(
        args, input="secret-not-json", capture_output=True, text=True
    )
    assert result.returncode == 2
    assert result.stderr.strip() == "error: invalid Compose JSON"
    old.unlink()
    result = subprocess.run(args, input="{}", capture_output=True, text=True)
    assert result.returncode == 2
    assert result.stderr.strip() == "error: unable to read parity input"
