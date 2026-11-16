"""`first-admin export` and `--no-prune` plan/apply against a mock client.

The DB-backed behaviour through the real server lives in test_resource_apply.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from first_common.errors import InvalidSpecError
from first_common.schema.resources import (
    LiveManifests,
    ResourceChangePlan,
    ResourceManifest,
)
from first_gateway.admin import cli as cli_module
from first_gateway.admin.cli import cli, load_resources_from_yaml

SPECS = Path(__file__).parent / "resource_specs"
runner = CliRunner()


@pytest.fixture
def live() -> list[ResourceManifest]:
    return load_resources_from_yaml(SPECS / "baseline")


@pytest.fixture
def client(live: list[ResourceManifest], monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    client = MagicMock()
    client.manifests.return_value = LiveManifests(config_version=7, resources=live)
    client.plan.return_value = ResourceChangePlan(
        previous_version=7, no_change=[], to_add=[], to_update=[], to_delete=[]
    )
    monkeypatch.setattr(cli_module, "AdminClient", lambda *_: client)
    return client


def _export(tmp_path: Path, *patterns: str) -> list[ResourceManifest]:
    result = runner.invoke(cli, ["export", *patterns], catch_exceptions=False)
    assert result.exit_code == 0, result.output
    assert result.output.startswith("# ConfigVersion 7\n")
    (tmp_path / "export.yaml").write_text(result.output)
    return load_resources_from_yaml(tmp_path / "export.yaml")


def test_export_prints_reloadable_manifests(
    client: MagicMock, live: list[ResourceManifest], tmp_path: Path
) -> None:
    assert _export(tmp_path) == live


def test_export_filters_by_name_or_kind_glob(client: MagicMock, tmp_path: Path) -> None:
    exported = _export(tmp_path, "sophia/*", "Model.*")
    assert [f"{r.kind}.{r.name}" for r in exported] == [
        "Model.meta-llama/llama-3-8b",
        "StaticDeployment.sophia/static/llama-3-8b",
        "PilotDeployment.sophia/pilot/llama-3-8b",
    ]


@pytest.mark.parametrize(("flag", "prune"), [([], True), (["--no-prune"], False)])
def test_plan_and_apply_pass_prune_through(
    client: MagicMock, live: list[ResourceManifest], flag: list[str], prune: bool
) -> None:
    spec = str(SPECS / "baseline" / "spec.yaml")  # a single file works too
    for command in ("plan", "apply"):
        result = runner.invoke(cli, [command, spec, *flag], catch_exceptions=False)
        assert result.exit_code == 0, result.output
        client.plan.assert_called_with(live, prune)


def test_missing_spec_path_raises(tmp_path: Path) -> None:
    with pytest.raises(InvalidSpecError, match="does not exist"):
        load_resources_from_yaml(tmp_path / "typo")


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_prune_refuses_empty_spec(
    client: MagicMock, tmp_path: Path, command: str
) -> None:
    result = runner.invoke(cli, [command, str(tmp_path)])
    assert isinstance(result.exception, InvalidSpecError)
    assert "entire live catalog" in str(result.exception)
    client.plan.assert_not_called()

    result = runner.invoke(cli, [command, str(tmp_path), "--no-prune"])
    assert result.exit_code == 0, result.output
    client.plan.assert_called_with([], False)


@pytest.mark.parametrize("command", ["plan", "apply"])
def test_prune_refuses_partial_export(
    client: MagicMock, tmp_path: Path, command: str
) -> None:
    exported = _export(tmp_path, "sophia/*")
    spec = str(tmp_path / "export.yaml")
    assert "partial export" in (tmp_path / "export.yaml").read_text()

    result = runner.invoke(cli, [command, spec])
    assert isinstance(result.exception, InvalidSpecError)
    assert "--no-prune" in str(result.exception)
    client.plan.assert_not_called()

    result = runner.invoke(cli, [command, spec, "--no-prune"])
    assert result.exit_code == 0, result.output
    client.plan.assert_called_with(exported, False)


def test_full_export_is_not_partial(client: MagicMock, tmp_path: Path) -> None:
    _export(tmp_path)
    assert "partial export" not in (tmp_path / "export.yaml").read_text()
    result = runner.invoke(cli, ["plan", str(tmp_path / "export.yaml")])
    assert result.exit_code == 0, result.output
