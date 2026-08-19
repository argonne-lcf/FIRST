import difflib
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import typer
from pydantic import ValidationError
from rich.console import Console
from rich.logging import RichHandler
from rich.padding import Padding
from rich.panel import Panel
from rich.pretty import Pretty, pretty_repr
from rich.table import Table
from rich.text import Text
from typer import Typer
from yaml import safe_load_all

from alcf_ai.cli import _print_error
from first_common.errors import FirstError, InvalidSpecError
from first_common.schema.resources import (
    ConfigVersion,
    ResourceChangePlan,
    ResourceManifest,
)

from .client import DEFAULT_BASE_URL, AdminClient

logger = logging.getLogger(__name__)
console = Console(stderr=True)

cli = Typer(no_args_is_help=True)


@cli.callback()
def _root(
    ctx: typer.Context,
    base_url: str | None = DEFAULT_BASE_URL,
    log_level: str = "INFO",
) -> None:
    """
    Inference Gateway CLI
    """
    logging.basicConfig(
        level=log_level,
        format="%(name)s:%(lineno)d %(message)s",
        handlers=[RichHandler(console=console)],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    client = AdminClient(base_url)
    ctx.obj = client
    logger.debug(f"Using client: {client.base_url}")


def format_validation_error(
    file: str | Path, kind: str, name: str, exc: ValidationError
) -> str:
    errors = []

    for err in exc.errors(include_url=False):
        location = ".".join(str(l) for l in err["loc"])
        message = err["msg"]
        errors.append(f" - {location}: {message}")

    return f"In {file} ({kind}.{name}):\n{'\n'.join(errors)}\n"


def load_resources_from_yaml(spec_dir: Path | str) -> list[ResourceManifest]:
    resources = []

    files = (
        f
        for ext in ("yml", "yaml")
        for f in Path(spec_dir).rglob(f"*.{ext}")
        if f.is_file()
    )

    errors = []

    for file in files:
        with file.open("r") as fp:
            try:
                raw_docs = list(safe_load_all(fp))
            except Exception as e:
                raise InvalidSpecError(f"Failed to load YAML {file}: {e}") from None

        for raw in raw_docs:
            if not raw:
                continue
            try:
                resource = ResourceManifest.model_validate(raw, extra="forbid")
            except ValidationError as exc:
                errors.append(
                    format_validation_error(file, raw.get("kind"), raw.get("name"), exc)
                )
            else:
                resources.append(resource)

    if errors:
        raise InvalidSpecError(
            "One or more resource specs were invalid.\n\n" + "\n".join(errors)
        )

    if not resources:
        logger.warning(f"No resources found in {spec_dir}")

    return resources


@dataclass
class _ChangeLabels:
    title: str
    no_changes_message: str
    add_summary: str  # "{n} to add" or "{n} added"
    update_summary: str
    delete_summary: str
    add_section: str  # "Resources to add" or "Added resources"
    update_section: str
    delete_section: str


PLAN_LABELS = _ChangeLabels(
    title="Plan",
    no_changes_message="[bold green]No changes.[/] Infrastructure is up-to-date.",
    add_summary="to add",
    update_summary="to update",
    delete_summary="to delete",
    add_section="Resources to add",
    update_section="Resources to update",
    delete_section="Resources to delete",
)

AUDIT_LABELS = _ChangeLabels(
    title="Changes",
    no_changes_message="[bold green]No changes recorded.[/]",
    add_summary="added",
    update_summary="updated",
    delete_summary="deleted",
    add_section="Added resources",
    update_section="Updated resources",
    delete_section="Deleted resources",
)


def _flatten_changes(
    old: Any, new: Any, prefix: str = ""
) -> list[tuple[str, Any, Any]]:
    """
    Recursively diff two JSON-like values, returning only the leaf paths that
    actually differ as ``(dotted_path, old_leaf, new_leaf)`` tuples.

    Nested dicts are descended into so that reordered-but-equal sub-objects and
    unchanged sibling keys produce no output. Lists and scalars are compared
    whole (a changed list is reported as a single leaf change).
    """
    if old == new:
        return []

    if isinstance(old, dict) and isinstance(new, dict):
        changes: list[tuple[str, Any, Any]] = []
        for key in dict.fromkeys([*old, *new]):  # preserve order, dedup
            path = f"{prefix}.{key}" if prefix else str(key)
            if key not in old:
                changes.append((path, _MISSING, new[key]))
            elif key not in new:
                changes.append((path, old[key], _MISSING))
            else:
                changes.extend(_flatten_changes(old[key], new[key], path))
        return changes

    return [(prefix, old, new)]


class _Missing:
    def __repr__(self) -> str:
        return "(absent)"


_MISSING = _Missing()

_WORD_RE = re.compile(r"\s+|\S+")


def _tokenize(text: str) -> list[str]:
    """Split into words and whitespace runs, keeping newlines visible."""
    return _WORD_RE.findall(text)


def _word_diff(old: str, new: str) -> Text:
    """Render a git-``--color-words``-style inline diff of two text blobs."""
    old_toks = _tokenize(old)
    new_toks = _tokenize(new)
    result = Text()
    matcher = difflib.SequenceMatcher(a=old_toks, b=new_toks, autojunk=False)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            result.append("".join(old_toks[i1:i2]), style="dim")
        else:
            if i1 != i2:
                result.append("".join(old_toks[i1:i2]), style="red strike")
            if j1 != j2:
                result.append("".join(new_toks[j1:j2]), style="green")
    return result


def _render_leaf_change(console: Console, path: str, old: Any, new: Any) -> None:
    """Print a single changed leaf, choosing an appropriate diff style."""
    is_multiline = (isinstance(old, str) and "\n" in old) or (
        isinstance(new, str) and "\n" in new
    )

    if is_multiline:
        console.print(f"        [bold]{path}[/]:")
        body = _word_diff(
            old if isinstance(old, str) else "",
            new if isinstance(new, str) else "",
        )
        console.print(Padding(body, (0, 0, 0, 12)))
        return

    line = Text()
    line.append("        ")
    line.append(f"{path}: ", style="bold")
    line.append(repr(old), style="red strike")
    line.append(" → ", style="dim")
    line.append(repr(new), style="green")
    console.print(line)


def print_plan(plan: ResourceChangePlan, labels: _ChangeLabels = PLAN_LABELS) -> None:
    """Print a terraform-plan-inspired summary of *plan* to *console*."""
    console = Console()

    n_add = len(plan.to_add)
    n_upd = len(plan.to_update)
    n_del = len(plan.to_delete)
    n_nop = len(plan.no_change)

    # No changes
    if n_add == 0 and n_upd == 0 and n_del == 0:
        console.print()
        console.print(
            Panel(
                labels.no_changes_message,
                title=labels.title,
                border_style="green",
            )
        )
        console.print(f"  [dim]{n_nop} unchanged[/]")
        console.print()
        return

    # Summary Banner
    parts: list[str] = []
    if n_add:
        parts.append(f"[bold green]+{n_add} {labels.add_summary}[/]")
    if n_upd:
        parts.append(f"[bold yellow]~{n_upd} {labels.update_summary}[/]")
    if n_del:
        parts.append(f"[bold red]-{n_del} {labels.delete_summary}[/]")
    if n_nop:
        parts.append(f"[dim]{n_nop} unchanged[/]")

    console.print()
    console.print(
        Panel(", ".join(parts), title=f"{labels.title} summary", border_style="bold")
    )

    # Additions
    if plan.to_add:
        console.print()
        console.print(f"[bold green]  + {labels.add_section}[/]")
        console.print()
        for res in plan.to_add:
            rid = f"{res.kind}.{res.name}"
            console.print(f"    [green]+[/] [bold]{rid}[/]")
            for field, value in res.model_dump(mode="json")["spec"].items():
                rendered = pretty_repr(value)
                lines = rendered.splitlines()
                # first line: "+ field = value_start"
                console.print(f"        [green]+[/] {field} = {lines[0]}")
                # continuation lines: align under the value
                pad = " " * (len(field) + 13)  # 8 spaces + "+ " + field + " = "
                for cont in lines[1:]:
                    console.print(f"{pad}{cont}")

    # Updates
    if plan.to_update:
        console.print()
        console.print(f"[bold yellow]  ~ {labels.update_section}[/]")
        console.print()
        for patch in plan.to_update:
            rid = f"{patch.kind}.{patch.name}"
            console.print(f"    [yellow]~[/] [bold]{rid}[/]")
            for field, change in patch.patch.items():
                leaves = _flatten_changes(change.old, change.new, field)
                for path, old, new in leaves:
                    _render_leaf_change(console, path, old, new)

    # Deletes
    if plan.to_delete:
        console.print()
        console.print(f"[bold red]  - {labels.delete_section}[/]")
        console.print()
        for r in plan.to_delete:
            rid = f"{r.kind}.{r.name}"
            console.print(f"    [red]-[/] [bold]{rid}[/]")

    console.print()


@cli.command()
def plan(ctx: typer.Context, spec_dir: Path) -> None:
    """
    Compare manifest to current state to review planned changes.
    """
    client: AdminClient = ctx.obj
    resources = load_resources_from_yaml(spec_dir)
    result = client.plan(resources)
    print_plan(result)


@cli.command()
def apply(ctx: typer.Context, spec_dir: Path) -> None:
    """
    Apply manifests to current state.
    """
    client: AdminClient = ctx.obj
    console = Console()
    resources = load_resources_from_yaml(spec_dir)
    plan = client.plan(resources)
    print_plan(plan)

    if not (plan.to_add or plan.to_update or plan.to_delete):
        return

    if not typer.confirm("Apply these changes?"):
        return

    result = client.apply(resources, plan)
    if result:
        console.print(
            f"\n[bold green]Applied ConfigVersion {result.uid} successfully.\n"
        )
    else:
        console.print("\nUnexpectedly, there was no ConfigVersion change.")


def print_config_version(version: ConfigVersion) -> None:
    """Print the details of a ConfigVersion, reusing the plan rendering."""
    console = Console()
    console.print()
    console.print(
        Panel(
            f"[bold]ConfigVersion {version.uid}[/]\n"
            f"applied_at: {version.applied_at.isoformat()}\n"
            f"applied_by: {version.applied_by}",
            title="ConfigVersion",
            border_style="bold",
        )
    )

    plan = ResourceChangePlan.model_validate(
        {**version.changes, "previous_version": version.uid - 1}
    )
    print_plan(plan, labels=AUDIT_LABELS)


@cli.command(name="audit")
def list_config_versions(ctx: typer.Context) -> None:
    """List all ConfigVersions (without the full changes payload)."""
    client: AdminClient = ctx.obj
    console = Console()
    versions = client.list_config_versions()

    table = Table(title="ConfigVersions")
    table.add_column("UID", justify="right", style="bold")
    table.add_column("Applied at")
    table.add_column("Applied by")

    for v in sorted(versions, key=lambda v: v.uid):
        table.add_row(str(v.uid), v.applied_at.isoformat(), v.applied_by)

    console.print(table)


@cli.command(name="audit-detail")
def get_config_version(ctx: typer.Context, uid: int) -> None:
    """Show the details of a single ConfigVersion, including its changes."""
    client: AdminClient = ctx.obj
    version = client.get_config_version(uid)
    print_config_version(version)


@cli.command(name="reconcile-reset")
def reconcile_reset(ctx: typer.Context, resource: str) -> None:
    """Reset reconcile backoff state for a resource (e.g. 'PilotJob.my-job')"""
    client: AdminClient = ctx.obj
    client.reconcile_reset(resource)
    Console().print(f"[bold green]Reconcile state reset for {resource}.[/]")


@cli.command()
def set_desired_replicas(
    ctx: typer.Context, deployment_name: str, num_replicas: int
) -> None:
    """Manually scale the number of replicas in a PilotDeployment"""
    client: AdminClient = ctx.obj
    deployment = client.set_desired_pilot_deployment_replicas(
        deployment_name, num_replicas
    )
    Console().print(Pretty(deployment.model_dump(mode="json")))


def main() -> None:
    try:
        cli()
    except FirstError as exc:
        _print_error(f"Error ({exc.status_code})", str(exc), info=exc.info or None)
        sys.exit(1)
    except httpx.HTTPError as exc:
        _print_error("HTTP Error", f"{type(exc).__name__}: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
