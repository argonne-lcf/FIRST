import logging
from dataclasses import dataclass
from pathlib import Path

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty, pretty_repr
from rich.table import Table
from rich.text import Text
from yaml import safe_load_all

from first_common.errors import InvalidSpecError
from first_common.schema.resources import (
    ConfigVersion,
    ResourceChangePlan,
    ResourceManifest,
)

from ._context import get_client

cli = typer.Typer(no_args_is_help=True)
logger = logging.getLogger(__name__)


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
                old_repr = repr(change.old)
                new_repr = repr(change.new)

                line = Text()
                line.append("        ")
                line.append(f"{field}: ", style="bold")
                line.append(old_repr, style="red strike")
                line.append(" → ", style="dim")
                line.append(new_repr, style="green")
                console.print(line)

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
    client = get_client(ctx)
    resources = load_resources_from_yaml(spec_dir)
    result = client.admin.plan(resources)
    print_plan(result)


@cli.command()
def apply(ctx: typer.Context, spec_dir: Path) -> None:
    client = get_client(ctx)
    console = Console()
    resources = load_resources_from_yaml(spec_dir)
    plan = client.admin.plan(resources)
    print_plan(plan)

    if not (plan.to_add or plan.to_update or plan.to_delete):
        return

    if not typer.confirm("Apply these changes?"):
        return

    result = client.admin.apply(resources, plan)
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
    client = get_client(ctx)
    console = Console()
    versions = client.admin.list_config_versions()

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
    client = get_client(ctx)
    version = client.admin.get_config_version(uid)
    print_config_version(version)


@cli.command(name="reconcile-reset")
def reconcile_reset(ctx: typer.Context, resource: str) -> None:
    """Reset reconcile backoff state for a resource (e.g. 'PilotJob.my-job')"""
    client = get_client(ctx)
    client.admin.reconcile_reset(resource)
    Console().print(f"[bold green]Reconcile state reset for {resource}.[/]")


@cli.command()
def set_desired_replicas(
    ctx: typer.Context, deployment_name: str, num_replicas: int
) -> None:
    """Manually scale the number of replicas in a PilotDeployment"""
    client = get_client(ctx)
    deployment = client.admin.set_desired_pilot_deployment_replicas(
        deployment_name, num_replicas
    )
    Console().print(Pretty(deployment.model_dump(mode="json")))
