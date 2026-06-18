import logging
from pathlib import Path

import typer
from pydantic import ValidationError
from rich.console import Console
from rich.panel import Panel
from rich.pretty import pretty_repr
from rich.text import Text
from yaml import safe_load_all

from first_common.errors import InvalidSpecError
from first_common.schema.resources import ResourceChangePlan, ResourceManifest

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


def print_plan(plan: ResourceChangePlan) -> None:
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
                "[bold green]No changes.[/] Infrastructure is up-to-date.",
                title="Plan",
                border_style="green",
            )
        )
        console.print(f"  [dim]{n_nop} unchanged[/]")
        console.print()
        return

    # Summary Banner
    parts: list[str] = []
    if n_add:
        parts.append(f"[bold green]+{n_add} to add[/]")
    if n_upd:
        parts.append(f"[bold yellow]~{n_upd} to update[/]")
    if n_del:
        parts.append(f"[bold red]-{n_del} to delete[/]")
    if n_nop:
        parts.append(f"[dim]{n_nop} unchanged[/]")

    console.print()
    console.print(Panel(", ".join(parts), title="Plan summary", border_style="bold"))

    # Additions
    if plan.to_add:
        console.print()
        console.print("[bold green]  + Resources to add[/]")
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
        console.print("[bold yellow]  ~ Resources to update[/]")
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
        console.print("[bold red]  - Resources to delete[/]")
        console.print()
        for r in plan.to_delete:
            rid = f"{r.kind}.{r.name}"
            console.print(f"    [red]-[/] [bold]{rid}[/]")

    console.print()


@cli.command()
def plan(spec_dir: Path) -> None:
    from ..cli import _cli_state

    resources = load_resources_from_yaml(spec_dir)
    client = _cli_state["client"]
    result = client.admin.plan_resources(resources)
    print_plan(result)


@cli.command()
def apply(spec_dir: Path) -> None:
    from ..cli import _cli_state

    console = Console()
    resources = load_resources_from_yaml(spec_dir)
    client = _cli_state["client"]
    plan = client.admin.plan_resources(resources)
    print_plan(plan)

    if not (plan.to_add or plan.to_update or plan.to_delete):
        return

    if not typer.confirm("Apply these changes?"):
        return

    result = client.admin.apply_resources(resources, plan)
    if result:
        console.print(
            f"\n[bold green]Applied ConfigVersion {result.uid} successfully.\n"
        )
    else:
        console.print("\nUnexpectedly, there was no ConfigVersion change.")
