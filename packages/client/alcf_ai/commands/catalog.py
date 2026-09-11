"""
Catalog commands: models, deployments, clusters, access-groups.
"""

from typing import Annotated

import typer
from rich.panel import Panel
from rich.pretty import Pretty
from rich.table import Table

from first_common.errors import NotFound
from first_common.schema.resources.read import (
    ModelSummary,
    PilotDeploymentDetail,
    StaticDeploymentDetail,
)

from ..inference import StartupStatus
from ._shared import (
    JsonFlag,
    console,
    countdown,
    endpoint_list,
    err_console,
    fmt_age,
    fmt_int,
    fmt_seconds,
    get_client,
    model_state,
    print_json,
    ready_deployments,
    styled_state,
)

models_cli = typer.Typer(no_args_is_help=True)
deployments_cli = typer.Typer(no_args_is_help=True)
clusters_cli = typer.Typer(no_args_is_help=True)
access_groups_cli = typer.Typer(no_args_is_help=True)


# --- models -------------------------------------------------------------------------


def _deployments_table(model: ModelSummary) -> Table:
    table = Table(title="Deployments", title_justify="left")
    table.add_column("Name", style="bold")
    table.add_column("Kind")
    table.add_column("Cluster")
    table.add_column("State")
    table.add_column("Desired", justify="right")
    table.add_column("Last startup", justify="right")
    table.add_column("Upstream model")
    for pd in model.pilot_deployments:
        table.add_row(
            pd.name,
            "pilot",
            pd.cluster_name,
            styled_state(pd.state.value),
            str(pd.desired_replicas),
            fmt_seconds(pd.last_startup_sec),
            "—",
        )
    for sd in model.static_deployments:
        table.add_row(
            sd.name,
            "static",
            sd.cluster_name,
            styled_state(sd.health.value),
            "—",
            "—",
            sd.upstream_model_name,
        )
    return table


def _maintenance(model: ModelSummary) -> str:
    notices = model.maintenance_notices.items()
    return "\n".join(f"{c}: {n}" for c, n in notices) or "—"


@models_cli.command("ls")
def models_ls(
    ctx: typer.Context,
    endpoint: Annotated[
        str | None,
        typer.Option("--endpoint", "-e", help="Only models supporting this endpoint."),
    ] = None,
    as_json: bool = JsonFlag,
) -> None:
    """List the Models you can use, with their endpoints and live state."""
    models = get_client(ctx).catalog.models()
    if endpoint:
        endpoint = endpoint.strip("/")
        models = [m for m in models if endpoint in m.supported_endpoints]
    if as_json:
        print_json(models)
        return

    table = Table(title="Models")
    table.add_column("Model", style="bold")
    table.add_column("Endpoints")
    table.add_column("Context", justify="right")
    table.add_column("State")
    table.add_column("Deployments", justify="right")
    table.add_column("In-flight", justify="right")
    table.add_column("Access group")
    table.add_column("Maintenance")

    for m in sorted(models, key=lambda m: m.name):
        ready, total = ready_deployments(m)
        table.add_row(
            m.name + (f"\n[dim]{', '.join(m.aliases)}[/]" if m.aliases else ""),
            endpoint_list(m.supported_endpoints),
            fmt_int(m.max_model_len),
            styled_state(model_state(m)),
            f"{ready}/{total}",
            str(m.runtime.total_inflight),
            m.access_group_name,
            _maintenance(m),
        )

    console.print(table)


@models_cli.command("get")
def models_get(ctx: typer.Context, name: str, as_json: bool = JsonFlag) -> None:
    """Show one Model in detail: endpoints, limits, capabilities, deployments."""
    m = get_client(ctx).catalog.get_model(name)
    if as_json:
        print_json(m)
        return

    limits = m.usage_limits.default
    facts = Table(show_header=False, box=None, pad_edge=False)
    facts.add_column(style="bold")
    facts.add_column()
    facts.add_row("Display name", m.display_name or "—")
    facts.add_row("Aliases", ", ".join(m.aliases) or "—")
    facts.add_row("Endpoints", endpoint_list(m.supported_endpoints))
    facts.add_row("Max context", fmt_int(m.max_model_len))
    facts.add_row("State", styled_state(model_state(m)))
    facts.add_row("In-flight requests", str(m.runtime.total_inflight))
    facts.add_row(
        "Capacity rejects",
        f"{m.runtime.capacity_rejects_total} "
        f"[dim](last {fmt_age(m.runtime.last_capacity_reject)})[/]",
    )
    facts.add_row("Access group", m.access_group_name)
    facts.add_row("Maintenance", _maintenance(m))
    facts.add_row(
        "Default limits",
        f"{fmt_int(limits.tpm)} tokens/min · {limits.rpm} req/min · "
        f"{limits.max_user_concurrency} concurrent",
    )
    facts.add_row("Created", fmt_age(m.created_at))
    console.print(Panel(facts, title=f"Model [bold]{m.name}[/]", title_align="left"))

    if m.capabilities:
        console.print(
            Panel(Pretty(m.capabilities), title="Capabilities", title_align="left")
        )
    console.print(_deployments_table(m))


@models_cli.command("warmup")
def models_warmup(
    ctx: typer.Context,
    name: str,
    endpoint: Annotated[
        str | None,
        typer.Option("--endpoint", "-e", help="Endpoint to probe (default: auto)."),
    ] = None,
    deployment: Annotated[
        str | None,
        typer.Option("--deployment", "-d", help="Pin the request to one deployment."),
    ] = None,
    timeout: Annotated[
        float, typer.Option("--timeout", help="Give up after this many seconds.")
    ] = 1800,
) -> None:
    """
    Request a cold start for a Model and wait until it is ready.

    Sends a minimal request, which registers demand with the autoscaler, then
    keeps polling and shows the gateway's startup ETA until a request succeeds.
    """
    client = get_client(ctx)
    api = client.deployment(deployment) if deployment else client.inference
    last: list[StartupStatus] = []

    with err_console.status(f"Requesting [bold]{name}[/]…") as live:

        def on_status(status: StartupStatus) -> None:
            last.append(status)

        def sleep(delay: float) -> None:
            countdown(live, name, last[-1], delay)

        elapsed = api.warmup(
            name, endpoint=endpoint, timeout=timeout, on_status=on_status, sleep=sleep
        )

    console.print(f"[bold green]{name}[/] is ready [dim](waited {elapsed:.0f}s)[/]")


# --- deployments -------------------------------------------------------------------


@deployments_cli.command("ls")
def deployments_ls(
    ctx: typer.Context,
    model: Annotated[
        str | None, typer.Option("--model", "-m", help="Only this Model's deployments.")
    ] = None,
    as_json: bool = JsonFlag,
) -> None:
    """List pilot and static deployments with their live state."""
    catalog = get_client(ctx).catalog
    pilots = catalog.pilot_deployments()
    statics = catalog.static_deployments()
    if model:
        pilots = [d for d in pilots if d.model_name == model]
        statics = [d for d in statics if d.model_name == model]
    if as_json:
        print_json([*pilots, *statics])
        return

    table = Table(title="Deployments")
    table.add_column("Name", style="bold")
    table.add_column("Kind")
    table.add_column("Model")
    table.add_column("Cluster")
    table.add_column("State")
    table.add_column("Desired", justify="right")
    table.add_column("Last startup", justify="right")
    table.add_column("In-flight", justify="right")

    for pd in sorted(pilots, key=lambda d: d.name):
        table.add_row(
            pd.name,
            "pilot",
            pd.model_name,
            pd.cluster_name,
            styled_state(pd.state.value),
            str(pd.desired_replicas),
            fmt_seconds(pd.last_startup_sec),
            "—",
        )
    for sd in sorted(statics, key=lambda d: d.name):
        table.add_row(
            sd.name,
            "static",
            sd.model_name,
            sd.cluster_name,
            styled_state(sd.health.value),
            "—",
            "—",
            str(sd.runtime.inflight),
        )
    console.print(table)


def _print_pilot_deployment(d: PilotDeploymentDetail) -> None:
    facts = Table(show_header=False, box=None, pad_edge=False)
    facts.add_column(style="bold")
    facts.add_column()
    facts.add_row("Model", d.model_name)
    facts.add_row("Cluster", d.cluster_name)
    facts.add_row("State", styled_state(d.state.value))
    facts.add_row("Desired replicas", str(d.desired_replicas))
    facts.add_row("Replica range", f"{d.min_replicas}–{d.max_replicas}")
    facts.add_row(
        "Autoscaling",
        d.scaling_strategy.strategy if d.scaling_strategy else "[dim]manual[/]",
    )
    facts.add_row("Launch template", d.launch_template_name)
    facts.add_row(
        "Shape",
        f"{d.launch_spec.num_nodes} node(s) × {d.launch_spec.gpus_per_node} GPU(s)",
    )
    facts.add_row("Last startup", fmt_seconds(d.last_startup_sec))
    facts.add_row("Launch failures", str(d.consecutive_launch_failures))
    if d.reconcile_last_error:
        facts.add_row("Reconcile error", f"[red]{d.reconcile_last_error}[/]")
    console.print(
        Panel(facts, title=f"PilotDeployment [bold]{d.name}[/]", title_align="left")
    )

    table = Table(title="Replicas", title_justify="left")
    table.add_column("Replica", style="bold")
    table.add_column("State")
    table.add_column("Pilot job")
    table.add_column("GPUs")
    table.add_column("In-flight", justify="right")
    table.add_column("Started")
    table.add_column("Message")
    for r in d.replicas:
        gpus = ", ".join(f"{c.hostname}:{','.join(c.gpu_ids)}" for c in r.resources)
        table.add_row(
            r.name,
            styled_state(r.state.value),
            r.pilot_job_name or "—",
            gpus or "—",
            str(r.runtime.inflight),
            fmt_age(r.started_at),
            r.state_message,
        )
    console.print(table)


def _print_static_deployment(d: StaticDeploymentDetail) -> None:
    facts = Table(show_header=False, box=None, pad_edge=False)
    facts.add_column(style="bold")
    facts.add_column()
    facts.add_row("Model", d.model_name)
    facts.add_row("Cluster", d.cluster_name)
    facts.add_row("Health", styled_state(d.health.value))
    facts.add_row("Upstream model", d.upstream_model_name)
    facts.add_row("In-flight requests", str(d.runtime.inflight))
    facts.add_row("Cooldown errors", str(d.runtime.cooldown_errors))
    if d.reconcile_last_error:
        facts.add_row("Reconcile error", f"[red]{d.reconcile_last_error}[/]")
    console.print(
        Panel(facts, title=f"StaticDeployment [bold]{d.name}[/]", title_align="left")
    )


@deployments_cli.command("get")
def deployments_get(ctx: typer.Context, name: str, as_json: bool = JsonFlag) -> None:
    """Show one deployment (pilot deployments include their replicas)."""
    catalog = get_client(ctx).catalog
    detail: PilotDeploymentDetail | StaticDeploymentDetail
    try:
        detail = catalog.get_pilot_deployment(name)
    except NotFound:
        static = next((d for d in catalog.static_deployments() if d.name == name), None)
        if static is None:
            raise NotFound(f"Deployment {name!r} not found (or not visible to you).")
        detail = static

    if as_json:
        print_json(detail)
    elif isinstance(detail, PilotDeploymentDetail):
        _print_pilot_deployment(detail)
    else:
        _print_static_deployment(detail)


@deployments_cli.command("logs")
def deployments_logs(ctx: typer.Context, replica: str) -> None:
    """Tail a PilotReplica's log by replica name or slug.  (Admin-only.)"""
    slug = replica.replace("/", "~")
    print(get_client(ctx).catalog.replica_logs(slug))


# --- clusters ----------------------------------------------------------------------


@clusters_cli.command("ls")
def clusters_ls(ctx: typer.Context, as_json: bool = JsonFlag) -> None:
    """List all configured Clusters."""
    clusters = get_client(ctx).catalog.clusters()
    if as_json:
        print_json(clusters)
        return

    table = Table(title="Clusters")
    table.add_column("Name", style="bold")
    table.add_column("Health")
    table.add_column("Pilot jobs")
    table.add_column("Maintenance notice")
    for c in sorted(clusters, key=lambda c: c.name):
        pilot = (
            f"{c.pilot_system.queue} · {c.pilot_system.max_num_nodes}×"
            f"{c.pilot_system.gpus_per_node} GPU"
            if c.pilot_system
            else "[dim]static only[/]"
        )
        table.add_row(
            c.name,
            styled_state(c.health.value),
            pilot,
            c.maintenance_notice or "—",
        )
    console.print(table)


@clusters_cli.command("get")
def clusters_get(ctx: typer.Context, name: str, as_json: bool = JsonFlag) -> None:
    """Show a Cluster with its pilot jobs.  (Admin-only.)"""
    c = get_client(ctx).catalog.get_cluster(name)
    if as_json:
        print_json(c)
        return

    console.print(
        Panel(
            f"Health: {styled_state(c.health.value)}\n"
            f"Maintenance: {c.maintenance_notice or '—'}",
            title=f"Cluster [bold]{c.name}[/]",
            title_align="left",
        )
    )
    table = Table(title="Pilot jobs", title_justify="left")
    table.add_column("Job", style="bold")
    table.add_column("Scheduler ID")
    table.add_column("State")
    table.add_column("Manager")
    table.add_column("Nodes", justify="right")
    table.add_column("Replicas", justify="right")
    table.add_column("Started")
    table.add_column("Idle since")
    for j in c.pilot_jobs:
        table.add_row(
            j.name,
            j.scheduler_job_id or "—",
            styled_state(j.scheduler_state.value),
            styled_state(j.manager_health.value),
            f"{j.num_nodes}×{j.gpus_per_node}",
            str(len(j.assigned_replicas)),
            fmt_age(j.time_started),
            fmt_age(j.idle_since),
        )
    console.print(table)


# --- access groups -----------------------------------------------------------------


@access_groups_cli.command("ls")
def access_groups_ls(ctx: typer.Context, as_json: bool = JsonFlag) -> None:
    """List the AccessGroups you qualify for."""
    groups = get_client(ctx).catalog.access_groups()
    if as_json:
        print_json(groups)
        return

    table = Table(title="AccessGroups")
    table.add_column("Name", style="bold")
    table.add_column("Allowed groups")
    table.add_column("Allowed domains")
    for g in groups:
        table.add_row(
            g.name,
            "\n".join(g.allowed_groups) or "[dim]everyone[/]",
            "\n".join(g.allowed_domains) or "—",
        )
    console.print(table)
