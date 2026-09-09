#!/usr/bin/env python

import argparse
import datetime
from collections.abc import Callable
from typing import Any

import polars as pl

# Import to trigger recipe registration
import first_stats_query.visualizations.cluster_stats  # noqa: F401
import first_stats_query.visualizations.inference_overview  # noqa: F401
import first_stats_query.visualizations.institution_stats  # noqa: F401
import first_stats_query.visualizations.status_code_stats  # noqa: F401
import first_stats_query.visualizations.token_breakdown  # noqa: F401

from .large_requests import pull_request
from .tables import CLUSTER_COL, STREAMS, TIMESTAMP_COL, generate_table
from .visualizations import get_recipes
from .visualizations.helpers import Recipe, filter_period

# Timestamp column of the request_metrics stream, the source of every time axis.
METRICS_TIME_COL = "timestamp_compute_request"


def load_data(
    dataset_dir: str, name: str, *, include_file_paths: str | None = None
) -> pl.LazyFrame:
    """Load a stream from ``<dataset_dir>/<stream>/``, which first-slogs
    fills with hive-partitioned parquet."""
    return pl.scan_parquet(
        f"{dataset_dir}/{name}",
        missing_columns="insert",
        extra_columns="ignore",
        include_file_paths=include_file_paths,
    )


def _parse_datetime(s: str) -> datetime.datetime:
    """Parse a UTC timestamp; accepts date-only or date + time."""
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.replace(tzinfo=datetime.timezone.utc)
        except ValueError:
            pass
    raise ValueError(
        f"invalid time: {s!r} (expected YYYY-MM-DD or YYYY-MM-DD HH:MM, UTC)"
    )


def _today_utc() -> datetime.datetime:
    """Start of today in UTC, used as the default window anchor."""
    return datetime.datetime.now(datetime.timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )


def _period_span(period: str) -> datetime.timedelta:
    """Fixed span back from today for a relative period."""
    match period:
        case "6m":
            return datetime.timedelta(days=183)
        case "1m":
            return datetime.timedelta(days=31)
        case _:
            raise ValueError(f"unknown period: {period!r}")


def _resolve_window(
    period: str | None, start: str | None, end: str | None
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Resolve an optional period preset to a concrete window, overridable with --start/--end."""
    if period is None:
        return (None, None)
    if period == "all":
        if start or end:
            raise SystemExit(
                "error: --period all cannot be combined with --start/--end; "
                "drop --period all to use them as a window"
            )
        return (None, None)
    today = _today_utc()
    delta = _period_span(period)
    start_dt = _parse_datetime(start) if start else today - delta
    end_dt = _parse_datetime(end) if end else today
    return (start_dt, end_dt)


def _resolve_window_start_end(
    args: argparse.Namespace,
) -> tuple[datetime.datetime | None, datetime.datetime | None]:
    """Resolve the time window from a subcommand's --period/--start/--end options."""
    if args.period:
        return _resolve_window(args.period, args.start, args.end)
    if bool(args.start) != bool(args.end):
        raise SystemExit(
            "error: --start and --end must be given together (or pick --period)"
        )
    if args.start and args.end:
        return (_parse_datetime(args.start), _parse_datetime(args.end))
    return (None, None)


def _add_window_args(
    parser: argparse.ArgumentParser, *, granularity: bool = False
) -> None:
    """Add shared window selection options to a subparser."""
    parser.add_argument(
        "--period",
        type=str,
        choices=["6m", "1m", "all"],
        default=None,
        help="Relative window preset from today (6m=last 6 months, 1m=last month, "
        "all=no filter); --start/--end override either end of the window "
        "(not with all)",
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start of time window (YYYY-MM-DD or YYYY-MM-DD HH:MM, UTC); "
        "overrides the preset start for --period, or pairs with --end for a custom range",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End of time window (YYYY-MM-DD or YYYY-MM-DD HH:MM, UTC); "
        "overrides the preset end for --period, or pairs with --start for a custom range",
    )
    parser.add_argument(
        "--cluster",
        type=str,
        default=None,
        help="Filter to a single cluster (e.g. minerva)",
    )
    if granularity:
        parser.add_argument(
            "--granularity",
            type=str,
            choices=["daily", "monthly", "yearly", "auto"],
            default=None,
            help="Time bucket for chart (auto uses period default)",
        )


class _ListRecipes(argparse.Action):
    """Print every registered category and recipe, then stop."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        _namespace: argparse.Namespace,
        _values: Any,
        _option_string: str | None = None,
    ) -> None:
        _print_recipes(get_recipes())
        parser.exit()


def _run_visualize(
    args: argparse.Namespace, recipes: dict[str, list[tuple[str, Recipe]]]
) -> None:
    """Run graph recipes under the given category/recipe choice."""
    start, end = _resolve_window_start_end(args)
    granularity = args.granularity or _infer_granularity(start, end)
    load_data_func = _make_load_data(args.dataset_dir, start, end, args.cluster)

    for name, func in recipes[args.category]:
        if args.recipe == "all" or args.recipe == name:
            func(load_data_func, start, end, granularity, args.cluster)
        if args.recipe == name:
            break


def _run_table(args: argparse.Namespace) -> None:
    """Print rows or column summations from an ingested stream."""
    start, end = _resolve_window_start_end(args)
    if args.cluster and CLUSTER_COL[args.stream] is None:
        raise SystemExit(f"error: the {args.stream!r} stream has no cluster column")
    if (start or end) and TIMESTAMP_COL[args.stream] is None:
        raise SystemExit(
            f"error: the {args.stream!r} stream has no timestamps to filter by"
        )
    columns = args.columns.split(",") if args.columns else None
    aggregate = args.aggregate.split(",") if args.aggregate else None
    filters = _parse_filters(args)

    def base_load(name: str) -> pl.LazyFrame:
        return load_data(args.dataset_dir, name)

    generate_table(
        base_load,
        args.stream,
        start,
        end,
        args.cluster,
        columns=columns,
        limit=args.limit,
        sort=args.sort,
        aggregate=aggregate,
        group=args.group,
        filters=filters,
    )


def _parse_filters(args: argparse.Namespace) -> dict[str, str] | None:
    """Parse repeated --filter COLUMN=value specs into a column-value map."""
    if not args.filter:
        return None
    filters: dict[str, str] = {}
    for spec in args.filter:
        name, sep, value = spec.partition("=")
        if not sep:
            raise SystemExit(f"error: --filter expects COLUMN=value, got {spec!r}")
        filters[name] = value
    return filters


def main() -> None:
    parser = argparse.ArgumentParser(description="Stats query CLI")
    parser.add_argument("dataset_dir", help="Path to the dataset directory")
    subparsers = parser.add_subparsers(dest="command", required=True)

    visualize = subparsers.add_parser(
        "visualize", help="Generate graph recipes from a dataset"
    )
    visualize.add_argument(
        "--help-categories",
        action=_ListRecipes,
        nargs=0,
        help="List available categories and recipes",
    )

    recipes = get_recipes()
    viz_cats = visualize.add_subparsers(dest="category", required=True)
    for cat, recipe_list in recipes.items():
        cat_parser = viz_cats.add_parser(cat)
        _add_window_args(cat_parser, granularity=True)
        cat_parser.add_argument(
            "recipe",
            nargs="?",
            choices=[name for name, _ in recipe_list] + ["all"],
            default="all",
        )

    table = subparsers.add_parser("table", help="Print rows from an ingested stream")
    _add_window_args(table)
    table.add_argument(
        "stream",
        choices=list(STREAMS),
        help="Stream to tabulate (request_metrics, request_log, access_log, "
        "batch_log, batch_metrics)",
    )
    table.add_argument(
        "--columns",
        type=str,
        default=None,
        help="Comma-separated columns to include in the table",
    )
    table.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of rows shown",
    )
    table.add_argument(
        "--sort",
        type=str,
        default=None,
        help="Sort by column; append ':desc' for descending order",
    )
    table.add_argument(
        "--filter",
        type=str,
        action="append",
        default=None,
        help="Filter rows by exact column value (COLUMN=value); repeatable",
    )
    table.add_argument(
        "--aggregate",
        type=str,
        default=None,
        help="Comma-separated numeric columns to sum instead of listing rows",
    )
    table.add_argument(
        "--group",
        type=str,
        default=None,
        help="Group aggregation rows by this column (requires --aggregate)",
    )

    request = subparsers.add_parser(
        "request", help="Print a bundled large request payload from the dataset"
    )
    request.add_argument(
        "request_id",
        help="Request id (uuid) whose large payload to print",
    )

    args = parser.parse_args()

    if args.command == "visualize":
        _run_visualize(args, recipes)
    elif args.command == "table":
        if args.group and not args.aggregate:
            parser.error("--group requires --aggregate")
        _run_table(args)
    elif args.command == "request":
        pull_request(load_data, args.dataset_dir, args.request_id)


def _infer_granularity(
    start: datetime.datetime | None, end: datetime.datetime | None
) -> str:
    """Infer plot granularity from the resolved window span."""
    if start is None or end is None:
        return "auto"  # no time window (e.g. all)
    days = (end - start).days
    if days <= 31:  # up to ~1 month
        return "daily"
    if days <= 366:  # up to ~1 year
        return "monthly"
    return "yearly"


def _make_load_data(
    dataset_dir: str,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    cluster: str | None,
) -> Callable[[str], pl.LazyFrame]:
    """Return a load_data function that filters request_metrics by time/cluster."""

    def load_data_func(name: str) -> pl.LazyFrame:
        lf = load_data(dataset_dir, name)
        if name != "request_metrics":
            return lf
        lf = filter_period(lf, METRICS_TIME_COL, start, end)
        if cluster is not None:
            lf = lf.filter(pl.col("cluster") == cluster)
        return lf

    return load_data_func


def _print_recipes(recipes: dict[str, list[tuple[str, Recipe]]]) -> None:
    print("Available categories and recipes:")
    for category_name, recipe_list in recipes.items():
        print(f"\n  {category_name}:")
        for name, _ in recipe_list:
            print(f"    - {name}")
        print("    - all")


if __name__ == "__main__":
    main()
