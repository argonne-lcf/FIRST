"""Scope-aware helpers for visualization recipes.

Encapsulates granularity-aware plotting so recipe bodies stay simple.
Granularity (daily/monthly/yearly/auto) controls the time bucket and
axis labels; the period (6m/1m/all) controls the data filter.
"""

import datetime
from collections.abc import Callable
from math import ceil, inf
from typing import Any, NamedTuple

import holoviews as hv  # type: ignore[import-untyped]
import hvplot.polars  # type: ignore  # noqa: F401 (registers DataFrame.hvplot accessor)
import polars as pl

# Recipes are saved as matplotlib images, so matplotlib must be the active
# backend: hvplot attaches plot options (title, size, stacking, the barh flip)
# to whichever backend is current, and options kept for another backend are
# dropped when the figure is rendered.
hv.extension("matplotlib")

# Loads one ingested stream; the signature every recipe is called with.
LoadData = Callable[[str], pl.LazyFrame]
Recipe = Callable[
    [LoadData, datetime.datetime | None, datetime.datetime | None, str, str | None],
    None,
]

# ── Data helpers ────────────────────────────────────────────────────────


def parse_time(df: pl.LazyFrame, time_col: str) -> pl.LazyFrame:
    """Return ``df`` with ``time_col`` converted to UTC datetimes.

    Ingested logs carry timestamps as ISO-8601 strings, so they must be
    parsed before any date part or axis is taken from them.
    """
    if df.collect_schema()[time_col] == pl.String:
        df = df.with_columns(
            pl.col(time_col).str.head(19).str.to_datetime(time_zone="UTC")
        )
    return df


def filter_period(
    df: pl.LazyFrame,
    time_col: str,
    start: datetime.datetime | None = None,
    end: datetime.datetime | None = None,
) -> pl.LazyFrame:
    """Parse ``time_col`` and keep rows no earlier than ``start``, no later than ``end``."""
    df = parse_time(df, time_col)
    if start is not None:
        df = df.filter(pl.col(time_col) >= start)
    if end is not None:
        df = df.filter(pl.col(time_col) <= end)
    return df


def join_request_log(df: pl.LazyFrame, load_data: LoadData) -> pl.LazyFrame:
    """Join metrics with request_log on request_id, keeping unmatched requests.

    A metrics row whose request_log row is missing from this dataset still
    counts toward request totals, so it must not be dropped by the join.
    """
    return df.join(
        load_data("request_log"),
        left_on="request_id",
        right_on="id",
        how="left",
        suffix="_rl",
    )


def join_user(df: pl.LazyFrame, load_data: LoadData) -> pl.LazyFrame:
    """Join with user table (deduplicated by id), keeping requests with no user row.

    Users authenticate once per session, so a dataset can be missing the user
    row of a request it does hold metrics for.
    """
    return df.join(
        load_data("user").unique(subset="id"),
        left_on="user_id",
        right_on="id",
        how="left",
        suffix="_u",
    )


def group_day(df: pl.LazyFrame, time_col: str) -> pl.LazyFrame:
    """Bucket rows by calendar date in the ``day`` column."""
    return parse_time(df, time_col).with_columns(
        pl.col(time_col).dt.strftime("%Y-%m-%d").alias("day")
    )


def group_month_year(df: pl.LazyFrame, time_col: str) -> pl.LazyFrame:
    """Bucket rows by calendar month, labelled with its year in ``month_year``."""
    return parse_time(df, time_col).with_columns(
        pl.col(time_col).dt.month().alias("month"),
        pl.col(time_col).dt.year().alias("year"),
        pl.col(time_col).dt.strftime("%Y-%m").alias("month_year"),
    )


def group_year(df: pl.LazyFrame, time_col: str) -> pl.LazyFrame:
    """Bucket rows by calendar year in the ``year`` column."""
    return parse_time(df, time_col).with_columns(
        pl.col(time_col).dt.strftime("%Y").alias("year")
    )


def group_time(
    df: pl.LazyFrame, time_col: str, granularity: str
) -> tuple[pl.LazyFrame, list[str]]:
    """Add the time bucket columns for ``granularity`` and name them.

    The bucket list is empty for ``auto``, which plots without a time axis.
    """
    match granularity:
        case "daily":
            return group_day(df, time_col), ["day"]
        case "monthly":
            return group_month_year(df, time_col), ["month_year"]
        case "yearly":
            return group_year(df, time_col), ["year"]
        case _:
            return df, []


def add_percent(
    aggregated: pl.LazyFrame, buckets: list[str], value: str
) -> pl.LazyFrame:
    """Add a ``pct_request`` share-of-total column, per time bucket when bucketed."""
    total = pl.col(value).sum().over(buckets) if buckets else pl.col(value).sum()
    return aggregated.with_columns((pl.col(value) / total).alias("pct_request"))


def rank(df: pl.LazyFrame, value: str, name: str) -> pl.LazyFrame:
    """Rank categories by ``value``, biggest first, ties in ``name`` order.

    Ordering by the measure alone leaves ties in whatever order the parallel
    sort found them, so the same command can draw a different chart twice.
    """
    return df.sort([value, name], descending=[True, False])


# ── Granularity-aware plotting helpers ──────────────────────────────────
# Granularity options: "daily", "monthly", "yearly", "auto"
#   - auto = no time dimension, so the categories themselves become the axis

# Every chart is drawn on the same wide figure. A category axis then stretches
# it taller if its labels do not fit, which needs the figure to exist already.
_FIGURE_WIDTH = 800
_TILT_DEG = 45  # long labels read better tilted than printed on each other
_LABEL_GAP_PX = 8.0
_MAX_TALLER = 3.0


class TimeAxis(NamedTuple):
    """How a granularity puts time on the x axis; both unset for ``auto``."""

    x_col: str | None
    label: str | None


def time_axis(granularity: str) -> TimeAxis:
    """Return the bucket column and caption a granularity plots time by."""
    match granularity:
        case "daily":
            return TimeAxis("day", "Date")
        case "monthly":
            return TimeAxis("month_year", "Month")
        case "yearly":
            return TimeAxis("year", "Year")
        case _:  # auto
            return TimeAxis(None, None)


def window_label(start: datetime.datetime | None, end: datetime.datetime | None) -> str:
    """Human-readable window label, computed from the window itself."""
    if start is None or end is None:
        return "All-time"
    return f"{start:%Y-%m-%d} to {end:%Y-%m-%d}"


def window_slug(start: datetime.datetime | None, end: datetime.datetime | None) -> str:
    """Filesystem-safe identifier derived from the window (for output filenames)."""
    if start is None or end is None:
        return "all"
    return f"{start:%Y-%m-%d}_{end:%Y-%m-%d}"


def no_rows(df: pl.DataFrame, save_path: str) -> bool:
    """Report a selection with no rows to plot, so callers can skip it."""
    if df.is_empty():
        print(f"{save_path}: no rows in the selected window, skipped")
        return True
    return False


def plot_bars(
    df: pl.DataFrame,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str,
    y_col: str,
    by_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    save_path: str,
    stacked: bool = False,
) -> None:
    """Bar chart of ``y_col`` per ``by_col`` value, over time if there is time.

    Without a time axis the categories take the axis and the chart turns
    horizontal, the form every per-category chart had before the port.
    """
    if no_rows(df, save_path):
        return
    x_col, label = time_axis(granularity)
    plot_obj = _bars(
        df,
        # With no time axis the categories themselves become the axis, and
        # have nothing left to group the bars by.
        x_col or by_col,
        y_col,
        f"{title} ({window_label(start, end)})",
        label or xlabel,
        ylabel,
        by_col=by_col if x_col else None,
        stacked=stacked,
        horizontal=x_col is None,
    )
    save_plot(plot_obj, save_path)


def plot_barh(
    df: pl.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    save_path: str,
) -> None:
    """Horizontal bar chart, one bar per value of ``x_col``."""
    if no_rows(df, save_path):
        return
    save_plot(
        _bars(df, x_col, y_col, title, xlabel, ylabel, horizontal=True), save_path
    )


def plot_stacked(
    df: pl.DataFrame,
    x_col: str,
    y_col: str,
    by_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    save_path: str,
    horizontal: bool = False,
) -> None:
    """Stacked bar chart of ``y_col`` split by ``by_col`` over ``x_col``."""
    if no_rows(df, save_path):
        return
    save_plot(
        _bars(
            df,
            x_col,
            y_col,
            title,
            xlabel,
            ylabel,
            by_col=by_col,
            stacked=True,
            horizontal=horizontal,
        ),
        save_path,
    )


def plot_hist(
    df: pl.DataFrame,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    save_path: str,
    **kwargs: Any,
) -> None:
    """Histogram of the values in ``y_col``, binned and labelled by ``kwargs``."""
    if no_rows(df, save_path):
        return
    plot_obj = _hvplot(df).hist(
        y=y_col,
        title=title,
        xlabel=xlabel,
        ylabel=ylabel,
        aspect="square",
        width=_FIGURE_WIDTH,
        **kwargs,
    )
    save_plot(plot_obj, save_path)


def _bars(
    df: pl.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    xlabel: str,
    ylabel: str,
    by_col: str | None = None,
    stacked: bool = False,
    horizontal: bool = False,
) -> Any:
    """The one hvplot bar call: horizontal charts read down the page instead."""
    opts: dict[str, Any] = {
        "x": x_col,
        "y": y_col,
        "title": title,
        "xlabel": xlabel,
        "ylabel": ylabel,
        "width": _FIGURE_WIDTH,
        "aspect": "auto" if horizontal else "square",
    }
    if by_col:
        opts["by"] = by_col
        opts["stacked"] = stacked
    plot_obj = (_hvplot(df).barh if horizontal else _hvplot(df).bar)(**opts)
    if by_col and not stacked:
        # Labelling every bar of every group is what crowded the old axes;
        # one label per bucket puts the series in the legend, where they belong.
        plot_obj = plot_obj.opts(multi_level=False)
    return plot_obj


def plot_figure(plot: Any) -> Any:
    """The figure ``plot`` renders to, with a crowded axis spaced out."""
    fig = hv.renderer("matplotlib").get_plot(plot).state
    space_labels(fig)
    fig.tight_layout()
    return fig


def save_plot(plot: Any, save_path: str) -> None:
    """Render ``plot`` to ``save_path``, spacing a crowded axis first."""
    plot_figure(plot).savefig(save_path)


def space_labels(fig: Any) -> None:
    """Space the axis labels the figure has no room for.

    A categorical axis puts one label on every position it has, and a long
    window or a long name is more of them than the figure can hold. Labels
    along the x axis tilt and then thin out; ones along the y axis, which
    usually name a bar, get a taller figure to sit in.
    """
    fig.canvas.draw()
    ax = fig.gca()

    y_labels = [t for t in ax.get_yticklabels() if t.get_text()]
    pitch, need = _pitch(y_labels, "y")
    if pitch < need:
        wider, taller = fig.get_size_inches()
        fig.set_size_inches(
            wider, taller * min(_MAX_TALLER, need / pitch), forward=True
        )

    x_labels = [t for t in ax.get_xticklabels() if t.get_text()]
    pitch, need = _pitch(x_labels, "x")
    if pitch >= need:
        return
    ax.tick_params(axis="x", labelrotation=_TILT_DEG)
    for label in x_labels:
        label.set_horizontalalignment("right")
    fig.canvas.draw()
    pitch, need = _pitch(x_labels, "x")
    stride = len(x_labels) if pitch <= 0 else max(1, ceil(need / pitch))
    if stride > 1:
        kept = x_labels[::stride]
        ax.set_xticks(
            [float(t.get_position()[0]) for t in kept],
            labels=[t.get_text() for t in kept],
        )


def _pitch(labels: list[Any], axis: str) -> tuple[float, float]:
    """The gap labels sit in, and the gap they need, measured in pixels."""
    if len(labels) < 2:
        return inf, 0.0
    boxes = [label.get_window_extent() for label in labels]
    if axis == "y":
        pitch = min(float(b.y0 - a.y0) for a, b in zip(boxes, boxes[1:]))
        need = max(float(b.height) for b in boxes) + _LABEL_GAP_PX
    else:
        pitch = min(float(b.x0 - a.x0) for a, b in zip(boxes, boxes[1:]))
        need = max(float(b.width) for b in boxes) + _LABEL_GAP_PX
    return pitch, need


def _hvplot(df: pl.DataFrame) -> Any:
    """The hvplot accessor, which mypy cannot see on ``pl.DataFrame``."""
    return df.hvplot  # type: ignore[attr-defined]
