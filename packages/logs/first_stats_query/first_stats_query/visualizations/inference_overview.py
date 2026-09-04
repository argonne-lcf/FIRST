"""Inference stats: top users, models, user distributions, request histograms."""

import datetime
import sys
from typing import Any

import polars as pl
from matplotlib.dates import DateFormatter
from matplotlib.ticker import StrMethodFormatter

from . import recipe
from .helpers import (
    LoadData,
    filter_period,
    join_request_log,
    join_user,
    plot_barh,
    plot_hist,
    rank,
    window_label,
    window_slug,
)

TIME_COL = "timestamp_compute_request"
ACCESS_TIME_COL = "timestamp_request"


@recipe("inference")
def top_users(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    _cluster: str | None = None,
) -> None:
    """Top 20 users by total tokens for the period."""
    result = (
        load_data("request_metrics")
        .pipe(join_request_log, load_data)
        .pipe(join_user, load_data)
        # requests with no user row here keep their tokens in the totals
        .with_columns(pl.col("user.name").fill_null("unknown"))
        .select("user.name", "total_tokens")
        .group_by("user.name")
        .agg(pl.col("total_tokens").sum())
        .pipe(rank, "total_tokens", "user.name")
        .limit(20)
        .collect(engine="streaming")
    )
    plot_barh(
        result,
        x_col="user.name",
        y_col="total_tokens",
        title=f"Total Token Usage By User (Top 20, {window_label(start, end)})",
        xlabel="Name",
        ylabel="# of Total Tokens",
        save_path=f"top_users_{window_slug(start, end)}.svg",
    )


@recipe("inference")
def top_models(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    _cluster: str | None = None,
) -> None:
    """Top 20 models by total tokens for the period."""
    result = (
        load_data("request_metrics")
        .select("model", "total_tokens")
        .group_by("model")
        .agg(pl.col("total_tokens").sum())
        .pipe(rank, "total_tokens", "model")
        .limit(20)
        .collect(engine="streaming")
    )
    plot_barh(
        result,
        x_col="model",
        y_col="total_tokens",
        title=f"Total Token Usage By Model (Top 20, {window_label(start, end)})",
        xlabel="Model Name",
        ylabel="# of Total Tokens",
        save_path=f"top_models_{window_slug(start, end)}.svg",
    )


@recipe("inference")
def top_models_requests(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    _cluster: str | None = None,
) -> None:
    """Top 20 models by request count for the period."""
    result = (
        load_data("request_metrics")
        .group_by("model")
        .agg(pl.col("model").count().alias("model_count"))
        .pipe(rank, "model_count", "model")
        .limit(20)
        .collect(engine="streaming")
    )
    plot_barh(
        result,
        x_col="model",
        y_col="model_count",
        title=f"Model Usage By Request (Top 20, {window_label(start, end)})",
        xlabel="Model Name",
        ylabel="# of Requests",
        save_path=f"top_models_requests_{window_slug(start, end)}.svg",
    )


@recipe("inference")
def top_models_users(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    _cluster: str | None = None,
) -> None:
    """Top 20 models by unique users for the period."""
    result = (
        load_data("request_metrics")
        .pipe(join_request_log, load_data)
        .pipe(join_user, load_data)
        .select("user.name", "model")
        .unique(subset=["model", "user.name"])
        .group_by("model")
        .agg(pl.col("user.name").count().alias("user_count"))
        .pipe(rank, "user_count", "model")
        .limit(20)
        .collect(engine="streaming")
    )
    plot_barh(
        result,
        x_col="model",
        y_col="user_count",
        title=f"Model Usage By Unique Users (Top 20, {window_label(start, end)})",
        xlabel="Model",
        ylabel="# of Users",
        save_path=f"top_models_users_{window_slug(start, end)}.svg",
    )


@recipe("inference")
def hist_users(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    cluster: str | None = None,
) -> None:
    """Histogram of unique users per bin over the period."""
    if cluster:
        print(
            "hist_users: access_log has no cluster column, --cluster ignored",
            file=sys.stderr,
        )
    setup = _hist_setup(start, end)
    accesses = (
        filter_period(
            # an access counts as user activity only once it names a user
            load_data("access_log").join(
                load_data("user").unique(subset="id"),
                left_on="user.id",
                right_on="id",
                how="inner",
                suffix="_u",
            ),
            ACCESS_TIME_COL,
            start,
            end,
        )
        .select("user.id", ACCESS_TIME_COL)
        .collect(engine="streaming")
    )
    # Binning twice, once to count and once to draw, only agrees if both get
    # the very same edges.
    unique, edges = _unique_per_bin(accesses, "user.id", ACCESS_TIME_COL, setup["bins"])
    setup["bins"] = edges
    plot_hist(
        unique,
        y_col=ACCESS_TIME_COL,
        title=f"User Access Distribution ({window_label(start, end)})",
        xlabel="Timestamp",
        ylabel="# of Unique Users",
        save_path=f"hist_users_{window_slug(start, end)}.svg",
        **setup,
    )


@recipe("inference")
def hist_requests(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    _granularity: str = "auto",
    _cluster: str | None = None,
) -> None:
    """Request distribution histogram for the period."""
    df = filter_period(
        load_data("request_metrics").select(TIME_COL), TIME_COL, start, end
    ).collect(engine="streaming")
    plot_hist(
        df,
        y_col=TIME_COL,
        title=f"Request Distribution ({window_label(start, end)})",
        xlabel="Timestamp",
        ylabel="Number of Requests",
        save_path=f"hist_requests_{window_slug(start, end)}.svg",
        **_hist_setup(start, end),
    )


def _hist_setup(
    start: datetime.datetime | None, end: datetime.datetime | None
) -> dict[str, Any]:
    """Histogram binning/x-axis config derived from the window span."""
    if start is None or end is None:
        return {
            "bins": 12,
            "xformatter": _date_formatter("%m/%y"),
            "yformatter": StrMethodFormatter("{x:,.0f}"),
        }
    span_days = (end - start).days
    if span_days <= 31:  # ~1 month -> daily bins
        return {
            "bins": 31,
            "xformatter": _date_formatter("%d"),
            "xticks": _hist_xticks_daily(start, end),
            "yformatter": StrMethodFormatter("{x:,.0f}"),
        }
    if span_days <= 183:  # ~6 months -> monthly bins
        return {
            "bins": 6,
            "xformatter": _date_formatter("%b"),
            "xticks": _hist_xticks_monthly(start, end),
        }
    return {
        "bins": 12,
        "xformatter": _date_formatter("%m/%y"),
        "yformatter": StrMethodFormatter("{x:,.0f}"),
    }


def _unique_per_bin(
    df: pl.DataFrame, user_col: str, time_col: str, bins: int
) -> tuple[pl.DataFrame, Any]:
    """Reduce accesses to one row per user per bin, and name the bins used.

    A user active several times in one bin is a single unique access there, and
    still counts in every bin they were active in. Each row keeps the timestamp
    of the access it stands for, so a histogram over these rows counts a user
    once per bin as long as it is binned by the edges returned here.

    With no rows, or with every access at the same instant, there is no span to
    divide, and the caller's bin count is passed straight through.
    """
    first, last = df.select(
        pl.col(time_col).min().alias("first"), pl.col(time_col).max().alias("last")
    ).row(0)
    if first is None or last is None:
        return df, bins
    span_us = (last - first) // datetime.timedelta(microseconds=1)
    if span_us == 0:
        return df.unique(subset=user_col), bins

    edges = [
        first + datetime.timedelta(microseconds=round(i * span_us / bins))
        for i in range(bins + 1)
    ]
    bin_of_row = (
        (pl.col(time_col) - first).dt.total_microseconds() * bins // span_us
    ).clip(0, bins - 1)
    binned = (
        df.with_columns(bin_of_row.alias("_bin"))
        .unique(subset=[user_col, "_bin"])
        .drop("_bin")
    )
    # Edges land on whole microseconds, the resolution the timestamps have, so
    # the rows this binned and the rows the histogram bins are the same rows.
    return binned, pl.Series("bin_edges", edges).to_numpy()


def _date_formatter(fmt: str) -> Any:
    """Wrap matplotlib's unannotated DateFormatter."""
    return DateFormatter(fmt)  # type: ignore[no-untyped-call]


def _hist_xticks_monthly(
    start: datetime.datetime, end: datetime.datetime
) -> list[datetime.datetime]:
    """Mid-month tick dates spanning the window."""
    ticks = []
    day = start.replace(day=15)
    while day <= end:
        ticks.append(day)
        day = (day + datetime.timedelta(days=30)).replace(day=15)
    return ticks


def _hist_xticks_daily(
    start: datetime.datetime, end: datetime.datetime
) -> list[datetime.datetime]:
    """Tick dates every ~5 days spanning the window."""
    return [
        start + datetime.timedelta(days=i) for i in range(0, (end - start).days + 1, 5)
    ]
