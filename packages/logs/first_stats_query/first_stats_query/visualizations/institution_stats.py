"""Requests by institution over time."""

import datetime

import polars as pl

from . import recipe
from .helpers import (
    LoadData,
    add_percent,
    filter_period,
    group_time,
    join_request_log,
    join_user,
    plot_bars,
    rank,
    window_slug,
)

TIME_COL = "timestamp_compute_request"


@recipe("institution")
def institution(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Requests by institution, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("request_metrics")
            .pipe(join_request_log, load_data)
            .pipe(join_user, load_data)
            .select(TIME_COL, "username"),
            TIME_COL,
            start,
            end,
        ),
        TIME_COL,
        granularity,
    )
    df = df.with_columns(
        pl.col("username")
        .str.split("@")
        .list.get(1, null_on_oob=True)
        .alias("institution"),
    ).filter(pl.col("institution").is_not_null())

    keys = ["institution", *buckets]
    aggregated = df.group_by(*keys).agg(
        pl.col("institution").count().alias("request_count")
    )
    aggregated = add_percent(aggregated, buckets, "request_count")
    aggregated = (
        aggregated.sort(*buckets, "institution")
        if buckets
        else rank(aggregated, "request_count", "institution")
    )
    plot_bars(
        aggregated.collect(engine="streaming"),
        start,
        end,
        granularity,
        "pct_request",
        "institution",
        "Requests By Institution",
        "Institution",
        "% of Requests",
        f"institution_{granularity}_{window_slug(start, end)}.svg",
        stacked=True,
    )
