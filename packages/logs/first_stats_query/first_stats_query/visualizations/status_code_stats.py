"""Requests by status code over time."""

import datetime

import polars as pl

from . import recipe
from .helpers import (
    LoadData,
    add_percent,
    filter_period,
    group_time,
    plot_bars,
    rank,
    window_slug,
)

TIME_COL = "timestamp_compute_request"


@recipe("status")
def status(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Requests by status code, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("metrics").select("status_code", TIME_COL), TIME_COL, start, end
        ),
        TIME_COL,
        granularity,
    )
    df = df.with_columns(
        pl.col("status_code").cast(pl.String).fill_null("null").alias("status_code"),
    )

    keys = ["status_code", *buckets]
    aggregated = df.group_by(*keys).agg(
        pl.col("status_code").count().alias("request_count")
    )
    aggregated = add_percent(aggregated, buckets, "request_count")
    aggregated = (
        aggregated.sort(*buckets, "status_code")
        if buckets
        else rank(aggregated, "request_count", "status_code")
    )
    plot_bars(
        aggregated.collect(engine="streaming"),
        start,
        end,
        granularity,
        "pct_request",
        "status_code",
        "Requests By Status",
        "Status Code",
        "% of Requests",
        f"status_{granularity}_{window_slug(start, end)}.svg",
        stacked=True,
    )
