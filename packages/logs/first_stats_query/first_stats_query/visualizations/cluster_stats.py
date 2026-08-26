"""Token usage and request counts by cluster over time."""

import datetime

import polars as pl

from . import recipe
from .helpers import (
    LoadData,
    filter_period,
    group_time,
    join_request_log,
    join_user,
    plot_bars,
    rank,
    window_slug,
)

TIME_COL = "timestamp_compute_request"


@recipe("cluster")
def cluster_tokens(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Total tokens by cluster, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("metrics").select("cluster", "total_tokens", TIME_COL),
            TIME_COL,
            start,
            end,
        ),
        TIME_COL,
        granularity,
    )
    keys = ["cluster", *buckets]
    result = df.group_by(*keys).agg(pl.col("total_tokens").sum())
    result = (
        result.sort(*buckets, "cluster")
        if buckets
        else rank(result, "total_tokens", "cluster")
    )
    plot_bars(
        result.collect(engine="streaming"),
        start,
        end,
        granularity,
        "total_tokens",
        "cluster",
        "Total Token Usage By Cluster",
        "Cluster",
        "# of Total Tokens",
        f"tokens_{granularity}_{window_slug(start, end)}.svg",
    )


@recipe("cluster")
def cluster_requests(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Request count by cluster, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("metrics").select("cluster", TIME_COL), TIME_COL, start, end
        ),
        TIME_COL,
        granularity,
    )
    keys = ["cluster", *buckets]
    result = df.group_by(*keys).agg(pl.col("cluster").count().alias("request_count"))
    result = (
        result.sort(*buckets, "cluster")
        if buckets
        else rank(result, "request_count", "cluster")
    )
    plot_bars(
        result.collect(engine="streaming"),
        start,
        end,
        granularity,
        "request_count",
        "cluster",
        "Request Count By Cluster",
        "Cluster",
        "# of Requests",
        f"requests_{granularity}_{window_slug(start, end)}.svg",
    )


@recipe("cluster")
def cluster_users(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Unique users by cluster, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("metrics")
            .pipe(join_request_log, load_data)
            .pipe(join_user, load_data)
            .select("cluster", "user.name", TIME_COL),
            TIME_COL,
            start,
            end,
        ),
        TIME_COL,
        granularity,
    )
    keys = ["cluster", *buckets]
    result = (
        df.unique(subset=[*keys, "user.name"])
        .group_by(*keys)
        .agg(pl.col("user.name").count().alias("user_count"))
    )
    result = (
        result.sort(*buckets, "cluster")
        if buckets
        else rank(result, "user_count", "cluster")
    )
    plot_bars(
        result.collect(engine="streaming"),
        start,
        end,
        granularity,
        "user_count",
        "cluster",
        "Unique Users By Cluster",
        "Cluster",
        "# of Users",
        f"users_{granularity}_{window_slug(start, end)}.svg",
    )
