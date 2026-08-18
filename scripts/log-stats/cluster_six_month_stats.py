#!/usr/bin/env python

import datetime
import holoviews as hv
import hvplot.polars
import polars as pl


def plot_tokens_by_cluster_six_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("cluster", "total_tokens", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.month().alias("month"),
            pl.col("timestamp_compute_request").dt.year().alias("year"),
        )
        .group_by("cluster", "year", "month")
        .agg(pl.col("total_tokens").sum())
        .sort("year", "month", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="month",
            y="total_tokens",
            by="cluster",
            title="Total Token Usage By Cluster Per Month (Mar-Aug 2026)",
            xlabel="Month",
            ylabel="# of Total Tokens",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    month_centers = [0, 1, 2, 3, 4, 5]
    month_labels = ["Mar", "Apr", "May", "Jun", "Jul", "Aug"]
    ax.set_xticks(month_centers)
    ax.set_xticklabels(month_labels)
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("tokens_by_cluster_six_month.svg")


def plot_requests_by_cluster_six_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("cluster", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.month().alias("month"),
            pl.col("timestamp_compute_request").dt.year().alias("year"),
        )
        .group_by("cluster", "year", "month")
        .agg(pl.col("cluster").count().alias("request_count"))
        .sort("year", "month", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="month",
            y="request_count",
            by="cluster",
            title="Request Count By Cluster Per Month (Mar-Aug 2026)",
            xlabel="Month",
            ylabel="# of Requests",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    month_centers = [0, 1, 2, 3, 4, 5]
    month_labels = ["Mar", "Apr", "May", "Jun", "Jul", "Aug"]
    ax.set_xticks(month_centers)
    ax.set_xticklabels(month_labels)
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("requests_by_cluster_six_month.svg")


def plot_users_by_cluster_six_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("cluster", "user.name", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.month().alias("month"),
            pl.col("timestamp_compute_request").dt.year().alias("year"),
        )
        .unique(subset=["cluster", "user.name", "year", "month"])
        .group_by("cluster", "year", "month")
        .agg(pl.col("user.name").count().alias("user_count"))
        .sort("year", "month", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="month",
            y="user_count",
            by="cluster",
            title="Unique Users By Cluster Per Month (Mar-Aug 2026)",
            xlabel="Month",
            ylabel="# of Users",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    month_centers = [0, 1, 2, 3, 4, 5]
    month_labels = ["Mar", "Apr", "May", "Jun", "Jul", "Aug"]
    ax.set_xticks(month_centers)
    ax.set_xticklabels(month_labels)
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("users_by_cluster_six_month.svg")


def plot_tokens_by_cluster_one_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("cluster", "total_tokens", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.day().alias("day"),
        )
        .group_by("cluster", "day")
        .agg(pl.col("total_tokens").sum())
        .sort("day", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="day",
            y="total_tokens",
            by="cluster",
            title="Total Token Usage By Cluster Per Day (August 2026)",
            xlabel="Day of Month",
            ylabel="# of Total Tokens",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    day_centers = list(range(1, 32))
    ax.set_xticks(day_centers)
    ax.set_xticklabels([str(d) for d in range(1, 32)])
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("tokens_by_cluster_one_month.svg")


def plot_requests_by_cluster_one_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("cluster", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.day().alias("day"),
        )
        .group_by("cluster", "day")
        .agg(pl.col("cluster").count().alias("request_count"))
        .sort("day", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="day",
            y="request_count",
            by="cluster",
            title="Request Count By Cluster Per Day (August 2026)",
            xlabel="Day of Month",
            ylabel="# of Requests",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    day_centers = list(range(1, 32))
    ax.set_xticks(day_centers)
    ax.set_xticklabels([str(d) for d in range(1, 32)])
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("requests_by_cluster_one_month.svg")


def plot_users_by_cluster_one_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("cluster", "user.name", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.day().alias("day"),
        )
        .unique(subset=["cluster", "user.name", "day"])
        .group_by("cluster", "day")
        .agg(pl.col("user.name").count().alias("user_count"))
        .sort("day", "cluster")
        .collect(engine="streaming")
        .hvplot.bar(
            x="day",
            y="user_count",
            by="cluster",
            title="Unique Users By Cluster Per Day (August 2026)",
            xlabel="Day of Month",
            ylabel="# of Users",
            aspect="square",
            stacked=False,
            width=800,
        )
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    day_centers = list(range(1, 32))
    ax.set_xticks(day_centers)
    ax.set_xticklabels([str(d) for d in range(1, 32)])
    fig.legend(loc="upper right", bbox_to_anchor=(0.85, 0.85))
    fig.savefig("users_by_cluster_one_month.svg")


def main() -> None:
    hv.extension("matplotlib")

    request_log = pl.scan_parquet(
        "logs/*.request_log.parquet", missing_columns="insert", extra_columns="ignore"
    )
    metrics = pl.scan_parquet(
        "logs/*.request_metrics.parquet",
        missing_columns="insert",
        extra_columns="ignore",
    )

    user = pl.scan_parquet("logs/*.user.parquet")

    plot_tokens_by_cluster_six_month(metrics)
    plot_requests_by_cluster_six_month(metrics)
    plot_users_by_cluster_six_month(metrics, request_log, user)
    plot_tokens_by_cluster_one_month(metrics)
    plot_requests_by_cluster_one_month(metrics)
    plot_users_by_cluster_one_month(metrics, request_log, user)


if __name__ == "__main__":
    main()
