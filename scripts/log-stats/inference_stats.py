#!/usr/bin/env python

import datetime
import holoviews as hv
import hvplot.polars
import polars as pl

from matplotlib.dates import DateFormatter
from matplotlib.ticker import StrMethodFormatter


def plot_top_20_users_all_time(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "total_tokens")
        .group_by("user.name")
        .agg(pl.col("total_tokens").sum())
        .sort("total_tokens", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="user.name",
            y="total_tokens",
            title="Total Token Usage By User (Top 20, All-Time)",
            xlabel="Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_20_users_all_time.svg")


def plot_top_20_users_one_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "total_tokens", "timestamp_compute_request")
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
        .select("user.name", "total_tokens")
        .group_by("user.name")
        .agg(pl.col("total_tokens").sum())
        .sort("total_tokens", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="user.name",
            y="total_tokens",
            title="Total Token Usage By User (Top 20, August 2026)",
            xlabel="Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_20_users_one_month.svg")


def plot_top_models_all_time(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("model", "total_tokens")
        .group_by("model")
        .agg(pl.col("total_tokens").sum())
        .sort("total_tokens", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="total_tokens",
            title="Total Token Usage By Model (Top 20, All-Time)",
            xlabel="Model Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_all_time.svg")


def plot_top_models_one_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("model", "timestamp_compute_request")
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
        .group_by("model")
        .agg(pl.col("model").count().alias("model_count"))
        .sort("model_count", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="model_count",
            title="Model Usage By Request (Top 20, August 2026)",
            xlabel="Model Name",
            ylabel="# of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_one_month.svg")


def plot_top_models_six_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("model", "total_tokens", "timestamp_compute_request")
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
        .select("model", "total_tokens")
        .group_by("model")
        .agg(pl.col("total_tokens").sum())
        .sort("total_tokens", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="total_tokens",
            title="Total Token Usage By Model (Top 20, Mar-Aug 2026)",
            xlabel="Model Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_six_month.svg")


def plot_top_models_by_users_all_time(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "model", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .unique(subset=["model", "user.name"])
        .group_by("model")
        .agg(pl.col("user.name").count().alias("user_count"))
        .sort("user_count", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="user_count",
            title="Model Usage By Unique Users (Top 20, All-Time)",
            xlabel="Model",
            ylabel="# of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_by_users_all_time.svg")


def plot_top_models_by_users_one_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "model", "timestamp_compute_request")
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
        .unique(subset=["model", "user.name"])
        .group_by("model")
        .agg(pl.col("user.name").count().alias("user_count"))
        .sort("user_count", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="user_count",
            title="Model Usage By Unique Users (Top 20, August 2026)",
            xlabel="Model",
            ylabel="# of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_by_users_one_month.svg")


def plot_users_served_all_time(access_log: pl.LazyFrame, user: pl.LazyFrame) -> None:
    plot = (
        access_log.join(
            user.unique(subset="id"), left_on="user.id", right_on="id", suffix="_u"
        )
        .select("user.name", "timestamp_request")
        .with_columns(
            pl.col("timestamp_request").str.head(19).str.to_datetime(time_zone="UTC")
        )
        .unique(
            subset=[
                pl.col("timestamp_request").dt.year(),
                pl.col("timestamp_request").dt.month(),
                pl.col("user.name"),
            ]
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_request",
            bins=12,
            title="Inference Service User Distribution (All-Time)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%m/%y"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "users_served_all_time.svg")


def plot_users_served_six_month(access_log: pl.LazyFrame, user: pl.LazyFrame) -> None:
    plot = (
        access_log.join(
            user.unique(subset="id"), left_on="user.id", right_on="id", suffix="_u"
        )
        .select("user.name", "timestamp_request")
        .with_columns(
            pl.col("timestamp_request").str.head(19).str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_request").is_between(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .unique(
            subset=[
                pl.col("timestamp_request").dt.year(),
                pl.col("timestamp_request").dt.month(),
                pl.col("user.name"),
            ]
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_request",
            bins=6,
            bin_range=(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            ),
            xticks=(
                datetime.datetime(2026, 3, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 4, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 5, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 6, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 7, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 15, tzinfo=datetime.timezone.utc),
            ),
            title="Inference Service User Distribution (Mar-Aug 2026)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%b"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "users_served_six_month.svg")


def plot_users_served_one_month(access_log: pl.LazyFrame, user: pl.LazyFrame) -> None:
    plot = (
        access_log.join(
            user.unique(subset="id"), left_on="user.id", right_on="id", suffix="_u"
        )
        .select("user.name", "timestamp_request")
        .with_columns(
            pl.col("timestamp_request").str.head(19).str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_request").is_between(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .unique(
            subset=[
                "user.name",
                pl.col("timestamp_request").dt.day(),
            ]
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_request",
            bins=31,
            bin_range=(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            ),
            title="Inference Service User Distribution (August 2026)",
            xlabel="Day of Month",
            xformatter=DateFormatter("%d"),
            xticks=[
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 5, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 10, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 20, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 25, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 30, tzinfo=datetime.timezone.utc),
            ],
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "users_served_one_month.svg")


def plot_requests_all_time(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request").str.to_datetime(time_zone="UTC")
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_compute_request",
            bins=12,
            title="Production Inference Request Distribution (All-Time)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%m/%y"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "requests_all_time.svg")


def plot_requests_six_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request").str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_compute_request",
            bins=6,
            bin_range=(
                datetime.datetime(2026, 3, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            ),
            xticks=(
                datetime.datetime(2026, 3, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 4, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 5, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 6, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 7, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 15, tzinfo=datetime.timezone.utc),
            ),
            title="Production Inference Request Distribution (Mar-Aug 2026)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%b"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "requests_six_month.svg")


def plot_requests_one_month(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request").str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            )
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_compute_request",
            bins=31,
            bin_range=(
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 31, tzinfo=datetime.timezone.utc),
            ),
            title="Production Inference Request Distribution (August 2026)",
            xlabel="Day of Month",
            xformatter=DateFormatter("%d"),
            xticks=[
                datetime.datetime(2026, 8, 1, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 5, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 10, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 15, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 20, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 25, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 30, tzinfo=datetime.timezone.utc),
            ],
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "requests_one_month.svg")


def plot_top_20_users_six_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "total_tokens", "timestamp_compute_request")
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
        .select("user.name", "total_tokens")
        .group_by("user.name")
        .agg(pl.col("total_tokens").sum())
        .sort("total_tokens", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="user.name",
            y="total_tokens",
            title="Total Token Usage By User (Top 20, Mar-Aug 2026)",
            xlabel="Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_20_users_six_month.svg")


def plot_top_models_by_users_six_month(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(request_log, left_on="request_id", right_on="id", suffix="_rl")
        .join(user.unique(subset="id"), left_on="user_id", right_on="id", suffix="_u")
        .select("user.name", "model", "timestamp_compute_request")
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
        .unique(subset=["model", "user.name"])
        .group_by("model")
        .agg(pl.col("user.name").count().alias("user_count"))
        .sort("user_count", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="user_count",
            title="Model Usage By Unique Users (Top 20, Mar-Aug 2026)",
            xlabel="Model",
            ylabel="# of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_by_users_six_month.svg")


def main() -> None:
    hv.extension("matplotlib")

    access_log = pl.scan_parquet(
        "logs/*.access_log.parquet", missing_columns="insert", extra_columns="ignore"
    )
    request_log = pl.scan_parquet(
        "logs/*.request_log.parquet", missing_columns="insert", extra_columns="ignore"
    )
    metrics = pl.scan_parquet(
        "logs/*.request_metrics.parquet",
        missing_columns="insert",
        extra_columns="ignore",
    )

    user = pl.scan_parquet("logs/*.user.parquet")

    plot_requests_all_time(metrics)
    plot_requests_six_month(metrics)
    plot_requests_one_month(metrics)
    plot_top_models_all_time(metrics)
    plot_top_models_six_month(metrics)
    plot_top_20_users_six_month(metrics, request_log, user)
    plot_top_models_by_users_six_month(metrics, request_log, user)
    plot_top_20_users_all_time(metrics, request_log, user)
    plot_top_20_users_one_month(metrics, request_log, user)
    plot_users_served_all_time(access_log, user)
    plot_users_served_six_month(access_log, user)
    plot_users_served_one_month(access_log, user)
    plot_top_models_one_month(metrics)
    plot_top_models_by_users_all_time(metrics, request_log, user)
    plot_top_models_by_users_one_month(metrics, request_log, user)
    # metrics.sum().sink_csv("metrics.csv")
    # user.unique(subset="id").sink_ndjson("user.ndjson")


if __name__ == "__main__":
    main()
