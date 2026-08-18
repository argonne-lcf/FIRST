#!/usr/bin/env python

import datetime
import holoviews as hv
import hvplot.polars
import polars as pl

from matplotlib.dates import DateFormatter
from matplotlib.ticker import StrMethodFormatter


def plot_top_20_users_minerva_aug6_12pm_7pm_cst(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(
            request_log, how="left", left_on="request_id", right_on="id", suffix="_rl"
        )
        .join(
            user.unique(subset="id"),
            how="left",
            left_on="user_id",
            right_on="id",
            suffix="_u",
        )
        .select("user.name", "total_tokens", "timestamp_compute_request", "cluster")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            pl.col("cluster").eq("minerva"),
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
            title="Minerva Total Token Usage By User (Top 20, Aug 6, 12pm-7pm CST)",
            xlabel="Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_20_users_minerva_aug6_12pm_7pm_cst.svg")


def plot_top_models_total_tokens_minerva_aug6_12pm_7pm_cst(
    metrics: pl.LazyFrame,
) -> None:
    plot = (
        metrics.select("model", "total_tokens", "timestamp_compute_request", "cluster")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            pl.col("cluster").eq("minerva"),
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
            title="Minerva Total Token Usage By Model (Top 20, Aug 6, 12pm-7pm CST)",
            xlabel="Model Name",
            ylabel="# of Total Tokens",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_total_tokens_minerva_aug6_12pm_7pm_cst.svg")


def plot_top_models_request_count_minerva_aug6_12pm_7pm_cst(
    metrics: pl.LazyFrame,
) -> None:
    plot = (
        metrics.select("model", "timestamp_compute_request", "cluster")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            pl.col("cluster").eq("minerva"),
        )
        .group_by("model")
        .agg(pl.col("model").count().alias("model_count"))
        .sort("model_count", descending=True)
        .limit(20)
        .collect(engine="streaming")
        .hvplot.barh(
            x="model",
            y="model_count",
            title="Minerva Model Usage By Request (Top 20, Aug 6, 12pm-7pm CST)",
            xlabel="Model Name",
            ylabel="# of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_request_count_minerva_aug6_12pm_7pm_cst.svg")


def plot_top_models_unique_users_minerva_aug6_12pm_7pm_cst(
    metrics: pl.LazyFrame, request_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        metrics.join(
            request_log, how="left", left_on="request_id", right_on="id", suffix="_rl"
        )
        .join(
            user.unique(subset="id"),
            how="left",
            left_on="user_id",
            right_on="id",
            suffix="_u",
        )
        .select("user.name", "model", "timestamp_compute_request", "cluster")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            pl.col("cluster").eq("minerva"),
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
            title="Minerva Model Usage By Unique Users (Top 20, Aug 6, 12pm-7pm CST)",
            xlabel="Model",
            ylabel="# of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "top_models_unique_users_minerva_aug6_12pm_7pm_cst.svg")


def plot_users_served_minerva_aug6_12pm_7pm_cst(
    access_log: pl.LazyFrame, user: pl.LazyFrame
) -> None:
    plot = (
        access_log.join(
            user.unique(subset="id"),
            how="left",
            left_on="user.id",
            right_on="id",
            suffix="_u",
        )
        .select("user.name", "timestamp_request")
        .with_columns(
            pl.col("timestamp_request").str.head(19).str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            )
        )
        .unique(
            subset=[
                pl.col("user.name"),
                pl.col("timestamp_request").dt.hour(),
            ]
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_request",
            bins=7,
            bin_range=(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            title="Minerva Inference Service User Distribution (Aug 6, 12pm-7pm CST)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%H:%M"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Users",
            aspect="square",
        )
    )

    hvplot.save(plot, "users_served_minerva_aug6_12pm_7pm_cst.svg")


def plot_requests_minerva_aug6_12pm_7pm_cst(metrics: pl.LazyFrame) -> None:
    plot = (
        metrics.select("timestamp_compute_request", "cluster")
        .with_columns(
            pl.col("timestamp_compute_request").str.to_datetime(time_zone="UTC")
        )
        .filter(
            pl.col("timestamp_compute_request").is_between(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            pl.col("cluster").eq("minerva"),
        )
        .collect(engine="streaming")
        .hvplot.hist(
            y="timestamp_compute_request",
            bins=7,
            bin_range=(
                datetime.datetime(2026, 8, 6, 17, tzinfo=datetime.timezone.utc),
                datetime.datetime(2026, 8, 7, 0, tzinfo=datetime.timezone.utc),
            ),
            title="Minerva Inference Request Distribution (Aug 6, 12pm-7pm CST)",
            xlabel="Timestamp",
            xformatter=DateFormatter("%H:%M"),
            yformatter=StrMethodFormatter("{x:,.0f}"),
            ylabel="Number of Requests",
            aspect="square",
        )
    )

    hvplot.save(plot, "requests_minerva_aug6_12pm_7pm_cst.svg")


def main() -> None:
    hv.extension("matplotlib")

    access_log = pl.scan_parquet(
        "logs/*2026-08-06.access_log.parquet",
        # missing_columns="insert",
        # extra_columns="ignore",
    )
    request_log = pl.scan_parquet(
        "logs/*2026-08-06.request_log.parquet",
        # missing_columns="insert",
        # extra_columns="ignore",
    )
    metrics = pl.scan_parquet(
        "logs/*2026-08-06.request_metrics.parquet",
        # missing_columns="insert",
        # extra_columns="ignore",
    )

    user = pl.scan_parquet("logs/*2026-08-06.user.parquet")

    plot_top_20_users_minerva_aug6_12pm_7pm_cst(metrics, request_log, user)
    plot_top_models_total_tokens_minerva_aug6_12pm_7pm_cst(metrics)
    plot_top_models_request_count_minerva_aug6_12pm_7pm_cst(metrics)
    plot_top_models_unique_users_minerva_aug6_12pm_7pm_cst(metrics, request_log, user)
    plot_users_served_minerva_aug6_12pm_7pm_cst(access_log, user)
    plot_requests_minerva_aug6_12pm_7pm_cst(metrics)

    metrics.lazy().filter(pl.col("cluster").eq("minerva")).sink_csv("metrics.csv")

    # metrics.sum().sink_csv("metrics.csv")
    # user.unique(subset="id").sink_ndjson("user.ndjson")


if __name__ == "__main__":
    main()
