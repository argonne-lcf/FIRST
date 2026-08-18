#!/usr/bin/env python

import datetime
import holoviews as hv
import hvplot.polars
import polars as pl


def plot_token_breakdown_by_cluster_six_month(metrics: pl.LazyFrame) -> None:
    aggregated = (
        metrics.select(
            "cluster",
            "prompt_tokens",
            "completion_tokens",
            "timestamp_compute_request",
        )
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
        .with_columns(
            pl.col("prompt_tokens").fill_null(0),
            pl.col("completion_tokens").fill_null(0),
        )
        .group_by("cluster", "year", "month")
        .agg(
            pl.col("prompt_tokens").sum().alias("prompt_tokens"),
            pl.col("completion_tokens").sum().alias("completion_tokens"),
        )
        .sort("year", "month", "cluster")
        .collect(engine="streaming")
    )

    melted = aggregated.unpivot(
        index=["cluster", "year", "month"],
        on=["prompt_tokens", "completion_tokens"],
        variable_name="token_type",
        value_name="tokens",
    )

    month_map = {3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug"}
    melted = melted.with_columns(
        pl.col("month")
        .map_elements(lambda x: month_map.get(x, x), return_dtype=pl.Utf8)
        .alias("month_name")
    )

    clusters = melted.select("cluster").unique().to_series().to_list()
    for cluster in clusters:
        plot_df = melted.filter(pl.col("cluster") == cluster)
        plot = plot_df.hvplot.bar(
            x="month_name",
            y="tokens",
            by="token_type",
            stacked=True,
            title=f"Token Usage Breakdown By Type - {cluster} (Mar-Aug 2026)",
            xlabel="Month",
            ylabel="# of Tokens",
            aspect="square",
            width=800,
        )

        renderer = hv.renderer("matplotlib")
        plot_state = renderer.get_plot(plot)
        fig = plot_state.state
        fig.savefig(f"token_breakdown_{cluster}_six_month.svg")


def plot_token_breakdown_by_cluster_one_month(metrics: pl.LazyFrame) -> None:
    aggregated = (
        metrics.select(
            "cluster",
            "prompt_tokens",
            "completion_tokens",
            "timestamp_compute_request",
        )
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
        .with_columns(
            pl.col("prompt_tokens").fill_null(0),
            pl.col("completion_tokens").fill_null(0),
        )
        .group_by("cluster", "day")
        .agg(
            pl.col("prompt_tokens").sum().alias("prompt_tokens"),
            pl.col("completion_tokens").sum().alias("completion_tokens"),
        )
        .sort("day", "cluster")
        .collect(engine="streaming")
    )

    melted = aggregated.unpivot(
        index=["cluster", "day"],
        on=["prompt_tokens", "completion_tokens"],
        variable_name="token_type",
        value_name="tokens",
    )

    clusters = melted.select("cluster").unique().to_series().to_list()
    for cluster in clusters:
        plot_df = melted.filter(pl.col("cluster") == cluster)
        plot = plot_df.hvplot.bar(
            x="day",
            y="tokens",
            by="token_type",
            stacked=True,
            title=f"Token Usage Breakdown By Type - {cluster} (August 2026)",
            xlabel="Day of Month",
            ylabel="# of Tokens",
            aspect="square",
            width=800,
        )

        renderer = hv.renderer("matplotlib")
        plot_state = renderer.get_plot(plot)
        fig = plot_state.state
        fig.savefig(f"token_breakdown_{cluster}_one_month.svg")


def main() -> None:
    hv.extension("matplotlib")

    metrics = pl.scan_parquet(
        "logs/*.request_metrics.parquet",
        missing_columns="insert",
        extra_columns="ignore",
    )

    plot_token_breakdown_by_cluster_six_month(metrics)
    plot_token_breakdown_by_cluster_one_month(metrics)


if __name__ == "__main__":
    main()
