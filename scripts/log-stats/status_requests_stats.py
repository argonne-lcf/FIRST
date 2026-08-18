#!/usr/bin/env python

import datetime
import holoviews as hv
import hvplot.polars
import polars as pl


def plot_requests_by_status_all_time(metrics: pl.LazyFrame) -> None:
    aggregated = (
        metrics.select("status_code", "timestamp_compute_request")
        .with_columns(
            pl.col("timestamp_compute_request")
            .str.head(19)
            .str.to_datetime(time_zone="UTC")
        )
        .with_columns(
            pl.col("timestamp_compute_request").dt.year().alias("year"),
            pl.col("timestamp_compute_request").dt.month().alias("month"),
        )
        .with_columns(
            pl.col("status_code").cast(pl.Utf8).fill_null("null").alias("status_code")
        )
        .filter(pl.col("status_code").is_not_null())
        .group_by("status_code", "year", "month")
        .agg(pl.col("status_code").count().alias("request_count"))
        .sort("year", "month", "status_code")
        .with_columns(
            (
                pl.col("request_count")
                / pl.col("request_count").sum().over(["year", "month"])
            ).alias("pct_request")
        )
        .with_columns(
            pl.concat_str(
                [
                    pl.col("year").cast(pl.Utf8),
                    pl.lit("-"),
                    pl.col("month").cast(pl.Utf8).str.zfill(2),
                ]
            ).alias("month_label")
        )
        .collect(engine="streaming")
    )

    plot = aggregated.hvplot.bar(
        x="month_label",
        y="pct_request",
        by="status_code",
        title="Requests By Status Over Time (All-Time) — %",
        xlabel="Year and Month",
        ylabel="% of Requests",
        aspect="square",
        stacked=True,
        width=900,
        rot=45,
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    month_labels_sorted = (
        aggregated.select("month_label")
        .unique()
        .sort("month_label")
        .to_series()
        .to_list()
    )
    ax.set_xticks(range(len(month_labels_sorted)))
    ax.set_xticklabels(month_labels_sorted, rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig("requests_by_status_all_time.svg")


def plot_requests_by_status_six_month(metrics: pl.LazyFrame) -> None:
    aggregated = (
        metrics.select("status_code", "timestamp_compute_request")
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
            pl.col("status_code").cast(pl.Utf8).fill_null("null").alias("status_code")
        )
        .filter(pl.col("status_code").is_not_null())
        .group_by("status_code", "year", "month")
        .agg(pl.col("status_code").count().alias("request_count"))
        .sort("year", "month", "status_code")
        .with_columns(
            (
                pl.col("request_count")
                / pl.col("request_count").sum().over(["year", "month"])
            ).alias("pct_request")
        )
        .collect(engine="streaming")
    )

    month_map = {3: "Mar", 4: "Apr", 5: "May", 6: "Jun", 7: "Jul", 8: "Aug"}
    aggregated = aggregated.with_columns(
        pl.col("month")
        .map_elements(lambda x: month_map.get(x, x), return_dtype=pl.Utf8)
        .alias("month_name")
    )

    plot = aggregated.hvplot.bar(
        x="month_name",
        y="pct_request",
        by="status_code",
        title="Requests By Status Per Month (Mar-Aug 2026) — %",
        xlabel="Month",
        ylabel="% of Requests",
        aspect="square",
        stacked=True,
        width=800,
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    month_centers = [0, 1, 2, 3, 4, 5]
    month_labels = ["Mar", "Apr", "May", "Jun", "Jul", "Aug"]
    ax.set_xticks(month_centers)
    ax.set_xticklabels(month_labels)
    fig.tight_layout()
    fig.savefig("requests_by_status_six_month.svg")


def plot_requests_by_status_one_month(metrics: pl.LazyFrame) -> None:
    aggregated = (
        metrics.select("status_code", "timestamp_compute_request")
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
            pl.col("status_code").cast(pl.Utf8).fill_null("null").alias("status_code")
        )
        .filter(pl.col("status_code").is_not_null())
        .group_by("status_code", "day")
        .agg(pl.col("status_code").count().alias("request_count"))
        .sort("day", "status_code")
        .with_columns(
            (
                pl.col("request_count") / pl.col("request_count").sum().over(["day"])
            ).alias("pct_request")
        )
        .collect(engine="streaming")
    )

    plot = aggregated.hvplot.bar(
        x="day",
        y="pct_request",
        by="status_code",
        title="Requests By Status Per Day (August 2026) — %",
        xlabel="Day of Month",
        ylabel="% of Requests",
        aspect="square",
        stacked=True,
        width=900,
    )

    renderer = hv.renderer("matplotlib")
    plot_state = renderer.get_plot(plot)
    fig = plot_state.state
    ax = fig.gca()
    day_centers = list(range(1, 32))
    ax.set_xticks(day_centers)
    ax.set_xticklabels([str(d) for d in day_centers], rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig("requests_by_status_one_month.svg")


def main() -> None:
    hv.extension("matplotlib")

    metrics = pl.scan_parquet(
        "logs/*.request_metrics.parquet",
        missing_columns="insert",
        extra_columns="ignore",
    )

    plot_requests_by_status_all_time(metrics)
    plot_requests_by_status_six_month(metrics)
    plot_requests_by_status_one_month(metrics)


if __name__ == "__main__":
    main()
