"""Token usage breakdown by cluster over time."""

import datetime

import polars as pl

from . import recipe
from .helpers import (
    LoadData,
    filter_period,
    group_time,
    plot_stacked,
    time_axis,
    window_label,
    window_slug,
)

TIME_COL = "timestamp_compute_request"
TOKEN_COLS = ["prompt_tokens", "completion_tokens"]


@recipe("token-breakdown")
def token_breakdown(
    load_data: LoadData,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    granularity: str = "monthly",
    _cluster: str | None = None,
) -> None:
    """Prompt vs completion tokens by cluster, adapted to granularity."""
    df, buckets = group_time(
        filter_period(
            load_data("request_metrics").select("cluster", *TOKEN_COLS, TIME_COL),
            TIME_COL,
            start,
            end,
        ),
        TIME_COL,
        granularity,
    )
    df = df.with_columns(
        *(pl.col(name).fill_null(0) for name in TOKEN_COLS),
    )
    keys = ["cluster", *buckets]
    melted = (
        df.group_by(*keys)
        .agg(*(pl.col(name).sum().alias(name) for name in TOKEN_COLS))
        .sort(*keys)
        .collect(engine="streaming")
        .unpivot(
            index=keys,
            on=TOKEN_COLS,
            variable_name="token_type",
            value_name="tokens",
        )
    )

    axis = time_axis(granularity)
    for cluster in melted.get_column("cluster").unique().to_list():
        plot_stacked(
            melted.filter(pl.col("cluster") == cluster),
            x_col=buckets[0] if buckets else "cluster",
            y_col="tokens",
            by_col="token_type",
            title=f"Token Usage Breakdown By Type - {cluster} ({window_label(start, end)})",
            xlabel=axis.label or "Cluster",
            ylabel="# of Tokens",
            save_path=f"token_breakdown_{cluster}_{granularity}_{window_slug(start, end)}.svg",
            horizontal=not buckets,
        )
