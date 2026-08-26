"""Generate tables for the streams ingested by the CLI.

Each stream (metrics, request_log, access_log, user) can be dumped as a
pretty-printed polars table or reduced to summations. Rows are filtered
by time range (--period/--start/--end) and cluster; columns are
selectable. With ``--aggregate``, numeric columns are summed instead of
listed, optionally grouped by a dimension column (``--group``). Rows can
be filtered by exact column values (``COLUMN=value``). The table is
printed to stdout and the same rows are exported as CSV to disk.
"""

import datetime
import json
from typing import Callable

import polars as pl

from .visualizations.helpers import window_slug

STREAMS = ("metrics", "request_log", "access_log", "user")

# Stream -> timestamp column used for time-range filtering (None = no time).
TIMESTAMP_COL: dict[str, str | None] = {
    "metrics": "timestamp_compute_request",
    "request_log": "timestamp_compute_request",
    "access_log": "timestamp_request",
    "user": None,
}

# Stream -> cluster column used for --cluster filtering (None = no cluster).
CLUSTER_COL: dict[str, str | None] = {
    "metrics": "cluster",
    "request_log": "cluster",
    "access_log": None,
    "user": None,
}


def _jsonify(value: object) -> str:
    """Serialize a nested value as a JSON string for CSV export."""
    if isinstance(value, pl.Series):
        value = value.to_list()
    return json.dumps(value, ensure_ascii=False)


def _csv_frame(df: pl.DataFrame) -> pl.DataFrame:
    """Return a CSV-safe copy, serializing nested columns as JSON strings."""
    nested = [
        name
        for name, dtype in df.schema.items()
        if isinstance(dtype, (pl.List, pl.Struct))
    ]
    if not nested:
        return df
    return df.with_columns(
        pl.col(name).map_elements(_jsonify, return_dtype=pl.Utf8) for name in nested
    )


def _csv_path(
    stream: str,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    cluster: str | None,
    aggregate: list[str] | None,
    filters: dict[str, str] | None,
) -> str:
    """Derive a filesystem-safe CSV export name from the selection."""
    parts = [stream, window_slug(start, end)]
    if cluster:
        parts.append(cluster)
    if filters:
        parts.extend(f"{name}={value}" for name, value in filters.items())
    if aggregate is not None:
        parts.append("agg")
    # A filter value is data, e.g. a model name, and can contain "/".
    return "_".join(parts).replace("/", "_") + ".csv"


def generate_table(
    load_data: Callable[[str], pl.LazyFrame],
    stream: str,
    start: datetime.datetime | None,
    end: datetime.datetime | None,
    cluster: str | None,
    columns: list[str] | None = None,
    limit: int | None = None,
    sort: str | None = None,
    aggregate: list[str] | None = None,
    group: str | None = None,
    filters: dict[str, str] | None = None,
) -> None:
    """Print rows from ``stream`` as a polars table, optionally as summations.

    Applies time-range, cluster, and exact column-value selection, then
    either lists rows (column/sort/limit selection) or sums the
    ``aggregate`` columns, grouped by ``group`` when given.
    """
    lf = load_data(stream)

    timestamp_col = TIMESTAMP_COL[stream]
    if timestamp_col is not None:
        timestamp = pl.col(timestamp_col).str.head(19).str.to_datetime(time_zone="UTC")
        if start is not None:
            lf = lf.filter(timestamp >= start)
        if end is not None:
            lf = lf.filter(timestamp <= end)

    cluster_col = CLUSTER_COL[stream]
    if cluster is not None and cluster_col is not None:
        lf = lf.filter(pl.col(cluster_col).eq(cluster))

    if filters is not None:
        for name, value in filters.items():
            lf = lf.filter(pl.col(name).cast(pl.Utf8).eq(value))

    if aggregate is not None:
        agg_exprs = [pl.col(name).sum().alias(name) for name in aggregate]
        if group is not None:
            lf = lf.group_by(group).agg(*agg_exprs)
        else:
            lf = lf.select(*agg_exprs)
        if sort is not None:
            column, sep, direction = sort.partition(":")
            lf = lf.sort(column, descending=sep == ":" and direction == "desc")
    else:
        if sort is not None:
            column, sep, direction = sort.partition(":")
            lf = lf.sort(column, descending=sep == ":" and direction == "desc")
        if columns is not None:
            lf = lf.select(*columns)

    if limit is not None:
        lf = lf.head(limit)

    df = lf.collect(engine="streaming")
    print(df)
    _csv_frame(df).write_csv(_csv_path(stream, start, end, cluster, aggregate, filters))
