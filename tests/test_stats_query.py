"""Regression tests for the ``first-stats-query`` CLI.

Each test pins down a bug in the log statistics utility: time buckets taken
from unparsed timestamps, the yearless monthly axis, yearly granularity,
percentages without a group key, empty windows, and silently ignored options.
"""

import datetime
import sys
from pathlib import Path
from typing import Any

import matplotlib
import polars as pl
import pytest

from first_stats_query.__main__ import main
from first_stats_query.visualizations import helpers, inference_overview

START = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
REQUESTS = 801


def _iso(moment: datetime.datetime) -> str:
    """Format a timestamp the way the ingested NDJSON stores them."""
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


@pytest.fixture
def dataset(tmp_path: Path) -> Path:
    """Write one parquet file per stream, like first-slogs does."""
    stamps = [_iso(START + datetime.timedelta(days=day)) for day in range(REQUESTS)]
    request_ids = [f"request{index}" for index in range(REQUESTS)]
    cycle = ["a-model", "b-model", "c-model"] * (REQUESTS // 3)

    pl.DataFrame(
        {
            "id": ["user1", "user2"],
            "username": ["one@anl.gov", "two@example.com"],
            "user.name": ["One Anl", "Two Example"],
            "stream": ["user", "user"],
        }
    ).write_parquet(tmp_path / "run.user.parquet")

    pl.DataFrame(
        {
            "request_id": request_ids,
            "cluster": [_cluster(index) for index in range(REQUESTS)],
            "model": cycle,
            "timestamp_compute_request": stamps,
            "status_code": [500 if index % 7 else 200 for index in range(REQUESTS)],
            "prompt_tokens": [100 + index for index in range(REQUESTS)],
            "completion_tokens": [10 + index for index in range(REQUESTS)],
            "total_tokens": [110 + 2 * index for index in range(REQUESTS)],
            "stream": ["request_metrics"] * REQUESTS,
        }
    ).write_parquet(tmp_path / "run.request_metrics.parquet")

    pl.DataFrame(
        {
            "id": request_ids,
            "access_log_id": [f"access{index}" for index in range(REQUESTS)],
            "user_id": [_user(index) for index in range(REQUESTS)],
            "cluster": [_cluster(index) for index in range(REQUESTS)],
            "model": cycle,
            "timestamp_compute_request": stamps,
            "stream": ["request_log"] * REQUESTS,
        }
    ).filter(pl.col("id") != "request13").write_parquet(
        tmp_path / "run.request_log.parquet"
    )

    pl.DataFrame(
        {
            "id": [f"access{index}" for index in range(REQUESTS)],
            "timestamp_request": stamps,
            "status_code": [500 if index % 7 else 200 for index in range(REQUESTS)],
            "user.id": [_user(index * 3) for index in range(REQUESTS)],
            "stream": ["access_log"] * REQUESTS,
        }
    ).write_parquet(tmp_path / "run.access_log.parquet")

    return tmp_path


def _cluster(index: int) -> str:
    return "minerva" if index % 2 else "theta"


def _user(index: int) -> str:
    """The two users in the user stream, plus ids the stream never saw."""
    if index % 11 == 5:
        return "user-gone"
    return "user1" if index % 2 else "user2"


def _run(monkeypatch: pytest.MonkeyPatch, dataset: Path, *args: str) -> None:
    """Invoke the CLI for ``dataset`` with ``args``, writing into the dataset dir."""
    matplotlib.use("Agg")
    monkeypatch.chdir(dataset)
    monkeypatch.setattr(sys, "argv", ["first-stats-query", str(dataset), *args])
    main()


def _visualize(
    monkeypatch: pytest.MonkeyPatch, dataset: Path, *args: str
) -> list[tuple[pl.DataFrame, str]]:
    """Run a visualize command, returning the frame and path of every plot."""
    captured: list[tuple[pl.DataFrame, str]] = []
    report = helpers.no_rows

    def record(df: pl.DataFrame, save_path: str) -> bool:
        captured.append((df, save_path))
        return report(df, save_path)

    monkeypatch.setattr(helpers, "no_rows", record)
    _run(monkeypatch, dataset, "visualize", *args)
    return captured


def _plot(captured: list[tuple[pl.DataFrame, str]], path_part: str) -> pl.DataFrame:
    """Return the single frame plotted to a path containing ``path_part``."""
    frames = [df for df, path in captured if path_part in path]
    assert len(frames) == 1, f"expected one plot for {path_part!r}"
    return frames[0]


def _plots(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Capture every plot object the recipes save, along with its path."""
    captured: list[tuple[str, Any]] = []
    report = helpers.save_plot

    def record(plot: Any, save_path: str) -> None:
        report(plot, save_path)
        captured.append((save_path, plot))

    monkeypatch.setattr(helpers, "save_plot", record)
    return captured


def _figure(captured: list[tuple[str, Any]], path_part: str) -> Any:
    """Return the axis of the one plot saved to a path containing ``path_part``."""
    plots = [plot for path, plot in captured if path_part in path]
    assert len(plots) == 1, f"expected one plot for {path_part!r}"
    fig = helpers.plot_figure(plots[0])
    fig.canvas.draw()
    return fig.gca()


def _colliding(labels: list[Any]) -> int:
    """How many neighbouring labels print on top of each other."""
    boxes = [label.get_window_extent() for label in labels]
    return sum(1 for a, b in zip(boxes, boxes[1:]) if a.overlaps(b))


def test_ranked_ties_keep_name_order() -> None:
    """Categories ranked equal must not shuffle between two runs."""
    frame = pl.DataFrame({"name": ["b", "a", "c"], "value": [5.0, 5.0, 9.0]}).lazy()
    assert helpers.rank(frame, "value", "name").collect()["name"].to_list() == [
        "c",
        "a",
        "b",
    ]


def test_yearly_granularity_buckets_by_year(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """Yearly plots one bar per year instead of an ungrouped total."""
    captured = _visualize(
        monkeypatch,
        dataset,
        "cluster",
        "cluster_tokens",
        "--granularity",
        "yearly",
        "--start",
        "2024-01-01",
        "--end",
        "2026-12-31",
    )
    frame = _plot(captured, "tokens_yearly_")
    assert sorted(frame["year"].unique().to_list()) == ["2024", "2025", "2026"]


def test_monthly_buckets_keep_their_year(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """June 2025 and June 2026 are different bars, in chronological order."""
    captured = _visualize(
        monkeypatch,
        dataset,
        "cluster",
        "cluster_requests",
        "--granularity",
        "monthly",
        "--start",
        "2024-01-01",
        "--end",
        "2026-12-31",
    )
    frame = _plot(captured, "requests_monthly_")
    months = sorted(frame["month_year"].unique().to_list())
    assert {month[:4] for month in months} == {"2024", "2025", "2026"}
    plotted = frame.filter(pl.col("cluster") == "minerva")["month_year"].to_list()
    assert plotted == sorted(plotted)


def test_time_buckets_parse_unfiltered_timestamps(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """An unwindowed run still buckets by date instead of failing on strings."""
    captured = _visualize(
        monkeypatch,
        dataset,
        "cluster",
        "cluster_tokens",
        "--period",
        "all",
        "--granularity",
        "monthly",
    )
    frame = _plot(captured, "tokens_monthly_all")
    assert frame["month_year"].str.contains("-").all()


def test_requests_without_a_user_row_stay_counted(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """Requests missing a request_log or user row are reported, not dropped."""
    captured = _visualize(
        monkeypatch, dataset, "inference", "top_users", "--period", "all"
    )
    frame = _plot(captured, "top_users_")
    metrics = pl.scan_parquet(dataset / "*.request_metrics.parquet").collect()
    assert frame["total_tokens"].sum() == metrics["total_tokens"].sum()
    assert "unknown" in frame["user.name"].to_list()


def test_user_histogram_counts_users_once_per_bin(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """A bin rises once per user active in it, not once per access they made."""
    calls: list[tuple[pl.DataFrame, dict[str, Any]]] = []

    def record(df: pl.DataFrame, **kwargs: Any) -> None:
        calls.append((df, kwargs))

    monkeypatch.setattr(inference_overview, "plot_hist", record)
    _run(
        monkeypatch, dataset, "visualize", "inference", "hist_users", "--period", "all"
    )
    frame, kwargs = calls[0]
    accesses = pl.scan_parquet(dataset / "*.access_log.parquet").collect()
    named = accesses.filter(pl.col("user.id").is_in(["user1", "user2"]))
    assert frame.height < named.height  # repeat hits in one bin collapse together
    # Both users are active every day, so both are active in every bin.
    assert frame.height == 2 * (len(kwargs["bins"]) - 1)
    assert kwargs["ylabel"] == "# of Unique Users"


def test_status_percentages_without_a_time_group(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """Status shares over the whole selection add up instead of erroring."""
    captured = _visualize(monkeypatch, dataset, "status", "--granularity", "auto")
    frame = _plot(captured, "status_auto_")
    assert frame["pct_request"].sum() == pytest.approx(1.0)


def test_top_models_ranks_by_request_count(
    monkeypatch: pytest.MonkeyPatch, dataset: Path
) -> None:
    """A "top" chart is ranked by its value, not by name."""
    captured = _visualize(monkeypatch, dataset, "inference", "top_models_requests")
    frame = _plot(captured, "top_models_requests_")
    counts = frame["model_count"].to_list()
    assert counts == sorted(counts, reverse=True)


def test_empty_window_skips_plots(
    dataset: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A window with no rows writes no SVG and does not crash matplotlib."""
    _run(
        monkeypatch,
        dataset,
        "visualize",
        "cluster",
        "--start",
        "2030-01-01",
        "--end",
        "2030-12-31",
    )
    assert not list(dataset.glob("*.svg"))
    assert "no rows in the selected window" in capsys.readouterr().out


def test_plot_options_reach_the_renderer(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """hvplot keeps options per backend, so matplotlib must be the active one."""
    captured = _plots(monkeypatch)
    _run(monkeypatch, dataset, "visualize", "cluster", "cluster_tokens")
    ax = _figure(captured, "tokens_auto_")
    assert "Total Token Usage By Cluster" in ax.get_title()
    # barh puts the value scale across, so the count label rides the x axis.
    assert ax.get_xlabel() == "# of Total Tokens"


def test_time_axis_labels_one_bucket_each(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Series go in the legend, not between the month labels."""
    captured = _plots(monkeypatch)
    _run(
        monkeypatch,
        dataset,
        "visualize",
        "cluster",
        "cluster_tokens",
        "--granularity",
        "monthly",
        "--start",
        "2024-06-01",
        "--end",
        "2024-08-31",
    )
    ax = _figure(captured, "tokens_monthly_")
    labels = [t.get_text() for t in ax.get_xticklabels() if t.get_text()]
    assert sorted(labels) == ["2024-06", "2024-07", "2024-08"]
    assert ax.get_legend() is not None


def test_daily_axis_stays_legible(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crowded date axis drops labels until they can be read again."""
    captured = _plots(monkeypatch)
    _run(
        monkeypatch,
        dataset,
        "visualize",
        "cluster",
        "cluster_tokens",
        "--granularity",
        "daily",
        "--start",
        "2024-06-01",
        "--end",
        "2024-08-31",
    )
    ax = _figure(captured, "tokens_daily_")
    labels = [t for t in ax.get_xticklabels() if t.get_text()]
    assert len(labels) < 92 // 3, f"92 daily buckets kept {len(labels)} labels"
    assert _colliding(labels) == 0


def test_category_charts_are_horizontal(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Top-N charts list their categories down the side, as they were written."""
    captured = _plots(monkeypatch)
    _run(monkeypatch, dataset, "visualize", "inference", "top_users")
    ax = _figure(captured, "top_users_")
    assert ax.patches and all(p.get_width() > p.get_height() for p in ax.patches)
    assert "One Anl" in [t.get_text() for t in ax.get_yticklabels()]
    assert ax.get_ylabel() == "Name"


def test_a_long_category_axis_grows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """One label per bar outgrows the default height, which then grows."""
    captured = _plots(monkeypatch)
    frame = pl.DataFrame(
        {
            "name": [f"institution-{index}.example.org" for index in range(25)],
            "value": [float(index) for index in range(25)],
        }
    )
    helpers.plot_barh(
        frame,
        "name",
        "value",
        "Many",
        "Name",
        "# of Things",
        str(tmp_path / "many.svg"),
    )
    ax = _figure(captured, "many.svg")
    labels = [t for t in ax.get_yticklabels() if t.get_text()]
    assert len(labels) == 25
    assert _colliding(labels) == 0


def test_visualize_needs_a_category(
    dataset: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """argparse requires a category, so no recipe runs without one."""
    with pytest.raises(SystemExit) as exit_info:
        _run(monkeypatch, dataset, "visualize")
    assert exit_info.value.code == 2
    assert "required: category" in capsys.readouterr().err


def test_help_categories_lists_recipes(
    dataset: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--help-categories lists the recipes and stops, like --help does."""
    with pytest.raises(SystemExit) as exit_info:
        _run(monkeypatch, dataset, "visualize", "--help-categories")
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    assert "inference:" in out
    assert "top_users" in out
    assert not list(dataset.glob("*.svg"))


def test_start_and_end_must_pair(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A half-given window is refused instead of silently ignored."""
    with pytest.raises(SystemExit) as exit_info:
        _run(monkeypatch, dataset, "table", "metrics", "--start", "2025-01-01")
    assert "--start and --end must be given together" in str(exit_info.value)


def test_all_period_cannot_take_a_window(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """--period all has no window to override, so --start/--end are refused."""
    with pytest.raises(SystemExit) as exit_info:
        _run(
            monkeypatch,
            dataset,
            "table",
            "metrics",
            "--period",
            "all",
            "--start",
            "2025-01-01",
            "--end",
            "2025-12-31",
        )
    assert "cannot be combined" in str(exit_info.value)


def test_group_requires_aggregate(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Grouping without aggregating is refused instead of dumping raw rows."""
    with pytest.raises(SystemExit):
        _run(monkeypatch, dataset, "table", "metrics", "--group", "cluster")


def test_unfilterable_stream_is_refused(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The user stream has no cluster column, so --cluster cannot be honored."""
    with pytest.raises(SystemExit) as exit_info:
        _run(monkeypatch, dataset, "table", "user", "--cluster", "minerva")
    assert "no cluster column" in str(exit_info.value)


def test_windowed_table_sums_only_matching_rows(
    dataset: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The table window restricts the rows it sums, and exports them as CSV."""
    _run(
        monkeypatch,
        dataset,
        "table",
        "metrics",
        "--aggregate",
        "total_tokens",
        "--start",
        "2025-01-01",
        "--end",
        "2025-12-31",
    )
    assert list(dataset.glob("*.csv"))
    printed = capsys.readouterr().out
    assert "total_tokens" in printed
    # 365 of the 801 days fall in 2025, each worth 110 + 2 * index tokens.
    assert str(sum(110 + 2 * index for index in range(214, 214 + 365))) in printed


def test_filter_needs_a_value(dataset: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A --filter without = is refused instead of matching an empty string."""
    with pytest.raises(SystemExit) as exit_info:
        _run(monkeypatch, dataset, "table", "metrics", "--filter", "cluster")
    assert "--filter expects COLUMN=value" in str(exit_info.value)


def test_filter_value_with_a_slash_still_exports(
    dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A filter value is data (e.g. a model name), so it cannot be a filename."""
    _run(monkeypatch, dataset, "table", "metrics", "--filter", "model=gpt-4o/mini")
    assert list(dataset.glob("metrics_all_model=gpt-4o_mini.csv"))
