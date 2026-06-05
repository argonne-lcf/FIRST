import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import (
    LogFormatterMathtext,
)

# ==============================================================================
# Configuration Constants
# ==============================================================================

# --- Plotting Style ---
plt.style.use("seaborn-v0_8-paper")
sns.set_context("paper", font_scale=1.2)

# --- Colors ---
COLORS = {
    "FIRST": "#1f77b4",  # Blue
    "vLLM Direct": "#ff7f0e",  # Orange
    "OpenAI API": "#2ca02c",  # Green
    "FIRST (1 Instance)": "#1f77b4",  # Blue (for scaling base)
    "FIRST (2 Instances)": "#d62728",  # Red
    "FIRST (3 Instances)": "#9467bd",  # Purple
    "FIRST (4 Instances)": "#8c564b",  # Brown
}

# --- Standard Font Sizes ---
PLOT_TITLE_FS = 18
PLOT_AXIS_LABEL_FS = 16
PLOT_TICK_LABEL_FS = 14
PLOT_LEGEND_FS = 14
PLOT_BAR_LABEL_FS = 13

# --- Model Mappings ---
MODEL_SIZES_MAP = {
    "Llama 3.1-8B": 8,
    "Llama 3.3-70B": 70,
    "GPT-4o-mini": 8,
}

MODEL_CONFIGS_MAP = {
    "Llama 3.1-8B": "4 GPUs, TP=4",
    "Llama 3.3-70B": "8 GPUs, TP=8",
    "GPT-4o-mini": "Unknown",
}

# --- Metrics ---
METRICS_TO_PLOT_BARS = {
    "request_throughput": "Request TP (req/s)",
    "output_throughput": "Output TP (tok/s)",
    "median_e2e_latency_s": "Median Latency (s)",
    "duration": "Duration (s)",
}

METRICS_TO_PLOT_LINES = {
    "req_tp": "Request Throughput (req/s)",
    "out_tp": "Output Token Throughput (tok/s)",
    "med_lat_s": "Median E2E Latency (s)",
    "avg_duration": "Avg. Benchmark Duration (s)",
}

# --- Data ---

# Direct vLLM 70B data points (used if loading from external source fails)
VLLM_70B_REQ_TP_DEFAULT = 5.671305779088315
VLLM_70B_OUT_TP_DEFAULT = 1059.0426272696145
VLLM_70B_MEDIAN_LAT_MS_DEFAULT = 77366.89644446597
VLLM_70B_MEAN_LAT_MS_DEFAULT = 81216.03193316469
VLLM_70B_P99_LAT_MS_DEFAULT = 147133.43299815897

# Benchmark data (vLLM Direct + FIRST + OpenAI + FIRST Scaling)
# Removed 405B and GPT-4o entries
BENCHMARK_DATA = [
    # vLLM Direct - Llama 3.1-8B
    {
        "model": "Llama 3.1-8B",
        "approach": "vLLM Direct",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 130628,
        "request_throughput": 19.1553,
        "output_throughput": 2502.7224,
        "mean_e2e_latency_ms": 24236.5392,
        "median_e2e_latency_ms": 24365.2347,
        "p99_e2e_latency_ms": 43260.5075,
        "concurrency": 462.3320,
        "duration": 52.3364,
    },
    # vLLM Direct - Llama 3.3-70B
    {
        "model": "Llama 3.3-70B",
        "approach": "vLLM Direct",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 186737,
        "request_throughput": VLLM_70B_REQ_TP_DEFAULT,
        "output_throughput": VLLM_70B_OUT_TP_DEFAULT,
        "mean_e2e_latency_ms": VLLM_70B_MEAN_LAT_MS_DEFAULT,
        "median_e2e_latency_ms": VLLM_70B_MEDIAN_LAT_MS_DEFAULT,
        "p99_e2e_latency_ms": VLLM_70B_P99_LAT_MS_DEFAULT,
        "duration": 176.32623578282073,
    },
    # FIRST - Llama 3.1-8B
    {
        "model": "Llama 3.1-8B",
        "approach": "FIRST",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 130826,
        "request_throughput": 25.0958,
        "output_throughput": 3282.8795,
        "mean_e2e_latency_ms": 16433.4670,
        "median_e2e_latency_ms": 16259.3126,
        "p99_e2e_latency_ms": 33693.2355,
        "concurrency": 405.3132,
        "duration": 40.2809,
    },
    # FIRST - Llama 3.3-70B (Single Instance - Base for Scaling)
    {
        "model": "Llama 3.3-70B",
        "approach": "FIRST",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": (172497 + 172811 + 172417) / 3,
        "request_throughput": 8.2950,
        "output_throughput": 1431.5623,
        "mean_e2e_latency_ms": 55522.4392,
        "median_e2e_latency_ms": 54504.7951,
        "p99_e2e_latency_ms": 112323.0454,
        "concurrency": 460.2720,
        "duration": 120.6668,
    },
    # FIRST - Llama 3.3-70B (2 Instances)
    {
        "model": "Llama 3.3-70B",
        "approach": "FIRST (2 Instances)",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 172668.33333333334,
        "request_throughput": 14.573098812301948,
        "output_throughput": 2516.340291109982,
        "mean_e2e_latency_ms": 30160.809862608478,
        "median_e2e_latency_ms": 30141.87261151771,
        "p99_e2e_latency_ms": 62105.06021719231,
        "concurrency": 438.6690397633557,
        "duration": 68.75972741364967,
    },
    # FIRST - Llama 3.3-70B (3 Instances)
    {
        "model": "Llama 3.3-70B",
        "approach": "FIRST (3 Instances)",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 172827,
        "request_throughput": 20.918326112665973,
        "output_throughput": 3615.251547073722,
        "mean_e2e_latency_ms": 19470.86226827395,
        "median_e2e_latency_ms": 18778.019371908158,
        "p99_e2e_latency_ms": 43768.45521016511,
        "concurrency": 407.29784662255764,
        "duration": 47.80497228191234,
    },
    # FIRST - Llama 3.3-70B (4 Instances)
    {
        "model": "Llama 3.3-70B",
        "approach": "FIRST (4 Instances)",
        "request_rate": float("inf"),
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 296523,
        "total_output_tokens": 173007,
        "request_throughput": 23.87841979057548,
        "output_throughput": 4131.133772708092,
        "mean_e2e_latency_ms": 16169.353013928048,
        "median_e2e_latency_ms": 16049.96080591809,
        "p99_e2e_latency_ms": 36689.87011950463,
        "concurrency": 386.0985990085808,
        "duration": 41.878818145021796,
    },
    # OpenAI API - GPT-4o-mini
    {
        "model": "GPT-4o-mini",
        "approach": "OpenAI API",
        "request_rate": 7.0,
        "max_concurrency": None,
        "completed": 1000,
        "total_input_tokens": 297156,
        "total_output_tokens": 180238,
        "request_throughput": 6.65452212953161,
        "output_throughput": 1199.3977595825183,
        "mean_e2e_latency_ms": 2912.5246543023386,
        "median_e2e_latency_ms": 1967.2636660106946,
        "p99_e2e_latency_ms": 12415.200222365089,
        "concurrency": 19.381459764861315,
        "duration": 150.27375077200122,
    },
]

RATE_COMPARISON_DATA = [
    {
        "approach": "vLLM Direct",
        "request_rate": "1",
        "req_tp": 0.99,
        "out_tp": 180.61,
        "med_lat_ms": 2985.40,
        "avg_duration": 1010.27,
    },
    {
        "approach": "vLLM Direct",
        "request_rate": "5",
        "req_tp": 4.68,
        "out_tp": 849.54,
        "med_lat_ms": 5517.47,
        "avg_duration": 213.94,
    },
    {
        "approach": "vLLM Direct",
        "request_rate": "10",
        "req_tp": 6.24,
        "out_tp": 1135.78,
        "med_lat_ms": 36970.39,
        "avg_duration": 160.30,
    },
    {
        "approach": "vLLM Direct",
        "request_rate": "20",
        "req_tp": 6.28,
        "out_tp": 1143.28,
        "med_lat_ms": 63163.51,
        "avg_duration": 159.15,
    },
    {
        "approach": "vLLM Direct",
        "request_rate": "inf",
        "req_tp": 5.78,
        "out_tp": 1054.27,
        "med_lat_ms": 80171.55,
        "avg_duration": 173.40,
    },
    {
        "approach": "FIRST",
        "request_rate": "1",
        "req_tp": 0.98,
        "out_tp": 177.89,
        "med_lat_ms": 9197.30,
        "avg_duration": 1023.12,
    },
    {
        "approach": "FIRST",
        "request_rate": "5",
        "req_tp": 4.50,
        "out_tp": 819.81,
        "med_lat_ms": 12763.06,
        "avg_duration": 222.54,
    },
    {
        "approach": "FIRST",
        "request_rate": "10",
        "req_tp": 6.07,
        "out_tp": 1103.70,
        "med_lat_ms": 39288.83,
        "avg_duration": 164.79,
    },
    {
        "approach": "FIRST",
        "request_rate": "20",
        "req_tp": 7.56,
        "out_tp": 1379.22,
        "med_lat_ms": 49397.43,
        "avg_duration": 138.11,
    },
    {
        "approach": "FIRST",
        "request_rate": "inf",
        "req_tp": 9.22,
        "out_tp": 1677.49,
        "med_lat_ms": 46927.70,
        "avg_duration": 108.48,
    },
]

# --- Output Files ---
CSV_SUMMARY_FILE = "benchmark_summary.csv"
LATEX_SUMMARY_FILE = "benchmark_summary_latex.tex"
PLOT_FIRST_VLLM_FILE = "first_vs_vllm_comparison.png"
PLOT_FIRST_OPENAI_FILE = "first_vs_openai_comparison.png"
PLOT_RATE_COMPARISON_FILE = "rate_comparison_70b.png"
PLOT_SCALING_FILE = "first_scaling_70b_comparison.png"

# ==============================================================================
# Data Processing Function
# ==============================================================================


def preprocess_data(data):
    """Converts raw benchmark data into a processed Pandas DataFrame."""
    df = pd.DataFrame(data)

    # Convert latency columns to seconds
    latency_cols_ms = [
        "mean_e2e_latency_ms",
        "median_e2e_latency_ms",
        "p99_e2e_latency_ms",
    ]
    for col_ms in latency_cols_ms:
        col_s = col_ms.replace("_ms", "_s")
        if col_ms in df.columns:
            df[col_s] = df[col_ms] / 1000

    # Add model size and hardware config from maps
    if "model" in df.columns:
        df["model_size"] = df["model"].map(MODEL_SIZES_MAP)
        df["hardware_config"] = df["model"].map(MODEL_CONFIGS_MAP)

    # Convert specific rate comparison columns
    if "med_lat_ms" in df.columns:
        df["med_lat_s"] = df["med_lat_ms"] / 1000

    return df


# ==============================================================================
# Plotting Helper Functions
# ==============================================================================


def setup_log_axis(ax):
    """Applies standard log scale formatting to the Y axis."""
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(LogFormatterMathtext())
    ax.yaxis.set_minor_formatter(LogFormatterMathtext(minor_thresholds=(2, 0.5)))
    ax.grid(True, linestyle="--", alpha=0.6, axis="y")
    ax.margins(y=0.25)  # Add margin for bar labels


def add_bar_labels(ax, rects_list, labels_list):
    """Adds rotated labels above bars."""
    label_formats = {"median_e2e_latency_s": "%.2f", "default": "%.1f"}
    for i, rects in enumerate(rects_list):
        labels = labels_list[i]
        metric_keys = list(
            METRICS_TO_PLOT_BARS.keys()
        )  # Assumes bar plots use these metrics
        formatted_labels = []
        for idx, metric_key in enumerate(metric_keys):
            fmt = label_formats.get(metric_key, label_formats["default"])
            try:
                formatted_labels.append(fmt % labels[idx])
            except (TypeError, IndexError):
                formatted_labels.append(
                    "N/A"
                )  # Handle cases where data might be missing

        ax.bar_label(
            rects,
            labels=formatted_labels,
            padding=3,
            rotation=90,
            size=PLOT_BAR_LABEL_FS,
        )


def plot_comparison_bars(
    df_plot, metrics_to_plot, approaches, filename, fig_size, title_prefix=""
):
    """Generates grouped bar plots comparing different approaches for various models."""
    num_models = df_plot["model_size"].nunique()
    model_sizes = sorted(df_plot["model_size"].unique())

    fig, axes = plt.subplots(1, num_models, figsize=fig_size, sharey=False)
    if num_models == 1:
        axes = [axes]  # Make it iterable

    bar_width = 0.35
    metric_keys = list(metrics_to_plot.keys())
    x_indices = np.arange(len(metric_keys))

    for i, model_size in enumerate(model_sizes):
        ax = axes[i]
        model_data = df_plot[
            (df_plot["model_size"] == model_size)
            & (df_plot["approach"].isin(approaches))
        ].set_index("approach")

        rects_list = []
        labels_list = []
        approach_labels = []

        # Plot bars for each approach
        for j, approach in enumerate(approaches):
            if approach in model_data.index:
                approach_data = model_data.loc[approach]
                values = [
                    approach_data.get(key, np.nan) for key in metric_keys
                ]  # Use .get for safety
                position = x_indices + (j - (len(approaches) - 1) / 2) * bar_width
                rects = ax.bar(
                    position,
                    values,
                    bar_width,
                    label=approach,
                    color=COLORS.get(approach, "#808080"),
                )
                rects_list.append(rects)
                labels_list.append(values)
                approach_labels.append(approach)
            else:
                print(
                    f"Warning: Data for approach '{approach}' not found for model size {model_size}."
                )

        if not rects_list:  # Skip if no data was plotted for this model size
            ax.text(
                0.5,
                0.5,
                "Data unavailable",
                horizontalalignment="center",
                verticalalignment="center",
                transform=ax.transAxes,
            )
            ax.set_xticks([])
            ax.set_yticks([])
        else:
            # Add bar labels
            add_bar_labels(ax, rects_list, labels_list)

            # Configure axes
            ax.set_ylabel("Metric Value", fontsize=PLOT_AXIS_LABEL_FS)
            ax.set_title(
                f"{title_prefix}{model_size}B Model Comparison", fontsize=PLOT_TITLE_FS
            )
            ax.set_xticks(x_indices)
            ax.set_xticklabels(
                [metrics_to_plot[key] for key in metric_keys],
                rotation=45,
                ha="right",
                fontsize=PLOT_TICK_LABEL_FS,
            )
            ax.tick_params(axis="y", labelsize=PLOT_TICK_LABEL_FS)
            setup_log_axis(ax)

            # Add legend
            handles, labels = ax.get_legend_handles_labels()
            if handles:
                ax.legend(
                    handles, labels, loc="best", fontsize=PLOT_LEGEND_FS, frameon=True
                )

    plt.tight_layout(rect=[0, 0.02, 1, 0.95])  # Adjusted bottom margin slightly
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Plot saved as {filename}")


def plot_rate_comparison_lines(df_rate, metrics_to_plot, filename, fig_size):
    """Generates line plots comparing performance across different request rates."""
    rate_order = ["1", "5", "10", "20", "inf"]
    num_metrics = len(metrics_to_plot)
    fig, axes = plt.subplots(1, num_metrics, figsize=fig_size)  # Removed sharex=True

    # Define larger font sizes specifically for this plot
    title_fs_rate = PLOT_TITLE_FS + 4
    axis_label_fs_rate = PLOT_AXIS_LABEL_FS + 4
    tick_label_fs_rate = PLOT_TICK_LABEL_FS + 4
    legend_fs_rate = PLOT_LEGEND_FS + 4

    for i, (metric_key, metric_name) in enumerate(metrics_to_plot.items()):
        ax = axes[i]
        sns.lineplot(
            data=df_rate,
            x="request_rate",
            y=metric_key,
            hue="approach",
            style="approach",
            markers=True,
            dashes=False,
            ax=ax,
            palette=COLORS,
            sort=False,
        )

        ax.set_title(metric_name, fontsize=title_fs_rate)
        ax.set_xlabel("Request Rate (req/s)", fontsize=axis_label_fs_rate)
        ax.set_ylabel(
            metric_name.split("(")[-1].split(")")[0].capitalize(),
            fontsize=axis_label_fs_rate,
        )  # Extract unit
        ax.grid(True, linestyle="--", alpha=0.6)
        ax.legend(fontsize=legend_fs_rate)
        ax.set_xticks(range(len(rate_order)))  # Need to set ticks for each ax
        ax.set_xticklabels(rate_order, fontsize=tick_label_fs_rate)
        ax.tick_params(axis="y", labelsize=tick_label_fs_rate)
        setup_log_axis(ax)

    plt.tight_layout(rect=[0, 0.05, 1, 0.97])  # Increased bottom margin
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Plot saved as {filename}")


def plot_scaling_bars(
    df_plot, metrics_to_plot, model_to_plot, approaches, filename, fig_size
):
    """Generates a grouped bar plot specifically for scaling comparison."""
    scaling_data = df_plot[
        (df_plot["model"] == model_to_plot) & (df_plot["approach"].isin(approaches))
    ].set_index("approach")

    # Rename 'FIRST' approach to 'FIRST (1 Instance)' for the legend
    scaling_data = scaling_data.rename(index={"FIRST": "FIRST (1 Instance)"})
    approaches_renamed = [
        "FIRST (1 Instance)" if a == "FIRST" else a for a in approaches
    ]

    fig, ax = plt.subplots(1, 1, figsize=fig_size)

    bar_width_scaling = 0.20  # Specific bar width for scaling plot
    metric_keys = list(metrics_to_plot.keys())
    x_indices = np.arange(len(metric_keys))

    rects_list = []
    labels_list = []
    all_approaches_present = True

    # Plot bars for each scaling approach
    for j, approach in enumerate(approaches_renamed):
        if approach in scaling_data.index:
            approach_data = scaling_data.loc[approach]
            values = [approach_data.get(key, np.nan) for key in metric_keys]
            position = (
                x_indices + (j - (len(approaches_renamed) - 1) / 2) * bar_width_scaling
            )
            rects = ax.bar(
                position,
                values,
                bar_width_scaling,
                label=approach,
                color=COLORS.get(approach, "#808080"),
            )
            rects_list.append(rects)
            labels_list.append(values)
        else:
            print(
                f"Warning: Data for scaling approach '{approach}' not found for model {model_to_plot}."
            )
            all_approaches_present = False

    if not all_approaches_present or not rects_list:
        ax.text(
            0.5,
            0.5,
            "Data unavailable",
            horizontalalignment="center",
            verticalalignment="center",
            transform=ax.transAxes,
        )
        ax.set_xticks([])
        ax.set_yticks([])
    else:
        # Add bar labels
        add_bar_labels(ax, rects_list, labels_list)

        # Configure axes
        ax.set_ylabel("Metric Value", fontsize=PLOT_AXIS_LABEL_FS)
        ax.set_title(
            f"FIRST Scaling Performance ({model_to_plot})", fontsize=PLOT_TITLE_FS
        )
        ax.set_xticks(x_indices)
        ax.set_xticklabels(
            [metrics_to_plot[key] for key in metric_keys],
            rotation=45,
            ha="right",
            fontsize=PLOT_TICK_LABEL_FS,
        )
        ax.tick_params(axis="y", labelsize=PLOT_TICK_LABEL_FS)
        setup_log_axis(ax)

        # Add legend
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(
                handles, labels, loc="best", fontsize=PLOT_LEGEND_FS, frameon=True
            )

    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Plot saved as {filename}")


# ==============================================================================
# Table Generation Function
# ==============================================================================


def generate_summary_tables(df_summary, csv_filename, latex_filename):
    """Generates CSV and LaTeX summary tables."""
    # Exclude models/approaches if necessary (already done in data definition)
    summary_df_sorted = df_summary.sort_values(by=["model_size", "approach"])

    summary_df_filtered = summary_df_sorted[
        [
            "model",
            "approach",
            "request_throughput",
            "output_throughput",
            "median_e2e_latency_s",
            "p99_e2e_latency_ms",
            "duration",
            "hardware_config",
        ]
    ].copy()  # Use copy to avoid SettingWithCopyWarning

    # --- CSV Output ---
    summary_df_filtered.to_csv(csv_filename, index=False, float_format="%.2f")
    print(f"CSV summary saved as {csv_filename}")

    # --- LaTeX Output ---
    header_map = {
        "model": "Model",
        "approach": "Approach",
        "request_throughput": "Req TP (req/s)",
        "output_throughput": "Output TP (tok/s)",
        "median_e2e_latency_s": "Median Latency (s)",
        "p99_e2e_latency_ms": "P99 Latency (ms)",
        "duration": "Duration (s)",
        "hardware_config": "Hardware Config",
    }
    latex_df = summary_df_filtered.rename(columns=header_map).copy()

    # Select only main approaches for the primary table
    latex_df_main = latex_df[latex_df["Approach"].isin(["FIRST", "vLLM Direct"])].copy()

    # Format P99 Latency to integer ms
    latex_df_main["P99 Latency (ms)"] = (
        latex_df_main["P99 Latency (ms)"].fillna(-1).astype(int)
    )
    latex_df_main.loc[latex_df_main["P99 Latency (ms)"] == -1, "P99 Latency (ms)"] = (
        "N/A"
    )

    # Define column format and generate LaTeX string
    col_formats = "llrrrrrL"
    try:
        latex_table = latex_df_main.to_latex(
            index=False,
            float_format="%.2f",
            header=True,  # Use column names from dataframe
            escape=False,
            column_format=col_formats,
            caption="Benchmark Comparison: FIRST vs. vLLM Direct",
            label="tab:benchmark_summary",
            position="!htbp",
        )

        # Add booktabs package and midrules for grouped headers
        latex_table = latex_table.replace(
            r"\begin{tabular}", r"\usepackage{booktabs}" + "\n" + r"\begin{tabular}"
        )
        lines = latex_table.splitlines()
        header_line_index = -1
        for idx, line in enumerate(lines):
            if r"\toprule" in line:
                header_line_index = idx + 1
                break

        if header_line_index != -1:
            # Create multi-column headers
            multicolumn_header = r" & & \multicolumn{2}{c}{Throughput} & \multicolumn{2}{c}{Latency} & & \\"
            cmidrule_header = r" \cmidrule(lr){3-4} \cmidrule(lr){5-6}"
            # Insert after the main header line derived from dataframe
            main_header_line = lines[header_line_index]
            lines.insert(header_line_index, multicolumn_header + cmidrule_header)
            # Make the original header span correctly
            # Need to count columns to generate the correct multicolumn spec
            # Example: Model & Approach & Req TP & Out TP & Med Lat & P99 Lat & Dur & HW Config \\
            # Becomes: \multicolumn{1}{l}{Model} & \multicolumn{1}{l}{Approach} & ... & \multicolumn{1}{l}{Hardware Config} \\
            original_headers = [h.strip() for h in main_header_line.split("&")]
            num_cols = len(original_headers)
            new_main_header_parts = []
            # Adjust based on actual headers and col_formats
            if len(col_formats) == num_cols:
                col_align = list(col_formats)
                new_main_header_parts = [
                    f"\multicolumn{{1}}{{{align}}}{{{head}}}"
                    for align, head in zip(col_align, original_headers)
                ]
                lines[header_line_index + 2] = (
                    " & ".join(new_main_header_parts) + r" \\"
                )  # Replace original header
            else:
                print(
                    "Warning: Column format length mismatch, skipping header modification."
                )

            latex_table = "\n".join(lines)

        with open(latex_filename, "w") as f:
            f.write(latex_table)
        print(f"LaTeX summary saved as {latex_filename}")

    except Exception as e:
        print(f"Error generating LaTeX table: {e}")


# ==============================================================================
# Main Execution
# ==============================================================================


def main():
    """Main function to process data, generate plots, and create tables."""
    print("Starting benchmark analysis...")

    # --- Preprocess Data ---
    df_processed = preprocess_data(BENCHMARK_DATA)
    df_rate_processed = preprocess_data(RATE_COMPARISON_DATA)

    # --- Generate Plots ---

    # Plot 1: FIRST vs vLLM Direct Comparison
    plot_comparison_bars(
        df_plot=df_processed,
        metrics_to_plot=METRICS_TO_PLOT_BARS,
        approaches=["vLLM Direct", "FIRST"],
        filename=PLOT_FIRST_VLLM_FILE,
        fig_size=(12, 7),
        title_prefix="",
    )

    # Plot 2: FIRST vs OpenAI Comparison
    plot_comparison_bars(
        df_plot=df_processed[
            df_processed["model_size"] == 8
        ],  # Only 8B comparison left
        metrics_to_plot=METRICS_TO_PLOT_BARS,
        approaches=["FIRST", "OpenAI API"],
        filename=PLOT_FIRST_OPENAI_FILE,
        fig_size=(10, 7),  # Adjusted size
        title_prefix="~",
    )

    # Plot 3: Performance vs. Request Rate
    plot_rate_comparison_lines(
        df_rate=df_rate_processed,
        metrics_to_plot=METRICS_TO_PLOT_LINES,
        filename=PLOT_RATE_COMPARISON_FILE,
        fig_size=(24, 9),  # Uses specific larger fonts inside function
    )

    # Plot 4: FIRST Scaling Comparison
    plot_scaling_bars(
        df_plot=df_processed,
        metrics_to_plot=METRICS_TO_PLOT_BARS,
        model_to_plot="Llama 3.3-70B",
        approaches=[
            "FIRST",
            "FIRST (2 Instances)",
            "FIRST (3 Instances)",
            "FIRST (4 Instances)",
        ],
        filename=PLOT_SCALING_FILE,
        fig_size=(12, 9),  # Adjusted size
    )

    # --- Generate Summary Tables ---
    generate_summary_tables(
        df_summary=df_processed,
        csv_filename=CSV_SUMMARY_FILE,
        latex_filename=LATEX_SUMMARY_FILE,
    )

    print("\nBenchmark analysis complete.")


if __name__ == "__main__":
    main()

# --- End Scaling Plot ---
