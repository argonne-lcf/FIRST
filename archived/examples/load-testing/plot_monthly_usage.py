import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

# --- Configuration ---
INTERACTIVE_CSV = "monthly_requests.csv"
BATCH_CSV = "batch_monthly_request.csv"
PLOT_OUTPUT_FILE = "monthly_usage_plot_combined.png"  # New output file name

# --- Plotting Style ---
plt.style.use("seaborn-v0_8-paper")
sns.set_context("paper", font_scale=1.2)

# Define standardized font sizes (similar to benchmark_analysis.py)
plot_title_fs = 18 + 2  # Making it slightly larger than benchmark plots
plot_axis_label_fs = 16 + 2
plot_tick_label_fs = 14 + 2
plot_legend_fs = 14 + 2


# --- Function to load and process data ---
def load_and_process(filepath, req_col_name, skip_footer=0):
    try:
        df = pd.read_csv(filepath, skipfooter=skip_footer, engine="python")
    except FileNotFoundError:
        print(f"Error: {filepath} not found.")
        return None
    except Exception as e:
        print(f"Error reading CSV file {filepath}: {e}")
        return None

    # Basic column check
    if "month_start" not in df.columns or req_col_name not in df.columns:
        print(
            f"Error: CSV {filepath} must contain 'month_start' and '{req_col_name}' columns."
        )
        return None

    # Convert month to datetime (use utc=True for consistency)
    df["month"] = pd.to_datetime(df["month_start"], errors="coerce", utc=True)
    df.dropna(subset=["month"], inplace=True)

    # Convert request count to numeric, coercing errors
    df[req_col_name] = pd.to_numeric(df[req_col_name], errors="coerce")
    df.dropna(subset=[req_col_name], inplace=True)
    df[req_col_name] = df[req_col_name].astype(int)

    # Keep only month and request count, rename request count
    df_processed = df[["month", req_col_name]].copy()
    df_processed.rename(columns={req_col_name: "requests"}, inplace=True)

    return df_processed


# --- Load Data ---
df_interactive = load_and_process(INTERACTIVE_CSV, "request_count", skip_footer=1)
df_batch = load_and_process(BATCH_CSV, "total_requests_in_completed")

if df_interactive is None or df_batch is None:
    print("Exiting due to errors loading data.")
    exit(1)

# --- Merge Data ---
print("Merging interactive and batch request data...")
df_combined = pd.merge(
    df_interactive.rename(columns={"requests": "requests_interactive"}),
    df_batch.rename(columns={"requests": "requests_batch"}),
    on="month",
    how="outer",
)

# Fill NaNs with 0 and calculate total
df_combined["requests_interactive"] = (
    df_combined["requests_interactive"].fillna(0).astype(int)
)
df_combined["requests_batch"] = df_combined["requests_batch"].fillna(0).astype(int)
df_combined["total_requests"] = (
    df_combined["requests_interactive"] + df_combined["requests_batch"]
)

# --- Totals and Sorting ---
interactive_total = df_combined["requests_interactive"].sum()
batch_total = df_combined["requests_batch"].sum()
grand_total = df_combined["total_requests"].sum()

print(f"Total Interactive Requests: {interactive_total:,}")
print(f"Total Batch Requests: {batch_total:,}")
print(f"Grand Total Requests: {grand_total:,}")

# Sort final data by month for plotting
df_combined_sorted = df_combined.sort_values("month")

# --- Plotting ---
fig, ax = plt.subplots(figsize=(12, 8))  # Increased figsize from (10, 6)

# Format month labels for x-axis
if pd.api.types.is_datetime64_any_dtype(df_combined_sorted["month"]):
    month_labels = df_combined_sorted["month"].dt.strftime("%Y-%m")
    x = np.arange(len(month_labels))  # the label locations
else:
    print("Error: 'month' column is not in datetime format after processing.")
    month_labels = range(len(df_combined_sorted))  # Fallback to numeric index
    x = np.arange(len(month_labels))

# Define bar width
bar_width = 0.35

# Create the grouped bar plot
rects1 = ax.bar(
    x - bar_width / 2,
    df_combined_sorted["requests_interactive"],
    bar_width,
    label="Interactive",
    color="#1f77b4",
)
rects2 = ax.bar(
    x + bar_width / 2,
    df_combined_sorted["requests_batch"],
    bar_width,
    label="Batch",
    color="#ff7f0e",
)


# Format y-axis to show millions (M) or thousands (k)
def millions_formatter(x, pos):
    if x >= 1e6:
        return f"{x * 1e-6:.1f}M"
    elif x >= 1e3:
        return f"{x * 1e-3:.0f}k"
    else:
        return f"{int(x)}"


ax.yaxis.set_major_formatter(ticker.FuncFormatter(millions_formatter))

# Add labels and title - Apply new font sizes
ax.set_xlabel("Month", fontsize=plot_axis_label_fs)
ax.set_ylabel(
    "Number of Inference Requests", fontsize=plot_axis_label_fs
)  # General Y-label
ax.set_title(
    "Monthly Inference Requests (Interactive vs. Batch)", fontsize=plot_title_fs
)  # Updated title
ax.set_xticks(x)
ax.set_xticklabels(month_labels, fontsize=plot_tick_label_fs)
ax.tick_params(axis="y", labelsize=plot_tick_label_fs)  # Apply font size to y ticks
plt.xticks(rotation=45, ha="right")  # Rotate x-axis labels
ax.grid(True, linestyle="--", alpha=0.6, axis="y")
ax.legend(fontsize=plot_legend_fs)

plt.tight_layout()

# Save the plot
plt.savefig(PLOT_OUTPUT_FILE, dpi=300)
print(f"Plot saved as {PLOT_OUTPUT_FILE}")

plt.close()  # Close the plot figure
