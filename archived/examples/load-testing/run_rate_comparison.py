import argparse
import asyncio
import csv
import json
import os
import random
import statistics
import time

import numpy as np

# Import necessary functions and classes from benchmark_serving
# Assuming benchmark_serving.py is in the same directory or accessible in PYTHONPATH
from benchmark_serving import (
    benchmark,
    get_dataset,
    get_tokenizer,
    set_global_args,  # Import the function to set global args
    set_ulimit,
)

# --- Configuration ---
# Remove hardcoded constants, will use args instead
# MODEL_ID = "meta-llama/Llama-3.3-70B-Instruct"
# REQUEST_RATES = [1.0, 5.0, 10.0, 20.0, float('inf')] # Use arg or keep as default
# NUM_RUNS_PER_CONFIG = 3
# NUM_PROMPTS = 1000
# DATASET_NAME = "sharegpt"
# DEFAULT_SEED = 42
# OUTPUT_CSV_FILE = "rate_comparison_results.csv"

# Define defaults here if needed for argparse
DEFAULT_MODEL_ID = "meta-llama/Llama-3.3-70B-Instruct"
DEFAULT_REQUEST_RATES_STR = (
    "1.0,5.0,10.0,20.0,inf"  # Use comma-separated string for arg
)
DEFAULT_NUM_RUNS_PER_CONFIG = 3
DEFAULT_NUM_PROMPTS = 1000
DEFAULT_DATASET_NAME = "sharegpt"
DEFAULT_SEED = 42
DEFAULT_OUTPUT_CSV_FILE = "rate_comparison_results.csv"

# Metrics to average
METRICS_TO_AVERAGE = [
    "request_throughput",
    "output_throughput",
    "median_e2e_latency_ms",
    "duration",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run benchmark comparisons at different request rates."
    )
    # parser.add_argument("--first-url", required=True, help="Base URL for the FIRST API endpoint (e.g., http://first-gateway/.../v1/chat/completions)")
    # parser.add_argument("--direct-url", required=True, help="Base URL for the direct vLLM endpoint (e.g., http://vllm-server:8000/v1/chat/completions)")
    parser.add_argument(
        "--target-url",
        required=True,
        help="Base URL for the target API endpoint (FIRST or Direct vLLM)",
    )
    parser.add_argument(
        "--target-name",
        required=True,
        choices=["FIRST", "vLLM_Direct"],
        help="Name for the target being benchmarked (e.g., FIRST, vLLM_Direct)",
    )
    parser.add_argument(
        "--dataset-path", required=True, help="Path to the ShareGPT JSON dataset file."
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL_ID,
        help=f"Name or path of the model (default: {DEFAULT_MODEL_ID})",
    )
    parser.add_argument(
        "--tokenizer-path",
        default=None,
        help="Path or name of the tokenizer (default: same as --model)",
    )
    parser.add_argument(
        "--dataset-name",
        type=str,
        default=DEFAULT_DATASET_NAME,
        choices=["sharegpt", "random", "generated-shared-prefix"],
        help=f"Dataset name (default: {DEFAULT_DATASET_NAME})",
    )
    parser.add_argument(
        "--num-prompts",
        type=int,
        default=DEFAULT_NUM_PROMPTS,
        help=f"Number of prompts per run (default: {DEFAULT_NUM_PROMPTS})",
    )
    parser.add_argument(
        "--runs-per-config",
        type=int,
        default=DEFAULT_NUM_RUNS_PER_CONFIG,
        help=f"Number of runs per config (default: {DEFAULT_NUM_RUNS_PER_CONFIG})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--disable-tqdm", action="store_true", help="Disable progress bars."
    )
    parser.add_argument(
        "--disable-stream",
        action="store_true",
        help="Disable streaming mode in benchmark calls.",
    )
    parser.add_argument(
        "--disable-ssl-verification",
        action="store_true",
        help="Disable SSL verification in benchmark calls.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help="Maximum concurrent requests for benchmark runs.",
    )
    parser.add_argument(
        "--extra-request-body",
        type=str,
        default="{}",
        help="JSON string for extra request body params.",
    )
    parser.add_argument(
        "--output-csv-file",
        type=str,
        default=DEFAULT_OUTPUT_CSV_FILE,
        help=f"Name for the output summary CSV file (default: {DEFAULT_OUTPUT_CSV_FILE})",
    )
    parser.add_argument(
        "--request-rates",
        type=str,
        default=DEFAULT_REQUEST_RATES_STR,
        help=f"Comma-separated list of request rates (req/s) to test, use 'inf' for infinity (default: {DEFAULT_REQUEST_RATES_STR})",
    )

    # Add other relevant args from benchmark_serving if needed, e.g., related to dataset sampling
    parser.add_argument("--sharegpt-output-len", type=int, default=None)
    parser.add_argument("--sharegpt-context-len", type=int, default=None)
    # Add random dataset args just for compatibility with get_dataset, though we use sharegpt
    parser.add_argument("--random-input-len", type=int, default=1024)
    parser.add_argument("--random-output-len", type=int, default=1024)
    parser.add_argument("--random-range-ratio", type=float, default=0.0)
    # Add generated-shared-prefix args for compatibility
    parser.add_argument("--gsp-num-groups", type=int, default=64)
    parser.add_argument("--gsp-prompts-per-group", type=int, default=16)
    parser.add_argument("--gsp-system-prompt-len", type=int, default=2048)
    parser.add_argument("--gsp-question-len", type=int, default=128)
    parser.add_argument("--gsp-output-len", type=int, default=256)

    # Required args by the imported benchmark function signature (even if not used by all backends)
    parser.add_argument(
        "--backend",
        type=str,
        default="vllm",
        help="Benchmark backend (keep as vllm for OpenAI compat)",
    )  # Keep compatible
    parser.add_argument("--lora-name", type=str, default="")
    parser.add_argument(
        "--prompt-suffix",
        type=str,
        default="",
        help="Suffix added to prompts (used by get_dataset).",
    )
    parser.add_argument(
        "--apply-chat-template",
        action="store_true",
        help="Apply chat template (used by get_dataset).",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Path for detailed JSONL output (from benchmark_serving, typically not used by this script).",
    )
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--pd-seperated", action="store_true")

    return parser.parse_args()


async def run_single_benchmark(config_args, tokenizer, input_requests):
    """Runs one instance of the benchmark and returns results."""
    print(f"  Running benchmark for rate: {config_args.request_rate}...")
    start_time = time.time()

    # Use the provided target URL directly
    api_url = config_args.target_url
    base_url_for_metrics = (
        config_args.target_url
    )  # Assumes metrics/info endpoints use the same base

    try:
        extra_body = json.loads(config_args.extra_request_body)
    except json.JSONDecodeError:
        print(
            f"Warning: Invalid JSON in extra-request-body: {config_args.extra_request_body}. Using empty dict."
        )
        extra_body = {}

    results = await benchmark(
        backend=config_args.backend,  # Should be vllm or openai compatible
        api_url=api_url,  # Pass the target URL directly
        base_url=base_url_for_metrics,  # For potential metrics/info calls inside benchmark
        model_id=config_args.model,  # Use the model from args
        tokenizer=tokenizer,
        input_requests=input_requests,
        request_rate=config_args.request_rate,
        max_concurrency=config_args.max_concurrency,
        disable_tqdm=config_args.disable_tqdm,
        lora_name=config_args.lora_name,
        extra_request_body=extra_body,
        profile=config_args.profile,
        pd_seperated=config_args.pd_seperated,
        # disable_stream and disable_ssl_verification are handled by global args now
    )
    run_duration = time.time() - start_time
    print(
        f"  Run completed in {run_duration:.2f}s. Throughput: {results.get('request_throughput', 0):.2f} req/s"
    )
    return results


def main():
    script_args = parse_args()

    # --- Setup ---
    # Set global args for benchmark_serving module *before* any benchmark calls
    set_global_args(script_args)

    set_ulimit()
    random.seed(script_args.seed)
    np.random.seed(script_args.seed)
    print("Initializing tokenizer...")
    tokenizer_path_resolved = (
        script_args.tokenizer_path if script_args.tokenizer_path else script_args.model
    )
    tokenizer = get_tokenizer(tokenizer_path_resolved)
    print("Loading dataset...")
    # Pass necessary args for get_dataset - it expects the full namespace
    input_requests = get_dataset(script_args, tokenizer)
    # Limit prompts if needed
    if len(input_requests) > script_args.num_prompts:
        print(f"Sampling {script_args.num_prompts} prompts from dataset...")
        input_requests = random.sample(input_requests, script_args.num_prompts)
    elif len(input_requests) < script_args.num_prompts:
        print(
            f"Warning: Dataset only contains {len(input_requests)} prompts, requested {script_args.num_prompts}. Using available prompts."
        )
        script_args.num_prompts = len(input_requests)

    print("Setup complete. Starting benchmark loops...")

    all_averaged_results = []
    target_name = script_args.target_name
    target_url = script_args.target_url

    # Parse request rates from comma-separated string arg
    request_rates_to_test = []
    for rate_str in script_args.request_rates.split(","):
        rate_str = rate_str.strip().lower()
        if rate_str == "inf":
            request_rates_to_test.append(float("inf"))
        else:
            try:
                request_rates_to_test.append(float(rate_str))
            except ValueError:
                print(f"Warning: Invalid request rate '{rate_str}', skipping.")
    if not request_rates_to_test:
        print("Error: No valid request rates provided.")
        exit(1)

    print(f"\n--- Testing Target: {target_name} ({target_url}) ---")
    for rate in request_rates_to_test:
        print(
            f"---\\nTesting Rate: {rate if rate != float('inf') else 'Infinity'} req/s ---"
        )
        run_results = []
        for i in range(script_args.runs_per_config):
            print(f"  Starting run {i + 1}/{script_args.runs_per_config}...")
            # Create a copy of args to modify for this specific run
            current_run_args = argparse.Namespace(**vars(script_args))
            current_run_args.request_rate = rate
            # current_run_args.target = target_name # No longer needed inside run_single_benchmark

            # Run the benchmark for the current config
            # Pass the single target URL
            result_dict = asyncio.run(
                run_single_benchmark(current_run_args, tokenizer, input_requests)
            )
            run_results.append(result_dict)
            time.sleep(2)  # Small delay between runs

        # --- Calculate Averages ---
        if not run_results:
            print("  No results collected for this configuration.")
            continue

        averaged_metrics = {
            "target": target_name,  # Use the target_name from args
            "request_rate": rate,
        }
        print(f"  Averaging results for {target_name} at rate {rate}...")
        for metric_key in METRICS_TO_AVERAGE:
            values = [
                r.get(metric_key)
                for r in run_results
                if r and r.get(metric_key) is not None
            ]
            if values:
                averaged_metrics[f"avg_{metric_key}"] = statistics.mean(values)
            else:
                averaged_metrics[f"avg_{metric_key}"] = None
            print(
                f"    Avg {metric_key}: {averaged_metrics[f'avg_{metric_key}']:.2f}"
                if averaged_metrics[f"avg_{metric_key}"] is not None
                else f"    Avg {metric_key}: N/A"
            )

        all_averaged_results.append(averaged_metrics)

    # --- Save Results to CSV ---
    output_csv_filepath = script_args.output_csv_file
    print(f"\nAppending averaged results to {output_csv_filepath}...")
    if all_averaged_results:
        header = ["target", "request_rate"] + [
            f"avg_{m}" for m in METRICS_TO_AVERAGE
        ]  # + [f"stdev_{m}" for m in METRICS_TO_AVERAGE]
        try:
            # Check if file exists to append or write header
            file_exists = os.path.isfile(output_csv_filepath)
            with open(
                output_csv_filepath, "a", newline=""
            ) as csvfile:  # Open in append mode 'a'
                writer = csv.DictWriter(csvfile, fieldnames=header)
                if not file_exists or os.path.getsize(output_csv_filepath) == 0:
                    writer.writeheader()  # Write header only if file is new/empty
                # Format infinity for CSV
                for row in all_averaged_results:
                    if row["request_rate"] == float("inf"):
                        row["request_rate"] = "inf"
                    writer.writerow(row)
            print("Results saved successfully.")
        except Exception as e:
            print(f"Error writing results to CSV: {e}")
    else:
        print("No results to save.")


if __name__ == "__main__":
    main()
