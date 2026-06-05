import json

import globus_compute_sdk


def chunked_vllm_inference_function(parameters):
    import os
    import re
    import signal
    import subprocess
    import sys
    import time
    import uuid
    from datetime import datetime

    # ---------------------------
    # Helper: run one chunk
    # ---------------------------
    def run_chunk_inference(
        lines_buffer,
        model_name,
        base_name,
        batch_id,
        final_output_file,
        token_pattern,
        chunk_index,
    ):
        """Run vLLM batch inference on one chunk and append results."""
        unique_id = uuid.uuid4().hex[:6]
        tmp_dir = os.path.join("/tmp", os.environ.get("USER", "gcuser"))
        os.makedirs(tmp_dir, exist_ok=True)

        chunk_prefix = f"{batch_id}_chunk{chunk_index}_{unique_id}_{base_name}"
        chunk_input = os.path.join(tmp_dir, f"{chunk_prefix}.input.jsonl")
        chunk_output = os.path.join(tmp_dir, f"{chunk_prefix}.output.jsonl")
        chunk_log = os.path.join(tmp_dir, f"{chunk_prefix}.log")

        with open(chunk_input, "w") as cf:
            cf.writelines(lines_buffer)

        cmd = [
            "python",
            "-m",
            "vllm.entrypoints.openai.run_batch",
            "-i",
            chunk_input,
            "-o",
            chunk_output,
            "--model",
            model_name,
            "--tensor-parallel-size",
            "8",
            "--max-model-len",
            "28672",
            "--trust-remote-code",
        ]

        # Dynamic environment per model type
        env = os.environ.copy()
        if "gpt-oss" in model_name.lower():
            env["VLLM_ATTENTION_BACKEND"] = "TRITON_ATTN"

        start_t = datetime.now()
        with open(chunk_log, "wb") as lf:
            completed = subprocess.run(cmd, stdout=lf, stderr=lf, env=env)
        end_t = datetime.now()
        elapsed = (end_t - start_t).total_seconds()

        if completed.returncode != 0:
            with open(chunk_log, "r", errors="replace") as lf:
                tail = "".join(lf.readlines()[-40:])  # last 40 lines
            raise RuntimeError(
                f"[ERROR] vLLM failed for chunk {chunk_index}. "
                f"Exit {completed.returncode}, duration {elapsed:.1f}s\n"
                f"Last log lines:\n{tail}"
            )

        # Parse output
        tokens = 0
        responses = 0
        with open(chunk_output, "r") as cof, open(final_output_file, "a") as fout:
            for line in cof:
                fout.write(line)
                match = token_pattern.search(line)
                if match:
                    tokens += int(match.group(1))
                    responses += 1

        # Clean up temporary files
        for f in (chunk_input, chunk_output):
            try:
                os.remove(f)
            except FileNotFoundError:
                pass

        return tokens, responses, elapsed, chunk_log

    # ---------------------------
    # Main batch orchestration
    # ---------------------------
    model_params = parameters.get("model_params", {})
    model_name = model_params.get("model")
    input_file = model_params.get("input_file")
    output_dir = model_params.get(
        "output_folder_path",
        "/lus/eagle/projects/argonne_tpc/inference-service-batch-results/",
    )
    chunk_size = model_params.get("chunk_size", 20000)
    batch_id = parameters.get("batch_id", f"batch_{uuid.uuid4().hex[:6]}")

    if not (model_name and input_file):
        raise ValueError(
            "Both 'model' and 'input_file' must be provided in model_params."
        )

    # Prepare directories
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(
        output_dir, f"{base_name}_{model_name.split('/')[-1]}_{batch_id}"
    )
    os.makedirs(output_dir, exist_ok=True)

    final_output_file = os.path.join(
        output_dir, f"{base_name}_{timestamp}.results.jsonl"
    )
    progress_file = os.path.join(output_dir, f"{base_name}_{timestamp}.progress.json")
    token_pattern = re.compile(r'"total_tokens":\s*(\d+)')

    # Load or initialize progress
    progress = {
        "lines_processed": 0,
        "total_tokens": 0,
        "num_responses": 0,
        "chunks": [],
    }
    if os.path.exists(progress_file):
        with open(progress_file) as pf:
            progress.update(json.load(pf))

    lines_done = progress["lines_processed"]
    total_tokens = progress["total_tokens"]
    total_resps = progress["num_responses"]
    chunk_idx = len(progress["chunks"])

    # --- Signal handler for safe checkpointing ---
    def checkpoint():
        with open(progress_file, "w") as pf:
            json.dump(progress, pf, indent=2)
        os.chmod(progress_file, 0o666)

    def sigterm_handler(signum, frame):
        print("[INFO] SIGTERM received, saving progress...")
        checkpoint()
        sys.exit(0)

    signal.signal(signal.SIGTERM, sigterm_handler)

    start_all = time.time()
    print(f"[INFO] Starting batch inference on {model_name}, chunk size={chunk_size}")

    with open(input_file, "r") as infile:
        # Skip already processed lines
        for _ in range(lines_done):
            next(infile)

        buffer = []
        for line in infile:
            buffer.append(line)
            if len(buffer) >= chunk_size:
                chunk_tokens, chunk_resps, dur, log_file = run_chunk_inference(
                    buffer,
                    model_name,
                    base_name,
                    batch_id,
                    final_output_file,
                    token_pattern,
                    chunk_idx,
                )
                total_tokens += chunk_tokens
                total_resps += chunk_resps
                lines_done += len(buffer)

                progress.update(
                    {
                        "lines_processed": lines_done,
                        "total_tokens": total_tokens,
                        "num_responses": total_resps,
                    }
                )
                progress["chunks"].append(
                    {
                        "chunk_index": chunk_idx,
                        "lines": len(buffer),
                        "tokens": chunk_tokens,
                        "responses": chunk_resps,
                        "time_sec": dur,
                        "log": log_file,
                    }
                )
                checkpoint()
                print(
                    f"[✓] Chunk {chunk_idx} done in {dur:.1f}s "
                    f"({chunk_tokens} tokens, {chunk_resps} responses)"
                )
                chunk_idx += 1
                buffer = []

        # Handle remaining lines
        if buffer:
            chunk_tokens, chunk_resps, dur, log_file = run_chunk_inference(
                buffer,
                model_name,
                base_name,
                batch_id,
                final_output_file,
                token_pattern,
                chunk_idx,
            )
            total_tokens += chunk_tokens
            total_resps += chunk_resps
            lines_done += len(buffer)
            progress["chunks"].append(
                {
                    "chunk_index": chunk_idx,
                    "lines": len(buffer),
                    "tokens": chunk_tokens,
                    "responses": chunk_resps,
                    "time_sec": dur,
                    "log": log_file,
                }
            )
            checkpoint()

    total_time = time.time() - start_all
    throughput = total_tokens / total_time if total_time > 0 else 0.0

    summary = {
        "results_file": final_output_file,
        "progress_file": progress_file,
        "total_tokens": total_tokens,
        "num_responses": total_resps,
        "lines_processed": lines_done,
        "duration_sec": total_time,
        "throughput_tokens_per_sec": throughput,
    }

    with open(progress_file, "w") as pf:
        json.dump(summary, pf, indent=2)
    os.chmod(progress_file, 0o666)

    print("[INFO] ✅ Completed all chunks.")
    print(json.dumps(summary, indent=2))
    return json.dumps(summary, indent=2)


# Register with Globus Compute
gcc = globus_compute_sdk.Client()
FUNC_ID = gcc.register_function(chunked_vllm_inference_function)
print("Registered Function ID:", FUNC_ID)
with open("vllm_inference_function_batch_single_node.txt", "w") as f:
    f.write(FUNC_ID + "\n")
