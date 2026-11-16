"""CLI: `uv run python -m tests.load {run,report,mock-upstream}` (see README.md)."""

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from .harness import Mode, ReplicaPoller, StepConfig, run_step
from .stats import Summary, comparison_table, steps_csv, steps_table

DEFAULT_TOKEN_CMD = "alcf-tokens get-token inference"


def _token(args: argparse.Namespace) -> str | None:
    """FIRST_TOKEN, else the output of --token-cmd; None with --no-auth."""
    if args.no_auth:
        return None
    if token := os.environ.get("FIRST_TOKEN"):
        return token
    try:
        result = subprocess.run(
            shlex.split(args.token_cmd), capture_output=True, text=True, check=True
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        sys.exit(
            f"Could not get an access token with {args.token_cmd!r} ({exc}).\n"
            "Refresh your login, set FIRST_TOKEN, or pass --no-auth."
        )
    return result.stdout.strip()


def _write_results(run_dir: Path, meta: dict[str, Any], steps: list[Summary]) -> str:
    """(Re)write the run's summary files; returns the markdown report."""
    stream = meta["mode"] == "stream"
    report = (
        f"# Load test: {meta['name']}\n\n"
        f"- target: `{meta['url']}` ({meta['mode']})\n"
        f"- model: `{meta['model']}`, about {meta['input_tokens']} tokens in, "
        f"{meta['output_tokens']} out\n"
        f"- each step: {meta['warmup']:.0f}s warm-up + {meta['duration']:.0f}s "
        f"measured, {meta['processes']} client processes\n"
        f"- started {meta['started_at']}\n\n"
        "Times are ms, shown as p50 / p95 / p99.  `overhead` = gateway − upstream "
        "and `network` = client − gateway − queue, both taken per request.\n\n"
        + steps_table(steps, stream=stream)
        + "\n"
    )
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=2) + "\n")
    (run_dir / "summary.json").write_text(json.dumps(steps, indent=2) + "\n")
    (run_dir / "summary.md").write_text(report)
    (run_dir / "steps.csv").write_text(steps_csv(steps))
    return report


def cmd_run(args: argparse.Namespace) -> None:
    mode: Mode = (
        "health" if args.model is None else "stream" if args.stream else "unary"
    )
    name = args.name or mode
    started = datetime.now()
    run_dir = Path(args.out) / f"{name}-{started:%Y%m%d-%H%M%S}"
    verify: bool | str = False if args.insecure else (args.ca or True)
    meta = {
        "name": name,
        "url": args.url,
        "mode": mode,
        "model": args.model,
        "input_tokens": args.input_tokens,
        "output_tokens": args.output_tokens,
        "warmup": args.warmup,
        "duration": args.duration,
        "processes": args.processes,
        "keepalive": not args.no_keepalive,
        "catalog_url": args.catalog_url,
        "started_at": started.isoformat(timespec="seconds"),
    }
    print(f"{name}: {mode} against {args.url}\nresults in {run_dir}/", flush=True)

    steps: list[Summary] = []
    for users in args.users:
        token = _token(args)  # per step: a long sweep can outlive a token
        poller = None
        if args.catalog_url:
            catalog_model = args.catalog_model or args.model
            if catalog_model is None:
                sys.exit("--catalog-url needs --model or --catalog-model.")
            poller = ReplicaPoller(args.catalog_url, catalog_model, token, verify)
        cfg = StepConfig(
            url=args.url,
            mode=mode,
            model=args.model,
            users=users,
            start_at=0.0,
            warmup=args.warmup,
            duration=args.duration,
            ramp=min(args.warmup, args.ramp),
            input_tokens=args.input_tokens,
            output_tokens=args.output_tokens,
            pin_output=not args.no_pin_output,
            unique_prompts=not args.cacheable_prompts,
            extra_body=json.loads(args.extra_body),
            token=token,
            timeout=args.timeout,
            keepalive=not args.no_keepalive,
            verify=verify,
            think_sec=args.think_ms / 1000,
            seed=users,
        )
        print(f"\n[{users} users] running {args.warmup + args.duration:.0f}s ...")
        step = run_step(
            cfg,
            processes=args.processes,
            out_dir=run_dir / f"users-{users:05d}",
            poller=poller,
            poll_interval=args.replica_poll,
        )
        steps.append(step)
        _write_results(run_dir, meta, steps)
        print(steps_table([step], stream=mode == "stream"), flush=True)
        if step["errors"]:
            print(f"  errors: {step['errors']}")
        if step["client_cpu"] > 0.85:
            print("  ⚠ a client process was CPU-bound: raise --processes.")
        if (rep := step.get("replicas")) and rep["min"] != rep["max"]:
            print("  ⚠ the replica count changed during this step.")
        if step["error_rate"] > args.stop_error_rate:
            print(
                f"  error rate {step['error_rate']:.0%} is above "
                f"--stop-error-rate; ending the sweep here."
            )
            break
        time.sleep(args.cooldown)

    print("\n" + _write_results(run_dir, meta, steps))
    print(f"Raw records and summaries: {run_dir}/")


def _load_run(path: str) -> dict[str, Any]:
    run_dir = Path(path)
    return {
        "meta": json.loads((run_dir / "meta.json").read_text()),
        "steps": json.loads((run_dir / "summary.json").read_text()),
    }


def cmd_report(args: argparse.Namespace) -> None:
    runs = [_load_run(path) for path in args.runs]
    parts = ["## Peak throughput\n", comparison_table(runs), ""]
    for run in runs:
        meta = run["meta"]
        parts += [
            f"## {meta['name']} ({meta['mode']}, `{meta['url']}`)\n",
            steps_table(run["steps"], stream=meta["mode"] == "stream"),
            "",
        ]
    report = "\n".join(parts)
    if args.out:
        Path(args.out).write_text(report)
    print(report)


def cmd_mock_upstream(args: argparse.Namespace) -> None:
    import uvicorn

    # Worker processes re-import the app, so its settings travel by environment.
    os.environ["MOCK_TTFT_MS"] = str(args.ttft_ms)
    os.environ["MOCK_TOKEN_MS"] = str(args.token_ms)
    os.environ["MOCK_TOKENS"] = str(args.tokens)
    uvicorn.run(
        "tests.load.mock_upstream:app",
        host=args.host,
        port=args.port,
        workers=args.workers,
        access_log=False,
        log_level="warning",
    )


def _user_counts(value: str) -> list[int]:
    return [int(v) for v in value.split(",")]


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m tests.load", description=__doc__)
    sub = parser.add_subparsers(required=True)

    run = sub.add_parser("run", help="sweep user counts against one URL")
    run.add_argument("--url", required=True, help="full URL to request")
    run.add_argument("--model", help="model name; omit to GET the URL (health)")
    run.add_argument("--stream", action="store_true", help="request an SSE stream")
    run.add_argument("--users", type=_user_counts, default=[1, 2, 4, 8, 16, 32])
    run.add_argument("--duration", type=float, default=60.0, help="measured seconds")
    run.add_argument("--warmup", type=float, default=10.0)
    run.add_argument("--ramp", type=float, default=5.0, help="spread user starts")
    run.add_argument("--cooldown", type=float, default=5.0, help="between steps")
    run.add_argument("--processes", type=int, default=min(8, os.cpu_count() or 1))
    run.add_argument("--input-tokens", type=int, default=32)
    run.add_argument("--output-tokens", type=int, default=8)
    run.add_argument(
        "--no-pin-output",
        action="store_true",
        help="omit vLLM's min_tokens/ignore_eos (output may stop early)",
    )
    run.add_argument(
        "--cacheable-prompts",
        action="store_true",
        help="drop the random prompt prefix that defeats prefix caching",
    )
    run.add_argument("--extra-body", default="{}", help="JSON merged into requests")
    run.add_argument("--think-ms", type=float, default=0.0, help="pause per request")
    run.add_argument("--timeout", type=float, default=300.0, help="per read, seconds")
    run.add_argument("--no-keepalive", action="store_true", help="reconnect each time")
    run.add_argument("--token-cmd", default=DEFAULT_TOKEN_CMD)
    run.add_argument("--no-auth", action="store_true", help="send no bearer token")
    run.add_argument("--insecure", action="store_true", help="skip TLS verification")
    run.add_argument("--ca", help="CA bundle for TLS verification")
    run.add_argument("--catalog-url", help="V2 base URL to read replica counts from")
    run.add_argument("--catalog-model", help="catalog model name (default: --model)")
    run.add_argument("--replica-poll", type=float, default=5.0, help="seconds")
    run.add_argument("--stop-error-rate", type=float, default=0.5)
    run.add_argument("--name", help="label for this run (default: the mode)")
    run.add_argument("--out", default="loadtest-runs", help="parent directory")
    run.set_defaults(func=cmd_run)

    report = sub.add_parser("report", help="compare finished runs")
    report.add_argument("runs", nargs="+", help="run directories, baseline first")
    report.add_argument("--out", help="also write the markdown here")
    report.set_defaults(func=cmd_report)

    mock = sub.add_parser("mock-upstream", help="serve the no-work model")
    mock.add_argument("--host", default="0.0.0.0")
    mock.add_argument("--port", type=int, default=9000)
    mock.add_argument("--workers", type=int, default=4)
    mock.add_argument("--ttft-ms", type=float, default=0.0)
    mock.add_argument("--token-ms", type=float, default=0.0)
    mock.add_argument("--tokens", type=int, default=16, help="when no max_tokens")
    mock.set_defaults(func=cmd_mock_upstream)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
