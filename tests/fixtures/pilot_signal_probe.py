"""No-model, no-network PBS signal witness; never submits or signals a job."""

import argparse
import json
import os
import re
import signal
import socket
import time
from types import FrameType


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("plain", "exec"), required=True)
    args = parser.parse_args()
    job_id = os.environ.get("PBS_JOBID", "")
    if re.fullmatch(r"[0-9]+\.tara-north-pbs-01\.lab\.alcf\.anl\.gov", job_id) is None:
        parser.error("requires an exact Tara PBS_JOBID")
    host = socket.gethostname()
    if re.fullmatch(r"x4820c6s[0-9]+b[0-9]+n[0-9]+(?:\..+)?", host) is None:
        parser.error("requires a c6 compute host")

    def emit(event: str, signum: int | None = None) -> None:
        print(
            json.dumps(
                {
                    "event": event,
                    "variant": args.variant,
                    "job_id": job_id,
                    "host": host,
                    "pid": os.getpid(),
                    "ppid": os.getppid(),
                    "pgid": os.getpgrp(),
                    "sid": os.getsid(0),
                    "signal": signum,
                    "time_ns": time.time_ns(),
                },
                sort_keys=True,
            ),
            flush=True,
        )

    def terminated(signum: int, _frame: FrameType | None) -> None:
        emit("term_handler", signum)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, terminated)
    # Stop naturally before the 120-second PBS walltime if the operator does
    # not send the exact default qdel. A timeout is not a signal-test PASS.
    deadline = time.monotonic() + 105
    try:
        emit("ready")
        while time.monotonic() < deadline:
            time.sleep(0.1)
        emit("deadline_without_term")
        return 3
    finally:
        emit("finally")


if __name__ == "__main__":
    raise SystemExit(main())
