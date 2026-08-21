from typing import Any


def qsub(args: list[str]) -> str:
    """
    Globus Compute Function to execute qsub and return Job ID from stdout
    """
    import subprocess

    try:
        p = subprocess.run(
            ["qsub", *args], text=True, check=True, capture_output=True, timeout=15
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{e.cmd!r} failed (returncode {e.returncode}):\nStderr:\n{e.stderr.strip()}"
        ) from None
    return p.stdout.strip()


def qstat(job_id: str | None = None) -> dict[str, Any]:
    """
    Globus Compute Function to execute qstat and capture JSON Jobs output.
    Lists all active jobs if job_id=None; otherwise retrieves one job.
    """
    import json
    import subprocess

    if job_id is None:
        args = ["qstat", "-fF", "JSON"]
    else:
        args = ["qstat", job_id, "-xfF", "JSON"]

    try:
        p = subprocess.run(
            args,
            text=True,
            check=True,
            capture_output=True,
            timeout=15,
        )
    except subprocess.CalledProcessError as e:
        if e.returncode == 153 and "Unknown Job Id" in e.stderr:
            return {}
        raise RuntimeError(
            f"{e.cmd!r} failed (returncode {e.returncode}):\nStderr:\n{e.stderr.strip()}"
        ) from None
    jobs: dict[str, Any] = json.loads(p.stdout)["Jobs"]
    assert isinstance(jobs, dict)
    return jobs


def qdel(job_id: str) -> None:
    """Globus Compute function to qdel a job"""
    import subprocess

    try:
        subprocess.run(
            ["qdel", str(job_id)],
            text=True,
            check=True,
            capture_output=True,
            timeout=15,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"{e.cmd!r} failed (returncode {e.returncode}):\nStderr:\n{e.stderr.strip()}"
        ) from None


def list_files(directory: str) -> list[str]:
    """Globus Compute function to list files in a directory"""
    from pathlib import Path

    return [f.name for f in Path(directory).iterdir() if f.is_file()]


def put_file(content: str, path: str, mode: int) -> None:
    """Globus Compute function to write file"""
    from pathlib import Path

    dest = Path(path)
    dest.parent.mkdir(exist_ok=True, parents=True)
    dest.touch(mode=mode)
    dest.chmod(mode)
    dest.write_text(content)


def read_file(path: str) -> str:
    """Globus Compute function to read file content"""
    from pathlib import Path

    return Path(path).read_text()
