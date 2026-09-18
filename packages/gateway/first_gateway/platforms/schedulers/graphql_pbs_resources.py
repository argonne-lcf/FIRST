"""Translate the supported PBS resource flags without executing shell text."""

import re
import shlex
from typing import Annotated, Any

from pydantic import BaseModel, Field, StringConstraints, field_validator

from first_common.schema.base_scheduler import JobSubmitPayload

_NAME = r"[A-Za-z_][A-Za-z0-9_]{0,63}"
_VALUE = r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}"
_HOST = re.compile(r"(x[0-9]{4}c[0-7])s[0-7]b[01]n[0-3]")
_RESERVED = {
    "host",
    "vnode",
    "ngpus",
    "ncpus",
    "mem",
    "vmem",
    "arch",
    "mpiprocs",
    "walltime",
    "select",
    "place",
}
ResourceName = Annotated[str, StringConstraints(strict=True, pattern=f"^{_NAME}$")]
ResourceValue = Annotated[str, StringConstraints(strict=True, pattern=f"^{_VALUE}$")]


class TaskResourceConfig(BaseModel):
    task_resources: dict[ResourceName, ResourceValue] = Field(default_factory=dict)

    @field_validator("task_resources")
    @classmethod
    def custom_resources_only(cls, value: dict[str, str]) -> dict[str, str]:
        if _RESERVED.intersection(value):
            raise ValueError("task_resources supports only custom host resources")
        return value


def _flags(text: str) -> dict[str, str]:
    args = iter(shlex.split(text))
    resources: dict[str, str] = {}
    for arg in args:
        if arg == "-l":
            value = next(args, "")
        elif arg.startswith("-l"):
            value = arg[2:]
        else:
            raise ValueError("GraphQL PBS supports only -l place/select flags")
        for item in value.split(","):
            key, sep, value = item.partition("=")
            if (
                not sep
                or not value
                or key not in {"place", "select"}
                or key in resources
            ):
                raise ValueError("unsupported or duplicate GraphQL PBS resource flag")
            resources[key] = value
    return resources


def requested_resources(
    job: JobSubmitPayload, task_resources: dict[str, str]
) -> dict[str, Any]:
    """Keep FIRST's node/GPU contract; reject unsupported flags before submission."""
    if job.num_nodes < 1 or job.gpus_per_node < 1:
        raise ValueError("PBS node and GPU counts must be positive")
    flags = _flags(job.scheduler_flags)
    result: dict[str, Any] = {
        "jobResources": {"index": "", "wallClockTime": job.walltime_min * 60},
        "taskCount": {"min": job.num_nodes, "max": job.num_nodes},
    }
    group = None
    if "place" in flags:
        match = re.fullmatch(
            rf"scatter:(excl|exclhost):group=({_NAME})", flags["place"]
        )
        if match is None:
            raise ValueError(
                "supported place syntax is scatter:excl[host]:group=RESOURCE"
            )
        sharing, group = match.groups()
        result.update(
            jobPlacement=4,
            jobPlacementSharing=1 if sharing == "excl" else 2,
            jobPlacementRescGroupName=group,
        )

    tasks: list[dict[str, Any]] = []
    hosts: list[str] = []
    tiers: set[str | None] = set()
    start = 0
    for chunk in flags.get("select", str(job.num_nodes)).split("+"):
        fields = chunk.split(":")
        count = int(fields.pop(0)) if fields[0].isdecimal() else 1
        if count < 1 or start + count > job.num_nodes:
            raise ValueError("select count differs from FIRST node count")
        options: dict[str, str] = {}
        for field in fields:
            key, sep, value = field.partition("=")
            if (
                not sep
                or key not in {"ngpus", "tier1", "host"}
                or key in options
                or re.fullmatch(_VALUE, value) is None
            ):
                raise ValueError("unsupported or duplicate select chunk resource")
            options[key] = value
        if "ngpus" in options and options["ngpus"] != str(job.gpus_per_node):
            raise ValueError("select ngpus differs from FIRST GPU count")
        custom = dict(task_resources)
        if "tier1" in options:
            if "tier1" in custom and custom["tier1"] != options["tier1"]:
                raise ValueError(
                    "select tier1 conflicts with configured task_resources"
                )
            custom["tier1"] = options["tier1"]
        tiers.add(custom.get("tier1"))
        task: dict[str, Any] = {
            "index": str(start) if count == 1 else f"{start}-{start + count - 1}",
            "gpus": job.gpus_per_node,
        }
        if custom:
            task["customResources"] = [
                {"name": key, "value": value} for key, value in sorted(custom.items())
            ]
        if "host" in options:
            host = options["host"]
            match = _HOST.fullmatch(host)
            if (
                count != 1
                or match is None
                or match[1] != custom.get("tier1")
                or group != "tier1"
                or host in hosts
            ):
                raise ValueError(
                    "explicit hosts require unique xnames in one constrained tier1"
                )
            hosts.append(host)
            task["candidateMachineName"] = host
        tasks.append(task)
        start += count
    if start != job.num_nodes:
        raise ValueError("select count differs from FIRST node count")
    if group == "tier1" and len(tiers - {None}) > 1:
        raise ValueError("select chunks conflict with same-tier1 placement")
    if hosts and (len(hosts) != job.num_nodes or len(tiers) != 1):
        raise ValueError("explicit hosts must cover every task in the same tier1")
    result["tasksResources"] = tasks
    return result
