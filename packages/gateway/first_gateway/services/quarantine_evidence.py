"""Pure native evidence validation; no probes, processes or scheduler mutations."""

import csv
import hashlib
import ipaddress
import json
import re
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from first_common.schema.quarantine import (
    QuarantineActivationPayload,
    QuarantineFaultPayload,
    QuarantineJobStatus,
    QuarantineScreenPayload,
)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate evidence JSON key")
        result[key] = value
    return result


def pinned_policy(
    payload: QuarantineFaultPayload | QuarantineScreenPayload, expected_sha256: str
) -> dict[str, Any]:
    if (
        not isinstance(payload.policy_json, str)
        or payload.policy_sha256 != expected_sha256
        or hashlib.sha256(payload.policy_json.encode()).hexdigest() != expected_sha256
    ):
        raise ValueError(
            "native policy bytes differ from the trusted configured digest"
        )
    policy = json.loads(payload.policy_json, object_pairs_hook=unique_object)
    if (
        not isinstance(policy, dict)
        or type(policy.get("schema_version")) is not int
        or policy["schema_version"] != 3
        or "excluded_hosts" in policy
        or "required_topology" in policy
        or not isinstance(policy.get("topology"), dict)
        or not isinstance(policy.get("allowed_racks"), list)
        or not policy["allowed_racks"]
    ):
        raise ValueError("unsupported native screening policy")
    return policy


def publication(
    payload: QuarantineFaultPayload | QuarantineScreenPayload, worker_path: str
) -> None:
    """The initial PBS implementation publishes workers under a full commit pin."""
    commit = PurePosixPath(worker_path).parent.name
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None or payload.bundle_commit != commit:
        raise ValueError(
            "native evidence publication differs from the trusted worker pin"
        )


def native_epoch(value: Any) -> int:
    if type(value) is int and value > 0:
        return value
    if isinstance(value, str):
        for fmt in ("%a %b %d %H:%M:%S %Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return int(
                    datetime.strptime(value, fmt)
                    .replace(tzinfo=timezone.utc)
                    .timestamp()
                )
            except ValueError:
                continue
    raise ValueError("native allocation timestamp is unverified")


def activation_proof(
    payload: QuarantineActivationPayload, status: QuarantineJobStatus
) -> None:
    """Require native physical-vnode evidence in addition to bridge live census."""
    identity = status.identity
    job, nodes = payload.native_job, payload.native_nodes
    resources = job.get("Resource_List")
    owner = job.get("Job_Owner")
    if (
        job.get("Job_Name") != identity.job_name
        or job.get("job_state") != "R"
        or native_epoch(job.get("ctime")) != identity.ctime
        or not isinstance(owner, str)
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*@[A-Za-z0-9.-]+", owner) is None
        or not isinstance(resources, dict)
        or re.fullmatch(
            r"1:(?:[^:+]+:)*host=" + re.escape(identity.host) + r"(?::[^:+]+)*",
            str(resources.get("select", "")),
        )
        is None
        or resources.get("place")
        not in {"exclhost", "scatter:exclhost", "pack:exclhost", "excl", "scatter:excl"}
        or not nodes
    ):
        raise ValueError(
            "native activation exact-host/exclusivity identity is unverified"
        )
    if resources["place"] in {"excl", "scatter:excl"} and len(nodes) != 1:
        raise ValueError(
            "native vnode-exclusive request does not prove whole-host isolation"
        )
    raw_vnodes = job.get("exec_vnode")
    if (
        not isinstance(raw_vnodes, str)
        or not raw_vnodes.startswith("(")
        or not raw_vnodes.endswith(")")
    ):
        raise ValueError("native activation allocation is unavailable")
    used = set(re.findall(r"(?:^|[+(])([^():+]+):", raw_vnodes))
    if not used or not used.issubset(nodes):
        raise ValueError(
            "native activation allocation differs from physical vnode census"
        )
    for row in nodes.values():
        available = row.get("resources_available")
        occupants = row.get("jobs")
        if isinstance(occupants, str):
            occupants = [part.strip() for part in occupants.split(",") if part.strip()]
        if (
            not isinstance(available, dict)
            or available.get("host", "").split(".")[0] != identity.host
            or not isinstance(occupants, list)
            or not occupants
            or any(
                not isinstance(item, str) or item.split("/", 1)[0] != identity.job_id
                for item in occupants
            )
            or "job-exclusive" not in str(row.get("state", "")).split(",")
        ):
            raise ValueError(
                "native physical-vnode census does not prove exclusive waiting-job ownership"
            )


def allocation_context(hosts: list[str], policy: dict[str, Any]) -> None:
    if not hosts or len(hosts) != len(set(hosts)):
        raise ValueError("native allocation hosts are missing or duplicated")
    racks = set()
    for host in hosts:
        topology = policy["topology"].get(host)
        if (
            not isinstance(topology, dict)
            or set(topology) != {"tier0", "tier1"}
            or topology["tier0"] not in policy["allowed_racks"]
            or not isinstance(topology["tier1"], str)
        ):
            raise ValueError("allocation host is outside the pinned allowed inventory")
        racks.add(topology["tier0"])
    if len(racks) != 1:
        raise ValueError("native allocation spans racks")


def gpu_rows(text: str, clients: str, expected_count: int) -> list[dict[str, Any]]:
    if not isinstance(text, str) or not isinstance(clients, str) or clients.strip():
        raise ValueError("native GPU query is invalid or GPU clients remain")
    result: list[dict[str, Any]] = []
    for row in csv.reader(text.splitlines()):
        if len(row) != 7:
            raise ValueError("native GPU query row has an unsupported shape")
        index, uuid, total, free, volatile, aggregate, driver = (
            value.strip() for value in row
        )
        if not all(
            re.fullmatch(r"[0-9]{1,9}", value)
            for value in (index, total, free, volatile, aggregate)
        ) or not re.fullmatch(
            r"GPU-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", uuid
        ):
            raise ValueError("native GPU query identity or numeric evidence is invalid")
        result.append(
            dict(
                index=int(index),
                uuid=uuid.lower(),
                total_mib=int(total),
                free_mib=int(free),
                uncorrected_volatile=int(volatile),
                uncorrected_aggregate=int(aggregate),
                driver=driver,
            )
        )
    if (
        not result
        or len({row["index"] for row in result}) != len(result)
        or len({row["uuid"] for row in result}) != len(result)
        or any(
            row["index"] not in range(expected_count)
            or not 0 < row["free_mib"] <= row["total_mib"]
            for row in result
        )
    ):
        raise ValueError("native GPU query contains invalid indices, UUIDs or memory")
    return result


def gpu_fault(rows: list[dict[str, Any]], count: int) -> dict[str, Any] | None:
    missing = sorted(set(range(count)) - {row["index"] for row in rows})
    if missing:
        label = "four" if count == 4 else str(count)
        return dict(
            code="missing_gpu",
            gpu_indices=missing,
            reason=f"expected exactly {label} distinct GPUs ({','.join(map(str, range(count)))})",
        )
    ecc = sorted(
        row["index"]
        for row in rows
        if row["uncorrected_volatile"] or row["uncorrected_aggregate"]
    )
    if ecc:
        return dict(
            code="uncorrected_ecc",
            gpu_indices=ecc,
            reason=f"GPU {min(ecc)} has uncorrected ECC",
        )
    low = [
        row
        for row in rows
        if row["free_mib"] < (row["total_mib"] * 95 + 99) // 100 + 256
    ]
    if low:
        first = min(low, key=lambda row: row["index"])
        required = (first["total_mib"] * 95 + 99) // 100 + 256
        return dict(
            code="unexplained_gpu_memory_loss",
            gpu_indices=sorted(row["index"] for row in low),
            reason=f"GPU {first['index']} headroom: free={first['free_mib']} required={required} MiB",
        )
    return None


def observation(
    report: dict[str, Any],
    *,
    host: str,
    hosts: list[str],
    attempt: str,
    master: str,
    policy: dict[str, Any],
    gpu_count: int,
    now_ns: int,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if (
        report.get("host", "").split(".")[0] != host
        or report.get("attempt") != attempt
        or type(report.get("schema_version")) is not int
        or report["schema_version"] != 1
        or type(report.get("collected_unix_ns")) is not int
        or not -5_000_000_000 <= now_ns - report["collected_unix_ns"] <= 120_000_000_000
        or report.get("outcome") not in {"PASS", "FAIL"}
        or "gpu_headroom_bypassed" in report
    ):
        raise ValueError("native observation identity, freshness or outcome differs")
    address = str(ipaddress.IPv4Address(report["hsn_ip"]))
    expected_master = str(ipaddress.IPv4Address(master))
    if (
        report.get("master_hsn") != expected_master
        or host == hosts[0]
        and address != expected_master
    ):
        raise ValueError("native HSN/master allocation context differs")
    if report.get("kernel") != policy.get("kernel") or report.get(
        "page_size"
    ) != policy.get("page_size"):
        raise ValueError("native system image differs from the pinned policy")
    rows = gpu_rows(report["gpu_query"], report["compute_pids"], gpu_count)
    if any(row["driver"] != policy.get("driver") for row in rows):
        raise ValueError("native GPU driver differs from the pinned policy")
    fault = gpu_fault(rows, gpu_count)
    if (
        report["outcome"] == "FAIL"
        and fault is None
        or report["outcome"] == "PASS"
        and fault is not None
        or fault is not None
        and report.get("fault") != fault
        or fault is None
        and "fault" in report
    ):
        raise ValueError(
            "native outcome is inconsistent or not an attributable hardware fault"
        )
    return rows, fault


def paired(
    payload: QuarantineFaultPayload | QuarantineScreenPayload,
    expected_sha256: str,
    gpu_count: int,
    *,
    now_ns: int | None = None,
) -> dict[str, Any] | None:
    policy = pinned_policy(payload, expected_sha256)
    if (
        payload.master_hsn is None
        or len(payload.reports) != 1
        or len(payload.confirmation) != 1
    ):
        raise ValueError("paired native screening evidence is incomplete")
    hosts = (
        payload.allocation_hosts
        if isinstance(payload, QuarantineFaultPayload)
        else [payload.host]
    )
    allocation_context(hosts, policy)
    if payload.host not in hosts:
        raise ValueError("reported fault node is not in the source allocation")
    now_ns = time.time_ns() if now_ns is None else now_ns
    attempt = (
        payload.attempt
        if isinstance(payload, QuarantineFaultPayload)
        else payload.nonce
    )
    before, after = payload.reports[0], payload.confirmation[0]
    old_rows, old_fault = observation(
        before,
        host=payload.host,
        hosts=hosts,
        attempt=attempt,
        master=payload.master_hsn,
        policy=policy,
        gpu_count=gpu_count,
        now_ns=now_ns,
    )
    new_rows, new_fault = observation(
        after,
        host=payload.host,
        hosts=hosts,
        attempt=attempt,
        master=payload.master_hsn,
        policy=policy,
        gpu_count=gpu_count,
        now_ns=now_ns,
    )
    elapsed = after["collected_unix_ns"] - before["collected_unix_ns"]

    def identity(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
        return sorted(
            (row["index"], row["uuid"], row["total_mib"], row["driver"]) for row in rows
        )

    def signature(fault: dict[str, Any] | None) -> tuple[Any, Any] | None:
        return None if fault is None else (fault["code"], fault["gpu_indices"])

    if (
        not 1_000_000_000 <= elapsed <= 30_000_000_000
        or identity(old_rows) != identity(new_rows)
        or before["hsn_ip"] != after["hsn_ip"]
        or signature(old_fault) != signature(new_fault)
    ):
        raise ValueError(
            "node fault/identity did not persist across the two bounded samples"
        )
    if isinstance(payload, QuarantineFaultPayload) and new_fault is None:
        raise ValueError("two native samples did not establish a node fault")
    return new_fault


def cleanup_proof(text: str, hosts: list[str]) -> None:
    if (
        not text
        or len(text.encode()) > 32768
        or re.search(r"tara_model_postcheck_[A-Za-z0-9_]*remaining(?:\s|$)", text)
    ):
        raise ValueError("lifecycle fault lacks valid process/socket cleanup evidence")
    markers = [
        "tara_model_postcheck_pals_group_query_valid=true rank=0 replicas=1",
        "tara_model_postcheck_uds_listener_query_valid=true rank=0 replicas=1 absent=true",
    ]
    for rank in range(len(hosts)):
        markers.extend(
            [
                f"tara_model_postcheck_engine_group_query_valid=true rank={rank} replicas=1",
                f"tara_model_postcheck_listener_query_valid=true rank={rank} replicas=1 ports_per_replica=2 absent=true",
                f"tara_model_postcheck_gpu_query_valid=true rank={rank} unique_pids=0",
            ]
        )
    for marker in markers:
        if (
            len(re.findall(r"(?m)(?:^|[ \t])" + re.escape(marker) + r"[ \t]*$", text))
            != 1
        ):
            raise ValueError(
                "lifecycle cleanup proof is missing/duplicated native all-node absence evidence"
            )
