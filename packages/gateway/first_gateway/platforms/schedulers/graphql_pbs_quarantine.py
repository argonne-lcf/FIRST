"""Tara/PBS isolation RPCs, separate from ordinary model submission/cleanup."""

import base64
import hashlib
import json
import re
import shlex
import time
from abc import abstractmethod
from typing import Any, Literal, cast

from first_common.schema.base_scheduler import JobSubmitResult, SchedulerJobState
from first_common.schema.quarantine import (
    MAX_ENCODED_REPORT_BYTES,
    MAX_REPORT_BYTES,
    QUARANTINE_PAYLOAD,
    QuarantineJobIdentity,
    QuarantineJobSpec,
    QuarantineJobStatus,
    QuarantineReport,
    QuarantineReportRejected,
    QuarantineScreenPayload,
    QuarantineScreenRequest,
    QuarantineSource,
    QuarantineSubmissionRejected,
    SchedulerQuarantineAdapter,
)

TAG = "FIRST_QUARANTINE_"
HOST = re.compile(r"x(?:4818|4819|4820)c[0-7]s[0-6]b[01]n0")
JOB = re.compile(r"[1-9][0-9]*\.tara-north-pbs-01\.lab\.alcf\.anl\.gov")
ACTIVE = {5, 6, 7, 8, 9}
STATES = {
    0: SchedulerJobState.queued,
    1: SchedulerJobState.queued,
    2: SchedulerJobState.queued,
    3: SchedulerJobState.queued,
    4: SchedulerJobState.gone,
    5: SchedulerJobState.starting,
    6: SchedulerJobState.exiting,
    7: SchedulerJobState.running,
    8: SchedulerJobState.exiting,
    9: SchedulerJobState.exiting,
    10: SchedulerJobState.gone,
    11: SchedulerJobState.gone,
    12: SchedulerJobState.gone,
    13: SchedulerJobState.gone,
    14: SchedulerJobState.queued,
}
FIELDS = """jobId name owner submitTime startTime env { name value }
    extension status { state } queue { name } accountingId
    resourcesRequested { jobPlacementSharing taskCount { min max }
      tasksResources { index candidateMachineName slots gpus customResources { name value } }
      jobResources { wallClockTime } }
    allocatedMachines { name resourcesAvail { slots gpus customResources { name value } }
      resourcesAssigned { slots gpus } }"""


def fail(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        fail(key not in result, "duplicate isolation metadata key")
        result[key] = value
    return result


def tags(row: dict[str, Any]) -> dict[str, str]:
    result: dict[str, str] = {}
    for pair in row.get("env") or []:
        key, value = pair.get("name"), pair.get("value")
        if not isinstance(key, str) or not key.startswith(TAG):
            continue
        if type(value) is int and key in {TAG + "VERSION", TAG + "SOURCE_CTIME"}:
            value = str(value)
        fail(
            isinstance(value, str) and key not in result,
            "invalid isolation metadata value",
        )
        result[key] = value
    return result


def common_tags(
    environment: str,
    cluster: str,
    host: str,
    operation: str,
    source: QuarantineSource,
    kind: str,
) -> dict[str, str]:
    return {
        TAG + "VERSION": "3",
        TAG + "KIND": kind,
        TAG + "ENV": environment,
        TAG + "CLUSTER": cluster,
        TAG + "HOST": host,
        TAG + "OPERATION": operation,
        TAG + "SOURCE_ID": source.id,
        TAG + "SOURCE_NAME": source.name,
        TAG + "SOURCE_CTIME": str(source.ctime),
    }


def owner(row: dict[str, Any]) -> str:
    raw = row.get("owner")
    if (
        not isinstance(raw, str)
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*(?:@[A-Za-z0-9.-]+)?", raw) is None
    ):
        raise RuntimeError("isolation job owner is unverified")
    return raw.split("@", 1)[0]


def created(row: dict[str, Any]) -> int:
    value = row.get("submitTime")
    if type(value) is not int or value <= 0:
        raise RuntimeError("isolation creation time is unverified")
    return value // 1_000_000


def allocation(row: dict[str, Any], *, required: bool = True) -> tuple[str, ...]:
    extension = row.get("extension") or {}
    raw = extension.get("exec_vnode") if isinstance(extension, dict) else None
    if raw is None and not required:
        return ()
    if (
        not isinstance(raw, str)
        or not 0 < len(raw) <= 65536
        or not raw.startswith("(")
        or not raw.endswith(")")
    ):
        raise RuntimeError("native physical allocation membership is unverified")
    result: list[str] = []
    for chunk in raw[1:-1].split(")+("):
        for part in chunk.split("+"):
            match = re.fullmatch(
                r"([^():+]+)(?::[A-Za-z_][A-Za-z0-9_]*=[^():+\s=]+)+", part
            )
            if match is None or HOST.fullmatch(match[1]) is None:
                raise RuntimeError("noncanonical native physical vnode")
            if match[1] not in result:
                result.append(match[1])
    return tuple(result)


class GraphQLQuarantineMixin(SchedulerQuarantineAdapter):
    owner: str
    quarantine_environment: Literal["dev", "prod"] | None

    @abstractmethod
    async def _post(
        self, query: str, variables: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...

    def _local(self, environment: str) -> None:
        fail(
            self.owner == "openinference_svc"
            and environment == self.quarantine_environment,
            "only the matching environment may mutate node-isolation jobs",
        )

    async def _quarantine_rows(
        self,
        *,
        ids: list[str] | None = None,
        all_owners: bool = False,
        history: bool = False,
    ) -> list[dict[str, Any]]:
        fields = "jobId name owner extension status { state }" if all_owners else FIELDS
        query = (
            """query IsolationJobs($owner:String,$ids:[String!],$history:Boolean!,$cursor:Cursor){
          jobs(filter:{owner:$owner,jobIds:$ids,withHistoryJobs:$history},count:500,from:$cursor){
            edges { node { """
            + fields
            + """ } error { errorCode } }
            pageInfo { hasNextPage endCursor } } }"""
        )
        result: list[dict[str, Any]] = []
        seen: set[str] = set()
        cursors: set[str] = set()
        cursor = None
        for _ in range(10):
            data = await self._post(
                query,
                {
                    "owner": None if all_owners else self.owner,
                    "ids": ids,
                    "history": history,
                    "cursor": cursor,
                },
            )
            connection = data["jobs"]
            for edge in connection["edges"]:
                fail(not edge.get("error"), "isolation inventory edge is unverified")
                row = edge["node"]
                job_id = row.get("jobId")
                fail(
                    isinstance(job_id, str)
                    and JOB.fullmatch(job_id) is not None
                    and job_id not in seen,
                    "isolation inventory identity is duplicated or unverified",
                )
                seen.add(job_id)
                result.append(row)
            page = connection["pageInfo"]
            if not page["hasNextPage"]:
                if ids is not None:
                    fail(
                        set(seen).issubset(ids),
                        "exact isolation query returned another job",
                    )
                return result
            cursor = page.get("endCursor")
            fail(
                isinstance(cursor, str) and bool(cursor) and cursor not in cursors,
                "isolation inventory pagination is unverified",
            )
            cursors.add(cursor)
        raise RuntimeError("isolation inventory exceeded bounded pagination")

    def _identity(self, row: dict[str, Any]) -> QuarantineJobIdentity:
        value = tags(row)
        fail(
            owner(row) == self.owner == "openinference_svc", "quarantine owner differs"
        )
        fail(
            value.get(TAG + "VERSION") == "3"
            and value.get(TAG + "KIND") == "quarantine",
            "quarantine protocol identity differs",
        )
        source = QuarantineSource(
            id=value[TAG + "SOURCE_ID"],
            name=value[TAG + "SOURCE_NAME"],
            ctime=int(value[TAG + "SOURCE_CTIME"]),
        )
        identity = QuarantineJobIdentity(
            job_id=row["jobId"],
            job_name=row["name"],
            ctime=created(row),
            environment=cast(Literal["dev", "prod"], value[TAG + "ENV"]),
            cluster=value[TAG + "CLUSTER"],
            host=value[TAG + "HOST"],
            operation=value[TAG + "OPERATION"],
            source=source,
        )
        fail(
            identity.cluster == "tara"
            and HOST.fullmatch(identity.host) is not None
            and source.name.startswith(f"first_{identity.environment}_tara_v2_")
            and identity.job_name
            == f"first_quar_{identity.environment}_{identity.host}_{identity.operation[:8]}"
            and value
            == common_tags(
                identity.environment,
                identity.cluster,
                identity.host,
                identity.operation,
                source,
                "quarantine",
            ),
            "quarantine full metadata/name differs",
        )
        return identity

    async def _isolation(
        self, row: dict[str, Any], identity: QuarantineJobIdentity
    ) -> bool:
        if (row.get("status") or {}).get("state") != 7 or allocation(row) != (
            identity.host,
        ):
            return False
        if not self._accepted_request(row, identity):
            return False
        resources = row.get("resourcesRequested") or {}
        tasks = resources.get("tasksResources") or []
        if (
            resources.get("jobPlacementSharing") not in {1, 2}
            or len(tasks) != 1
            or tasks[0].get("candidateMachineName") != identity.host
        ):
            return False
        machines = row.get("allocatedMachines") or []
        if len(machines) != 1 or machines[0].get("name") != identity.host:
            return False
        machine = machines[0]
        available, assigned = (
            machine.get("resourcesAvail") or {},
            machine.get("resourcesAssigned") or {},
        )
        host_resources = [
            pair["value"]
            for pair in available.get("customResources") or []
            if pair.get("name") == "host"
        ]
        if (
            host_resources != [identity.host]
            or type(available.get("slots")) is not int
            or available["slots"] <= 0
            or available.get("gpus") != 4
            or assigned.get("slots") != available["slots"]
            or assigned.get("gpus") != 4
        ):
            return False
        occupants = []
        for other in await self._quarantine_rows(all_owners=True):
            state = (other.get("status") or {}).get("state")
            fail(state in STATES, "unknown all-owner allocation state")
            if state in ACTIVE and identity.host in allocation(other):
                occupants.append(other["jobId"])
        return occupants == [identity.job_id]

    def _accepted_request(
        self, row: dict[str, Any], identity: QuarantineJobIdentity
    ) -> bool:
        resources = row.get("resourcesRequested") or {}
        tasks = resources.get("tasksResources") or []
        count = resources.get("taskCount") or {}
        return (
            type(resources.get("jobPlacementSharing")) is int
            and resources["jobPlacementSharing"] in {1, 2}
            and type(count.get("min")) is int
            and count["min"] in {0, 1}  # Live PBS getter normalizes fixed min to 0.
            and type(count.get("max")) is int
            and count["max"] == 1
            and len(tasks) == 1
            and tasks[0].get("index") == "0"
            and tasks[0].get("candidateMachineName") == identity.host
        )

    async def _status(self, row: dict[str, Any]) -> QuarantineJobStatus:
        identity = self._identity(row)
        code = (row.get("status") or {}).get("state")
        if type(code) is not int or code not in STATES:
            raise RuntimeError("unknown quarantine state")
        resources = (row.get("resourcesRequested") or {}).get("jobResources") or {}
        started, walltime = row.get("startTime"), resources.get("wallClockTime")
        expires = (
            started // 1_000_000 + walltime
            if type(started) is int
            and started > 0
            and type(walltime) is int
            and walltime > 0
            else None
        )
        return QuarantineJobStatus(
            identity=identity,
            state=STATES[code],
            allocated_hosts=allocation(row, required=code in ACTIVE),
            exclusive=await self._isolation(row, identity),
            expires_unix=expires,
            hold_type=(row.get("extension") or {}).get("Hold_Types"),
            request_verified=self._accepted_request(row, identity),
        )

    async def list_quarantine_jobs(self) -> list[QuarantineJobStatus]:
        result = []
        for row in await self._quarantine_rows():
            hint = next(
                (
                    pair.get("value")
                    for pair in row.get("env") or []
                    if pair.get("name") == TAG + "KIND"
                ),
                None,
            )
            if (
                not str(row.get("name", "")).startswith(
                    ("first_quar_", "first_tara_quarantine_")
                )
                and hint != "quarantine"
            ):
                continue
            if owner(row) != self.owner:
                continue  # A known different owner is not our infrastructure.
            result.append(await self._status(row))
        return result

    async def get_quarantine_status(self, job_id: str) -> QuarantineJobStatus | None:
        fail(JOB.fullmatch(job_id) is not None, "invalid exact quarantine ID")
        rows = await self._quarantine_rows(ids=[job_id], history=True)
        return await self._status(rows[0]) if rows else None

    async def get_source_allocation(self, source: QuarantineSource) -> tuple[str, ...]:
        fail(JOB.fullmatch(source.id) is not None, "invalid fault source job ID")
        rows = await self._quarantine_rows(ids=[source.id], history=True)
        fail(
            len(rows) == 1
            and owner(rows[0]) == self.owner == "openinference_svc"
            and rows[0].get("name") == source.name
            and created(rows[0]) == source.ctime,
            "fault source allocation identity is unverified",
        )
        return allocation(rows[0])

    def _report(self, row: dict[str, Any]) -> QuarantineReport:
        value = tags(row)
        fail(
            owner(row) == self.owner == "openinference_svc"
            and (row.get("status") or {}).get("state") == 3
            and (row.get("extension") or {}).get("Hold_Types") == "u",
            "report must be a verified owned, held nonexecuting job",
        )
        raw = value.get(TAG + "REPORT", "")
        fail(
            0 < len(raw.encode()) <= MAX_ENCODED_REPORT_BYTES,
            "isolation report exceeded its encoded bound",
        )
        decoded = base64.b64decode(raw, altchars=b"-_", validate=True)
        fail(
            len(decoded) <= MAX_REPORT_BYTES,
            "isolation report exceeded its decoded bound",
        )
        payload = QUARANTINE_PAYLOAD.validate_python(
            json.loads(decoded, object_pairs_hook=unique)
        )
        kind = {
            "node_fault": "fault",
            "activated": "activation",
            "screen_result": "screen",
        }[payload.event]
        suffix = (
            payload.nonce[:8]
            if payload.event == "screen_result"
            else payload.operation[:8]
        )
        fail(
            row.get("name") == f"first_qreport_{payload.environment}_{kind}_{suffix}"
            and payload.cluster == "tara"
            and HOST.fullmatch(payload.host) is not None
            and payload.origin.name.startswith(f"first_{payload.environment}_tara_v2_")
            and value
            == {
                **common_tags(
                    payload.environment,
                    payload.cluster,
                    payload.host,
                    payload.operation,
                    payload.origin,
                    kind,
                ),
                TAG + "REPORT": raw,
            },
            "report envelope identity/name/tags disagree",
        )
        return QuarantineReport(
            job_id=row["jobId"],
            job_name=row["name"],
            ctime=created(row),
            payload=payload,
            fingerprint=hashlib.sha256(raw.encode()).hexdigest(),
        )

    async def list_quarantine_reports(
        self,
    ) -> list[QuarantineReport | QuarantineReportRejected]:
        result: list[QuarantineReport | QuarantineReportRejected] = []
        for row in await self._quarantine_rows():
            hint = next(
                (
                    pair.get("value")
                    for pair in row.get("env") or []
                    if pair.get("name") == TAG + "KIND"
                ),
                None,
            )
            if not str(row.get("name", "")).startswith(
                "first_qreport_"
            ) and hint not in {"fault", "activation", "screen"}:
                continue
            try:
                if owner(row) != self.owner:
                    continue
                result.append(self._report(row))
            except (ValueError, RuntimeError, KeyError, TypeError):
                raw = next(
                    (
                        pair.get("value")
                        for pair in row.get("env") or []
                        if pair.get("name") == TAG + "REPORT"
                    ),
                    "",
                )
                # Never persist/log an untrusted payload or environment dump.
                fingerprint = hashlib.sha256(str(raw).encode()).hexdigest()
                result.append(
                    QuarantineReportRejected(
                        job_id=row["jobId"],
                        job_name=str(row.get("name", "")),
                        ctime=created(row)
                        if type(row.get("submitTime")) is int and row["submitTime"] > 0
                        else None,
                        environment=None,
                        fingerprint=fingerprint,
                        reason="malformed_or_unverified_report",
                    )
                )
        return result

    async def _create(self, request: dict[str, Any]) -> JobSubmitResult:
        data = await self._post(
            """mutation IsolationSubmit($input:JobInput!){createJob(input:$input){
          node{jobId} error{errorCode}}}""",
            {"input": request},
        )
        payload = data["createJob"]
        if payload.get("error"):
            if not (payload.get("node") or {}).get("jobId"):
                raise QuarantineSubmissionRejected(
                    "PBS explicitly rejected the node-isolation submission"
                )
            raise RuntimeError(
                "node-isolation submission has an ambiguous error and job acknowledgement"
            )
        job_id = ((payload.get("node") or {}).get("jobId") or "").strip()
        fail(
            JOB.fullmatch(job_id) is not None,
            "isolation submission acknowledgement is ambiguous",
        )
        return JobSubmitResult(job_name=request["name"], scheduler_id=job_id)

    async def submit_quarantine(self, spec: QuarantineJobSpec) -> JobSubmitResult:
        self._local(spec.environment)
        fail(
            spec.cluster == "tara"
            and HOST.fullmatch(spec.host) is not None
            and spec.policy.environment == spec.environment
            and spec.source.name.startswith(f"first_{spec.environment}_tara_v2_")
            and JOB.fullmatch(spec.source.id) is not None,
            "invalid Tara quarantine request",
        )
        for path in (
            spec.policy.worker_path,
            spec.policy.admission_path,
            spec.policy.policy_path,
        ):
            fail(
                isinstance(path, str)
                and path.startswith("/vast/draco/tara/projects/inference_service/"),
                "unscoped Tara worker asset",
            )
        fail(
            spec.policy.worker_sha256 is not None
            and spec.policy.admission_sha256 is not None,
            "PBS quarantine requires pinned worker and admission assets",
        )
        worker_path = cast(str, spec.policy.worker_path)
        admission_path = cast(str, spec.policy.admission_path)
        policy_path = cast(str, spec.policy.policy_path)
        worker_checksum = cast(str, spec.policy.worker_sha256)
        admission_checksum = cast(str, spec.policy.admission_sha256)
        args = [
            "/opt/cray/pe/python/3.12.12/bin/python3",
            "-IB",
            worker_path,
            "--environment",
            spec.environment,
            "--cluster",
            spec.cluster,
            "--host",
            spec.host,
            "--operation",
            spec.operation,
            "--source-id",
            spec.source.id,
            "--source-name",
            spec.source.name,
            "--source-ctime",
            str(spec.source.ctime),
            "--walltime-sec",
            str(spec.policy.walltime_min * 60),
            "--worker-sha256",
            worker_checksum,
            "--admission-helper",
            admission_path,
            "--admission-helper-sha256",
            admission_checksum,
            "--admission-policy",
            policy_path,
            "--admission-policy-sha256",
            spec.policy.policy_sha256,
            "--queue",
            spec.queue,
            "--account",
            spec.account,
        ]
        script = (
            '#!/bin/sh\nset -eu\ntest -z "${LD_PRELOAD:-}" || exit 125\n'
            + 'test "$(/usr/bin/sha256sum '
            + shlex.quote(worker_path)
            + " | /usr/bin/cut -d ' ' -f1)\" = "
            + shlex.quote(worker_checksum)
            + " || exit 125\nexec "
            + shlex.join(args)
            + "\n"
        )
        metadata = common_tags(
            spec.environment,
            spec.cluster,
            spec.host,
            spec.operation,
            spec.source,
            "quarantine",
        )
        return await self._create(
            {
                "name": spec.job_name,
                "scriptContent": base64.urlsafe_b64encode(script.encode()).decode(),
                "env": [
                    {"name": key, "value": value} for key, value in metadata.items()
                ],
                "queue": {"name": spec.queue},
                "accountingId": spec.account,
                "noEmail": True,
                "rerunnable": False,
                "outputPath": spec.log_path,
                "errorPath": spec.log_path,
                "joinFiles": True,
                "resourcesRequested": {
                    "taskCount": {"min": 1, "max": 1},
                    "jobPlacement": 4,
                    "jobPlacementSharing": 2,
                    "jobPlacementRescGroupName": "tier0",
                    "jobResources": {
                        "index": "",
                        "wallClockTime": spec.policy.walltime_min * 60,
                    },
                    "tasksResources": [
                        {
                            "index": "0",
                            "candidateMachineName": spec.host,
                            "slots": 1,
                            "gpus": 0,
                            "physicalMemory": 65536,
                        }
                    ],
                },
            }
        )

    async def _delete_auxiliary(self, job_id: str) -> None:
        data = await self._post(
            """mutation IsolationDelete($jobId:String!){deleteJob(jobId:$jobId,input:{force:false}){
            node{jobId}error{errorCode}}}""",
            {"jobId": job_id},
        )
        fail(
            not data["deleteJob"].get("error"),
            "owned isolation deletion was not acknowledged",
        )
        returned = (data["deleteJob"].get("node") or {}).get("jobId")
        fail(returned in (None, job_id), "owned isolation deletion identity differs")

    async def delete_quarantine(self, identity: QuarantineJobIdentity) -> None:
        self._local(identity.environment)
        rows = await self._quarantine_rows(ids=[identity.job_id], history=True)
        if not rows:
            return
        fail(
            self._identity(rows[0]) == identity, "quarantine deletion identity changed"
        )
        await self._delete_auxiliary(identity.job_id)

    async def acknowledge_quarantine_report(self, report: QuarantineReport) -> None:
        self._local(report.environment)
        rows = await self._quarantine_rows(ids=[report.job_id], history=True)
        if not rows:
            return
        fail(self._report(rows[0]) == report, "report acknowledgement identity changed")
        if report.payload.event == "activated":
            current = await self.get_quarantine_status(report.payload.quarantine_job_id)
            if current is not None and current.state != SchedulerJobState.gone:
                return  # Other environments still need immutable activation proof.
        await self._delete_auxiliary(report.job_id)

    async def request_quarantine_screen(
        self, identity: QuarantineJobIdentity, nonce: str
    ) -> JobSubmitResult:
        self._local(identity.environment)
        status = await self.get_quarantine_status(identity.job_id)
        fail(
            status is not None and status.identity == identity and status.exclusive,
            "fresh screen requires independently verified current isolation",
        )
        request = QuarantineScreenRequest(
            schema_version=3,
            operation=identity.operation,
            host=identity.host,
            quarantine_job_id=identity.job_id,
            nonce=nonce,
            requested_unix=int(time.time()),
        )
        encoded = base64.urlsafe_b64encode(request.model_dump_json().encode()).decode()
        metadata = common_tags(
            identity.environment,
            identity.cluster,
            identity.host,
            identity.operation,
            identity.source,
            "screen_request",
        )
        metadata[TAG + "REQUEST"] = encoded
        current = (await self._quarantine_rows(ids=[identity.job_id], history=True))[0]
        fail(
            self._identity(current) == identity,
            "screen request target identity changed",
        )
        queue = (current.get("queue") or {}).get("name")
        account = current.get("accountingId")
        fail(
            isinstance(queue, str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+", queue) is not None
            and isinstance(account, str)
            and re.fullmatch(r"[A-Za-z0-9_.-]+", account) is not None,
            "screen request queue/account is unverified",
        )
        return await self._create(
            {
                "name": f"first_qrequest_{identity.environment}_screen_{nonce[:8]}",
                "scriptContent": base64.urlsafe_b64encode(
                    b"#!/bin/sh\nexit 125\n"
                ).decode(),
                "submitAsHold": True,
                "holdType": "u",
                "noEmail": True,
                "rerunnable": False,
                "queue": {"name": queue},
                "accountingId": account,
                "env": [
                    {"name": key, "value": value} for key, value in metadata.items()
                ],
                "resourcesRequested": {
                    "taskCount": {"min": 1, "max": 1},
                    "jobResources": {"index": "", "wallClockTime": 60},
                    "tasksResources": [{"index": "0", "slots": 1, "gpus": 0}],
                },
            }
        )

    def _screen_request_receipt(
        self, row: dict[str, Any], identity: QuarantineJobIdentity, nonce: str
    ) -> tuple[str, int, str]:
        value = tags(row)
        fail(
            owner(row) == self.owner == "openinference_svc"
            and (row.get("status") or {}).get("state") == 3
            and (row.get("extension") or {}).get("Hold_Types") == "u"
            and row.get("startTime") in (None, 0)
            and not (row.get("extension") or {}).get("exec_vnode")
            and not (row.get("extension") or {}).get("exec_host")
            and not row.get("allocatedMachines"),
            "screen request is not an owned, held, never-executed carrier",
        )
        raw = value.get(TAG + "REQUEST", "")
        fail(0 < len(raw.encode()) <= 16384, "screen request payload exceeds its bound")
        decoded = base64.b64decode(raw, altchars=b"-_", validate=True)
        request = QuarantineScreenRequest.model_validate(
            json.loads(decoded, object_pairs_hook=unique)
        )
        fail(
            row.get("name")
            == f"first_qrequest_{identity.environment}_screen_{nonce[:8]}"
            and request.operation == identity.operation
            and request.host == identity.host
            and request.quarantine_job_id == identity.job_id
            and request.nonce == nonce
            and value
            == {
                **common_tags(
                    identity.environment,
                    identity.cluster,
                    identity.host,
                    identity.operation,
                    identity.source,
                    "screen_request",
                ),
                TAG + "REQUEST": raw,
            },
            "screen request full identity or nonce differs",
        )
        return row["jobId"], created(row), hashlib.sha256(raw.encode()).hexdigest()

    async def acknowledge_quarantine_screen_request(
        self, report: QuarantineReport
    ) -> None:
        """Consume only the exact request whose result FIRST already persisted."""
        self._local(report.environment)
        payload = report.payload
        fail(
            isinstance(payload, QuarantineScreenPayload),
            "only a screen result may consume a request",
        )
        assert isinstance(payload, QuarantineScreenPayload)
        carriers = await self._quarantine_rows(ids=[report.job_id], history=True)
        fail(
            len(carriers) == 1 and self._report(carriers[0]) == report,
            "screen result carrier identity changed before request acknowledgement",
        )
        waiting = await self._quarantine_rows(
            ids=[payload.quarantine_job_id], history=True
        )
        fail(
            len(waiting) == 1,
            "screen-result waiting allocation identity is unavailable",
        )
        identity = self._identity(waiting[0])
        fail(
            identity.environment == payload.environment
            and identity.cluster == payload.cluster
            and identity.host == payload.host
            and identity.operation == payload.operation
            and identity.source == payload.origin,
            "screen result differs from its exact waiting allocation",
        )
        name = f"first_qrequest_{payload.environment}_screen_{payload.nonce[:8]}"
        requests = []
        for row in await self._quarantine_rows():
            if row.get("name") != name:
                continue
            if owner(row) != self.owner:
                continue
            requests.append(row)
        if not requests:
            return  # Independent absence after a possibly lost qdel ACK, not node recovery.
        fail(
            len(requests) == 1,
            "duplicate screen request requires operator reconciliation",
        )
        receipt = self._screen_request_receipt(requests[0], identity, payload.nonce)
        current = await self._quarantine_rows(ids=[receipt[0]], history=True)
        fail(
            len(current) == 1
            and self._screen_request_receipt(current[0], identity, payload.nonce)
            == receipt,
            "screen request identity changed before acknowledgement",
        )
        await self._delete_auxiliary(receipt[0])
