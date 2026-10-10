"""Optional PBS isolation RPCs; no database, VM, scheduler or GPU writes."""

import base64
import copy
import hashlib
import json
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

from first_common.schema.base_scheduler import JobSubmitPayload
from first_common.schema.quarantine import (
    NodeQuarantinePolicy,
    QuarantineJobSpec,
    QuarantineReportRejected,
    QuarantineSource,
    QuarantineSubmissionRejected,
)
from first_gateway.platforms.schedulers.graphql_pbs import GraphQLPBSAdapter
from first_gateway.platforms.schedulers.graphql_pbs_quarantine import common_tags

HOST = "x4820c0s4b0n0"
OTHER = "x4820c1s0b0n0"
JOB = "9000.tara-north-pbs-01.lab.alcf.anl.gov"
OP = "a" * 32
SOURCE = QuarantineSource(
    id="8000.tara-north-pbs-01.lab.alcf.anl.gov",
    name="first_dev_tara_v2_tara-pilot-12345678",
    ctime=100,
)


def connection(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "jobs": {
            "edges": [{"node": row, "error": None} for row in rows],
            "pageInfo": {"hasNextPage": False, "endCursor": None},
        }
    }


def waiting(state: int = 7, env: str = "dev") -> dict[str, Any]:
    source = SOURCE.model_copy(
        update={"name": SOURCE.name.replace("_dev_", f"_{env}_")}
    )
    metadata = common_tags(env, "tara", HOST, OP, source, "quarantine")
    return {
        "jobId": JOB,
        "name": f"first_quar_{env}_{HOST}_{OP[:8]}",
        "owner": "openinference_svc@native-submit-host",
        "submitTime": 200_000_000,
        "startTime": 300_000_000,
        "status": {"state": state},
        "env": [{"name": key, "value": value} for key, value in metadata.items()],
        "extension": {
            "job_state": "R" if state == 7 else "Q",
            "exec_vnode": f"({HOST}:ncpus=288:ngpus=4)",
        }
        if state == 7
        else {"job_state": "Q"},
        "queue": {"name": "workq"},
        "accountingId": "inference_service",
        "resourcesRequested": {
            "jobPlacementSharing": 1,
            "taskCount": {"min": 1, "max": 1},
            "tasksResources": [{"index": "0", "candidateMachineName": HOST}],
            "jobResources": {"wallClockTime": 604800},
        },
        "allocatedMachines": [
            {
                "name": HOST,
                "resourcesAvail": {
                    "slots": 288,
                    "gpus": 4,
                    "customResources": [{"name": "host", "value": HOST}],
                },
                "resourcesAssigned": {"slots": 288, "gpus": 4},
                "runningJobs": [],
            }
        ]
        if state == 7
        else [],
    }


def payload(env: str = "dev", event: str = "node_fault") -> dict[str, Any]:
    origin = {
        "id": SOURCE.id,
        "name": SOURCE.name.replace("_dev_", f"_{env}_"),
        "ctime": 100,
    }
    result: dict[str, Any] = {
        "schema_version": 3,
        "event": event,
        "environment": env,
        "cluster": "tara",
        "operation": OP,
        "origin": origin,
        "host": HOST,
        "observed_unix_ns": 1_000_000_000,
    }
    if event == "activated":
        result.update(
            quarantine_job_id=JOB,
            worker_identity={
                "pid": 10,
                "pgid": 10,
                "sid": 10,
                "starttime": 100,
                "uid": 39205,
            },
            gpu_initialized=False,
            expires_unix=604800,
            native_job={"job_state": "R"},
            native_nodes={HOST: {"state": "job-exclusive"}},
        )
    else:
        result.update(
            source="pre_pilot_admission",
            attempt="b" * 32,
            allocation_hosts=[HOST, OTHER],
            master_hsn="10.124.164.33",
            bundle_commit="c" * 40,
            policy_sha256=hashlib.sha256(b"{}").hexdigest(),
            policy_json="{}",
            evidence_path=f"/vast/draco/tara/projects/inference_service/{env}/evidence/pilot-admission/test",
            reports=[{"host": HOST}],
            confirmation=[{"host": HOST}],
            cleanup_proof=None,
        )
    return result


def report(env: str = "dev", event: str = "node_fault") -> dict[str, Any]:
    body = payload(env, event)
    source = QuarantineSource.model_validate(body["origin"])
    kind = "activation" if event == "activated" else "fault"
    encoded = base64.urlsafe_b64encode(json.dumps(body).encode()).decode()
    metadata = {
        **common_tags(env, "tara", HOST, OP, source, kind),
        "FIRST_QUARANTINE_REPORT": encoded,
    }
    return {
        "jobId": "9001.tara-north-pbs-01.lab.alcf.anl.gov",
        "name": f"first_qreport_{env}_{kind}_{OP[:8]}",
        "owner": "openinference_svc@host",
        "submitTime": 400_000_000,
        "status": {"state": 3},
        "extension": {"Hold_Types": "u", "job_state": "H"},
        "env": [{"name": key, "value": value} for key, value in metadata.items()],
    }


class NativeQuarantineAdapterTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.adapter = GraphQLPBSAdapter(
            AsyncMock(),
            "openinference_svc",
            "https://bridge.invalid",
            quarantine_environment="dev",
        )
        self.rpc = AsyncMock()
        patcher = patch.object(self.adapter, "_post", self.rpc)
        patcher.start()
        self.addCleanup(patcher.stop)

    def mock(self, *responses: dict[str, Any]) -> None:
        self.rpc.reset_mock(return_value=True, side_effect=True)
        self.rpc.side_effect = list(responses)

    async def test_normal_submission_remains_unheld_and_untagged(self) -> None:
        self.mock({"createJob": {"node": {"jobId": SOURCE.id}, "error": None}})
        await self.adapter.submit_job(
            JobSubmitPayload(
                name=SOURCE.name,
                queue="workq",
                account="inference_service",
                scheduler_flags="",
                num_nodes=1,
                gpus_per_node=4,
                walltime_min=10,
                log_path=Path("/tmp/model.log"),
                script="#!/bin/sh\nexit 0\n",
            )
        )
        assert self.rpc.await_args is not None
        request = self.rpc.await_args.args[1]["input"]
        self.assertNotIn("submitAsHold", request)
        self.assertNotIn("holdType", request)
        self.assertNotIn("env", request)

    async def test_malformed_auxiliary_metadata_cannot_gate_normal_job_polling(
        self,
    ) -> None:
        bad = report()
        bad["env"] = [{"name": "FIRST_QUARANTINE_REPORT", "value": False}]
        normal = {
            "jobId": SOURCE.id,
            "name": SOURCE.name,
            "owner": "openinference_svc",
            "status": {"state": 0},
            "submitTime": 100_000_000,
        }
        self.mock(connection([bad, normal]))
        statuses = await self.adapter.get_job_statuses()
        self.assertEqual([status.id for status in statuses], [SOURCE.id])

    async def test_ordinary_cleanup_protects_both_environments_and_legacy(self) -> None:
        for name in (
            "first_quar_prod_x4820c0s4b0n0_aaaaaaaa",
            "first_qreport_dev_fault_aaaaaaaa",
            "first_qrequest_prod_screen_aaaaaaaa",
            "first_tara_quarantine_old",
        ):
            self.mock(
                connection(
                    [
                        {
                            "jobId": JOB,
                            "name": name,
                            "owner": "openinference_svc",
                            "status": {"state": 0},
                            "submitTime": 100_000_000,
                        }
                    ]
                )
            )
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                await self.adapter.terminate_job(JOB)
            self.assertEqual(self.rpc.await_count, 1)

    async def test_running_job_proof_uses_all_owners_not_empty_runningjobs(
        self,
    ) -> None:
        row = waiting()
        self.mock(connection([row]), connection([row]))
        status = await self.adapter.get_quarantine_status(JOB)
        assert status is not None
        self.assertTrue(status.exclusive)
        self.assertTrue(status.request_verified)
        self.assertEqual(status.allocated_hosts, (HOST,))
        self.assertEqual(status.expires_unix, 605100)
        assert self.rpc.await_args is not None
        self.assertIsNone(self.rpc.await_args.args[1]["owner"])

    async def test_disguised_structured_infrastructure_is_never_orphan_deleted(
        self,
    ) -> None:
        row = waiting(state=0)
        row["name"] = SOURCE.name
        self.mock(connection([row]))
        self.assertEqual(await self.adapter.get_job_statuses(), [])
        self.mock(connection([row]))
        with self.assertRaises(RuntimeError):
            await self.adapter.terminate_job(JOB)
        self.assertEqual(self.rpc.await_count, 1)

    async def test_measured_native_minimum_zero_normalization_is_narrow(self) -> None:
        row = waiting(state=0)
        row["resourcesRequested"]["taskCount"]["min"] = 0
        self.mock(connection([row]))
        status = await self.adapter.get_quarantine_status(JOB)
        assert status is not None
        self.assertTrue(status.request_verified)
        for key, value in (("max", 2), ("min", True)):
            invalid = copy.deepcopy(row)
            invalid["resourcesRequested"]["taskCount"][key] = value
            self.mock(connection([invalid]))
            with self.subTest(key=key):
                status = await self.adapter.get_quarantine_status(JOB)
                assert status is not None
                self.assertFalse(status.request_verified)

    async def test_foreign_allocation_or_partial_resources_never_prove_isolation(
        self,
    ) -> None:
        for variant in ("foreign", "partial", "wrong-request"):
            row = waiting()
            responses = [connection([row])]
            if variant == "foreign":
                foreign = copy.deepcopy(row)
                foreign["jobId"] = "9002.tara-north-pbs-01.lab.alcf.anl.gov"
                foreign["owner"] = "otheruser"
                responses.append(connection([row, foreign]))
            elif variant == "partial":
                row["allocatedMachines"][0]["resourcesAssigned"]["gpus"] = 0
            else:
                row["resourcesRequested"]["tasksResources"][0][
                    "candidateMachineName"
                ] = OTHER
            self.mock(*responses)
            with self.subTest(variant=variant):
                status = await self.adapter.get_quarantine_status(JOB)
                assert status is not None
                self.assertFalse(status.exclusive)

    async def test_unknown_all_owner_membership_is_not_ignored(self) -> None:
        row = waiting()
        unknown = copy.deepcopy(row)
        unknown["jobId"] = "9002.tara-north-pbs-01.lab.alcf.anl.gov"
        unknown["extension"] = {}
        self.mock(connection([row]), connection([row, unknown]))
        with self.assertRaises(RuntimeError):
            await self.adapter.get_quarantine_status(JOB)

    async def test_quarantine_discovery_uses_name_or_protocol_kind(self) -> None:
        row = waiting(state=0)
        row["name"] = "disguised-quarantine"
        self.mock(connection([row]))
        with self.assertRaises(RuntimeError):
            await self.adapter.list_quarantine_jobs()

    async def test_valid_foreign_owner_is_ignored_but_unknown_owner_is_not(
        self,
    ) -> None:
        row = waiting(state=0)
        row["owner"] = "otheruser@host"
        self.mock(connection([row]))
        self.assertEqual(await self.adapter.list_quarantine_jobs(), [])
        row["owner"] = None
        self.mock(connection([row]))
        with self.assertRaises(RuntimeError):
            await self.adapter.list_quarantine_jobs()

    async def test_reports_from_both_environments_and_malformed_carrier_continue(
        self,
    ) -> None:
        bad = report()
        bad["jobId"] = "9002.tara-north-pbs-01.lab.alcf.anl.gov"
        bad["env"][-1]["value"] = "not-base64"
        other = report("prod")
        other["jobId"] = "9003.tara-north-pbs-01.lab.alcf.anl.gov"
        self.mock(connection([bad, report(), other]))
        reports = await self.adapter.list_quarantine_reports()
        rejected = reports[0]
        assert isinstance(rejected, QuarantineReportRejected)
        self.assertEqual([r.environment for r in reports[1:]], ["dev", "prod"])
        self.assertNotIn("not-base64", rejected.reason)

    async def test_foreign_report_acknowledgement_has_no_mutation(self) -> None:
        parsed = self.adapter._report(report("prod"))
        self.mock()
        with self.assertRaises(RuntimeError):
            await self.adapter.acknowledge_quarantine_report(parsed)
        self.rpc.assert_not_awaited()

    async def test_acknowledgement_rechecks_full_report_fingerprint(self) -> None:
        parsed = self.adapter._report(report())
        changed = report()
        changed["submitTime"] += 1_000_000
        self.mock(connection([changed]))
        with self.assertRaises(RuntimeError):
            await self.adapter.acknowledge_quarantine_report(parsed)
        self.assertEqual(self.rpc.await_count, 1)

    async def test_activation_report_retained_while_quarantine_is_live(self) -> None:
        parsed = self.adapter._report(report(event="activated"))
        row = waiting(state=0)
        self.mock(connection([report(event="activated")]), connection([row]))
        await self.adapter.acknowledge_quarantine_report(parsed)
        self.assertEqual(self.rpc.await_count, 2)

    async def test_source_membership_requires_exact_owner_name_and_ctime(self) -> None:
        row = waiting()
        row.update(jobId=SOURCE.id, name=SOURCE.name, submitTime=100_000_000)
        self.mock(connection([row]))
        self.assertEqual(await self.adapter.get_source_allocation(SOURCE), (HOST,))
        row["submitTime"] += 1_000_000
        self.mock(connection([row]))
        with self.assertRaises(RuntimeError):
            await self.adapter.get_source_allocation(SOURCE)

    async def test_quarantine_submit_is_exact_lightweight_unheld_and_pinned(
        self,
    ) -> None:
        policy = NodeQuarantinePolicy(
            environment="dev",
            worker_path="/vast/draco/tara/projects/inference_service/deployment/artifacts/test/quarantine_wait.py",
            worker_sha256="1" * 64,
            admission_path="/vast/draco/tara/projects/inference_service/deployment/artifacts/test/allocation_admission.py",
            admission_sha256="2" * 64,
            policy_path="/vast/draco/tara/projects/inference_service/deployment/artifacts/test/policy.json",
            policy_sha256="3" * 64,
        )
        spec = QuarantineJobSpec(
            environment="dev",
            cluster="tara",
            host=HOST,
            operation=OP,
            source=SOURCE,
            queue="workq",
            account="inference_service",
            log_path="/tmp/quar.log",
            policy=policy,
        )
        self.mock({"createJob": {"node": {"jobId": JOB}, "error": None}})
        await self.adapter.submit_quarantine(spec)
        assert self.rpc.await_args is not None
        request = self.rpc.await_args.args[1]["input"]
        self.assertNotIn("submitAsHold", request)
        self.assertEqual(request["resourcesRequested"]["jobPlacementSharing"], 2)
        self.assertEqual(
            request["resourcesRequested"]["tasksResources"][0]["candidateMachineName"],
            HOST,
        )
        script = base64.urlsafe_b64decode(request["scriptContent"]).decode()
        assert policy.worker_sha256 is not None
        self.assertIn(policy.worker_sha256, script)
        self.assertIn("LD_PRELOAD", script)
        self.assertNotIn("torch", script)

    async def test_explicit_rejection_differs_from_ambiguous_acknowledgement(
        self,
    ) -> None:
        self.mock({"createJob": {"node": None, "error": {"errorCode": 1}}})
        with self.assertRaises(QuarantineSubmissionRejected):
            await self.adapter._create({"name": "probe"})
        self.mock({"createJob": {"node": {"jobId": JOB}, "error": {"errorCode": 1}}})
        with self.assertRaises(RuntimeError) as error:
            await self.adapter._create({"name": "probe"})
        self.assertNotIsInstance(error.exception, QuarantineSubmissionRejected)

    async def test_generic_policy_needs_no_pbs_paths_but_pbs_creation_requires_pins(
        self,
    ) -> None:
        policy = NodeQuarantinePolicy(environment="dev", policy_sha256="a" * 64)
        self.assertIsNone(policy.worker_path)
        spec = QuarantineJobSpec(
            environment="dev",
            cluster="tara",
            host=HOST,
            operation=OP,
            source=SOURCE,
            queue="workq",
            account="inference_service",
            log_path="/tmp/quar.log",
            policy=policy,
        )
        self.mock()
        with self.assertRaises(RuntimeError):
            await self.adapter.submit_quarantine(spec)
        self.rpc.assert_not_awaited()
        with self.assertRaises(ValueError):
            NodeQuarantinePolicy(
                environment="dev",
                policy_sha256="a" * 64,
                worker_path="/opt/site/worker",
            )


if __name__ == "__main__":
    unittest.main()
