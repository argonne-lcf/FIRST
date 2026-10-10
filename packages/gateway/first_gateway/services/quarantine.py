"""Durable node isolation, independent of ordinary model scheduling/retries."""

import asyncio
import hashlib
import secrets
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.quarantine import (
    NodeQuarantinePolicy,
    QuarantineActivationPayload,
    QuarantineFaultPayload,
    QuarantineJobIdentity,
    QuarantineJobSpec,
    QuarantineJobStatus,
    QuarantineReport,
    QuarantineReportRejected,
    QuarantineScreenPayload,
    QuarantineSource,
    QuarantineSubmissionRejected,
    SchedulerQuarantineAdapter,
)
from first_common.schema.types import PilotConfig

from ..database.models import Cluster, NodeQuarantine, PilotJob, QuarantineReportInbox
from ..platforms.schedulers import build_scheduler
from ..settings import ClientState
from .quarantine_evidence import activation_proof, cleanup_proof, paired, publication

RPC_TIMEOUT = 30.0
INBOX_RETENTION = timedelta(days=30)
MAX_INBOX_ROWS = 2048
ACK_FAILURE = (
    "Immutable report acknowledgement failed; bounded infrastructure retry only."
)


class ReportPending(Exception):
    """A valid immutable envelope precedes its independent live-state proof."""


def now() -> datetime:
    return datetime.now(timezone.utc)


def transition(row: NodeQuarantine, state: str, message: str) -> None:
    if row.state != state:
        row.transitioned_at = now()
    row.state = state
    row.state_message = message[:300]


def isolated(
    status: QuarantineJobStatus | None, activation: dict[str, Any] | None
) -> bool:
    return bool(
        status is not None
        and status.state == SchedulerJobState.running
        and status.allocated_hosts == (status.identity.host,)
        and status.exclusive
        and status.request_verified
        and status.hold_type in (None, "n")
        and status.expires_unix is not None
        and status.expires_unix > int(now().timestamp())
        and activation is not None
        and activation.get("quarantine_job_id") == status.identity.job_id
        and activation.get("operation") == status.identity.operation
        and activation.get("host") == status.identity.host
        and activation.get("gpu_initialized") is False
    )


class QuarantineService:
    def __init__(self, clients: ClientState):
        self.clients = clients

    async def adapter(self, pilot: PilotConfig) -> SchedulerQuarantineAdapter:
        adapter = await build_scheduler(pilot, self.clients)
        if not isinstance(adapter, SchedulerQuarantineAdapter):
            raise ValueError(
                "configured scheduler does not support optional node quarantine"
            )
        return adapter

    async def ingest_cluster(
        self,
        cluster: Cluster,
        pilot: PilotConfig,
        progress: Callable[[], None] = lambda: None,
    ) -> None:
        policy = pilot.quarantine
        if policy is None:
            return
        adapter = await self.adapter(pilot)
        await self.discover_jobs(cluster, pilot, adapter)
        reports = await asyncio.wait_for(adapter.list_quarantine_reports(), RPC_TIMEOUT)
        for report in reports:
            progress()
            if isinstance(report, QuarantineReportRejected):
                if report.environment not in (None, policy.environment):
                    continue
                await self.rejected_report(cluster, policy, report)
                continue
            if report.payload.cluster != cluster.name:
                continue
            if report.environment != policy.environment and not isinstance(
                report.payload, QuarantineActivationPayload
            ):
                continue  # Another control plane owns this envelope.
            try:
                await self.ingest_report(cluster, pilot, adapter, report)
            except Exception as error:
                await self._infrastructure_receipt(
                    cluster,
                    policy,
                    f"transport-report:{report.job_id}",
                    report.fingerprint,
                    {"job_name": report.job_name},
                    f"Envelope processing unavailable ({type(error).__name__}); no scheduling gate.",
                )
                continue
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(QuarantineReportInbox)
                    .where(
                        QuarantineReportInbox.report_job_id
                        == f"transport-report:{report.job_id}"
                    )
                    .values(validation_error=None)
                )
        # Only acknowledged inbox rows age out. Rejected/unacknowledged notices
        # remain durable; reaching the cap alerts instead of dropping evidence.
        async with self.clients.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.delete(QuarantineReportInbox).where(
                    QuarantineReportInbox.acknowledged_at < now() - INBOX_RETENTION
                )
            )
            await sess.execute(
                sa.update(QuarantineReportInbox)
                .where(
                    QuarantineReportInbox.report_job_id == f"transport:{cluster.name}"
                )
                .values(validation_error=None)
            )

    async def discover_jobs(
        self, cluster: Cluster, pilot: PilotConfig, adapter: SchedulerQuarantineAdapter
    ) -> None:
        """Reconcile already-existing local/foreign jobs, without duplicate submit."""
        assert pilot.quarantine is not None
        jobs = await asyncio.wait_for(adapter.list_quarantine_jobs(), RPC_TIMEOUT)
        for job in jobs:
            if (
                job.identity.cluster == cluster.name
                and job.state != SchedulerJobState.gone
                and not job.request_verified
            ):
                key = f"quarantine-unverified:{job.identity.job_id}"
                await self._infrastructure_receipt(
                    cluster,
                    pilot.quarantine,
                    key,
                    hashlib.sha256(key.encode()).hexdigest(),
                    {"job_name": job.identity.job_name},
                    "Existing quarantine exact-host/exclusivity request is unverified; not adopted.",
                )
        async with self.clients.db_sessionmaker.begin() as sess:
            for job in jobs:
                if (
                    job.identity.cluster != cluster.name
                    or job.state == SchedulerJobState.gone
                    or not job.request_verified
                ):
                    continue
                identity = job.identity
                statement = insert(NodeQuarantine).values(
                    name=f"{cluster.name}/{identity.host}",
                    cluster_name=cluster.name,
                    host=identity.host,
                    environment=identity.environment,
                    operation=identity.operation,
                    origin=identity.source.model_dump(mode="json"),
                    fault={
                        "code": "existing_quarantine",
                        "reason": "Existing owned quarantine discovered after restart.",
                    },
                    evidence={
                        "path": "scheduler immutable job metadata",
                        "policy_sha256": pilot.quarantine.policy_sha256,
                    },
                    job_identity=identity.model_dump(mode="json"),
                    runtime_policy=pilot.quarantine.model_dump(mode="json")
                    if identity.environment == pilot.quarantine.environment
                    else None,
                    state="pending",
                    state_message="Existing quarantine discovered; live isolation verification pending.",
                )
                mutable: dict[str, Any] = {
                    key: statement.excluded[key]
                    for key in (
                        "environment",
                        "operation",
                        "origin",
                        "fault",
                        "evidence",
                        "job_identity",
                        "runtime_policy",
                        "state",
                        "state_message",
                    )
                }
                mutable.update(
                    {
                        key: None
                        for key in (
                            "activation",
                            "expires_at",
                            "submission_started_at",
                            "repair_reference",
                            "screen_nonce",
                            "screen_requested_at",
                            "screen_submission_started_at",
                            "screen_result",
                            "release_requested_at",
                            "deletion_started_at",
                            "deletion_acknowledged_at",
                        )
                    }
                )
                mutable.update(
                    submission_attempts=0,
                    submission_uncertain=False,
                    faulted_at=now(),
                    transitioned_at=now(),
                )
                mutable["evidence"] = statement.excluded.evidence.op("||")(
                    sa.func.jsonb_build_object(
                        "previous_fault",
                        sa.func.jsonb_build_object(
                            "operation",
                            NodeQuarantine.operation,
                            "origin",
                            NodeQuarantine.origin,
                            "fault",
                            NodeQuarantine.fault,
                            "path",
                            NodeQuarantine.evidence["path"],
                            "job_identity",
                            NodeQuarantine.job_identity,
                        ),
                    )
                )
                await sess.execute(
                    statement.on_conflict_do_update(
                        index_elements=[
                            NodeQuarantine.cluster_name,
                            NodeQuarantine.host,
                        ],
                        set_=mutable,
                        where=NodeQuarantine.state == "released",
                    )
                )

    async def _infrastructure_receipt(
        self,
        cluster: Cluster,
        policy: NodeQuarantinePolicy,
        job_id: str,
        fingerprint: str,
        payload: dict[str, Any],
        reason: str,
    ) -> None:
        async with self.clients.db_sessionmaker.begin() as sess:
            exists = await sess.scalar(
                sa.select(QuarantineReportInbox.uid).where(
                    QuarantineReportInbox.report_job_id == job_id
                )
            )
            if exists is None:
                count = await sess.scalar(
                    sa.select(sa.func.count()).select_from(QuarantineReportInbox)
                )
                if (count or 0) >= MAX_INBOX_ROWS and not job_id.startswith(
                    "transport:"
                ):
                    raise ValueError("node quarantine receipt inbox exhausted")
            await sess.execute(
                insert(QuarantineReportInbox)
                .values(
                    name=f"quarantine-report-{job_id}",
                    cluster_name=cluster.name,
                    environment=policy.environment,
                    report_job_id=job_id,
                    fingerprint=fingerprint,
                    payload=payload,
                    validation_error=reason[:300],
                    processed_at=now(),
                )
                .on_conflict_do_update(
                    index_elements=[QuarantineReportInbox.report_job_id],
                    set_={"validation_error": reason[:300]},
                )
            )

    async def rejected_report(
        self,
        cluster: Cluster,
        policy: NodeQuarantinePolicy,
        report: QuarantineReportRejected,
    ) -> None:
        await self._infrastructure_receipt(
            cluster,
            policy,
            report.job_id,
            report.fingerprint,
            {
                "job_name": report.job_name,
                "ctime": report.ctime,
                "reason": report.reason,
            },
            report.reason,
        )
        # Malformed/unverified carriers are never acknowledged or deleted.

    async def ingestion_failure(
        self, cluster: Cluster, pilot: PilotConfig, error: Exception
    ) -> None:
        assert pilot.quarantine is not None
        key = f"transport:{cluster.name}"
        await self._infrastructure_receipt(
            cluster,
            pilot.quarantine,
            key,
            hashlib.sha256(key.encode()).hexdigest(),
            {"kind": "infrastructure"},
            f"Auxiliary quarantine inventory unavailable ({type(error).__name__}); ordinary model scheduling is unaffected.",
        )

    async def ingest_report(
        self,
        cluster: Cluster,
        pilot: PilotConfig,
        adapter: SchedulerQuarantineAdapter,
        report: QuarantineReport,
    ) -> None:
        async with self.clients.db_sessionmaker.begin() as sess:
            exists = await sess.scalar(
                sa.select(QuarantineReportInbox.uid).where(
                    QuarantineReportInbox.report_job_id == report.job_id
                )
            )
            if exists is None:
                count = await sess.scalar(
                    sa.select(sa.func.count()).select_from(QuarantineReportInbox)
                )
                if count is not None and count >= MAX_INBOX_ROWS:
                    raise ValueError(
                        "node quarantine report inbox is full; infrastructure review required"
                    )
            await sess.execute(
                insert(QuarantineReportInbox)
                .values(
                    name=f"quarantine-report-{report.job_id}",
                    cluster_name=cluster.name,
                    environment=report.environment,
                    report_job_id=report.job_id,
                    fingerprint=report.fingerprint,
                    payload=report.payload.model_dump(mode="json"),
                )
                .on_conflict_do_nothing(
                    index_elements=[QuarantineReportInbox.report_job_id]
                )
            )
            inbox = await sess.scalar(
                sa.select(QuarantineReportInbox)
                .where(QuarantineReportInbox.report_job_id == report.job_id)
                .with_for_update()
            )
            assert inbox is not None
            if inbox.fingerprint != report.fingerprint:
                inbox.validation_error = (
                    "scheduler report identity changed payload fingerprint"
                )
                return
            processed = inbox.processed_at is not None
            acknowledged = inbox.acknowledged_at is not None
            attempts = inbox.reconcile_failures
            retry_at = inbox.reconcile_retry_at
            receipt_error = inbox.validation_error
        if not processed:
            try:
                if isinstance(report.payload, QuarantineFaultPayload):
                    await self._fault(cluster, pilot, adapter, report)
                else:
                    await self._worker_report(cluster, pilot, adapter, report)
            except ReportPending:
                return  # Retain and retry the same envelope, never resubmit work.
            except (ValueError, KeyError, TypeError, RuntimeError) as error:
                receipt_error = str(error)[:300]
                async with self.clients.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(QuarantineReportInbox)
                        .where(QuarantineReportInbox.report_job_id == report.job_id)
                        .values(validation_error=receipt_error, processed_at=now())
                    )
            else:
                async with self.clients.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(QuarantineReportInbox)
                        .where(QuarantineReportInbox.report_job_id == report.job_id)
                        .values(processed_at=now())
                    )
        local = (
            pilot.quarantine is not None
            and report.environment == pilot.quarantine.environment
        )
        activation_live = False
        if isinstance(report.payload, QuarantineActivationPayload):
            current = await asyncio.wait_for(
                adapter.get_quarantine_status(report.payload.quarantine_job_id),
                RPC_TIMEOUT,
            )
            activation_live = (
                current is not None and current.state != SchedulerJobState.gone
            )
        if (
            not acknowledged
            and local
            and not activation_live
            and attempts < 3
            and (retry_at is None or retry_at <= now())
        ):
            try:
                await asyncio.wait_for(
                    adapter.acknowledge_quarantine_report(report), RPC_TIMEOUT
                )
            except Exception:
                async with self.clients.db_sessionmaker.begin() as sess:
                    await sess.execute(
                        sa.update(QuarantineReportInbox)
                        .where(QuarantineReportInbox.report_job_id == report.job_id)
                        .values(
                            reconcile_failures=attempts + 1,
                            reconcile_retry_at=now()
                            + timedelta(seconds=30 * 2**attempts),
                            validation_error=receipt_error or ACK_FAILURE,
                        )
                    )
                return
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(QuarantineReportInbox)
                    .where(QuarantineReportInbox.report_job_id == report.job_id)
                    .values(
                        acknowledged_at=now(),
                        reconcile_failures=0,
                        reconcile_retry_at=None,
                        validation_error=None
                        if receipt_error == ACK_FAILURE
                        else receipt_error,
                    )
                )

    async def _fault(
        self,
        cluster: Cluster,
        pilot: PilotConfig,
        adapter: SchedulerQuarantineAdapter,
        report: QuarantineReport,
    ) -> None:
        payload = report.payload
        assert (
            isinstance(payload, QuarantineFaultPayload) and pilot.quarantine is not None
        )
        async with self.clients.db_sessionmaker() as sess:
            job = await sess.scalar(
                sa.select(PilotJob).where(
                    PilotJob.cluster_name == cluster.name,
                    PilotJob.scheduler_job_id == payload.origin.id,
                )
            )
            if job is None or payload.origin.name != pilot.job_name_prefix + job.name:
                raise ValueError(
                    "node fault did not originate in this control plane's registered allocation"
                )
        allocation = await asyncio.wait_for(
            adapter.get_source_allocation(payload.origin), RPC_TIMEOUT
        )
        if tuple(payload.allocation_hosts) != allocation:
            raise ValueError(
                "fault envelope allocation differs from independently verified scheduler ownership"
            )
        if pilot.quarantine.worker_path is not None:
            publication(payload, pilot.quarantine.worker_path)
        fault = paired(payload, pilot.quarantine.policy_sha256, pilot.gpus_per_node)
        assert fault is not None
        if payload.source == "lifecycle_post_cleanup":
            cleanup_proof(payload.cleanup_proof or "", payload.allocation_hosts)
        async with self.clients.db_sessionmaker.begin() as sess:
            row = await sess.scalar(
                sa.select(NodeQuarantine)
                .where(
                    NodeQuarantine.cluster_name == cluster.name,
                    NodeQuarantine.host == payload.host,
                )
                .with_for_update()
            )
            if row is not None and row.state != "released":
                return  # One unresolved operation per physical node; no scheduling gate.
            values: dict[str, Any] = dict(
                environment=payload.environment,
                operation=payload.operation,
                origin=payload.origin.model_dump(mode="json"),
                fault=fault,
                evidence={
                    "path": payload.evidence_path,
                    "report_id": report.job_id,
                    "fingerprint": report.fingerprint,
                    "policy_sha256": payload.policy_sha256,
                },
                state="pending",
                state_message="Node fault recorded; isolation pending.",
                faulted_at=now(),
                transitioned_at=now(),
                submission_attempts=0,
                submission_started_at=None,
                submission_uncertain=False,
                job_identity=None,
                runtime_policy=None,
                activation=None,
                expires_at=None,
                repair_reference=None,
                screen_nonce=None,
                screen_requested_at=None,
                screen_submission_started_at=None,
                screen_result=None,
                release_requested_at=None,
                deletion_started_at=None,
                deletion_acknowledged_at=None,
            )
            if row is None:
                sess.add(
                    NodeQuarantine(
                        name=f"{cluster.name}/{payload.host}",
                        cluster_name=cluster.name,
                        host=payload.host,
                        **values,
                    )
                )
            else:
                values["evidence"]["previous_fault"] = {
                    "operation": row.operation,
                    "origin": row.origin,
                    "fault": row.fault,
                    "path": row.evidence["path"],
                    "job_identity": row.job_identity,
                }
                for key, value in values.items():
                    setattr(row, key, value)

    async def _worker_report(
        self,
        cluster: Cluster,
        pilot: PilotConfig,
        adapter: SchedulerQuarantineAdapter,
        report: QuarantineReport,
    ) -> None:
        payload = report.payload
        assert isinstance(
            payload, (QuarantineActivationPayload, QuarantineScreenPayload)
        )
        async with self.clients.db_sessionmaker() as sess:
            row = await sess.scalar(
                sa.select(NodeQuarantine).where(
                    NodeQuarantine.cluster_name == cluster.name,
                    NodeQuarantine.host == payload.host,
                )
            )
        if row is None or row.job_identity is None:
            raise ReportPending(
                "worker envelope awaits durable quarantine job identity"
            )
        identity = QuarantineJobIdentity.model_validate(row.job_identity)
        if (
            identity.environment != payload.environment
            or identity.operation != payload.operation
            or identity.job_id != payload.quarantine_job_id
            or identity.source != payload.origin
        ):
            raise ValueError(
                "worker envelope does not match the current quarantine operation"
            )
        status = await asyncio.wait_for(
            adapter.get_quarantine_status(identity.job_id), RPC_TIMEOUT
        )
        if (
            status is not None
            and status.identity == identity
            and status.state in (SchedulerJobState.queued, SchedulerJobState.starting)
        ):
            raise ReportPending("worker envelope awaits independent running status")
        if (
            status is None
            or status.identity != identity
            or status.state != SchedulerJobState.running
        ):
            raise ValueError(
                "worker envelope's exact quarantine allocation is not running"
            )
        if isinstance(payload, QuarantineActivationPayload):
            if payload.expires_unix != status.expires_unix:
                raise ValueError(
                    "waiting worker expiration differs from scheduler evidence"
                )
            activation_proof(payload, status)
            values = {"activation": payload.model_dump(mode="json")}
        else:
            assert pilot.quarantine is not None
            if row.screen_nonce != payload.nonce or row.screen_requested_at is None:
                raise ValueError("screen envelope nonce was not explicitly requested")
            if payload.observed_unix_ns < int(
                row.screen_requested_at.timestamp() * 1e9
            ):
                raise ValueError("screen envelope predates its requested nonce")
            expected_sha = (row.runtime_policy or {}).get(
                "policy_sha256", row.evidence["policy_sha256"]
            )
            if not isinstance(expected_sha, str):
                raise ValueError("trusted quarantine screening digest is unavailable")
            worker_path = (
                row.runtime_policy or pilot.quarantine.model_dump(mode="json")
            ).get("worker_path")
            if isinstance(worker_path, str):
                publication(payload, worker_path)
            fault = paired(payload, expected_sha, pilot.gpus_per_node)
            if payload.outcome != "PASS" or fault is not None:
                raise ValueError(
                    "fresh native screening did not pass; node remains quarantined"
                )
            values = {"screen_result": payload.model_dump(mode="json")}
        async with self.clients.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(NodeQuarantine)
                .where(
                    NodeQuarantine.uid == row.uid,
                    NodeQuarantine.operation == row.operation,
                    NodeQuarantine.job_identity == row.job_identity,
                )
                .values(**values)
            )

    async def reconcile(self, uid: int) -> None:
        async with self.clients.db_sessionmaker() as sess:
            row = await sess.get(NodeQuarantine, uid)
            if row is None or row.state == "released":
                return
            cluster = await sess.scalar(
                sa.select(Cluster).where(Cluster.name == row.cluster_name)
            )
        if cluster is None or cluster.pilot_system is None:
            raise ValueError("quarantine record has no cluster scheduler configuration")
        pilot = PilotConfig.model_validate(cluster.pilot_system)
        policy = pilot.quarantine
        if policy is None:
            raise ValueError(
                "quarantine policy is disabled while an unresolved record remains"
            )
        adapter = await self.adapter(pilot)
        jobs = await asyncio.wait_for(adapter.list_quarantine_jobs(), RPC_TIMEOUT)
        matches = [
            job
            for job in jobs
            if job.identity.cluster == row.cluster_name
            and job.identity.host == row.host
            and job.state != SchedulerJobState.gone
        ]
        if len(matches) > 1:
            await self._state(
                uid,
                "failed",
                "Multiple quarantine jobs target this node; operator reconciliation required.",
            )
            return
        if row.job_identity is None and matches:
            job = matches[0]
            if not job.request_verified:
                await self._state(
                    uid,
                    "failed",
                    "Existing quarantine request is unverified; no adoption or duplicate submission.",
                )
                return
            if row.submission_started_at is not None and (
                job.identity.environment != policy.environment
                or job.identity.operation != row.operation
                or job.identity.cluster != row.cluster_name
                or job.identity.host != row.host
                or job.identity.source != QuarantineSource.model_validate(row.origin)
            ):
                await self._state(
                    uid,
                    "failed",
                    "Existing quarantine does not match the full durable submit intent; operator reconciliation required.",
                )
                return
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(NodeQuarantine)
                    .where(
                        NodeQuarantine.uid == uid, NodeQuarantine.job_identity.is_(None)
                    )
                    .values(
                        job_identity=job.identity.model_dump(mode="json"),
                        submission_uncertain=False,
                    )
                )
            # Another environment's job is observer-only; never release it.
            return
        if row.job_identity is None:
            await self._submit(row, pilot, adapter)
            return
        identity = QuarantineJobIdentity.model_validate(row.job_identity)
        status = await asyncio.wait_for(
            adapter.get_quarantine_status(identity.job_id), RPC_TIMEOUT
        )
        if status is not None and status.identity != identity:
            raise ValueError("quarantine job identity changed")
        if row.deletion_acknowledged_at is not None and (
            status is None or status.state == SchedulerJobState.gone
        ):
            await self._state(
                uid,
                "released",
                "Explicitly repaired, screened and returned to service.",
            )
            return
        if status is None or status.state == SchedulerJobState.gone:
            await self._state(
                uid, "lost", "Quarantine job disappeared; no node recovery is inferred."
            )
            return
        if row.deletion_started_at is not None:
            await self._state(
                uid,
                "failed",
                "Explicit deletion acknowledgement is uncertain; no repeated deletion or recovery inference.",
            )
            return
        active = isolated(status, row.activation)
        if row.release_requested_at is not None:
            await self._release(row, pilot, adapter, status, active)
            return
        if not active:
            await self._state(
                uid,
                "lost" if row.state == "isolated" else "pending",
                "Isolation unverified: exact running host, exclusivity or worker activation is missing.",
            )
            return
        async with self.clients.db_sessionmaker.begin() as sess:
            current = await sess.get(NodeQuarantine, uid, with_for_update=True)
            assert current is not None
            transition(
                current,
                "isolated",
                "Quarantine running on the exact exclusive physical host.",
            )
            current.expires_at = datetime.fromtimestamp(
                status.expires_unix or 0, timezone.utc
            )
        if (
            identity.environment == policy.environment
            and row.screen_nonce is not None
            and row.screen_result is None
            and row.screen_submission_started_at is None
        ):
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(NodeQuarantine)
                    .where(
                        NodeQuarantine.uid == uid,
                        NodeQuarantine.screen_nonce == row.screen_nonce,
                    )
                    .values(screen_submission_started_at=now())
                )
            await asyncio.wait_for(
                adapter.request_quarantine_screen(identity, row.screen_nonce),
                RPC_TIMEOUT,
            )

    async def _submit(
        self,
        row: NodeQuarantine,
        pilot: PilotConfig,
        adapter: SchedulerQuarantineAdapter,
    ) -> None:
        policy = pilot.quarantine
        assert policy is not None
        if row.submission_uncertain or row.submission_started_at is not None:
            await self._state(
                row.uid,
                "failed",
                "Submission acknowledgement is uncertain; reconcile exact operation, never blindly resubmit.",
            )
            return
        async with self.clients.db_sessionmaker.begin() as sess:
            current = await sess.get(NodeQuarantine, row.uid, with_for_update=True)
            assert current is not None
            if current.submission_attempts >= policy.max_submission_attempts:
                transition(
                    current,
                    "failed",
                    "Bounded quarantine submission retries exhausted.",
                )
                return
            recent = await sess.scalar(
                sa.select(sa.func.count())
                .select_from(NodeQuarantine)
                .where(
                    NodeQuarantine.cluster_name == row.cluster_name,
                    NodeQuarantine.environment == policy.environment,
                    NodeQuarantine.faulted_at >= now() - timedelta(minutes=15),
                    NodeQuarantine.uid != row.uid,
                    NodeQuarantine.submission_attempts > 0,
                )
            )
            active = await sess.scalar(
                sa.select(sa.func.count())
                .select_from(NodeQuarantine)
                .where(
                    NodeQuarantine.cluster_name == row.cluster_name,
                    NodeQuarantine.environment == policy.environment,
                    NodeQuarantine.state != "released",
                    NodeQuarantine.uid != row.uid,
                    sa.or_(
                        NodeQuarantine.submission_attempts > 0,
                        NodeQuarantine.job_identity.is_not(None),
                    ),
                )
            )
            if (recent or 0) >= policy.max_new_nodes_15min or (
                active or 0
            ) >= policy.max_active_nodes:
                transition(
                    current,
                    "failed",
                    "Local quarantine intake budget reached; infrastructure review required.",
                )
                return
            current.submission_attempts += 1
            current.submission_started_at = now()
            current.runtime_policy = policy.model_dump(mode="json")
        spec = QuarantineJobSpec(
            environment=policy.environment,
            cluster=row.cluster_name,
            host=row.host,
            operation=row.operation,
            source=QuarantineSource.model_validate(row.origin),
            queue=pilot.queue,
            account=pilot.account,
            log_path=str(
                pilot.workdir
                / "submit_scripts"
                / f"quarantine-{row.host}-{row.operation[:8]}.log"
            ),
            policy=policy,
        )
        try:
            result = await asyncio.wait_for(
                adapter.submit_quarantine(spec), RPC_TIMEOUT
            )
        except QuarantineSubmissionRejected:
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(NodeQuarantine)
                    .where(NodeQuarantine.uid == row.uid)
                    .values(
                        submission_started_at=None,
                        state="pending",
                        state_message="Quarantine submission explicitly rejected; bounded retry pending.",
                    )
                )
            raise
        except Exception:
            async with self.clients.db_sessionmaker.begin() as sess:
                await sess.execute(
                    sa.update(NodeQuarantine)
                    .where(NodeQuarantine.uid == row.uid)
                    .values(
                        submission_uncertain=True,
                        state="failed",
                        state_message="Quarantine submit acknowledgement uncertain; exact-operation adoption required.",
                    )
                )
            raise
        status = await asyncio.wait_for(
            adapter.get_quarantine_status(result.scheduler_id), RPC_TIMEOUT
        )
        if (
            status is None
            or not status.request_verified
            or status.identity.operation != row.operation
            or status.identity.host != row.host
            or status.identity.environment != spec.environment
            or status.identity.cluster != spec.cluster
            or status.identity.source != spec.source
            or status.identity.job_name != spec.job_name
        ):
            raise ValueError(
                "submitted quarantine acknowledgement lacks exact requested identity"
            )
        async with self.clients.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(NodeQuarantine)
                .where(NodeQuarantine.uid == row.uid)
                .values(
                    job_identity=status.identity.model_dump(mode="json"),
                    submission_uncertain=False,
                )
            )

    async def _release(
        self,
        row: NodeQuarantine,
        pilot: PilotConfig,
        adapter: SchedulerQuarantineAdapter,
        status: QuarantineJobStatus,
        active: bool,
    ) -> None:
        assert pilot.quarantine is not None
        if status.identity.environment != pilot.quarantine.environment:
            raise ValueError(
                "only the owning control-plane environment may release this quarantine"
            )
        if (
            not active
            or not row.repair_reference
            or row.screen_result is None
            or row.screen_requested_at is None
        ):
            raise ValueError(
                "explicit remediation, current isolation and matching fresh screen are required"
            )
        payload = QuarantineScreenPayload.model_validate(row.screen_result)
        if payload.nonce != row.screen_nonce or payload.observed_unix_ns < int(
            row.screen_requested_at.timestamp() * 1e9
        ):
            raise ValueError(
                "return-to-service screen does not match the current request"
            )
        expected_sha = (row.runtime_policy or {}).get(
            "policy_sha256", row.evidence["policy_sha256"]
        )
        if not isinstance(expected_sha, str):
            raise ValueError("trusted quarantine screening digest is unavailable")
        if (
            payload.outcome != "PASS"
            or paired(payload, expected_sha, pilot.gpus_per_node) is not None
        ):
            raise ValueError("fresh native screen failed; isolation retained")
        async with self.clients.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(NodeQuarantine)
                .where(NodeQuarantine.uid == row.uid)
                .values(deletion_started_at=now())
            )
        await asyncio.wait_for(adapter.delete_quarantine(status.identity), RPC_TIMEOUT)
        async with self.clients.db_sessionmaker.begin() as sess:
            await sess.execute(
                sa.update(NodeQuarantine)
                .where(
                    NodeQuarantine.uid == row.uid,
                    NodeQuarantine.job_identity == row.job_identity,
                )
                .values(deletion_acknowledged_at=now())
            )
        # Keep the durable record unresolved until the next exact lookup proves
        # the deliberately deleted allocation gone. qdel ACK is not absence.

    async def _state(self, uid: int, state: str, message: str) -> None:
        async with self.clients.db_sessionmaker.begin() as sess:
            row = await sess.get(NodeQuarantine, uid, with_for_update=True)
            if row is not None:
                transition(row, state, message)

    async def request_screen(self, uid: int, reference: str) -> None:
        async with self.clients.db_sessionmaker.begin() as sess:
            row = await sess.get(NodeQuarantine, uid, with_for_update=True)
            if row is None or row.state != "isolated" or row.job_identity is None:
                raise ValueError("only an isolated node may be deliberately screened")
            await self._require_local(sess, row)
            row.repair_reference = reference
            row.screen_nonce = secrets.token_hex(16)
            row.screen_requested_at = now()
            row.screen_submission_started_at = None
            row.screen_result = None
            row.release_requested_at = None
            row.deletion_started_at = None
            row.deletion_acknowledged_at = None
            row.reconcile_retry_at = None

    async def request_release(self, uid: int, reference: str) -> None:
        async with self.clients.db_sessionmaker.begin() as sess:
            row = await sess.get(NodeQuarantine, uid, with_for_update=True)
            if (
                row is None
                or row.state != "isolated"
                or row.repair_reference != reference
                or row.screen_result is None
            ):
                raise ValueError(
                    "matching explicit repair reference and completed screen are required"
                )
            await self._require_local(sess, row)
            row.release_requested_at = now()
            row.reconcile_retry_at = None

    async def request_reestablish(self, uid: int, reference: str) -> None:
        async with self.clients.db_sessionmaker() as sess:
            row = await sess.get(NodeQuarantine, uid)
            if row is None or row.state not in {"lost", "failed"}:
                raise ValueError(
                    "only lost/failed isolation may be explicitly reestablished"
                )
            cluster = await sess.scalar(
                sa.select(Cluster).where(Cluster.name == row.cluster_name)
            )
        if cluster is None or cluster.pilot_system is None:
            raise ValueError("quarantine cluster ownership is unresolved")
        pilot = PilotConfig.model_validate(cluster.pilot_system)
        if pilot.quarantine is None or row.environment != pilot.quarantine.environment:
            raise ValueError(
                "only the originating environment may reestablish isolation"
            )
        if (
            row.job_identity is not None
            and QuarantineJobIdentity.model_validate(row.job_identity).environment
            != pilot.quarantine.environment
        ):
            raise ValueError(
                "another environment owns this quarantine; observer-only access"
            )
        adapter = await self.adapter(pilot)
        jobs = await asyncio.wait_for(adapter.list_quarantine_jobs(), RPC_TIMEOUT)
        if any(
            job.identity.cluster == row.cluster_name
            and job.identity.host == row.host
            and job.state != SchedulerJobState.gone
            for job in jobs
        ):
            raise ValueError("existing isolation must be reconciled, not duplicated")
        async with self.clients.db_sessionmaker.begin() as sess:
            current = await sess.get(NodeQuarantine, uid, with_for_update=True)
            if (
                current is None
                or current.operation != row.operation
                or current.state not in {"lost", "failed"}
            ):
                raise ValueError(
                    "quarantine changed while validating explicit recovery"
                )
            current.evidence = {
                **current.evidence,
                "isolation_history": [
                    *(current.evidence.get("isolation_history", []))[-2:],
                    {
                        "operation": current.operation,
                        "job_identity": current.job_identity,
                        "state": current.state,
                        "operator_reference": reference,
                    },
                ],
            }
            current.operation = secrets.token_hex(16)
            current.job_identity = None
            current.activation = None
            current.submission_started_at = None
            current.submission_uncertain = False
            current.repair_reference = reference
            current.screen_nonce = None
            current.screen_requested_at = None
            current.screen_submission_started_at = None
            current.screen_result = None
            current.release_requested_at = None
            current.deletion_started_at = None
            current.deletion_acknowledged_at = None
            current.reconcile_retry_at = None
            # Preserve lifetime submit count and fault evidence; explicit recovery
            # is not an unlimited retry budget or proof of node remediation.
            transition(
                current,
                "pending",
                "Explicitly requested isolation recovery; bounded attempts remain.",
            )

    async def _require_local(self, sess: Any, row: NodeQuarantine) -> None:
        cluster = await sess.scalar(
            sa.select(Cluster).where(Cluster.name == row.cluster_name)
        )
        if cluster is None or cluster.pilot_system is None or row.job_identity is None:
            raise ValueError("quarantine ownership is unresolved")
        pilot = PilotConfig.model_validate(cluster.pilot_system)
        identity = QuarantineJobIdentity.model_validate(row.job_identity)
        if (
            pilot.quarantine is None
            or identity.environment != pilot.quarantine.environment
        ):
            raise ValueError(
                "the other environment owns this quarantine; observer-only access"
            )
