"""
Admin catalog: the full view of every declarative resource, with the
operational detail (cluster pilot configuration, replicas, launch specs,
reconcile errors, logs) that the public catalog on the gateway leaves out.

Served only by the loopback-only control server, behind admin authentication.
"""

from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from typing import Annotated, Any, cast

import sqlalchemy as sa
from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.errors import BadPilotRequest, FirstError
from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterDetail,
    ClusterHealth,
    ClusterSummary,
    LaunchTemplate,
    ModelSummary,
    PilotDeploymentDetail,
    PilotDeploymentSummary,
    PilotJob,
    PilotJobFailure,
    PilotReplica,
    ResourceHealth,
    StaticDeploymentDetail,
    SystemHealth,
)
from first_common.schema.types import PilotConfig, ReplicaState

from ...apiserver.dependencies import AppState, DbSession, RedisRepo
from ...database import models as db
from ...database.redis.router_config import RouterConfig
from ...services.pilot_control import PilotControlClient

router = APIRouter(prefix="/catalog/v1")


async def get_pilot_control(request: Request) -> PilotControlClient:
    """The control server's pilot mTLS client (built once in `serve`)."""
    return cast(PilotControlClient, request.app.state.pilot_control_client)


PilotControl = Annotated[PilotControlClient, Depends(get_pilot_control)]


@router.get("/access-groups", response_model=list[AccessGroup])
async def list_access_groups(sess: DbSession) -> list[db.AccessGroup]:
    return await db.AccessGroup.list(sess)


@router.get("/models", response_model=list[ModelSummary])
async def list_models(sess: DbSession, repo: RedisRepo) -> list[ModelSummary]:
    """Every Model with its deployments, live runtime and maintenance notices."""
    models = await db.Model.list(sess)
    runtimes = await repo.get_many_model_runtimes([m.name for m in models])
    notices = await db.Cluster.maintenance_notices(sess)
    return [
        ModelSummary.merge(
            model,
            runtime=rt,
            maintenance_notices={
                c: notices[c] for c in sorted(model.cluster_names) if c in notices
            },
        )
        for (model, rt) in zip(models, runtimes)
    ]


@router.get("/launch-templates", response_model=list[LaunchTemplate])
async def list_launch_templates(sess: DbSession) -> list[db.LaunchTemplate]:
    return await db.LaunchTemplate.list(sess)


@router.get("/deployments/pilot", response_model=list[PilotDeploymentSummary])
async def list_pilot_deployments(sess: DbSession) -> list[db.PilotDeployment]:
    return await db.PilotDeployment.list(sess)


@router.get("/deployments/pilot/{name:path}", response_model=PilotDeploymentDetail)
async def get_pilot_deployment(
    sess: DbSession, name: str, repo: RedisRepo
) -> PilotDeploymentDetail:
    """Get a single PilotDeployment with its replicas and their live runtime."""
    deployment = await db.PilotDeployment.get_detail(sess, name)
    keys = [(deployment.model_name, r.backend_id) for r in deployment.replicas]
    runtimes = await repo.get_many_backend_runtimes(keys)
    merged_replicas = [
        PilotReplica.merge(r, runtime=rt)
        for r, rt in zip(deployment.replicas, runtimes)
    ]
    return PilotDeploymentDetail.merge(deployment, replicas=merged_replicas)


@router.get("/deployments/static", response_model=list[StaticDeploymentDetail])
async def list_static_deployments(
    sess: DbSession, repo: RedisRepo
) -> list[StaticDeploymentDetail]:
    rows = await db.StaticDeployment.list(sess)
    keys = [(sd.model_name, sd.backend_id) for sd in rows]
    runtimes = await repo.get_many_backend_runtimes(keys)
    return [
        StaticDeploymentDetail.merge(sd, runtime=rt) for sd, rt in zip(rows, runtimes)
    ]


@router.get("/clusters", response_model=list[ClusterSummary])
async def list_clusters(sess: DbSession) -> list[db.Cluster]:
    return await db.Cluster.list(sess)


@router.get("/clusters/{name:path}", response_model=ClusterDetail)
async def get_cluster(sess: DbSession, name: str, repo: RedisRepo) -> ClusterDetail:
    """
    Get a Cluster with its pilot jobs.
    """
    cluster = await db.Cluster.get_detail(sess, name)
    runtimes = await repo.get_pilot_job_runtimes([j.uid for j in cluster.pilot_jobs])

    return ClusterDetail.merge(
        cluster,
        pilot_jobs=[
            PilotJob.merge(job, runtime=rt)
            for job, rt in zip(cluster.pilot_jobs, runtimes)
        ],
    )


# How far back SystemHealth.failed_pilot_jobs reaches, beyond current streaks.
_FAILED_PILOT_JOB_WINDOW = timedelta(days=1)


@router.get("/system-health", response_model=SystemHealth)
async def get_system_health(sess: DbSession) -> SystemHealth:
    """
    One-glance operational snapshot: the health/state and any reconcile error
    of every operational resource, grouped by type, plus pilot jobs that died
    before their manager came up.
    """

    def health_cols(
        model: type[db.ResourceRow], status_col: Any, detail_col: Any = None
    ) -> list[Any]:
        return [
            model.name,
            model.uid,
            status_col.label("status"),
            (detail_col if detail_col is not None else sa.null()).label(
                "health_detail"
            ),
            model.reconcile_failures,
            model.reconcile_last_error,
            model.reconcile_last_traceback,
            model.reconcile_retry_at,
        ]

    async def fetch(
        model: type[db.ResourceRow],
        status_col: sa.orm.Mapped[str],
        *where: Any,
        detail_col: Any = None,
    ) -> list[ResourceHealth]:
        return [
            ResourceHealth.model_validate(row, from_attributes=True)
            for row in await sess.execute(
                sa.select(*health_cols(model, status_col, detail_col)).where(*where)
            )
        ]

    clusters: list[ClusterHealth] = []
    for row in await sess.execute(
        sa.select(
            *health_cols(db.Cluster, db.Cluster.health, db.Cluster.health_detail),
            db.Cluster.pilot_start_failures,
            db.Cluster.pilot_system,
        )
    ):
        max_failures = (
            PilotConfig.model_validate(
                row.pilot_system
            ).max_consecutive_pilot_start_failures
            if row.pilot_system is not None
            else None
        )
        clusters.append(
            ClusterHealth.model_validate(
                {
                    **row._asdict(),
                    "max_consecutive_pilot_start_failures": max_failures,
                }
            )
        )

    return SystemHealth(
        clusters=clusters,
        static_deployments=await fetch(
            db.StaticDeployment,
            db.StaticDeployment.health,
            detail_col=db.StaticDeployment.health_detail,
        ),
        pilot_deployments=await fetch(db.PilotDeployment, db.PilotDeployment.state),
        pilot_jobs=await fetch(
            db.PilotJob, db.PilotJob.manager_health, db.PilotJob.deleted_at.is_(None)
        ),
        pilot_replicas=await fetch(
            db.PilotReplica, db.PilotReplica.state, db.PilotReplica.deleted_at.is_(None)
        ),
        failed_pilot_jobs=await _failed_pilot_jobs(sess, clusters),
    )


async def _failed_pilot_jobs(
    sess: AsyncSession, clusters: list[ClusterHealth]
) -> list[PilotJobFailure]:
    """Pilot jobs with a failure_reason: ended within the window, or part of a
    cluster's current start failure streak. Newest first."""
    failed = db.PilotJob.failure_reason.is_not(None)
    recent = (
        await sess.scalars(
            sa.select(db.PilotJob).where(
                failed,
                db.PilotJob.deleted_at
                > datetime.now(timezone.utc) - _FAILED_PILOT_JOB_WINDOW,
            )
        )
    ).all()
    by_uid = {j.uid: j for j in recent}
    for c in clusters:
        if c.pilot_start_failures == 0:
            continue
        streak = await sess.scalars(
            sa.select(db.PilotJob)
            .where(failed, db.PilotJob.cluster_name == c.name)
            .order_by(db.PilotJob.uid.desc())
            .limit(c.pilot_start_failures)
        )
        by_uid.update((j.uid, j) for j in streak)
    return [
        PilotJobFailure.model_validate(j, from_attributes=True)
        for _, j in sorted(by_uid.items(), reverse=True)
    ]


@router.get("/router-config", response_model=RouterConfig)
async def get_router_config(state: AppState) -> RouterConfig:
    """The RouterConfig currently published to the gateway workers."""
    return await RouterConfig.load(state.redis)


@router.get("/pilot-replicas/{slug}/logs")
async def tail_replica_logs(
    sess: DbSession, slug: str, pilot_control: PilotControl
) -> str:
    """
    Read tail of logs generated by a live replica, from its pilot.
    """
    replica = await db.PilotReplica.get_by_slug(sess, slug)

    if replica.pilot_job_name is None:
        raise BadPilotRequest(f"Replica {replica.name!r} has no PilotJob assigned yet.")

    job = await db.PilotJob.get_by_name(sess, replica.pilot_job_name)

    # Prefer to get logs directly from Pilot (no scheduler adapter)
    if (
        job.manager_url
        and job.scheduler_state == SchedulerJobState.running
        and replica.state
        in (ReplicaState.launching, ReplicaState.ready, ReplicaState.unhealthy)
    ):
        return await pilot_control.get_logs(
            job.manager_url, replica.name, cluster_name=job.cluster_name
        )

    if replica.log_path:
        raise FirstError(
            f"Replica {replica.name!r} is no longer running, so its logs cannot "
            f"be read through the API. Operators can read them on cluster "
            f"{job.cluster_name!r} at {replica.log_path}.",
            status_code=HTTPStatus.CONFLICT,
        )

    raise BadPilotRequest(
        f"Replica {replica.name!r} is not running and has no log_path set yet."
    )
