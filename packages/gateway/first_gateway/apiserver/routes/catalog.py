import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from fastapi import APIRouter
from sqlalchemy.ext.asyncio import AsyncSession

from first_common.errors import AccessDenied, BadPilotRequest, FirstError
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
from first_common.schema.resources.runtime import ModelRuntime
from first_common.schema.types import PilotConfig, ReplicaState
from first_gateway.platforms.schedulers import build_scheduler

from ...database import models as db
from ...database.redis.router_config import RouterConfig
from ..auth import user_can_access_group
from ..dependencies import (
    AppState,
    AuthUser,
    DbSession,
    IsUserAdmin,
    PilotControl,
    RedisRepo,
    RouterConfigDep,
)

admin_router = APIRouter(prefix="/catalog/v1")
user_router = APIRouter(prefix="/catalog/v1")


@user_router.get("/access-groups", response_model=list[AccessGroup])
async def list_access_groups(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.AccessGroup]:
    """
    List AccessGroups.  Admins see all; ordinary users see only the AccessGroups
    they qualify for.
    """
    groups = await db.AccessGroup.list(sess)
    if is_admin:
        return groups
    return [g for g in groups if user_can_access_group(user, g)]


@user_router.get("/models", response_model=list[ModelSummary])
async def list_models(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin, repo: RedisRepo
) -> list[ModelSummary]:
    """
    List Models.  Admins see all; ordinary users see only Models whose
    AccessGroup grants them access.
    """
    models = await db.Model.list(sess)
    if not is_admin:
        models = [m for m in models if user_can_access_group(user, m.access_group)]

    runtimes = await repo.get_many_model_runtimes([m.name for m in models])
    notices = await db.Cluster.maintenance_notices(sess)
    return [model_summary(model, rt, notices) for (model, rt) in zip(models, runtimes)]


def model_summary(
    model: db.Model, runtime: ModelRuntime, notices: dict[str, str]
) -> ModelSummary:
    """Merge a Model row with its runtime and its clusters' maintenance notices."""
    return ModelSummary.merge(
        model,
        runtime=runtime,
        maintenance_notices={
            c: notices[c] for c in sorted(model.cluster_names) if c in notices
        },
    )


@admin_router.get("/launch-templates", response_model=list[LaunchTemplate])
async def list_launch_templates(sess: DbSession) -> list[db.LaunchTemplate]:
    return await db.LaunchTemplate.list(sess)


@user_router.get("/deployments/pilot", response_model=list[PilotDeploymentSummary])
async def list_pilot_deployments(
    sess: DbSession, user: AuthUser, is_admin: IsUserAdmin
) -> list[db.PilotDeployment]:
    """
    List PilotDeployments.  Admins see all; ordinary users see only deployments
    whose parent Model authorizes them.
    """
    if is_admin:
        return await db.PilotDeployment.list(sess)

    return [
        dep
        for model in await db.Model.list(sess)
        for dep in model.pilot_deployments
        if user_can_access_group(user, model.access_group)
    ]


@user_router.get("/deployments/pilot/{name:path}", response_model=PilotDeploymentDetail)
async def get_pilot_deployment(
    sess: DbSession,
    user: AuthUser,
    name: str,
    is_admin: IsUserAdmin,
    repo: RedisRepo,
) -> PilotDeploymentDetail:
    """Get a single PilotDeployment with its replicas."""
    deployment = await db.PilotDeployment.get_detail(sess, name)
    if not (is_admin or user_can_access_group(user, deployment.model.access_group)):
        raise AccessDenied(f"Permission denied for PilotDeployment {name!r}.")

    keys = [(deployment.model_name, r.backend_id) for r in deployment.replicas]
    runtimes = await repo.get_many_backend_runtimes(keys)
    merged_replicas = [
        PilotReplica.merge(r, runtime=rt)
        for r, rt in zip(deployment.replicas, runtimes)
    ]
    return PilotDeploymentDetail.merge(deployment, replicas=merged_replicas)


@user_router.get("/deployments/static", response_model=list[StaticDeploymentDetail])
async def list_static_deployments(
    sess: DbSession,
    user: AuthUser,
    is_admin: IsUserAdmin,
    repo: RedisRepo,
) -> list[StaticDeploymentDetail]:
    """
    List StaticDeployments.  Admins see all; ordinary users see only deployments
    whose parent Model authorizes them.
    """
    if is_admin:
        rows = await db.StaticDeployment.list(sess)
    else:
        rows = [
            dep
            for model in await db.Model.list(sess)
            for dep in model.static_deployments
            if user_can_access_group(user, model.access_group)
        ]

    keys = [(sd.model_name, sd.backend_id) for sd in rows]
    runtimes = await repo.get_many_backend_runtimes(keys)
    return [
        StaticDeploymentDetail.merge(sd, runtime=rt) for sd, rt in zip(rows, runtimes)
    ]


@user_router.get("/clusters", response_model=list[ClusterSummary])
async def list_clusters(sess: DbSession) -> list[db.Cluster]:
    """List all configured Cluster resources.  Visible to all users."""
    return await db.Cluster.list(sess)


@admin_router.get("/clusters/{name:path}", response_model=ClusterDetail)
async def get_cluster(sess: DbSession, name: str, repo: RedisRepo) -> ClusterDetail:
    """
    Get a Cluster with its pilot jobs. Admin-only.
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


@admin_router.get("/system-health", response_model=SystemHealth)
async def get_system_health(sess: DbSession) -> SystemHealth:
    """
    One-glance operational snapshot: the health/state and any reconcile error
    of every operational resource, grouped by type, plus pilot jobs that died
    before their manager came up.  Admin-only.
    """

    def health_cols(model: type[db.ResourceRow], status_col: Any) -> list[Any]:
        return [
            model.name,
            model.uid,
            status_col.label("status"),
            model.reconcile_failures,
            model.reconcile_last_error,
            model.reconcile_last_traceback,
            model.reconcile_retry_at,
        ]

    async def fetch(
        model: type[db.ResourceRow], status_col: sa.orm.Mapped[str], *where: Any
    ) -> list[ResourceHealth]:
        return [
            ResourceHealth.model_validate(row, from_attributes=True)
            for row in await sess.execute(
                sa.select(*health_cols(model, status_col)).where(*where)
            )
        ]

    clusters: list[ClusterHealth] = []
    for row in await sess.execute(
        sa.select(
            *health_cols(db.Cluster, db.Cluster.health),
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
        static_deployments=await fetch(db.StaticDeployment, db.StaticDeployment.health),
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


@admin_router.get("/router-config", response_model=RouterConfig)
async def get_router_config(cfg: RouterConfigDep) -> RouterConfig:
    return cfg


@admin_router.get("/pilot-replicas/{slug}/logs")
async def tail_replica_logs(
    sess: DbSession, slug: str, client_state: AppState, pilot_control: PilotControl
) -> str:
    """Read tail of logs generated by replica."""
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
        return await pilot_control.get_logs(job.manager_url, replica.name)

    # If job is history, we'll have to read the log file:
    if replica.log_path:
        cluster = await db.Cluster.get_by_name(sess, job.cluster_name)
        adapter = await build_scheduler(
            PilotConfig.model_validate(cluster.pilot_system),
            client_state,
        )
        try:
            async with asyncio.timeout(10):
                return await adapter.read_file(Path(replica.log_path))
        except TimeoutError:
            raise FirstError("Could not read log file within timeout")

    raise BadPilotRequest(
        f"Replica {replica.name!r} is not running and has no log_path set yet."
    )
