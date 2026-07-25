from typing import Any

import sqlalchemy as sa
from fastapi import APIRouter

from first_common.errors import AccessDenied
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterDetail,
    ClusterSummary,
    ModelSummary,
    PilotDeploymentDetail,
    PilotDeploymentSummary,
    PilotJob,
    PilotReplica,
    ResourceHealth,
    StaticDeploymentDetail,
    SystemHealth,
)

from ...database import models as db
from ...database.redis.router_config import RouterConfig
from ..auth import user_can_access_group
from ..dependencies import (
    AuthUser,
    DbSession,
    IsUserAdmin,
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
    return [
        ModelSummary.merge(model, runtime=rt) for (model, rt) in zip(models, runtimes)
    ]


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
    Get a Cluster with its pilot jobs.  Admin-only: pilot job details are
    sensitive operational state.
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


@admin_router.get("/system-health", response_model=SystemHealth)
async def get_system_health(sess: DbSession) -> SystemHealth:
    """
    One-glance operational snapshot: the health/state and any reconcile error
    of every operational resource, grouped by type.  Admin-only.
    """

    async def fetch(
        model: type[db.ResourceRow], status_col: sa.orm.Mapped[str], *where: Any
    ) -> list[ResourceHealth]:
        return [  # type: ignore[var-annotated]
            ResourceHealth.model_validate(row, from_attributes=True)
            for row in await sess.execute(
                sa.select(
                    model.name,
                    model.uid,
                    status_col.label("status"),
                    model.reconcile_failures,
                    model.reconcile_last_error,
                    model.reconcile_retry_at,
                ).where(*where)
            )
        ]

    return SystemHealth(
        clusters=await fetch(db.Cluster, db.Cluster.health),
        static_deployments=await fetch(db.StaticDeployment, db.StaticDeployment.health),
        pilot_deployments=await fetch(db.PilotDeployment, db.PilotDeployment.state),
        pilot_jobs=await fetch(
            db.PilotJob, db.PilotJob.manager_health, db.PilotJob.deleted_at.is_(None)
        ),
        pilot_replicas=await fetch(
            db.PilotReplica, db.PilotReplica.state, db.PilotReplica.deleted_at.is_(None)
        ),
    )


@admin_router.get("/router-config", response_model=RouterConfig)
async def get_router_config(cfg: RouterConfigDep) -> RouterConfig:
    return cfg
