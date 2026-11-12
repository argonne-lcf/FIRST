"""
Public catalog: what every authenticated user may read about the resources
their AccessGroups grant, as narrow `*Public` views (see read.py).

Admins are ordinary users here.  The full resources, and every admin-only
read, are served by the loopback-only control server
(controllers/routes/catalog.py).
"""

from collections import Counter

from fastapi import APIRouter

from first_common.errors import AccessDenied
from first_common.schema.resources.read import (
    AccessGroup,
    ClusterPublic,
    ModelPublic,
    PilotDeploymentPublic,
    PilotDeploymentPublicDetail,
    StaticDeploymentPublic,
)
from first_common.schema.types import UsagePolicy

from ...database import models as db
from ..auth import user_can_access_group
from ..dependencies import AuthUser, DbSession, RedisRepo

router = APIRouter(prefix="/catalog/v1")


async def _visible_models(sess: DbSession, user: AuthUser) -> list[db.Model]:
    """The Models whose AccessGroup grants `user` access."""
    return [
        m
        for m in await db.Model.list(sess)
        if user_can_access_group(user, m.access_group)
    ]


@router.get("/access-groups", response_model=list[AccessGroup])
async def list_access_groups(sess: DbSession, user: AuthUser) -> list[db.AccessGroup]:
    """List the AccessGroups the caller qualifies for."""
    return [
        g for g in await db.AccessGroup.list(sess) if user_can_access_group(user, g)
    ]


@router.get("/models", response_model=list[ModelPublic])
async def list_models(
    sess: DbSession, user: AuthUser, repo: RedisRepo
) -> list[ModelPublic]:
    """List the Models the caller may use, with the caller's own usage limits."""
    models = await _visible_models(sess, user)
    runtimes = await repo.get_many_model_runtimes([m.name for m in models])
    notices = await db.Cluster.maintenance_notices(sess)
    return [
        ModelPublic.merge(
            model,
            runtime=rt,
            maintenance_notices={
                c: notices[c] for c in sorted(model.cluster_names) if c in notices
            },
            limits=UsagePolicy.model_validate(model.usage_limits).get_limits(user),
        )
        for (model, rt) in zip(models, runtimes)
    ]


@router.get("/deployments/pilot", response_model=list[PilotDeploymentPublic])
async def list_pilot_deployments(
    sess: DbSession, user: AuthUser
) -> list[PilotDeploymentPublic]:
    """List the PilotDeployments of the Models the caller may use."""
    return [
        PilotDeploymentPublic.model_validate(dep)
        for model in await _visible_models(sess, user)
        for dep in model.pilot_deployments
    ]


@router.get(
    "/deployments/pilot/{name:path}", response_model=PilotDeploymentPublicDetail
)
async def get_pilot_deployment(
    sess: DbSession, user: AuthUser, name: str
) -> PilotDeploymentPublicDetail:
    """Get a PilotDeployment with the number of its live replicas in each state."""
    deployment = await db.PilotDeployment.get_detail(sess, name)
    if not user_can_access_group(user, deployment.model.access_group):
        raise AccessDenied(f"Permission denied for PilotDeployment {name!r}.")

    live = Counter(r.state for r in deployment.replicas if r.deleted_at is None)
    return PilotDeploymentPublicDetail.merge(deployment, replica_counts=live)


@router.get("/deployments/static", response_model=list[StaticDeploymentPublic])
async def list_static_deployments(
    sess: DbSession, user: AuthUser
) -> list[StaticDeploymentPublic]:
    """List the StaticDeployments of the Models the caller may use."""
    return [
        StaticDeploymentPublic.model_validate(dep)
        for model in await _visible_models(sess, user)
        for dep in model.static_deployments
    ]


@router.get("/clusters", response_model=list[ClusterPublic])
async def list_clusters(sess: DbSession) -> list[ClusterPublic]:
    """List every Cluster's health and maintenance notice."""
    return [ClusterPublic.model_validate(c) for c in await db.Cluster.list(sess)]
