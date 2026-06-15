from datetime import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any, Self

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from first_common.errors import SpecApplyError
from first_common.schema.types import (
    ClusterStatus,
    DeploymentHealth,
    HealthEndpointStatus,
    PilotJobPhase,
    ReplicaPhase,
    ResourceName,
)

from .connection import AsyncSession

if TYPE_CHECKING:
    from first_common.schema import resource_specs as spec

StrArray = Annotated[
    list[str], mapped_column(MutableList.as_mutable(sa.ARRAY(sa.Text)))
]
DictJsonb = Annotated[dict[str, Any], mapped_column(JSONB)]
DictJsonbOrNone = Annotated[dict[str, Any] | None, mapped_column(JSONB)]
DateTimeOrNone = Annotated[datetime | None, mapped_column(sa.DateTime(timezone=True))]

resource_registry: dict[str, type["ResourceBase"]] = {}


class Base(DeclarativeBase):
    metadata = sa.MetaData(schema="first")

    uid: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)


class ResourceBase(Base):
    __abstract__ = True

    name: Mapped[ResourceName] = mapped_column(sa.Text(), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )
    scheduled_deletion: Mapped[bool] = mapped_column(default=False)

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        resource_registry[cls.__name__] = cls

    @classmethod
    async def list(cls, sess: AsyncSession) -> list[Self]:
        q = sa.select(cls)
        return list(await sess.scalars(q))

    @classmethod
    async def get_by_name(cls, sess: AsyncSession, name: str) -> Self:
        res = await sess.execute(sa.select(cls).where(cls.name == name))
        return res.scalar_one()

    @classmethod
    def create_from_spec(
        cls, sess: AsyncSession, name: str, spec: "spec.ResourceSpec"
    ) -> Self:
        obj = cls(name=name, **spec.model_dump(mode="json"))
        sess.add(obj)
        return obj

    async def delete(self, sess: AsyncSession) -> None:
        await sess.delete(self)

    def apply_patch(self, patch: dict[str, "spec.FieldChange"]) -> None:
        for key, change in patch.items():
            setattr(self, key, change.new)


class ConfigHistory(Base):
    __tablename__ = "config_history"

    applied_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )
    changes: Mapped[DictJsonb]

    @classmethod
    async def get_latest_version(cls, sess: AsyncSession) -> int:
        res = await sess.scalar(sa.select(sa.func.max(cls.uid)))
        return res or 0

    @classmethod
    async def record_new_version(
        cls, previous_version: int, changes: dict[str, Any], sess: AsyncSession
    ) -> Self:
        q = sa.select(sa.exists().where(cls.uid == previous_version))
        previous_exists = await sess.scalar(q)

        if previous_version > 0 and not previous_exists:
            raise SpecApplyError(
                f"The given {previous_version=} does not exist.",
                status_code=HTTPStatus.BAD_REQUEST,
            )

        obj = cls(uid=previous_version + 1, changes=changes)

        try:
            async with sess.begin_nested():
                sess.add(obj)
                await sess.flush()
        except IntegrityError as exc:
            raise SpecApplyError(
                "Stale configuration version: config has already advanced past "
                f"{previous_version=}. Please try again to resolve the conflict.",
                status_code=HTTPStatus.CONFLICT,
            ) from exc

        return obj


class AccessGroup(ResourceBase):
    __tablename__ = "access_group"

    allowed_groups: Mapped[StrArray]
    allowed_domains: Mapped[StrArray]


class Model(ResourceBase):
    __tablename__ = "model"

    access_group_name: Mapped[str] = mapped_column(sa.ForeignKey("access_group.name"))
    supported_endpoints: Mapped[StrArray]

    access_group: Mapped[AccessGroup] = relationship()
    pilot_deployments: Mapped[list["PilotDeployment"]] = relationship(
        back_populates="model"
    )
    static_deployments: Mapped[list["StaticDeployment"]] = relationship(
        back_populates="model"
    )


class Cluster(ResourceBase):
    __tablename__ = "cluster"

    status_method: Mapped[str]
    status_kwargs: Mapped[DictJsonb]
    maintenance_notice: Mapped[str | None]
    pilot_system: Mapped[DictJsonbOrNone]

    status: Mapped[str] = mapped_column(default=ClusterStatus.unknown.value)
    last_status_check: Mapped[DateTimeOrNone]

    pilot_jobs: Mapped[list["PilotJob"]] = relationship(
        back_populates="cluster", cascade="all, delete-orphan"
    )
    pilot_deployments: Mapped[list["PilotDeployment"]] = relationship(
        back_populates="cluster"
    )
    static_deployments: Mapped[list["StaticDeployment"]] = relationship(
        back_populates="cluster"
    )


class StaticDeployment(ResourceBase):
    __tablename__ = "static_deployment"

    cluster_name: Mapped[str] = mapped_column(sa.ForeignKey("cluster.name"))
    model_name: Mapped[str] = mapped_column(sa.ForeignKey("model.name"))

    api_url: Mapped[str]
    api_key: Mapped[str | None]
    upstream_model_name: Mapped[str]

    router_params: Mapped[DictJsonb]

    health_check_method: Mapped[str]
    health_check_kwargs: Mapped[DictJsonb]

    prometheus_metrics_path: Mapped[str | None]
    prometheus_scrape_interval: Mapped[int]
    health: Mapped[str] = mapped_column(default=DeploymentHealth.offline.value)
    last_health_check: Mapped[DateTimeOrNone]

    cluster: Mapped[Cluster] = relationship(back_populates="static_deployments")
    model: Mapped[Model] = relationship(back_populates="static_deployments")


class PilotDeployment(ResourceBase):
    __tablename__ = "pilot_deployment"

    cluster_name: Mapped[str] = mapped_column(sa.ForeignKey("cluster.name"))
    model_name: Mapped[str] = mapped_column(sa.ForeignKey("model.name"))
    router_params: Mapped[DictJsonb]

    health_check_method: Mapped[str]
    health_check_kwargs: Mapped[DictJsonb]

    prometheus_metrics_path: Mapped[str | None]
    prometheus_scrape_interval: Mapped[int]

    scaling_strategy: Mapped[DictJsonbOrNone]
    min_replicas: Mapped[int]
    max_replicas: Mapped[int]

    launch_spec: Mapped[DictJsonb]

    desired_replicas: Mapped[int] = mapped_column(default=0)
    health: Mapped[str] = mapped_column(default=DeploymentHealth.offline.value)
    last_health_check: Mapped[DateTimeOrNone]
    consecutive_launch_failures: Mapped[int] = mapped_column(default=0)

    replicas: Mapped[list["PilotReplica"]] = relationship(
        back_populates="pilot_deployment",
        cascade="all, delete-orphan",
    )
    cluster: Mapped[Cluster] = relationship(back_populates="pilot_deployments")
    model: Mapped[Model] = relationship(back_populates="pilot_deployments")


class PilotJob(ResourceBase):
    __tablename__ = "pilot_job"

    cluster_uid: Mapped[int] = mapped_column(sa.ForeignKey("cluster.uid"))
    scheduler_job_id: Mapped[str | None]
    phase: Mapped[str] = mapped_column(default=PilotJobPhase.pending_submit.value)
    manager_url: Mapped[str | None]
    manager_health: Mapped[str] = mapped_column(
        default=HealthEndpointStatus.unknown.value
    )
    resources: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    time_started: Mapped[DateTimeOrNone]
    idle_since: Mapped[DateTimeOrNone]
    walltime_sec: Mapped[int]

    cluster: Mapped[Cluster] = relationship(back_populates="pilot_jobs")
    assigned_replicas: Mapped[list["PilotReplica"]] = relationship(
        back_populates="pilot_job"
    )


class PilotReplica(ResourceBase):
    __tablename__ = "pilot_replica"
    pilot_deployment_uid: Mapped[int] = mapped_column(
        sa.ForeignKey("pilot_deployment.uid")
    )
    pilot_job_uid: Mapped[int | None] = mapped_column(sa.ForeignKey("pilot_job.uid"))
    used_resources: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    model_url: Mapped[str | None]
    observed_served_name: Mapped[str | None]

    phase: Mapped[str] = mapped_column(default=ReplicaPhase.pending.value)
    health: Mapped[str] = mapped_column(default=HealthEndpointStatus.unknown.value)
    status_info: Mapped[DictJsonb] = mapped_column(default=dict)
    last_health_check: Mapped[DateTimeOrNone]

    pilot_deployment: Mapped[PilotDeployment] = relationship(back_populates="replicas")
    pilot_job: Mapped[PilotJob] = relationship(back_populates="assigned_replicas")
