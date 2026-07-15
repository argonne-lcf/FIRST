from datetime import datetime
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Self

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    defer,
    joinedload,
    mapped_column,
    relationship,
    selectinload,
)

from first_common.errors import NotFound, SpecApplyError
from first_common.schema.auth import UserAuthEvent
from first_common.schema.base_scheduler import SchedulerJobState
from first_common.schema.pilot import PilotResources
from first_common.schema.types import (
    GpuClaim,
    HealthCheckResult,
    PilotDeploymentState,
    ReplicaState,
    ResourceName,
)

if TYPE_CHECKING:
    from first_common.schema.resources import FieldChange, spec

StrArray = Annotated[
    list[str], mapped_column(MutableList.as_mutable(sa.ARRAY(sa.Text)))
]
DictJsonb = Annotated[dict[str, Any], mapped_column(JSONB)]
DictJsonbOrNone = Annotated[dict[str, Any] | None, mapped_column(JSONB)]
DateTimeOrNone = Annotated[datetime | None, mapped_column(sa.DateTime(timezone=True))]

resource_registry: dict[str, type["ResourceRow"]] = {}


class Base(DeclarativeBase):
    metadata = sa.MetaData(schema="first")
    uid: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True)


controller_manager_lease = sa.Table(
    "controller_manager_lease",
    Base.metadata,
    sa.Column(
        "singleton", sa.Boolean, primary_key=True, server_default=sa.text("true")
    ),
    sa.Column("holder_id", sa.Text, nullable=False),
    sa.Column("renewed_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column(
        "lease_duration",
        sa.Interval,
        nullable=False,
        server_default=sa.text("'30 seconds'"),
    ),
    sa.CheckConstraint("singleton", name="single_row"),
)


class ResourceRow(Base):
    __abstract__ = True
    _BACKOFF_BASE: ClassVar[float] = 10.0
    _MAX_BACKOFF_SEC: ClassVar[float] = 3600.0

    name: Mapped[ResourceName] = mapped_column(sa.Text(), unique=True)
    created_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )

    reconcile_failures: Mapped[int] = mapped_column(default=0)
    reconcile_last_error: Mapped[str | None] = mapped_column(sa.Text(), default=None)
    reconcile_retry_at: Mapped[DateTimeOrNone]

    def __init_subclass__(cls, **kw: Any) -> None:
        super().__init_subclass__(**kw)
        resource_registry[cls.__name__] = cls

    @property
    def kind(self) -> str:
        return self.__class__.__name__

    @classmethod
    async def reset_reconcile_state(
        cls, sess: AsyncSession, uid: int, cascade: bool = False
    ) -> None:
        """Reset reconcile backoff, optionally cascading to children."""
        await sess.execute(
            sa.update(cls)
            .where(cls.uid == uid, cls.reconcile_failures != 0)
            .values(
                reconcile_failures=0,
                reconcile_last_error=None,
                reconcile_retry_at=None,
            )
        )
        if cascade:
            name_subq = sa.select(cls.name).where(cls.uid == uid).scalar_subquery()
            for child_cls, fk_col in _RECONCILE_CASCADES.get(cls.__name__, []):
                await sess.execute(
                    sa.update(child_cls)
                    .where(getattr(child_cls, fk_col) == name_subq)
                    .values(
                        reconcile_failures=0,
                        reconcile_last_error=None,
                        reconcile_retry_at=None,
                    )
                )

    @classmethod
    async def record_failure(cls, sess: AsyncSession, uid: int, exc: Exception) -> None:
        await sess.execute(
            sa.update(cls)
            .where(cls.uid == uid)
            .values(
                reconcile_failures=cls.reconcile_failures + 1,
                reconcile_last_error=str(exc),
                reconcile_retry_at=sa.func.now()
                + sa.func.make_interval(
                    secs=sa.func.least(
                        cls._BACKOFF_BASE * sa.func.power(2, cls.reconcile_failures),
                        cls._MAX_BACKOFF_SEC,
                    )
                ),
            )
        )

    @classmethod
    async def list(cls, sess: AsyncSession) -> list[Self]:
        q = sa.select(cls)
        return list(await sess.scalars(q))

    @classmethod
    async def get_by_name(cls, sess: AsyncSession, name: str) -> Self:
        res = await sess.scalar(sa.select(cls).where(cls.name == name))
        if res is None:
            raise NotFound(f"No {cls.__name__} with {name=!r} was found.")
        return res

    @classmethod
    def create_from_spec(
        cls, sess: AsyncSession, name: str, spec: "spec.ResourceSpec"
    ) -> Self:
        obj = cls(name=name, **spec.model_dump(mode="json"))
        sess.add(obj)
        return obj

    async def delete(self, sess: AsyncSession) -> None:
        await sess.delete(self)

    def apply_patch(self, patch: dict[str, "FieldChange"]) -> None:
        for key, change in patch.items():
            setattr(self, key, change.new)


class ConfigVersion(Base):
    __tablename__ = "config_version"

    applied_at: Mapped[datetime] = mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )
    applied_by: Mapped[str]
    changes: Mapped[DictJsonb]

    @classmethod
    async def get_latest_version(cls, sess: AsyncSession) -> int:
        res = await sess.scalar(sa.select(sa.func.max(cls.uid)))
        return res or 0

    @classmethod
    async def list(cls, sess: AsyncSession) -> list[Self]:
        q = sa.select(cls).options(defer(cls.changes))
        return list(await sess.scalars(q))

    @classmethod
    async def get_detail(cls, sess: AsyncSession, uid: int) -> Self:
        res = await sess.scalar(sa.select(cls).where(cls.uid == uid))
        if res is None:
            raise NotFound(f"No ConfigVersion with {uid=} found.")
        return res

    @classmethod
    async def record_new_version(
        cls,
        previous_version: int,
        changes: dict[str, Any],
        user: UserAuthEvent,
        sess: AsyncSession,
    ) -> Self:
        q = sa.select(sa.exists().where(cls.uid == previous_version))
        previous_exists = await sess.scalar(q)

        if previous_version > 0 and not previous_exists:
            raise SpecApplyError(
                f"The given {previous_version=} does not exist.",
                status_code=HTTPStatus.BAD_REQUEST,
            )

        obj = cls(uid=previous_version + 1, applied_by=user.username, changes=changes)

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


class AccessGroup(ResourceRow):
    __tablename__ = "access_group"

    allowed_groups: Mapped[StrArray]
    allowed_domains: Mapped[StrArray]


class Model(ResourceRow):
    __tablename__ = "model"

    access_group_name: Mapped[str] = mapped_column(sa.ForeignKey("access_group.name"))
    supported_endpoints: Mapped[StrArray]

    aliases: Mapped[StrArray] = mapped_column(default=list)
    usage_limits: Mapped[DictJsonb] = mapped_column(default=dict)
    overload: Mapped[DictJsonb] = mapped_column(default=dict)

    access_group: Mapped[AccessGroup] = relationship(lazy="raise")
    pilot_deployments: Mapped[list["PilotDeployment"]] = relationship(
        back_populates="model", lazy="raise"
    )
    static_deployments: Mapped[list["StaticDeployment"]] = relationship(
        back_populates="model", lazy="raise"
    )

    @classmethod
    async def list(
        cls, sess: AsyncSession, *, load_pilot_replicas: bool = False
    ) -> list[Self]:
        q = sa.select(cls).options(
            joinedload(cls.access_group),
            selectinload(cls.pilot_deployments),
            selectinload(cls.static_deployments),
        )
        if load_pilot_replicas:
            q = q.options(
                selectinload(cls.pilot_deployments).selectinload(
                    PilotDeployment.replicas
                )
            )
        return list(await sess.scalars(q))


class Cluster(ResourceRow):
    __tablename__ = "cluster"

    health_check: Mapped[DictJsonb]
    maintenance_notice: Mapped[str | None]
    pilot_system: Mapped[DictJsonbOrNone]

    health: Mapped[str] = mapped_column(default=HealthCheckResult.unknown.value)

    pilot_jobs: Mapped[list["PilotJob"]] = relationship(
        back_populates="cluster", cascade="all, delete-orphan", lazy="raise"
    )
    pilot_deployments: Mapped[list["PilotDeployment"]] = relationship(
        back_populates="cluster", lazy="raise"
    )
    static_deployments: Mapped[list["StaticDeployment"]] = relationship(
        back_populates="cluster", lazy="raise"
    )

    @classmethod
    async def get_detail(cls, sess: AsyncSession, name: str) -> Self:
        q = (
            sa.select(cls)
            .where(cls.name == name)
            .options(
                selectinload(cls.pilot_jobs).selectinload(PilotJob.assigned_replicas)
            )
        )
        res = await sess.scalar(q)
        if res is None:
            raise NotFound(f"No Cluster with {name=!r} was found.")
        return res


class StaticDeployment(ResourceRow):
    __tablename__ = "static_deployment"

    cluster_name: Mapped[str] = mapped_column(sa.ForeignKey("cluster.name"), index=True)
    model_name: Mapped[str] = mapped_column(sa.ForeignKey("model.name"), index=True)

    api_url: Mapped[str]
    api_key: Mapped[str | None]
    upstream_model_name: Mapped[str]

    router_params: Mapped[DictJsonb]

    health_check: Mapped[DictJsonb]
    health: Mapped[str] = mapped_column(default=HealthCheckResult.unknown.value)

    prometheus_metrics_path: Mapped[str | None]
    prometheus_scrape_interval_sec: Mapped[int]

    cluster: Mapped[Cluster] = relationship(
        back_populates="static_deployments", lazy="raise"
    )
    model: Mapped[Model] = relationship(
        back_populates="static_deployments", lazy="raise"
    )

    @property
    def backend_id(self) -> str:
        """Unique identifier for routeable backend"""
        return f"static_deployment/{self.uid}"


class PilotDeployment(ResourceRow):
    __tablename__ = "pilot_deployment"

    cluster_name: Mapped[str] = mapped_column(sa.ForeignKey("cluster.name"), index=True)
    model_name: Mapped[str] = mapped_column(sa.ForeignKey("model.name"), index=True)
    router_params: Mapped[DictJsonb]

    prometheus_metrics_path: Mapped[str | None]
    prometheus_scrape_interval_sec: Mapped[int]

    scaling_strategy: Mapped[DictJsonbOrNone]
    min_replicas: Mapped[int]
    max_replicas: Mapped[int]

    launch_spec: Mapped[DictJsonb]

    desired_replicas: Mapped[int] = mapped_column(default=0)
    state: Mapped[str] = mapped_column(default=PilotDeploymentState.offline.value)
    consecutive_launch_failures: Mapped[int] = mapped_column(default=0)

    replicas: Mapped[list["PilotReplica"]] = relationship(
        back_populates="pilot_deployment",
        cascade="all, delete-orphan",
        lazy="raise",
    )
    cluster: Mapped[Cluster] = relationship(
        back_populates="pilot_deployments", lazy="raise"
    )
    model: Mapped[Model] = relationship(
        back_populates="pilot_deployments", lazy="raise"
    )

    def set_desired_replicas(self, n: int) -> None:
        self.desired_replicas = n

    @classmethod
    async def get_detail(cls, sess: AsyncSession, name: str) -> Self:
        q = (
            sa.select(cls)
            .where(cls.name == name)
            .options(
                selectinload(cls.replicas),
                joinedload(cls.model).joinedload(Model.access_group),
            )
        )
        res = await sess.scalar(q)
        if res is None:
            raise NotFound(f"No PilotDeployment with {name=!r} was found.")
        return res


class SoftDeletable:
    """
    Mixin class to support soft-deletion:

    - Flip scheduled_deletion to trigger controller cleanup
    - Controller sets `deleted_at` when the resource has cleaned up.
    - sweep_expired() hard-deletes rows where the retention_days has past.
    """

    scheduled_deletion: Mapped[bool] = mapped_column(default=False)
    deleted_at: Mapped[DateTimeOrNone]
    retention_days: Mapped[int] = mapped_column(default=7)

    @classmethod
    async def sweep_expired(cls, sess: AsyncSession) -> int:
        """
        Hard-delete all table resources where deleted_at is set and now() >
        deleted_at + retention_days.  Returns the number of rows deleted.
        """
        stmt = sa.delete(cls).where(
            cls.deleted_at.is_not(None),
            cls.deleted_at
            + sa.cast(
                sa.func.concat(cls.retention_days, " days"),
                sa.Interval(),
            )
            < sa.func.now(),
        )
        cursor = await sess.execute(stmt)
        return int(cursor.rowcount)  # type: ignore[attr-defined]


class PilotJob(ResourceRow, SoftDeletable):
    __tablename__ = "pilot_job"

    cluster_name: Mapped[str] = mapped_column(
        sa.ForeignKey("cluster.name", ondelete="CASCADE"), index=True
    )
    scheduler_job_id: Mapped[str | None]
    scheduler_state: Mapped[str] = mapped_column(
        default=SchedulerJobState.pending_submit.value
    )
    manager_url: Mapped[str | None]
    manager_health: Mapped[str] = mapped_column(default=HealthCheckResult.unknown.value)
    manager_unhealthy_since: Mapped[DateTimeOrNone]
    resources: Mapped[DictJsonb] = mapped_column(JSONB, default=dict)
    used_resources: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    time_started: Mapped[DateTimeOrNone]
    idle_since: Mapped[DateTimeOrNone]
    walltime_min: Mapped[int]
    num_nodes: Mapped[int]
    gpus_per_node: Mapped[int]

    cluster: Mapped[Cluster] = relationship(back_populates="pilot_jobs", lazy="raise")
    assigned_replicas: Mapped[list["PilotReplica"]] = relationship(
        back_populates="pilot_job", lazy="raise"
    )

    @classmethod
    async def assign_replica(
        cls,
        sess: AsyncSession,
        pilot_job_uid: int,
        replica_uid: int,
        resources: list[GpuClaim],
    ) -> bool:
        job = await sess.scalar(
            sa.select(PilotJob).where(PilotJob.uid == pilot_job_uid).with_for_update()
        )
        replica_row = await sess.scalar(
            sa.select(PilotReplica)
            .where(PilotReplica.uid == replica_uid)
            .with_for_update()
        )
        assert job is not None and replica_row is not None

        pilot_resources = PilotResources.model_validate(job.resources)
        known_gpus = {
            (host.hostname, gpu.index)
            for host in pilot_resources.hosts
            for gpu in host.gpus
        }
        requested_gpus = {
            (claim.hostname, gpu_id) for claim in resources for gpu_id in claim.gpu_ids
        }
        if not requested_gpus.issubset(known_gpus):
            raise ValueError(
                f"PilotJob does not possess requested resources: {requested_gpus - known_gpus}"
            )

        used_gpus = {
            (entry["hostname"], gpu_id)
            for entry in job.used_resources
            for gpu_id in entry["gpu_ids"]
        }
        if requested_gpus & used_gpus:
            return False

        new_claims = [claim.model_dump() for claim in resources]
        job.used_resources = job.used_resources + new_claims
        replica_row.used_resources = new_claims
        replica_row.pilot_job_name = job.name
        return True

    @classmethod
    async def unassign_replica(
        cls, sess: AsyncSession, pilot_job_uid: int, replica_uid: int
    ) -> None:
        job = await sess.scalar(
            sa.select(PilotJob).where(PilotJob.uid == pilot_job_uid).with_for_update()
        )
        replica_row = await sess.scalar(
            sa.select(PilotReplica)
            .where(PilotReplica.uid == replica_uid)
            .with_for_update()
        )
        assert job is not None and replica_row is not None
        assert replica_row.pilot_job_name == job.name

        to_remove = {
            (entry["hostname"], gpu_id)
            for entry in replica_row.used_resources
            for gpu_id in entry["gpu_ids"]
        }
        new_used = []
        for entry in job.used_resources:
            remaining = [
                gid
                for gid in entry["gpu_ids"]
                if (entry["hostname"], gid) not in to_remove
            ]
            if remaining:
                new_used.append({"hostname": entry["hostname"], "gpu_ids": remaining})

        job.used_resources = new_used
        replica_row.used_resources = []
        replica_row.pilot_job_name = None


class PilotReplica(ResourceRow, SoftDeletable):
    __tablename__ = "pilot_replica"
    pilot_deployment_name: Mapped[str] = mapped_column(
        sa.ForeignKey("pilot_deployment.name", ondelete="CASCADE"), index=True
    )
    pilot_job_name: Mapped[str | None] = mapped_column(
        sa.ForeignKey("pilot_job.name", ondelete="SET NULL"), index=True
    )
    used_resources: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    model_url: Mapped[str | None]
    observed_served_name: Mapped[str | None]

    state: Mapped[str] = mapped_column(default=ReplicaState.pending.value)
    state_message: Mapped[str] = mapped_column(default="Replica created.")
    started_at: Mapped[DateTimeOrNone]
    stopped_at: Mapped[DateTimeOrNone]

    pilot_deployment: Mapped[PilotDeployment] = relationship(
        back_populates="replicas", lazy="raise"
    )
    pilot_job: Mapped[PilotJob | None] = relationship(
        back_populates="assigned_replicas", lazy="raise"
    )

    @property
    def backend_id(self) -> str:
        """Unique identifier for routeable backend"""
        return f"pilot_replica/{self.uid}"

    @property
    def is_draining(self) -> bool:
        return self.scheduled_deletion or self.state == ReplicaState.terminating


_RECONCILE_CASCADES: dict[str, list[tuple[type[ResourceRow], str]]] = {
    "Cluster": [(PilotJob, "cluster_name")],
    "PilotDeployment": [(PilotReplica, "pilot_deployment_name")],
}
