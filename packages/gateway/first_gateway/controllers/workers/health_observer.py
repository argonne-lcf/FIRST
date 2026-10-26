import asyncio
import logging
from collections import defaultdict

import sqlalchemy as sa
from httpx import Client
from pydantic import ValidationError

from first_common.health import perform_health_check
from first_common.schema.types import (
    HealthCheckParams,
    HealthCheckResult,
    MTLSClientConfig,
    MTLSLoadError,
)

from ...database.models import Cluster, StaticDeployment
from ...settings import ClientState
from ..wakeup import WakeupDispatcher
from ..worker import Worker

logger = logging.getLogger(__name__)


class HealthObserver(Worker):
    """
    Polls the configured health endpoint of Clusters and StaticDeployments.

    Writes the aggregated `health` to Postgres only on transition, along with
    a `health_detail` describing the latest failure. Healthy->Unhealthy
    transitions are debounced to mitigate intermittent failures.

    The failure reason is logged at WARNING only on the first failure of a
    streak; repeats are logged at DEBUG.
    """

    poll_interval = 120.0

    def __init__(
        self,
        name: str,
        client_state: ClientState,
        wakeup_dispatcher: WakeupDispatcher,
        *,
        restart_backoff: float = 1.0,
        max_backoff: float = 30.0,
        heartbeat_timeout: float = 240.0,
    ) -> None:
        super().__init__(
            name,
            client_state,
            wakeup_dispatcher,
            restart_backoff=restart_backoff,
            max_backoff=max_backoff,
            heartbeat_timeout=heartbeat_timeout,
        )
        self.fail_counts: dict[tuple[str, int], int] = defaultdict(int)
        self.health_client = Client()
        # One client per StaticDeployment mTLS certificate set
        self.mtls_clients: dict[MTLSClientConfig, Client] = {}

    async def run(self) -> None:
        self.hb = self.register_heartbeat("poll")
        while True:
            self.hb.beat()
            await self._poll()
            await self.wait_for_wake()

    async def _poll(self) -> None:
        async with self.client_state.db_sessionmaker() as sess:
            clusters = await Cluster.list(sess)
            deployments = await StaticDeployment.list(sess)

        # Skip the broader health check on clusters with reconcile failures:
        # those take precedence
        cluster_checks = [self._check(c) for c in clusters if c.reconcile_failures == 0]
        sd_checks = [self._check(d) for d in deployments]
        checks = cluster_checks + sd_checks
        results = [r for r in await asyncio.gather(*checks) if r is not None]

        by_health: dict[str, dict[tuple[HealthCheckResult, str | None], list[int]]] = {
            "Cluster": defaultdict(list),
            "StaticDeployment": defaultdict(list),
        }

        for kind, uid, health, detail in results:
            by_health[kind][health, detail].append(uid)

        async with self.client_state.db_sessionmaker.begin() as sess:
            for ResourceCls in (Cluster, StaticDeployment):
                kind = ResourceCls.__name__

                for health, detail in sorted(
                    by_health[kind], key=lambda k: (k[0], k[1] or "")
                ):
                    uids = sorted(by_health[kind][health, detail])

                    await sess.execute(
                        sa.update(ResourceCls)
                        .where(
                            ResourceCls.uid.in_(uids),
                            sa.or_(
                                ResourceCls.health.is_distinct_from(health.value),
                                ResourceCls.health_detail.is_distinct_from(detail),
                            ),
                        )
                        .values(health=health.value, health_detail=detail)
                    )

    async def _check(
        self, resource: Cluster | StaticDeployment
    ) -> tuple[str, int, HealthCheckResult, str | None] | None:
        """Run one health check.

        Returns `(kind, uid, health, detail)` for the transition batch, where
        `detail` describes the failure (None when healthy). Returns ``None``
        when no healthcheck URL is configured (health is owned by
        PilotJobObserver in that case) or when a first failure is being
        debounced.
        """
        params = None
        try:
            params = HealthCheckParams.model_validate(resource.health_check)
            if not params.url:
                return None
            client = self._client_for(resource)
            result, detail = await perform_health_check(client, params)
        except Exception as exc:
            # A bad secret/configuration belongs to this resource, not the
            # entire poll. Never log raw exception text or config: either can
            # carry credentials. CancelledError remains uncaught for worker
            # shutdown.
            result = HealthCheckResult.unhealthy
            detail = _describe_exception(exc)

        self.hb.beat()
        key = (resource.kind, resource.uid)

        if result == HealthCheckResult.unhealthy:
            self.fail_counts[key] += 1
            logger.log(
                logging.WARNING if self.fail_counts[key] == 1 else logging.DEBUG,
                "Health check failed for %s %s (uid=%s, failures=%d): %s",
                resource.kind,
                resource.name,
                resource.uid,
                self.fail_counts[key],
                detail,
            )
            debounce = (
                params.debounce
                if params is not None
                else HealthCheckParams(url="").debounce
            )
            if self.fail_counts[key] < debounce:
                return None
        else:
            if failures := self.fail_counts.pop(key, 0):
                logger.info(
                    "Health check recovered for %s %s (uid=%s) after %d failures",
                    resource.kind,
                    resource.name,
                    resource.uid,
                    failures,
                )
            detail = ""

        return *key, result, detail or None

    def _client_for(self, resource: Cluster | StaticDeployment) -> Client:
        if not isinstance(resource, StaticDeployment) or not resource.mtls:
            return self.health_client
        mtls = MTLSClientConfig.model_validate(resource.mtls)
        if mtls not in self.mtls_clients:
            self.mtls_clients[mtls] = Client(verify=mtls.ssl_context())
        return self.mtls_clients[mtls]


def _describe_exception(exc: Exception) -> str:
    """A credential-safe description of an exception raised by a health check."""
    if isinstance(exc, ValidationError):
        # Validation messages and locations, but not the offending input
        problems = "; ".join(
            f"{'.'.join(map(str, err['loc'])) or '<root>'}: {err['msg']}"
            for err in exc.errors(include_input=False, include_url=False)
        )
        return f"Invalid health_check configuration: {problems}"
    if isinstance(exc, MTLSLoadError):
        return str(exc)
    return f"Unexpected {type(exc).__name__} while checking health"
