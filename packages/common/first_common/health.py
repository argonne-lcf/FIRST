import asyncio
import logging
import re
import time

from httpx import Client, HTTPError, Timeout

from first_common.schema.types import HealthCheckParams, HealthCheckResult

logger = logging.getLogger(__name__)


async def perform_health_check(
    client: Client, params: HealthCheckParams
) -> HealthCheckResult:
    return await asyncio.to_thread(perform_health_check_sync, client, params)


def perform_health_check_sync(
    client: Client,
    params: HealthCheckParams,
) -> HealthCheckResult:
    health = HealthCheckResult.unknown

    for attempt in range(params.attempts_per_check):
        health = _check_once(client, params)

        if (
            health == HealthCheckResult.unhealthy
            and attempt < params.attempts_per_check - 1
        ):
            time.sleep(params.attempt_delay)
            continue

        break

    return health


def _check_once(client: Client, params: HealthCheckParams) -> HealthCheckResult:
    if not params.url:
        return HealthCheckResult.unknown

    try:
        resp = client.request(
            method=params.http_method,
            url=params.url,
            timeout=Timeout(params.read_timeout, connect=params.connect_timeout),
            json=params.json_body,
        )
    except HTTPError as exc:
        logger.warning(f"Request error for health check to {params.url!r}: {exc}")
        return HealthCheckResult.unhealthy

    if not params.status_range[0] <= resp.status_code <= params.status_range[1]:
        logger.warning(
            f"Health check {params.url!r} status code {resp.status_code} out of range"
        )
        return HealthCheckResult.unhealthy

    if params.match_pattern:
        if not re.search(params.match_pattern, resp.content.decode(errors="ignore")):
            logger.warning(
                f"Health check {params.url!r} response body did not find {params.match_pattern=!r}"
            )
            return HealthCheckResult.unhealthy

    return HealthCheckResult.healthy
