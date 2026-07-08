import logging
import re
from http import HTTPMethod
from typing import Any

from httpx import AsyncClient, Client, HTTPError, Timeout

from first_common.schema.types import HealthCheckResult

logger = logging.getLogger(__name__)


async def perform_health_check(
    client: AsyncClient,
    health_url: str,
    *,
    connect_timeout: float = 3.1,
    read_timeout: float = 12.0,
    http_method: HTTPMethod = HTTPMethod.GET,
    json_body: Any | None = None,
    status_range: tuple[int, int] = (200, 299),
    match_pattern: str | None = None,
) -> HealthCheckResult:

    if not health_url:
        # No URL: disabled check
        return HealthCheckResult.unknown

    try:
        resp = await client.request(
            method=http_method,
            url=health_url,
            timeout=Timeout(read_timeout, connect=connect_timeout),
            json=json_body,
        )
    except HTTPError:
        logger.exception(f"Request error for health check to {health_url!r}")
        return HealthCheckResult.unhealthy

    if not status_range[0] <= resp.status_code <= status_range[1]:
        logger.warning(
            f"Health check {health_url!r} status code {resp.status_code} out of range"
        )
        return HealthCheckResult.unhealthy

    if match_pattern:
        if not re.search(match_pattern, resp.content.decode(errors="ignore")):
            logger.warning(
                f"Health check {health_url!r} response body did not find {match_pattern=!r}"
            )
            return HealthCheckResult.unhealthy

    return HealthCheckResult.healthy


def perform_health_check_sync(
    client: Client,
    health_url: str,
    *,
    connect_timeout: float = 3.1,
    read_timeout: float = 12.0,
    http_method: HTTPMethod = HTTPMethod.GET,
    json_body: Any | None = None,
    status_range: tuple[int, int] = (200, 299),
    match_pattern: str | None = None,
) -> HealthCheckResult:

    try:
        resp = client.request(
            method=http_method,
            url=health_url,
            timeout=Timeout(read_timeout, connect=connect_timeout),
            json=json_body,
        )
    except HTTPError:
        logger.exception(f"Request error for health check to {health_url!r}")
        return HealthCheckResult.unhealthy

    if not status_range[0] <= resp.status_code <= status_range[1]:
        logger.warning(
            f"Health check {health_url!r} status code {resp.status_code} out of range"
        )
        return HealthCheckResult.unhealthy

    if match_pattern:
        if not re.search(match_pattern, resp.content.decode(errors="ignore")):
            logger.warning(
                f"Health check {health_url!r} response body did not find {match_pattern=!r}"
            )
            return HealthCheckResult.unhealthy

    return HealthCheckResult.healthy
