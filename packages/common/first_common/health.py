import asyncio
import re
import time
from typing import NamedTuple

from httpx import Client, HTTPError, Response, Timeout

from first_common.schema.types import HealthCheckParams, HealthCheckResult

_BODY_EXCERPT_CHARS = 500

# httpx does not wrap SOCKS handshake failures (e.g. the proxy closing the
# connection when it cannot reach the upstream) in HTTPError. socksio is only
# present where httpx[socks] is installed.
_REQUEST_ERRORS: tuple[type[Exception], ...] = (HTTPError,)
try:
    from socksio import SOCKSError

    _REQUEST_ERRORS += (SOCKSError,)
except ImportError:  # pragma: no cover
    pass


class HealthCheckOutcome(NamedTuple):
    """
    A health check result plus a human-readable reason when it is not healthy.

    `detail` never carries the resolved api_key: callers may log it or persist
    it for alerting.
    """

    result: HealthCheckResult
    detail: str = ""


async def perform_health_check(
    client: Client, params: HealthCheckParams
) -> HealthCheckOutcome:
    return await asyncio.to_thread(perform_health_check_sync, client, params)


def perform_health_check_sync(
    client: Client,
    params: HealthCheckParams,
) -> HealthCheckOutcome:
    if not params.url:
        return HealthCheckOutcome(HealthCheckResult.unknown)

    try:
        secret = params.api_key.resolve().get_secret_value() if params.api_key else ""
    except Exception as exc:
        # The ref names where the secret lives, not the secret itself. The
        # exception text is omitted in case a resolver echoes what it found.
        return HealthCheckOutcome(
            HealthCheckResult.unhealthy,
            f"Could not resolve api_key {params.api_key} ({type(exc).__name__})",
        )

    outcome = HealthCheckOutcome(HealthCheckResult.unknown)

    for attempt in range(params.attempts_per_check):
        outcome = _check_once(client, params, secret)

        if (
            outcome.result == HealthCheckResult.unhealthy
            and attempt < params.attempts_per_check - 1
        ):
            time.sleep(params.attempt_delay)
            continue

        break

    return outcome


def _check_once(
    client: Client, params: HealthCheckParams, secret: str
) -> HealthCheckOutcome:
    headers = {"Authorization": f"Bearer {secret}"} if secret else None

    try:
        resp = client.request(
            method=params.http_method,
            headers=headers,
            url=params.url,
            timeout=Timeout(params.read_timeout, connect=params.connect_timeout),
            json=params.json_body,
        )
    except _REQUEST_ERRORS as exc:
        name = type(exc).__name__
        if not isinstance(exc, HTTPError):
            name = f"SOCKS proxy {name}"
        reason = f"{name}: {exc}" if str(exc) else name
        return _unhealthy(f"Request failed: {reason}", secret)

    lo, hi = params.status_range
    if not lo <= resp.status_code <= hi:
        return _unhealthy(
            f"HTTP {resp.status_code} (expected {lo}-{hi}){_body_excerpt(resp)}",
            secret,
        )

    if params.match_pattern:
        if not re.search(params.match_pattern, resp.content.decode(errors="ignore")):
            return _unhealthy(
                f"Response body did not match {params.match_pattern!r}"
                f"{_body_excerpt(resp)}",
                secret,
            )

    return HealthCheckOutcome(HealthCheckResult.healthy)


def _unhealthy(detail: str, secret: str) -> HealthCheckOutcome:
    if secret:
        detail = detail.replace(secret, "<redacted>")
    return HealthCheckOutcome(HealthCheckResult.unhealthy, detail)


def _body_excerpt(resp: Response) -> str:
    body = resp.content.decode(errors="replace").strip()
    if not body:
        return ""
    if len(body) > _BODY_EXCERPT_CHARS:
        body = body[:_BODY_EXCERPT_CHARS] + "…"
    return f": {body}"
