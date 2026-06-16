from http import HTTPMethod, HTTPStatus

from httpx import AsyncClient, HTTPError

from first_common.errors import HealthCheckError
from first_common.schema.types import HealthEndpointStatus


async def check_health_endpoint(
    httpx_client: AsyncClient,
    base_url: str,
    health_path: str,
    timeout: int,
    headers: dict[str, str] | None = None,
    method: HTTPMethod = HTTPMethod.GET,
    expected_status: HTTPStatus = HTTPStatus.OK,
) -> HealthEndpointStatus:
    """
    ALCF IRI API Cluster Status Check
    """
    try:
        resp = await httpx_client.request(
            method,
            f"{base_url}/{health_path.strip('/')}",
            timeout=timeout,
            headers=headers,
        )
        resp.raise_for_status()
    except HTTPError as e:
        raise HealthCheckError(f"{e.request.url} - {e}")

    if resp.status_code == expected_status:
        return HealthEndpointStatus.healthy
    else:
        return HealthEndpointStatus.unhealthy
