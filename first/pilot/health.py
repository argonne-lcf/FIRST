from http import HTTPMethod, HTTPStatus

from httpx import Client, HTTPError

from first.errors import HealthCheckError
from first.http_client import client
from first.schema.types import HealthEndpointStatus


def check_health_endpoint(
    base_url: str,
    health_path: str,
    timeout: int,
    headers: dict[str, str] | None = None,
    method: HTTPMethod = HTTPMethod.GET,
    expected_status: HTTPStatus = HTTPStatus.OK,
    httpx_client: Client | None = None,
) -> HealthEndpointStatus:
    """
    ALCF IRI API Cluster Status Check
    """
    if httpx_client is None:
        httpx_client = client

    try:
        resp = httpx_client.request(
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
