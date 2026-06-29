import logging
from typing import Any, List

from pydantic import BaseModel

from resource_server_async.clusters.cluster import (
    BaseCluster,
)
from resource_server_async.httpx_client import AsyncHttpClient

log = logging.getLogger(__name__)


class ClusterConfig(BaseModel):
    status_url: str
    api_request_timeout: int = 10
    ca_cert_path: str | None = None
    client_cert_path: str | None = None
    client_key_path: str | None = None
    check_hostname: bool = True
    trust_env: bool = True


# Direct API implementation of a BaseCluster
class DirectAPICluster(BaseCluster):
    """Direct API implementation of BaseCluster."""

    # Class initialization
    def __init__(
        self,
        id: str,
        cluster_name: str,
        cluster_adapter: str,
        frameworks: List[str],
        openai_endpoints: List[str],
        config: dict[str, Any],
        allowed_globus_groups: List[str] = [],
        allowed_domains: List[str] = [],
    ):
        # Validate endpoint configuration
        self.__config = ClusterConfig(**config)

        # Create HTTPx async client
        self.__httpx_client = AsyncHttpClient(
            timeout=self.__config.api_request_timeout,
            ca_cert_path=self.__config.ca_cert_path,
            client_cert_path=self.__config.client_cert_path,
            client_key_path=self.__config.client_key_path,
            check_hostname=self.__config.check_hostname,
            trust_env=self.__config.trust_env,
        )

        # Initialize the rest of the common attributes
        super().__init__(
            id,
            cluster_name,
            cluster_adapter,
            frameworks,
            openai_endpoints,
            allowed_globus_groups,
            allowed_domains,
        )

    # Read-only access to the configuration
    @property
    def config(self) -> ClusterConfig:
        return self.__config

    # Read-only access to HTTPx client
    @property
    def httpx_client(self) -> AsyncHttpClient:
        return self.__httpx_client
