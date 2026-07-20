import ssl
from typing import Any

import httpx


def create_ssl_context(
    ca_cert_path: str | None = None,
    client_cert_path: str | None = None,
    client_key_path: str | None = None,
    check_hostname: bool = True,
) -> ssl.SSLContext | bool:
    """Build an HTTPX verify context for optional mTLS configuration."""
    if not ca_cert_path and not client_cert_path and not client_key_path:
        return True

    context = ssl.create_default_context(cafile=ca_cert_path)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.check_hostname = check_hostname
    if client_cert_path or client_key_path:
        if not client_cert_path or not client_key_path:
            raise ValueError(
                "client_cert_path and client_key_path must be configured together"
            )
        context.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
    return context


class AsyncHttpClient:
    def __init__(
        self,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        ca_cert_path: str | None = None,
        client_cert_path: str | None = None,
        client_key_path: str | None = None,
        check_hostname: bool = True,
        trust_env: bool = True,
    ):
        if headers is None:
            headers = {"Content-Type": "application/json"}
        self.headers = headers
        verify = create_ssl_context(
            ca_cert_path=ca_cert_path,
            client_cert_path=client_cert_path,
            client_key_path=client_key_path,
            check_hostname=check_hostname,
        )
        self._client = httpx.AsyncClient(
            timeout=timeout, headers=self.headers, verify=verify, trust_env=trust_env
        )

    async def get(self, url: str) -> Any:
        response = await self._client.get(url)
        response.raise_for_status()
        return response.json()

    async def post(self, url: str, data: Any = None) -> Any:
        response = await self._client.post(url, json=data)
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        await self._client.aclose()
