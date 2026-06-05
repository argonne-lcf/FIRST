from typing import Any

import httpx


class AsyncHttpClient:
    def __init__(
        self,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
    ):
        if headers is None:
            headers = {"Content-Type": "application/json"}
        self.headers = headers
        self._client = httpx.AsyncClient(timeout=timeout, headers=self.headers)

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
