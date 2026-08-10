import asyncio
import logging
import time
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from first_gateway.settings import KeycloakSettings

logger = logging.getLogger(__name__)

_TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"
_ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"

_REFRESH_MARGIN = 30.0


class KeycloakServiceTokenAuth(httpx.Auth):
    """
    httpx auth flow that injects a Keycloak impersonation access token,
    refreshes it proactively before expiry, and re-authenticates once on a 401.
    """

    def __init__(self, settings: "KeycloakSettings") -> None:
        self._settings = settings
        self._lock = asyncio.Lock()
        self._token: str | None = None
        self._expires_at = 0.0  # monotonic deadline; 0 means "no token yet"

    async def async_auth_flow(
        self, request: httpx.Request
    ) -> AsyncGenerator[httpx.Request, httpx.Response]:
        token = await self._get_token()
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request

        if response.status_code == httpx.codes.UNAUTHORIZED:
            logger.info("Keycloak token rejected (401); re-authenticating")
            token = await self._refresh(stale=token)
            request.headers["Authorization"] = f"Bearer {token}"
            yield request

    async def _get_token(self) -> str:
        """Return a cached token if still fresh, otherwise refresh."""
        if self._token is not None and time.monotonic() < self._expires_at:
            return self._token
        return await self._refresh(stale=self._token)

    async def _refresh(self, *, stale: str | None) -> str:
        """
        Fetch a new access token, serialized behind a lock. ``stale`` is the
        token the caller found lacking; if another coroutine already replaced it
        while we waited for the lock, that fresh token is returned instead of
        fetching again.
        """
        async with self._lock:
            if (
                self._token is not None
                and self._token != stale
                and time.monotonic() < self._expires_at
            ):
                return self._token

            token, expires_in = await self._fetch_access_token()
            self._token = token
            self._expires_at = time.monotonic() + max(expires_in - _REFRESH_MARGIN, 0.0)
            logger.info("Acquired Keycloak service token (expires_in=%ss)", expires_in)
            return token

    async def _fetch_access_token(self) -> tuple[str, int]:
        """Run the two-step client-credentials -> token-exchange flow."""
        settings = self._settings
        async with httpx.AsyncClient(
            verify=settings.ssl_verify, timeout=httpx.Timeout(10.0)
        ) as client:
            subject_token = await self._client_credentials_token(client)
            return await self._exchange_token(client, subject_token)

    async def _client_credentials_token(self, client: httpx.AsyncClient) -> str:
        """Step 1: obtain the impersonation service-account token."""
        settings = self._settings
        resp = await client.post(
            settings.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": settings.impersonation_client_id,
                "client_secret": settings.impersonation_client_secret.get_secret_value(),
            },
        )
        resp.raise_for_status()
        return str(resp.json()["access_token"])

    async def _exchange_token(
        self, client: httpx.AsyncClient, subject_token: str
    ) -> tuple[str, int]:
        """Step 2: exchange the subject token for an impersonated access token."""
        settings = self._settings
        resp = await client.post(
            settings.token_url,
            data={
                "grant_type": _TOKEN_EXCHANGE_GRANT,
                "client_id": settings.impersonation_client_id,
                "client_secret": settings.impersonation_client_secret.get_secret_value(),
                "subject_token": subject_token,
                "requested_token_type": _ACCESS_TOKEN_TYPE,
                "requested_subject": settings.requested_subject,
                "audience": settings.audience,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        return str(body["access_token"]), int(body.get("expires_in", 0))
