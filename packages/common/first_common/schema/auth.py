"""
Reference: https://docs.globus.org/api/auth/reference/#token-introspect
"""

import logging
from enum import Enum
from typing import Any, Literal, NotRequired, TypedDict

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class AuthService(str, Enum):
    GLOBUS = "globus"


class GlobusAuthentication(TypedDict, total=False):
    """One authentication event within session_info.authentications.

    Keys are present per-IDP; many IDPs omit `acr`/`amr` entirely
    (e.g. Google), so all fields are optional.
    """

    acr: str | None  # OIDC Authentication Context Class Reference
    amr: list[str] | None  # OIDC Authentication Methods References
    idp: str  # UUID of the identity provider
    auth_time: int  # Unix timestamp of the authentication event
    custom_claims: dict[str, object]  # Provider-specific extras (e.g. RAS passport)


class GlobusSessionInfo(TypedDict):
    """Returned when `include=session_info` is in the request."""

    session_id: str  # UUID of the session
    # Map of identity UUID -> authentication record
    authentications: dict[str, GlobusAuthentication]


class GlobusIdentitySetDetail(TypedDict, total=False):
    """One entry in identity_set_detail.

    Returned when `include=identity_set_detail` is in the request.
    Per Globus get_identities response shape.
    """

    id: str  # Identity UUID
    username: str  # e.g. "user@example.com"
    name: str | None
    email: str | None
    organization: str | None
    identity_provider: str  # IdP UUID
    identity_type: Literal["login", "link"]
    status: Literal["used", "unused", "private", "closed"]


class GlobusInactiveIntrospectResponse(TypedDict):
    """
    Response shape for revoked, expired, or otherwise invalid tokens.
    """

    active: Literal[False]


class GlobusActiveIntrospectResponse(TypedDict):
    """
    Response shape for a valid, active token.

    Required fields per Globus Auth API Reference. Optional fields are
    either RFC 7662 OPTIONAL or are returned only when requested via
    the `include` query/body parameter.
    """

    active: Literal[True]
    scope: str  # Space-separated list of scopes
    client_id: str  # UUID of the OAuth client the token was issued to
    sub: str  # Effective identity UUID (the resource owner)
    username: str  # Username of the effective identity
    aud: list[str]  # Audience: resource server names + client_ids
    iss: str  # Issuer, always "https://auth.globus.org/"
    exp: int  # Expiration time (Unix timestamp, seconds)
    iat: int  # Issued-at time (Unix timestamp, seconds)
    nbf: int  # Not-before time (Unix timestamp, seconds)
    name: str | None  # Display name of the resource owner
    email: str | None  # Email of the resource owner

    token_type: NotRequired[Literal["Bearer"]]
    dependent_tokens_cache_id: NotRequired[str]  # For dependent-token caching

    # `include=identity_set` (or legacy `identities_set`)
    identity_set: NotRequired[list[str]]  # UUIDs of all linked identities
    identities_set: NotRequired[list[str]]  # Legacy alias; same content

    # `include=identity_set_detail`
    identity_set_detail: NotRequired[list[GlobusIdentitySetDetail]]

    # `include=session_info`
    session_info: NotRequired[GlobusSessionInfo]
    policy_evaluations: dict[str, Any]


# Discriminated union for callers
GlobusIntrospectResponse = (
    GlobusActiveIntrospectResponse | GlobusInactiveIntrospectResponse
)


class UserAuthEvent(BaseModel):
    id: str
    name: str
    username: str
    user_group_uuids: list[str]
    authorized_group_uuids: str | None
    idp_id: str
    idp_name: str
    auth_service: str
    stream: Literal["user"] = "user"

    def emit(self) -> None:
        """
        Emit user info to log
        """
        logger.info(
            "authenticated",
            extra={
                **self.model_dump(mode="json", exclude={"name"}),
                "user.name": self.name,
            },
        )
