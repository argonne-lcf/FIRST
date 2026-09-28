import asyncio
import hashlib
import logging
import time
from typing import Protocol

import globus_sdk
from fastapi.security import HTTPAuthorizationCredentials
from pydantic import BaseModel

from first_common.errors import AccessDenied, Unauthorized
from first_common.schema.auth import (
    AuthService,
    GlobusActiveIntrospectResponse,
    GlobusIdentitySetDetail,
    GlobusIntrospectResponse,
    UserAuthEvent,
)
from first_gateway.settings import ClientState

log = logging.getLogger(__name__)


class _AccessGroupLike(Protocol):
    allowed_groups: list[str]
    allowed_domains: list[str]


class TokenIntrospectionResult(BaseModel):
    token_data: GlobusActiveIntrospectResponse | None
    user_groups: list[str]
    error: str = ""


class GlobusAuthService:
    def __init__(self, client_state: ClientState) -> None:
        self.cfg = client_state.settings.globus
        self.client = client_state.auth_client
        self.repo = client_state.redis_repo

    async def introspect_token(self, bearer_token: str) -> TokenIntrospectionResult:
        """
        Introspect a token with policies, collect group memberships, and return the response.
        Uses Redis cache for multi-worker support.
        """
        token_hash = hashlib.sha256(bearer_token.encode()).hexdigest()

        cached_result = await self.repo.get_cached_token(token_hash)
        if cached_result is not None:
            return TokenIntrospectionResult.model_validate_json(cached_result)

        # If not in cache, perform introspection
        try:
            result = await self._perform_token_introspection(bearer_token)
        except Unauthorized as e:
            # Introspection error!  60 seconds cooldown period before retrying:
            error_result = TokenIntrospectionResult(
                token_data=None, user_groups=[], error=str(e)
            )
            await self.repo.set_cached_token(
                token_hash, error_result.model_dump_json(), ttl=60
            )
            raise

        # If the introspection was successful ...
        assert result.token_data is not None
        try:
            introspection_exp = result.token_data["exp"]
            seconds_until_expiration = introspection_exp - int(time.time())
        except Exception as e:
            log.warning(f"Failed to extract token introspection exp claim: {e}")
            seconds_until_expiration = 0

        # Set cache time and make sure it is not shorter than the time until token expiration
        ttl = min(600, seconds_until_expiration)

        await self.repo.set_cached_token(token_hash, result.model_dump_json(), ttl=ttl)
        return result

    async def _perform_token_introspection(
        self, bearer_token: str
    ) -> TokenIntrospectionResult:
        """
        Perform the actual token introspection and return serializable data.
        """
        # Include the access token and Globus policies (if needed) in the instrospection
        introspect_body = {"token": bearer_token}
        if len(self.cfg.policies) > 0:
            introspect_body["authentication_policies"] = self.cfg.policies_str
        introspect_body["include"] = "session_info,identity_set_detail"

        # Introspect the token through the Globus Auth API (including policy evaluation)
        try:
            introspection = await asyncio.to_thread(
                self.client.post,
                "/v2/oauth2/token/introspect",
                data=introspect_body,
                encoding="form",
            )
            # Convert to serializable dict
            token_data: GlobusIntrospectResponse = (
                dict(introspection.data)  # type: ignore[assignment]
                if hasattr(introspection, "data")
                else dict(introspection)  # type: ignore[call-overload]
            )
        except Exception as e:
            raise Unauthorized(
                f"Could not introspect token with Globus /v2/oauth2/token/introspect. {e}"
            )

        # Error if the token is invalid
        if token_data["active"] is False:
            raise Unauthorized("Token is either not active or invalid")

        # Get dependent access token to view group membership
        try:
            dependent_tokens = await asyncio.to_thread(
                self.client.oauth2_get_dependent_tokens, bearer_token
            )
            access_token = dependent_tokens.by_resource_server["groups.api.globus.org"][
                "access_token"
            ]
        except Exception as e:
            raise Unauthorized(
                f"Could not recover dependent access token for groups.api.globus.org. {e}"
            )

        # Create a Globus Group Client using the access token sent by the user
        try:
            authorizer = globus_sdk.AccessTokenAuthorizer(access_token)
            groups_client = globus_sdk.GroupsClient(authorizer=authorizer)
        except Exception as e:
            raise Unauthorized(f"Error: Could not create GroupsClient. {e}")

        # Get the list of user's group memberships
        try:
            user_groups_response = await asyncio.to_thread(groups_client.get_my_groups)
            user_groups: list[str] = [group["id"] for group in user_groups_response]
        except Exception as e:
            raise Unauthorized(f"Error: Could not recover user group memberships. {e}")

        # Return the introspection data along with the group (with empty error message)
        return TokenIntrospectionResult(token_data=token_data, user_groups=user_groups)

    def check_globus_policies(
        self,
        introspection: GlobusActiveIntrospectResponse,
    ) -> None:
        """
        Check that an authenticated user meets every Globus policy requirement.

        Raises Unauthorized otherwise.
        """
        if len(introspection["policy_evaluations"]) != len(self.cfg.policies):
            raise Unauthorized(
                "Error: Some Globus policies could not be passed to the introspect API call."
            )

        for policies in introspection["policy_evaluations"].values():
            if not policies.get("evaluation", False):
                raise Unauthorized(
                    "Error: Permission denied from internal policies. "
                    "This is likely due to a high-assurance timeout. "
                    "Please logout by visiting https://app.globus.org/logout, "
                    "and re-authenticate with the following command: "
                    "'alcf-tokens login'. "
                    "Make sure you authenticate with an authorized identity provider: "
                    f"{self.cfg.authorized_idp_domains_str}."
                )

    def check_globus_groups(self, user_groups: list[str]) -> None:
        """
        Check that an authenticated user is a member of at least one of the
        allowed Globus groups.

        Raises Unauthorized otherwise.
        """
        if not set(user_groups).intersection(self.cfg.user_groups):
            raise Unauthorized(
                "Error: User is not a member of an allowed Globus Group."
            )

    def check_session_info(
        self, introspection: GlobusActiveIntrospectResponse, user_groups: list[str]
    ) -> UserAuthEvent:
        """
        Look into the session_info field of the token introspection and check that
        the authentication was made through one of the authorized identity
        providers.

        Returns the authenticated user's details.  Raises Unauthorized otherwise.
        """
        # Find the first active authentication session from an authorized domain
        session_identities: list[GlobusIdentitySetDetail] = []
        authorized_identity: GlobusIdentitySetDetail | None = None
        try:
            for session_idp in [
                auth["idp"]
                for auth in introspection["session_info"]["authentications"].values()
            ]:
                # Recover the identity (and its domain, e.g. anl.gov) tied to the session
                identity = next(
                    i
                    for i in introspection["identity_set_detail"]
                    if i["identity_provider"] == session_idp
                )
                session_identities.append(identity)

                if (
                    identity["username"].split("@")[1]
                    in self.cfg.authorized_idp_domains
                ):
                    authorized_identity = identity
                    break
        except Exception as e:
            raise Unauthorized(f"Error: Could not inspect session info: {e}")

        # Revoke access if authentication did not come from an authorized provider
        if authorized_identity is None:
            user_str = (
                ", ".join(
                    f"{identity.get('name')} ({identity.get('username')})"
                    for identity in session_identities
                )
                or "Unknown (no active session found)"
            )
            raise Unauthorized(
                f"Error: Permission denied. Must authenticate with {self.cfg.authorized_idp_domains_str}. "
                f"Currently authenticated as {user_str}. "
                "If you are passing an access token directly to this API, "
                "please logout from Globus by visiting https://app.globus.org/logout "
                "and re-authenticate with the following command: "
                "'alcf-tokens login'."
            )

        # Create the User object from the Globus introspection
        try:
            return UserAuthEvent(
                id=authorized_identity["sub"],
                name=authorized_identity["name"]
                if isinstance(authorized_identity["name"], str)
                else "",
                username=authorized_identity["username"],
                user_group_uuids=user_groups,
                idp_id=authorized_identity["identity_provider"],
                idp_name=authorized_identity["identity_provider_display_name"],
                auth_service=AuthService.GLOBUS.value,
            )
        except Exception as e:
            raise Unauthorized(f"Error: Could not create User object: {e}")

    def check_groups_per_idp(
        self, user: UserAuthEvent, user_groups: list[str]
    ) -> str | None:
        """
        Make sure the user is part of an authorized Globus Group (if any)
        associated with their identity provider.

        Returns the overlapping groups, or None if the IdP has no group
        restriction.  Raises Unauthorized otherwise.
        """
        # Extract the user's IdP domain
        try:
            idp_domain = user.username.split("@")[1]
        except IndexError:
            raise Unauthorized(
                "Error: Could not extract IdP domain from user.username.split('@')[1]."
            )

        # Grant the request if no group restriction is tied to this identity provider
        if idp_domain not in self.cfg.authorized_groups_per_idp:
            return None

        group_overlap = set(user_groups) & set(
            self.cfg.authorized_groups_per_idp[idp_domain]
        )
        if not group_overlap:
            raise Unauthorized(
                f"Error: Permission denied. User ({user.name} - {user.username}) "
                f"not part of the Globus Groups applied for {user.idp_name}."
            )

        return ", ".join(group_overlap)

    def extract_service_account_client(
        self, introspection: GlobusActiveIntrospectResponse, client_groups: list[str]
    ) -> UserAuthEvent | None:
        """
        Extract and return the user object if the identity is an authorized Globus
        client, or None if it is not.
        """
        client_id = introspection.get("client_id", "")
        username = introspection.get("username", "")
        name = introspection.get("name", "") or ""
        iss = introspection.get("iss", "")
        _, _, domain = username.partition("@")

        # Skip client recognition if not enough details
        if not (client_id and username and domain and name and iss):
            return None

        # Return nothing if this is not an authorized Globus service account client
        if username not in self.cfg.authorized_service_usernames:
            return None

        return UserAuthEvent(
            id=client_id,
            name=name,
            username=username,
            user_group_uuids=client_groups,
            idp_id=domain,
            idp_name=iss,
            auth_service=AuthService.GLOBUS.value,
            authorized_group_uuids=None,
        )

    async def validate_access_token(
        self,
        token: HTTPAuthorizationCredentials,
    ) -> UserAuthEvent:
        """
        Returns UserAuthEvent if and only if the user is authenticated.  Raises
        Unauthorized otherwise.
        """
        # Make sure the request is authenticated
        if token.scheme != "Bearer":
            raise Unauthorized("Authorization type should be Bearer.")

        # Introspect the access token
        introspection = await self.introspect_token(token.credentials)

        if introspection.token_data is None:
            raise Unauthorized(f"Token introspection: {introspection.error}")

        # Make sure the token is not expired
        expires_in = introspection.token_data["exp"] - time.time()
        if expires_in <= 0:
            raise Unauthorized("Access token expired.")

        # Try to identify an authorized Globus service account client
        user = self.extract_service_account_client(
            introspection.token_data, introspection.user_groups
        )

        # If the token is NOT from an authorized Globus client ...
        if user is None:
            # Make sure the authentication was made by an authorized identity provider
            user = self.check_session_info(
                introspection.token_data, introspection.user_groups
            )

            # Make sure the authenticated user comes from an allowed domain
            # Those must be a high-assurance policies
            if self.cfg.policies:
                self.check_globus_policies(introspection.token_data)

        # Make sure the user is part of a per-IdP authorized group (if any)
        user.authorized_group_uuids = self.check_groups_per_idp(
            user, introspection.user_groups
        )

        # Make sure the authenticated user is at least in one of the allowed Globus Groups
        if self.cfg.user_groups:
            self.check_globus_groups(introspection.user_groups)

        # Make sure the user's identity can be recorded
        if len(user.username) == 0:
            raise Unauthorized("Username could not be recovered.")

        # Make sure the user's identity is valid
        # TODO: Add more checks here
        if "<" in user.username or ">" in user.username:
            raise Unauthorized(
                f"Username {user.username} includes non-authorized characters."
            )

        # Return valid token response
        log.debug(f"{user.name} requesting {introspection.token_data['scope']}")
        return user


def user_can_access_group(user: UserAuthEvent, access_group: _AccessGroupLike) -> bool:
    """
    Returns True if user is permitted to access a resource based on group and
    domain restrictions.
    """
    if access_group.allowed_groups:
        if not any(set(user.user_group_uuids) & set(access_group.allowed_groups)):
            return False

    if access_group.allowed_domains:
        try:
            user_domain = user.username.split("@")[1]
        except IndexError:
            return False

        if user_domain not in access_group.allowed_domains:
            return False

    return True


def enforce_permission(user: UserAuthEvent, access_group: _AccessGroupLike) -> None:
    """
    Verify that the user is permitted to access a resource based on group and
    domain restrictions.

    Raises AccessDenied.
    """
    if not user_can_access_group(user, access_group):
        raise AccessDenied(
            "Permission denied due to Globus Group or IdP domain restrictions."
        )
