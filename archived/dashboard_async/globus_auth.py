"""
Globus OAuth2 authentication utilities for dashboard.
"""

import hashlib
import logging

import globus_sdk
from cachetools import TTLCache, cached
from django.conf import settings
from django.contrib.auth import get_user_model

log = logging.getLogger(__name__)

User = get_user_model()


# User info class for dashboard authentication
class DashboardUserInfo:
    """User information extracted from Globus token validation."""

    def __init__(self, username, sub, token_info):
        self.username = username
        self.id = sub
        self.name = token_info.get("name", username)
        self.email = token_info.get("email", "")
        self.idp_id = token_info.get("identity_provider", "")
        self.idp_name = token_info.get("identity_provider_display_name", "")


def get_globus_oauth_client():
    """Get configured Globus OAuth2 client for dashboard."""
    return globus_sdk.ConfidentialAppAuthClient(
        settings.GLOBUS_DASHBOARD_APPLICATION_ID,
        settings.GLOBUS_DASHBOARD_APPLICATION_SECRET,
    )


def get_authorization_url(state=None):
    """
    Generate Globus authorization URL for OAuth2 flow.

    Args:
        state: Optional state parameter for CSRF protection

    Returns:
        str: authorization_url
    """
    client = get_globus_oauth_client()

    # Generate auth URL
    client.oauth2_start_flow(
        settings.GLOBUS_DASHBOARD_REDIRECT_URI,
        requested_scopes=settings.GLOBUS_DASHBOARD_SCOPES,
        refresh_tokens=True,
        state=state,  # Pass state to the flow initialization
    )

    # Get the authorize URL
    auth_url = client.oauth2_get_authorize_url()

    # Add optional policy parameters to the URL if configured
    if settings.NUMBER_OF_GLOBUS_DASHBOARD_POLICIES > 0:
        # Append policy parameters to the URL
        separator = "&" if "?" in auth_url else "?"
        auth_url = f"{auth_url}{separator}session_required_policies={settings.GLOBUS_DASHBOARD_POLICIES}"

    return auth_url


def exchange_code_for_tokens(auth_code):
    """
    Exchange authorization code for access tokens.

    Args:
        auth_code: Authorization code from callback

    Returns:
        dict: Token response with access_token, refresh_token, expires_in, etc.
    """
    client = get_globus_oauth_client()

    client.oauth2_start_flow(
        settings.GLOBUS_DASHBOARD_REDIRECT_URI,
        requested_scopes=settings.GLOBUS_DASHBOARD_SCOPES,
        refresh_tokens=True,
    )

    token_response = client.oauth2_exchange_code_for_tokens(auth_code)

    # Return all tokens as dict (includes resource_server keys)
    return token_response.by_resource_server


def validate_dashboard_token(access_token, groups_token=None):
    """
    Validate dashboard access token with caching (Redis + in-memory fallback).
    Checks policies and group membership.

    Args:
        access_token: Globus access token
        groups_token: Optional Groups API token for group membership checks

    Returns:
        tuple: (is_valid, user_data, error_message)
    """
    from django.core.cache import cache

    # Create cache key from token hash (don't store raw tokens in cache keys)
    token_hash = hashlib.sha256(access_token.encode()).hexdigest()[:16]
    cache_key = f"dashboard_token_validation:{token_hash}"

    # Try Redis cache first
    try:
        cached_result = cache.get(cache_key)
        if cached_result is not None:
            log.debug("Using cached token validation result")
            return cached_result
    except Exception as e:
        log.warning(f"Redis cache error for dashboard token validation: {e}")
        # Fall back to in-memory cache
        return _validate_dashboard_token_memory_cache(access_token, groups_token)

    # Cache miss - perform validation
    result = _perform_dashboard_token_validation(access_token, groups_token)

    # Cache successful validations for 10 minutes, errors for 1 minute
    ttl = 600 if result[0] else 60
    try:
        cache.set(cache_key, result, ttl)
    except Exception as e:
        log.warning(f"Failed to cache dashboard token validation: {e}")

    return result


# In-memory cache fallback for token validation
@cached(cache=TTLCache(maxsize=1024, ttl=60 * 10))
def _validate_dashboard_token_memory_cache(access_token, groups_token=None):
    """Fallback in-memory cache for dashboard token validation"""
    return _perform_dashboard_token_validation(access_token, groups_token)


def _perform_dashboard_token_validation(access_token, groups_token=None):
    """
    Perform the actual token validation (called when cache misses).
    """
    try:
        client = get_globus_oauth_client()

        # Build introspection body with policy if configured
        introspect_body = {"token": access_token}
        dashboard_policy_id = getattr(settings, "GLOBUS_DASHBOARD_POLICY_ID", "")
        if dashboard_policy_id:
            introspect_body["authentication_policies"] = dashboard_policy_id
        introspect_body["include"] = "session_info,identity_set_detail"

        # Introspect token with policy evaluation
        try:
            introspection = client.post(
                "/v2/oauth2/token/introspect", data=introspect_body, encoding="form"
            )
            token_info = (
                dict(introspection.data)
                if hasattr(introspection, "data")
                else dict(introspection)
            )
        except Exception as e:
            log.error(f"Token introspection error: {e}")
            return False, None, f"Error: Could not introspect token with Globus. {e}"

        if not token_info.get("active", False):
            return False, None, "Error: Token is not active or has expired"

        # Check high-assurance policy if configured
        if dashboard_policy_id:
            policy_valid, policy_error = check_dashboard_policies(token_info)
            if not policy_valid:
                return False, None, policy_error

        # Extract user info from token
        username = token_info.get("username")
        sub = token_info.get("sub")

        if not username or not sub:
            return False, None, "Error: Token missing user information"

        # Create user object
        user = DashboardUserInfo(username, sub, token_info)

        # Check dashboard group membership if configured
        dashboard_group_enabled = getattr(settings, "DASHBOARD_GROUP_ENABLED", False)
        dashboard_group_id = getattr(settings, "GLOBUS_DASHBOARD_GROUP", "")
        if dashboard_group_enabled and groups_token and dashboard_group_id:
            is_member = check_group_membership(groups_token, sub, dashboard_group_id)

            if not is_member:
                log.warning(
                    f"Dashboard access denied for {username}: not member of required group"
                )
                return (
                    False,
                    None,
                    (
                        f"Dashboard access denied. User '{user.name}' ({user.username}) "
                        f"is not a member of the required Globus Group. "
                        f"Please contact the ALCF operations team to request access to the "
                        f"'ALCF AI Inference Service Dashboard Users' group."
                    ),
                )

        log.info(f"Dashboard token validation successful for {username}")
        return True, user, None

    except Exception as e:
        log.error(f"Token validation error: {e}")
        return False, None, f"Validation error: {str(e)}"


def check_dashboard_policies(token_info):
    """
    Check if the authenticated user meets the dashboard high-assurance policy requirements.
    Similar to check_globus_policies in auth_utils.py but for dashboard.

    Args:
        token_info: Token introspection response from Globus

    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        policy_evaluations = token_info.get("policy_evaluations", {})
        dashboard_policy_id = getattr(settings, "GLOBUS_DASHBOARD_POLICY_ID", "")

        # If no policy evaluations present but policy was requested, that's an error
        if not policy_evaluations and dashboard_policy_id:
            return False, "Error: Dashboard policy could not be evaluated by Globus."

        # Check if policy was satisfied
        for policy_id, policy_result in policy_evaluations.items():
            if policy_result.get("evaluation", False) is False:
                error_message = "Error: Dashboard access denied due to high-assurance policy requirement. "
                error_message += "This is likely due to a session timeout. "
                error_message += "Please clear your Globus session by clicking 'Clear Globus session' on the login page, "
                error_message += (
                    "then log in again with an authorized identity provider."
                )
                return False, error_message

        return True, ""

    except Exception as e:
        log.error(f"Policy evaluation error: {e}")
        return False, f"Error: Could not evaluate dashboard policies. {e}"


def check_group_membership(groups_token, user_id, group_id):
    """
    Check if user is a member of the specified Globus Group (with caching).

    Args:
        groups_token: Groups API access token
        user_id: User's Globus ID (sub claim)
        group_id: Globus Group ID to check

    Returns:
        bool: True if user is a member, False otherwise
    """
    from django.core.cache import cache

    # Cache key based on user ID and group ID
    cache_key = f"globus_group_membership:{user_id}:{group_id}"

    # Check cache first
    cached_result = cache.get(cache_key)
    if cached_result is not None:
        log.debug(f"Using cached group membership for user {user_id}: {cached_result}")
        return cached_result

    # Cache miss - check with Globus API
    try:
        from globus_sdk import AccessTokenAuthorizer, GroupsClient

        authorizer = AccessTokenAuthorizer(groups_token)
        groups_client = GroupsClient(authorizer=authorizer)

        # Get user's group memberships
        is_member = False
        for group in groups_client.get_my_groups():
            if group["id"] == group_id:
                is_member = True
                break

        if is_member:
            log.info(f"User is member of group {group_id}")
        else:
            log.warning(f"User is not a member of group {group_id}")

        # Cache result for 10 minutes (600 seconds)
        # This balances security (timely revocation) with performance
        cache.set(cache_key, is_member, timeout=600)

        return is_member

    except Exception as e:
        log.error(f"Group membership check error: {e}")
        # On error, don't cache and deny access for security
        return False


def refresh_access_token(refresh_token):
    """
    Refresh an expired access token.

    Args:
        refresh_token: Globus refresh token

    Returns:
        dict: New token response
    """
    client = get_globus_oauth_client()

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        refresh_token, client, access_token=None, expires_at=None
    )

    # Force token refresh
    new_access_token = authorizer.get_authorization_header()

    # Get new token info
    authorizer.check_expiration_time()

    return {
        "access_token": new_access_token.split(" ")[1],  # Remove 'Bearer ' prefix
        "expires_at": authorizer.expires_at,
    }


def revoke_token(token):
    """
    Revoke a Globus token (logout).

    Args:
        token: Access or refresh token to revoke
    """
    try:
        client = get_globus_oauth_client()
        client.oauth2_revoke_token(token)
    except Exception as e:
        log.warning(f"Token revocation error: {e}")
