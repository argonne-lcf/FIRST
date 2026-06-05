import logging
import sys
import time
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Any

import globus_sdk
import globus_sdk.gare
from globus_sdk.authorizers import GlobusAuthorizer
from globus_sdk.scopes import GCSCollectionScopeBuilder, TransferScopes
from typer import Option, Typer

logger = logging.getLogger(__name__)


class InferenceAuthError(Exception):
    """
    Error during Inference authentication
    """


class TimeUnit(str, Enum):
    auto = "auto"
    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"


# Globus UserApp name
APP_NAME = "inference_app"

# Public inference auth client
AUTH_CLIENT_ID = "58fdd3bc-e1c3-4ce5-80ea-8d6b87cfb944"

# Inference gateway API scope
GATEWAY_CLIENT_ID = "681c10cc-f684-4540-bcd7-0b4df3bc26ef"
GATEWAY_SCOPE = f"https://auth.globus.org/scopes/{GATEWAY_CLIENT_ID}/action_all"

TOKENS_PATH = Path.home() / f".globus/app/{AUTH_CLIENT_ID}/{APP_NAME}/tokens.json"

# Globus authorizer parameters to point to specific identity providers
GA_PARAMS = globus_sdk.gare.GlobusAuthorizationParameters(
    session_required_policies=["83732ff2-9c42-4548-b5ce-17e498c84f6a"]
)

# inference_data_staging Globus Guest Collection:
TRANSFER_RESOURCE_SERVER = TransferScopes.resource_server
STAGING_COLLECTION_ID = "96c7390b-a3e8-4dd4-a327-1af7d143283e"
STAGING_COLLECTION_ROOT = "/eagle/IRIBeta/inference_data_staging/"

_collection_opt = Option(
    None,
    "--authorize-transfers",
    help=(
        "A Globus collection UUID to authorize transfers against.  "
        "When provided, login will request scopes "
        f"for data transfers between this collection and the inference service data staging "
        f"collection ({STAGING_COLLECTION_ID}). Append :data_access to the "
        "UUID to request the data_access dependency if needed."
    ),
)


# Error handler to guide user through specific identity providers
class DomainBasedErrorHandler:
    def __call__(self, app: globus_sdk.GlobusApp, error: Exception) -> None:
        logger.error(f"Encountered error '{error}', initiating login...")
        app.login(auth_params=GA_PARAMS)


def _build_scope_requirements(
    transfer_collection_id: str | None = None,
) -> dict[str, Any]:
    """
    Build the scope_requirements dict for the UserApp.

    Always includes the gateway scope. When transfer_collection_id is provided,
    transfer scopes are added, optionally with the data_access dependency.
    """
    # Transfer API Scope: initiate transfer tasks
    transfer_scope = TransferScopes.make_mutable("all")

    # HTTPS Scope: push files directly to an endpoint
    https_scope = GCSCollectionScopeBuilder(STAGING_COLLECTION_ID).make_mutable(
        "https", optional=True
    )

    # Gather scopes for inference, transfer, https
    scopes: dict[str, Any] = {
        GATEWAY_CLIENT_ID: [GATEWAY_SCOPE],
        TRANSFER_RESOURCE_SERVER: [transfer_scope],
        STAGING_COLLECTION_ID: https_scope,
    }

    # The source collection may require `data_access` scope; add if needed:
    if transfer_collection_id is not None:
        transfer_collection_id, *gcs_scopes = transfer_collection_id.split(":")
        if "data_access" in gcs_scopes:
            data_access = GCSCollectionScopeBuilder(
                transfer_collection_id
            ).make_mutable("data_access", optional=True)
            transfer_scope.add_dependency(data_access)
            scopes[transfer_collection_id] = [data_access]

    return scopes


def build_user_app(transfer_collection_id: str | None = None) -> globus_sdk.UserApp:
    return globus_sdk.UserApp(
        APP_NAME,
        client_id=AUTH_CLIENT_ID,
        scope_requirements=_build_scope_requirements(transfer_collection_id),
        config=globus_sdk.GlobusAppConfig(
            request_refresh_tokens=True,
            token_validation_error_handler=DomainBasedErrorHandler(),
        ),
    )


def get_inference_authorizer() -> GlobusAuthorizer:
    """
    Get Authorizer for Inference gateway
    """
    app = build_user_app()
    return app.get_authorizer(GATEWAY_CLIENT_ID)


def get_transfer_authorizer(transfer_collection_id: str) -> GlobusAuthorizer:
    """
    Get Authorizer for Globus Transfer
    """
    app = build_user_app(transfer_collection_id)
    return app.get_authorizer(TRANSFER_RESOURCE_SERVER)


def get_https_authorizer(transfer_collection_id: str) -> GlobusAuthorizer:
    app = build_user_app(transfer_collection_id)
    return app.get_authorizer(transfer_collection_id.split(":")[0])


def format_timedelta(td: timedelta, units: TimeUnit = TimeUnit.auto) -> str:
    total = td.total_seconds()
    if units == "seconds":
        return f"{total:.2f}"
    if units == "minutes":
        return f"{total / 60:.2f}"
    if units == "hours":
        return f"{total / 3600:.2f}"

    days, rem = divmod(int(total), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    parts = []

    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if seconds or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)


# Get time until token expiration
def get_time_until_token_expiration() -> timedelta:
    """
    Returns the time until the access token expires, in units of
    seconds, minutes, or hours. Negative times reveal that the token
    is expired already.
    """

    # Get authorizer object
    auth = get_inference_authorizer()

    # Gather the time difference between now and the expiration time (both Unix timestamps)
    now = time.time()
    return timedelta(seconds=auth.expires_at - now)  # type: ignore[attr-defined]


cli = Typer(no_args_is_help=True)


@cli.command()
def login(authorize_transfers: str | None = _collection_opt) -> None:
    """
    Log in with Globus. Stores credentials in your home directory.
    """
    app = build_user_app(authorize_transfers)
    app.login(auth_params=GA_PARAMS)


@cli.command()
def get_access_token() -> None:
    """
    Fetch an access token for the inference API.

    Automatically utilizes locally cached tokens, refreshing the access token if
    necessary, and returns a valid access token. If there is no token stored in
    the home directory, or if the refresh token is expired following 6 months of
    inactivity, an authentication will be triggered.
    """
    # Make sure tokens exist
    # This is important otherwise the CLI will print more than just the access token
    if not TOKENS_PATH.is_file():
        raise InferenceAuthError(
            "Access token does not exist. "
            f'Please authenticate by running "{sys.argv[0]} login".'
        )

    # Get authorizer object and authenticate if need be
    auth = get_inference_authorizer()

    # Make sure the stored access token if valid, and refresh otherwise
    auth.ensure_valid_token()  # type: ignore[attr-defined]

    # Return the access token
    print(auth.access_token)  # type: ignore[attr-defined]


@cli.command()
def get_token_expiration(units: TimeUnit = TimeUnit.auto) -> None:
    """
    Show how much time remains until the token expires.
    """
    # Make sure tokens exist
    # This is important otherwise the CLI will print more than just the access token
    if not TOKENS_PATH.is_file():
        raise InferenceAuthError(
            "Access token does not exist. "
            'Please authenticate by running "python3 inference_auth_token.py authenticate".'
        )

    print(format_timedelta(get_time_until_token_expiration(), units=units))
