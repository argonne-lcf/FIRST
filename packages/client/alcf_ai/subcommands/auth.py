import sys

from typer import Typer

from ..auth import (
    GA_PARAMS,
    TOKENS_PATH,
    InferenceAuthError,
    TimeUnit,
    _collection_opt,
    build_user_app,
    format_timedelta,
    get_inference_authorizer,
    get_time_until_token_expiration,
)

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
    if not TOKENS_PATH.is_file():
        raise InferenceAuthError(
            "Access token does not exist. "
            f'Please authenticate by running "{sys.argv[0]} login".'
        )

    auth = get_inference_authorizer()
    auth.ensure_valid_token()  # type: ignore[attr-defined]
    print(auth.access_token)  # type: ignore[attr-defined]


@cli.command()
def get_token_expiration(units: TimeUnit = TimeUnit.auto) -> None:
    """
    Show how much time remains until the token expires.
    """
    if not TOKENS_PATH.is_file():
        raise InferenceAuthError(
            "Access token does not exist. "
            'Please authenticate by running "python3 inference_auth_token.py authenticate".'
        )

    print(format_timedelta(get_time_until_token_expiration(), units=units))
