import globus_sdk
from django.apps import AppConfig
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from ninja.constants import NOT_SET_TYPE


class AuthCheckConfig(AppConfig):
    name = "inference_gateway"

    def ready(self) -> None:
        # Skip is this is an automated test suite
        if settings.RUNNING_AUTOMATED_TEST_SUITE:
            return

        # Make sure a single Globus policy is in place
        if len(settings.GLOBUS_POLICIES) == 0:
            raise ImproperlyConfigured(
                "A Globus High Assurance Policy must be in place."
            )
        if not settings.NUMBER_OF_GLOBUS_POLICIES == 1:
            raise ImproperlyConfigured(
                "Only one Globus High Assurance Policy must be used."
            )

        # Make sure the authorization safety net is in place
        if len(settings.AUTHORIZED_IDP_DOMAINS) == 0:
            raise ImproperlyConfigured("AUTHORIZED_IDP_DOMAINS must be defined.")
        for idp_name in settings.AUTHORIZED_IDP_DOMAINS:
            if len(idp_name) == 0:
                raise ImproperlyConfigured("AUTHORIZED_IDP_DOMAINS cannot be empty.")

        # Recover the Globus policy
        client = globus_sdk.ConfidentialAppAuthClient(
            str(settings.SERVICE_ACCOUNT_ID), str(settings.SERVICE_ACCOUNT_SECRET)
        )
        token_response = client.oauth2_client_credentials_tokens()
        globus_auth_token = token_response.by_resource_server["auth.globus.org"][
            "access_token"
        ]
        auth_client = globus_sdk.AuthClient(
            authorizer=globus_sdk.AccessTokenAuthorizer(globus_auth_token)
        )
        policy_response = auth_client.get_policy(settings.GLOBUS_POLICIES)

        # Make sure the Globus policy is a high-assurance policy
        if not policy_response["policy"]["high_assurance"]:
            raise ImproperlyConfigured("The Globus Policy must be High Assurance.")

        # Make sure the policy and the authorization safety net are consistent
        if not sorted(
            policy_response["policy"]["domain_constraints_include"]
        ) == sorted(settings.AUTHORIZED_IDP_DOMAINS):
            raise ImproperlyConfigured(
                "The Globus Policy and AUTHORIZED_IDP_DOMAINS are inconsistent."
            )

        # Make sure the auth check is enforced to all routes within the API
        from resource_server_async.api import GlobalAuth, api

        if (
            not hasattr(api, "auth")
            or api.auth is None
            or isinstance(api.auth, NOT_SET_TYPE)
        ):
            raise ImproperlyConfigured(
                "The Django Ninja API does not have an `.auth` attribute defined."
            )
        if (
            not isinstance(api.auth, list)
            and len(api.auth) == 1
            and isinstance(api.auth[0], GlobalAuth)
        ):
            raise ImproperlyConfigured(
                "The Django Ninja API `.auth` attribute must be a list containing one GlobalAuth instance."
            )
