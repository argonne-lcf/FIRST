import json

from django.test import TestCase

import resource_server_async.tests.mock_utils as mock_utils
from resource_server_async.tests import (
    ACTIVE_TOKEN,
    CLIENT,
    EXPIRED_TOKEN,
    HEADERS,
    INVALID_TOKEN,
    KWARGS,
)


class EndpointPostTestsMixin(TestCase):
    """
    POST request related test templates.
    """

    async def non_post_request(self, endpoint):
        """
        Make sure non-POST requests are not allowed.
        """
        for method in [
            CLIENT.get,
            CLIENT.put,
            CLIENT.delete,
        ]:
            with self.subTest(method=method):
                response = await method(endpoint)
                self.assertEqual(response.status_code, 405)

    async def unsupported_post_request(self, endpoint):
        """
        Make sure POST requests fail when targetting an unsupported cluster, framework, or openai endpoint.
        """
        try:
            response = await CLIENT.post(endpoint, headers=HEADERS)
        except Exception as exc:
            self.assertIn("Cannot resolve", str(exc))
        else:
            self.assertGreaterEqual(response.status_code, 400)

    async def inaccessible_post_request(self, endpoint, valid_params):
        """
        Make sure users can't access private endpoint if not in allowed groups.
        """
        response = await CLIENT.post(
            endpoint,
            data=json.dumps(valid_params).encode("utf-8"),
            headers=HEADERS,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 401)

    async def invalid_post_request(self, endpoint, invalid_params, headers):
        """
        Make sure POST requests fail when providing invalid inputs.
        """
        response = await CLIENT.post(
            endpoint,
            data=json.dumps(invalid_params).encode("utf-8"),
            headers=headers,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 422)


class HeaderFailuresTestMixin(TestCase):
    """
    Verifies headers failures.
    """

    async def verify_headers_failures(self, endpoint, method):
        """
        Make sure requests fail if something is wrong with the authentication.
        """
        # Should fail (not authenticated, missing token)
        headers = mock_utils.get_mock_headers(access_token="")
        response = await method(endpoint, headers=headers)
        self.assertEqual(response.status_code, 401)

        # Should fail (not a bearer token)
        headers = mock_utils.get_mock_headers(access_token=ACTIVE_TOKEN, bearer=False)
        response = await method(endpoint, headers=headers)
        self.assertEqual(response.status_code, 401)

        # Should fail (not a valid token)
        headers = mock_utils.get_mock_headers(access_token=INVALID_TOKEN, bearer=True)
        response = await method(endpoint, headers=headers)
        self.assertEqual(response.status_code, 401)

        # Should fail (expired token)
        headers = mock_utils.get_mock_headers(access_token=EXPIRED_TOKEN, bearer=True)
        response = await method(endpoint, headers=headers)
        self.assertEqual(response.status_code, 401)
