from http import HTTPStatus

import resource_server_async.tests.mock_utils as mock_utils
from resource_server_async.tests import (
    CLIENT,
    DB_ENDPOINTS,
    HEADERS,
    PREMIUM_HEADERS,
    ResourceServerTestCase,
    get_response_json,
)
from resource_server_async.tests.mixins import HeaderFailuresTestMixin


class EndpointsViewTestCase(HeaderFailuresTestMixin, ResourceServerTestCase):
    # Define the targeted Django URL
    url = "/list-endpoints"

    async def test_non_get(self):
        """
        Make sure non-GET requests are not allowed.
        Ninja's test client raises Exception when the route
        cannot be resolved for the given HTTP method.
        """
        for method in [CLIENT.post, CLIENT.put, CLIENT.delete]:
            resp = await method(self.url)
            assert resp.status_code == HTTPStatus.METHOD_NOT_ALLOWED

    async def good_get_request(self, headers):
        """
        Make sure GET requests succeed when providing a valid access token
        """
        response = await CLIENT.get(self.url, headers=headers)
        response_data = get_response_json(response)
        self.assertEqual(response.status_code, 200, str(response_data))

        # Define the total number of expected endpoints
        (
            db_endpoints_public,
            db_endpoints_premium,
        ) = EndpointsViewTestCase._get_endpoint_object_counts()
        nb_endpoints_expected = db_endpoints_public
        if headers == PREMIUM_HEADERS:
            nb_endpoints_expected += db_endpoints_premium

        # Make sure the GET request returns the correct number of endpoints
        nb_endpoints = 0
        for cluster in response_data["clusters"]:
            for framework in response_data["clusters"][cluster]["frameworks"]:
                nb_endpoints += len(
                    response_data["clusters"][cluster]["frameworks"][framework][
                        "models"
                    ]
                )
        self.assertEqual(nb_endpoints_expected, nb_endpoints)

    @classmethod
    def _get_endpoint_object_counts(self):
        """
        Extract number of public and premium Globus Compute endpoint objects from the database
        """
        # TODO: Re work this to test number of models with clusters that have direct API access
        db_endpoints_public = 0
        db_endpoints_premium = 0
        for endpoint in DB_ENDPOINTS:
            if (
                "allowed_globus_groups" not in endpoint
                or endpoint["allowed_globus_groups"] == []
            ):
                db_endpoints_public += 1
            elif endpoint["allowed_globus_groups"] == [mock_utils.MOCK_GROUP_UUID]:
                db_endpoints_premium += 1

        return db_endpoints_public, db_endpoints_premium


# Template tests
# Make sure GET requests fail if something is wrong with the authentication
EndpointsViewTestCase.template_test(
    "verify_headers_failures", EndpointsViewTestCase.url, CLIENT.get
)

# For valid tokens with and without premium access ...
EndpointsViewTestCase.template_test("good_get_request", headers=HEADERS)
EndpointsViewTestCase.template_test("good_get_request", headers=PREMIUM_HEADERS)
