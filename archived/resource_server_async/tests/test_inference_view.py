import json

from resource_server_async.tests import (
    CLIENT,
    DB_ENDPOINTS,
    INVALID_PARAMS,
    KWARGS,
    PREMIUM_HEADERS,
    VALID_PARAMS,
    ResourceServerTestCase,
    get_endpoint_urls,
    get_response_json,
    get_wrong_endpoint_urls,
    mock_utils,
)
from resource_server_async.tests.mixins import (
    EndpointPostTestsMixin,
    HeaderFailuresTestMixin,
)


class InferenceViewTestCase(
    EndpointPostTestsMixin, HeaderFailuresTestMixin, ResourceServerTestCase
):
    async def good_post_request(self, endpoint, valid_params, headers):
        """
        Make sure valid POST requests succeed.
        """
        response = await CLIENT.post(
            endpoint,
            data=json.dumps(valid_params).encode("utf-8"),
            headers=headers,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 200)

        # Check the response
        response_data = get_response_json(response)
        self.assertEqual(response_data, mock_utils.MOCK_RESPONSE)


# Template tests
for endpoint in get_wrong_endpoint_urls():
    InferenceViewTestCase.template_test("unsupported_post_request", endpoint)

for endpoint in DB_ENDPOINTS:
    if "model-removed" in endpoint["endpoint_slug"]:
        continue

    # Build the targeted Django URLs
    url_dict = get_endpoint_urls(endpoint)

    # For each URL (openai endpoint) ...
    for openai_endpoint, url in url_dict.items():
        InferenceViewTestCase.template_test("verify_headers_failures", url, CLIENT.post)
        InferenceViewTestCase.template_test(
            "non_post_request",
            url,
        )

        groups = endpoint.get("allowed_globus_groups", [])
        if groups not in [[], [mock_utils.MOCK_GROUP_UUID]]:
            continue

        # If the endpoint can be accessed by the mock access token ...
        headers = PREMIUM_HEADERS

        # For each valid set of input parameters ...
        for valid_params in VALID_PARAMS[openai_endpoint]:
            params_copy = {**valid_params, "model": endpoint["model"]}

            # Make sure the request is not streaming (this is tested in another function)
            # "if" statement needed since not all openai endpoints support streaming
            if "stream" in params_copy:
                params_copy["stream"] = False

            InferenceViewTestCase.template_test(
                "good_post_request", url, params_copy, headers
            )

            if groups == [mock_utils.MOCK_GROUP_UUID]:
                InferenceViewTestCase.template_test(
                    "inaccessible_post_request",
                    url,
                    params_copy,
                )

        for invalid_params in INVALID_PARAMS[openai_endpoint]:
            InferenceViewTestCase.template_test(
                "invalid_post_request",
                url,
                invalid_params,
                headers,
            )
