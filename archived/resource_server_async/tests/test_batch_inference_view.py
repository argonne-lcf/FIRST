import json
import uuid

import resource_server_async.tests.mock_utils as mock_utils
from resource_server_async.tests import (
    CLIENT,
    DB_ENDPOINTS,
    INVALID_PARAMS,
    KWARGS,
    PREMIUM_HEADERS,
    VALID_PARAMS,
    ResourceServerTestCase,
    get_response_json,
    get_wrong_batch_urls,
)
from resource_server_async.tests.mixins import (
    EndpointPostTestsMixin,
    HeaderFailuresTestMixin,
)


class BatchInferenceViewTestCase(
    EndpointPostTestsMixin, HeaderFailuresTestMixin, ResourceServerTestCase
):
    async def good_batch_post_request(self, endpoint, valid_params, headers):
        """
        Make sure valid batch POST requests succeed.
        """
        response = await CLIENT.post(
            endpoint,
            data=json.dumps(valid_params).encode("utf-8"),
            headers=headers,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 200)

        # Check whether the response makes sense (do not check batch_id, it's randomly generated in the view)
        response_json = get_response_json(response)
        self.assertEqual(response_json["input_file"], valid_params["input_file"])


# Template tests
# Make sure POST requests fail when targetting an unsupported cluster or framework
for wrong_url in get_wrong_batch_urls():
    BatchInferenceViewTestCase.template_test("unsupported_post_request", wrong_url)

# For each endpoint that supports batch in the database ...
for endpoint in DB_ENDPOINTS:
    if "model-removed" in endpoint["endpoint_slug"]:
        continue

    if "batch_endpoint_uuid" in endpoint["config"]:
        # Build the targeted Django URL
        url = f"/{endpoint['cluster']}/{endpoint['framework']}/v1/batches"

        # Make sure POST requests fail if something is wrong with the authentication
        BatchInferenceViewTestCase.template_test(
            "verify_headers_failures", url, CLIENT.post
        )

        # Make sure non-POST requests are not allowed
        BatchInferenceViewTestCase.template_test("non_post_request", url)

        groups = endpoint.get("allowed_globus_groups", [])
        if groups not in [[], [mock_utils.MOCK_GROUP_UUID]]:
            continue

        # If the endpoint can be accessed by the mock access token ...
        headers = PREMIUM_HEADERS

        # For each valid set of input parameters ...
        for valid_params in VALID_PARAMS["batch"]:
            params_copy = {
                **valid_params,
                "model": endpoint["model"],
                "input_file": f"/path/{str(uuid.uuid4())}",
            }

            # Make sure POST requests succeed
            BatchInferenceViewTestCase.template_test(
                "good_batch_post_request", url, params_copy, headers
            )

            # Make sure users can't access private endpoint if not in allowed groups
            if groups == [mock_utils.MOCK_GROUP_UUID]:
                BatchInferenceViewTestCase.template_test(
                    "inaccessible_post_request", url, params_copy
                )

        # Make sure POST requests fail when providing invalid inputs
        for invalid_params in INVALID_PARAMS["batch"]:
            BatchInferenceViewTestCase.template_test(
                "invalid_post_request", url, invalid_params, headers
            )
