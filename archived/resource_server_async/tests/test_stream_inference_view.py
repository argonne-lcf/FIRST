import json

import resource_server_async.tests.mock_utils as mock_utils
from resource_server_async.tests import (
    ALLOWED_OPENAI_ENDPOINTS,
    CLIENT,
    DB_ENDPOINTS,
    KWARGS,
    PREMIUM_HEADERS,
    STREAMING_TEST_CASES,
    ResourceServerTestCase,
    get_response_json,
)


class StreamInferenceViewTestCase(ResourceServerTestCase):
    """
    Test streaming functionality (POST)
    """

    async def good_streaming_post_request(self, endpoint, streaming_params):
        """
        This simply test streaming, most of the POST inference tests are done elsewhere.
        """
        response = await CLIENT.post(
            endpoint,
            data=json.dumps(streaming_params).encode("utf-8"),
            headers=PREMIUM_HEADERS,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 200)

        # In a real streaming response, we'd get Server-Sent Events
        # But in our mock implementation, we just verify the request is processed
        # The response format might differ for streaming vs non-streaming
        response_data = get_response_json(response)
        self.assertIsNotNone(response_data)  # Just verify we got some response


# Skip if no streaming test cases are available
if STREAMING_TEST_CASES:
    # For each endpoint in the database ...
    for endpoint in DB_ENDPOINTS:
        if "model-removed" in endpoint["endpoint_slug"]:
            continue

        # If the endpoint's cluster supports chat/completions
        cluster = endpoint["cluster"]
        if "chat/completions" in ALLOWED_OPENAI_ENDPOINTS[cluster]:
            # Build the targeted Django URL for chat/completions
            url = f"/{cluster}/{endpoint['framework']}/v1/chat/completions"

            groups = endpoint.get("allowed_globus_groups", [])
            if groups not in [[], [mock_utils.MOCK_GROUP_UUID]]:
                continue

            # If the endpoint can be accessed by the mock access token ...
            # Test each streaming test case from the JSON data
            for streaming_params in STREAMING_TEST_CASES:
                params_copy = {**streaming_params, "model": endpoint["model"]}

                # Test streaming request
                StreamInferenceViewTestCase.template_test(
                    "good_streaming_post_request", url, params_copy
                )
