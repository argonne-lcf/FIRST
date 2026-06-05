import copy
import json
import os
import re
from contextlib import ContextDecorator
from inspect import iscoroutinefunction
from typing import override
from unittest.mock import patch

from django.core.management import call_command

# Tools to test with Django Ninja
from django.test import TestCase
from ninja.testing import TestAsyncClient

import resource_server_async.tests.mock_utils as mock_utils
from resource_server_async.api import api as ninja_api
from resource_server_async.logging import (
    RequestContext,
    _request_context,
)

# Create mock access tokens
ACTIVE_TOKEN = mock_utils.get_mock_access_token(
    active=True, expired=False, has_premium_access=False
)
ACTIVE_PREMIUM_TOKEN = mock_utils.get_mock_access_token(
    active=True, expired=False, has_premium_access=True
)
EXPIRED_TOKEN = mock_utils.get_mock_access_token(
    active=True, expired=True, has_premium_access=False
)
INVALID_TOKEN = mock_utils.get_mock_access_token(
    active=False, expired=False, has_premium_access=False
)

# Create headers with a valid access token
HEADERS = mock_utils.get_mock_headers(access_token=ACTIVE_TOKEN, bearer=True)
PREMIUM_HEADERS = mock_utils.get_mock_headers(
    access_token=ACTIVE_PREMIUM_TOKEN, bearer=True
)

# Load valid test input data (OpenAI format)
base_path = "resource_server_async/tests/json"
VALID_PARAMS = {}
with open(f"{base_path}/valid_completions.json") as json_file:
    VALID_PARAMS["completions"] = json.load(json_file)
with open(f"{base_path}/valid_chat_completions.json") as json_file:
    VALID_PARAMS["chat/completions"] = json.load(json_file)
with open(f"{base_path}/valid_embeddings.json") as json_file:
    VALID_PARAMS["embeddings"] = json.load(json_file)
with open(f"{base_path}/valid_batch.json") as json_file:
    VALID_PARAMS["batch"] = json.load(json_file)
VALID_PARAMS["health"] = {}
VALID_PARAMS["metrics"] = {}

# Extract streaming test cases from valid chat completions
STREAMING_TEST_CASES = copy.deepcopy(VALID_PARAMS["chat/completions"])
for i in range(len(STREAMING_TEST_CASES)):
    STREAMING_TEST_CASES[i]["stream"] = True

# Load invalid test input data (OpenAI format)
INVALID_PARAMS = {}
with open(f"{base_path}/invalid_completions.json") as json_file:
    INVALID_PARAMS["completions"] = json.load(json_file)
with open(f"{base_path}/invalid_chat_completions.json") as json_file:
    INVALID_PARAMS["chat/completions"] = json.load(json_file)
with open(f"{base_path}/invalid_embeddings.json") as json_file:
    INVALID_PARAMS["embeddings"] = json.load(json_file)
with open(f"{base_path}/invalid_batch.json") as json_file:
    INVALID_PARAMS["batch"] = json.load(json_file)
INVALID_PARAMS["health"] = {}
INVALID_PARAMS["metrics"] = {}

# Collect available clusters and endpoints from database
with open("fixtures/endpoints.json") as json_file:
    DB_ENDPOINTS = [e["fields"] for e in json.load(json_file)]
with open("fixtures/clusters.json") as json_file:
    DB_CLUSTERS = [c["fields"] for c in json.load(json_file)]

# Collect available information for each cluster
ALLOWED_CLUSTERS = []
ALLOWED_FRAMEWORKS = {}
ALLOWED_OPENAI_ENDPOINTS = {}
for cluster in DB_CLUSTERS:
    cluster_name = cluster["cluster_name"]

    ALLOWED_CLUSTERS.append(cluster_name)
    ALLOWED_FRAMEWORKS[cluster_name] = cluster["frameworks"]
    ALLOWED_OPENAI_ENDPOINTS[cluster_name] = [
        e for e in cluster["openai_endpoints"] if e not in ["health", "metrics"]
    ]

del base_path


def get_endpoint_urls(endpoint):
    """
    Get endpoint URLs from `ALLOWED_OPENAI_ENDPOINTS`.
    """
    return {
        openai_endpoint: f"/{endpoint['cluster']}/{endpoint['framework']}/v1/{openai_endpoint}"
        for openai_endpoint in ALLOWED_OPENAI_ENDPOINTS[endpoint["cluster"]]
    }


def get_wrong_endpoint_urls():
    """
    Get list of URLS with unsupported cluster, framework, and openai endpoints.
    """
    # A valid cluster, framework, endpoint set
    cluster = ALLOWED_CLUSTERS[0]
    framework = ALLOWED_FRAMEWORKS[cluster][0]
    endpoint = ALLOWED_OPENAI_ENDPOINTS[cluster][0]

    return [
        f"/{c}/{f}/v1/{e}"
        for c, f, e in (
            ("unsupported-cluster", framework, endpoint),
            (cluster, "unsupported-framework", endpoint),
            (cluster, framework, "unsupported-endpoint"),
        )
    ]


# Get wrong batch URLs
def get_wrong_batch_urls():
    """
    Get list of batch URLS with unsupported cluster and framework
    """
    # A valid cluster, framework set
    cluster = ALLOWED_CLUSTERS[0]
    framework = ALLOWED_FRAMEWORKS[cluster][0]

    return [
        f"/{c}/{f}/v1/batches"
        for c, f in (
            ("unsupported-cluster", framework),
            (cluster, "unsupported-framework"),
        )
    ]


# This is because Django Ninja client does not take content-type json for some reason...
def get_response_json(response):
    """
    Convert bytes response to dictionary.
    """
    # First check if this is a StreamingHttpResponse
    is_streaming = hasattr(response, "streaming_content")

    try:
        # Handle streaming responses
        if is_streaming:
            # For streaming responses, collect all chunks
            try:
                streaming_content = response.streaming_content
                if streaming_content is not None:
                    if hasattr(streaming_content, "__iter__"):
                        # If it's iterable, join the chunks
                        content = b"".join(streaming_content)
                    else:
                        # If it's not iterable, treat it as single content
                        content = streaming_content
                        if isinstance(content, str):
                            content = content.encode("utf-8")
                    return json.loads(content.decode("utf-8"))
                else:
                    # streaming_content is None, return a default response
                    return "streaming response processed"
            except (TypeError, AttributeError, json.JSONDecodeError):
                # If streaming parsing fails, return a generic response
                return "streaming response processed"

        # Handle regular responses (non-streaming)
        if hasattr(response, "_container"):
            return json.loads(response._container[0].decode("utf-8"))
        elif hasattr(response, "content"):
            return json.loads(response.content.decode("utf-8"))
        else:
            return str(response)

    except json.JSONDecodeError:
        # If it's not JSON, return the raw content
        try:
            if is_streaming:
                try:
                    streaming_content = response.streaming_content
                    if streaming_content is not None:
                        if hasattr(streaming_content, "__iter__"):
                            content = b"".join(streaming_content)
                        else:
                            content = streaming_content
                            if isinstance(content, str):
                                content = content.encode("utf-8")
                        return content.decode("utf-8")
                    else:
                        return "streaming response"
                except (TypeError, AttributeError):
                    return "streaming response"

            if hasattr(response, "_container"):
                return response._container[0].decode("utf-8")
            elif hasattr(response, "content"):
                return response.content.decode("utf-8")
            else:
                return str(response)
        except:
            # Final fallback
            if is_streaming:
                return "streaming response"
            return str(response)


class mock_override(ContextDecorator):
    """
    Decorator to apply all `mock_utils` patches needed for `TestCase`s.
    """

    PATCHERS = (
        # Overwrite Globus SDK classes and functions
        patch(
            "resource_server_async.auth.get_globus_client", mock_utils.get_globus_client
        ),
        patch(
            "resource_server_async.globus_utils.get_compute_client_from_globus_app",
            mock_utils.get_compute_client_from_globus_app,
        ),
        patch(
            "resource_server_async.globus_utils.get_compute_executor",
            mock_utils.get_compute_executor,
        ),
        patch(
            "resource_server_async.auth.introspect_token", mock_utils.introspect_token
        ),
        # Overwrite future
        patch("asyncio.wrap_future", mock_utils.wrap_future),
        patch("asyncio.wait_for", mock_utils.wait_for),
        # Overwrite httpx client
        patch("httpx.AsyncClient", mock_utils.MockAsyncClient),
        # Overwrite StreamingHttpResponse in endpoint modules where it's actually imported
        patch(
            "resource_server_async.endpoints.globus_compute.StreamingHttpResponse",
            mock_utils.MockStreamingHttpResponse,
        ),
        patch(
            "resource_server_async.endpoints.direct_api.StreamingHttpResponse",
            mock_utils.MockStreamingHttpResponse,
        ),
        # Overwrite metis fetch status call
        patch(
            "resource_server_async.clusters.metis.MetisCluster._fetch_metis_status",
            mock_utils.mock_fetch_metis_status,
        ),
        # Overwrite settings variables
        patch("django.conf.settings.MAX_BATCHES_PER_USER", 1000),
        patch("django.conf.settings.AUTHORIZED_IDP_DOMAINS", [mock_utils.MOCK_DOMAIN]),
        patch("django.conf.settings.NUMBER_OF_GLOBUS_POLICIES", 1),
        patch("django.conf.settings.GLOBUS_POLICIES", mock_utils.MOCK_POLICY_UUID),
    )

    def __enter__(self):
        for p in self.PATCHERS:
            p.start()

    def __exit__(self, *_):
        for p in self.PATCHERS:
            p.stop()


with mock_override():
    # Import views to trigger route registration on the Ninja API/router
    from resource_server_async import views as _  # noqa: E402, F401

    # Skip Ninja's namespace registry check — Django's URL loading already
    # registered the NinjaAPI namespace, so TestAsyncClient(api) would hit a
    # false duplicate.
    os.environ["NINJA_SKIP_REGISTRY"] = "true"

    # Create request Django Ninja test client instance
    KWARGS = {"content_type": "application/json"}
    CLIENT = TestAsyncClient(ninja_api)


class ResourceServerTestCase(TestCase):
    @override
    def setUp(self):
        """
        Initialization that will happen per test.
        """
        super().setUp()

        self.enterContext(mock_override())
        _request_context.set(
            RequestContext(mock_utils.mock_initialize_access_log_data(None, None))
        )

    @classmethod
    @override
    def setUpTestData(cls):
        """
        Initialization that will only happen once before running all tests.
        """

        # Fill Django test database
        call_command("loaddata", "fixtures/endpoints.json")
        call_command("loaddata", "fixtures/clusters.json")

        return super().setUpTestData()

    @classmethod
    def template_test(cls, test_name, *args, **kwargs):
        """
        Templates a test given an argument list.
        """
        test = getattr(cls, test_name)
        to_alphanumeric = lambda x: re.sub(r"[^a-zA-Z0-9_]+", "", str(x))
        templated_name = (
            f"test_{test_name}_{to_alphanumeric(args)}{to_alphanumeric(kwargs)}"
        )

        if iscoroutinefunction(test):

            async def async_lambda(self):
                return await test(self, *args, **kwargs)

            setattr(
                cls,
                templated_name,
                async_lambda,
            )
        else:
            setattr(
                cls,
                templated_name,
                lambda self: test(self, *args, **kwargs),
            )
