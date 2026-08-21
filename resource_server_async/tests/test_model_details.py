import ast
from unittest.mock import patch

from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.models import Endpoint
from resource_server_async.tests import (
    CLIENT,
    HEADERS,
    PREMIUM_HEADERS,
    ResourceServerTestCase,
    get_response_json,
    mock_utils,
)

PUBLIC_MODEL_DETAILS_KEYS = ["display_name", "description", "capabilities"]
TEST_CAPABILITIES = {
    "schema_version": 1,
    "api_protocols": ["chat_completions", "responses"],
    "context_window_tokens": 131072,
    "input_modalities": ["text"],
    "streaming": True,
    "reasoning": {"supported": True, "separate_output": True},
    "tool_calling": {"supported": True},
}


class ModelDetailsBuilderTests(ResourceServerTestCase):
    def test_only_allowlisted_public_metadata_is_returned(self) -> None:
        config = {
            "display_name": "Test Model",
            "description": "Public description",
            "capabilities": TEST_CAPABILITIES,
            "api_url": "https://private-backend.example.test/v1",
            "api_key_env_name": "PRIVATE_API_KEY",
            "ca_cert_path": "/private/ca.crt",
            "client_cert_path": "/private/client.crt",
            "client_key_path": "/private/client.key",
        }

        with patch(
            "resource_server_async.endpoints.endpoint.MODEL_DETAILS_KEYS",
            PUBLIC_MODEL_DETAILS_KEYS,
        ):
            details = BaseEndpoint.build_model_details(
                "test-cluster", "api", "test-model", 0, 0, config
            )

        self.assertEqual(details["display_name"], "Test Model")
        self.assertEqual(details["description"], "Public description")
        self.assertEqual(details["capabilities"], TEST_CAPABILITIES)
        for private_key in (
            "api_url",
            "api_key_env_name",
            "ca_cert_path",
            "client_cert_path",
            "client_key_path",
        ):
            self.assertNotIn(private_key, details)

    def test_missing_optional_metadata_remains_backward_compatible(self) -> None:
        with patch(
            "resource_server_async.endpoints.endpoint.MODEL_DETAILS_KEYS",
            PUBLIC_MODEL_DETAILS_KEYS,
        ):
            details = BaseEndpoint.build_model_details(
                "test-cluster",
                "api",
                "legacy-model",
                0,
                0,
                {"api_url": "https://private-backend.example.test/v1"},
            )

        self.assertEqual(
            details,
            {
                "id": "legacy-model",
                "object": "model",
                "cluster": "test-cluster",
                "framework": "api",
            },
        )


class ModelsViewTests(ResourceServerTestCase):
    cluster = "your-other-cluster"
    model = "Your-Model-120B"
    url = f"/{cluster}/models?model_id={model}"

    def setUp(self) -> None:
        super().setUp()
        self._clear_adapter_caches()

    def tearDown(self) -> None:
        self._clear_adapter_caches()
        super().tearDown()

    @staticmethod
    def _clear_adapter_caches() -> None:
        from resource_server_async.clusters.cluster import (
            _adapter_cache as cluster_adapter_cache,
        )
        from resource_server_async.endpoints.endpoint import (
            _adapter_cache as endpoint_adapter_cache,
        )

        cluster_adapter_cache.clear()
        endpoint_adapter_cache.clear()

    async def _add_public_metadata(self, *, restricted: bool = False) -> None:
        endpoint = await Endpoint.objects.aget(cluster=self.cluster, model=self.model)
        config = ast.literal_eval(endpoint.config)
        config.update(
            {
                "display_name": "Test Model 120B",
                "description": "unique-public-model-description",
                "capabilities": TEST_CAPABILITIES,
            }
        )
        endpoint.config = repr(config)
        endpoint.allowed_globus_groups = (
            [mock_utils.MOCK_GROUP_UUID] if restricted else []
        )
        await endpoint.asave(update_fields=["config", "allowed_globus_groups"])
        self._clear_adapter_caches()

    async def test_model_filter_returns_public_metadata_without_private_config(
        self,
    ) -> None:
        await self._add_public_metadata()

        with patch(
            "resource_server_async.endpoints.endpoint.MODEL_DETAILS_KEYS",
            PUBLIC_MODEL_DETAILS_KEYS,
        ):
            response = await CLIENT.get(self.url, headers=HEADERS)

        response_data = get_response_json(response)
        self.assertEqual(response.status_code, 200, str(response_data))
        self.assertEqual(len(response_data), 1)
        self.assertEqual(response_data[0]["id"], self.model)
        self.assertEqual(response_data[0]["capabilities"], TEST_CAPABILITIES)
        self.assertNotIn("api_url", response_data[0])
        self.assertNotIn("api_key_env_name", response_data[0])

    async def test_unauthorized_model_metadata_does_not_leak(self) -> None:
        await self._add_public_metadata(restricted=True)

        with patch(
            "resource_server_async.endpoints.endpoint.MODEL_DETAILS_KEYS",
            PUBLIC_MODEL_DETAILS_KEYS,
        ):
            unauthorized = await CLIENT.get(self.url, headers=HEADERS)
            authorized = await CLIENT.get(self.url, headers=PREMIUM_HEADERS)

        unauthorized_data = get_response_json(unauthorized)
        self.assertEqual(unauthorized.status_code, 404, str(unauthorized_data))
        self.assertNotIn("unique-public-model-description", str(unauthorized_data))

        authorized_data = get_response_json(authorized)
        self.assertEqual(authorized.status_code, 200, str(authorized_data))
        self.assertEqual(
            authorized_data[0]["description"], "unique-public-model-description"
        )
