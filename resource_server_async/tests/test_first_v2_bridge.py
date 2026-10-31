"""Tests for the First V2 bridge.

The bridge reads the V2 config from Redis and keeps the matching V1
database rows up to date. These tests check that it reads the new
capability fields, copies them into those rows, and still produces
today's rows when the config carries none of them.
"""

from typing import Any
from unittest.mock import patch

from django.test import SimpleTestCase

from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.first_v2_bridge.mapping import desired_endpoints
from resource_server_async.first_v2_bridge.router_config import RouterConfig
from resource_server_async.first_v2_bridge.settings import BridgeSettings

# The keys /models may show users (MODEL_DETAILS_KEYS in env.example).
# Nothing outside this list may end up in a public response.
PUBLIC_MODEL_DETAILS_KEYS = ["display_name", "description", "capabilities"]

# What a bridge-managed row looks like today when the config says nothing
# about capabilities. Tests below compare against it to prove that old
# configs change nothing.
LEGACY_ROW_CONFIG = {
    "model_urls": ["https://backend-0.example"],
    "backend_model_name": "served-model",
    "ca_cert_path": "/certs/ca.crt",
    "client_cert_path": "/certs/client.crt",
    "client_key_path": "/certs/client.key",
    "proxy_url": "socks5h://proxy.example:1080",
    "check_hostname": True,
    "trust_env": False,
}


def bridge_settings() -> BridgeSettings:
    """Bridge settings pinned to fixed values, so tests don't depend on the
    environment."""
    settings = BridgeSettings()
    settings.ca_cert_path = "/certs/ca.crt"
    settings.client_cert_path = "/certs/client.crt"
    settings.client_key_path = "/certs/client.key"
    settings.proxy_url = "socks5h://proxy.example:1080"
    settings.check_hostname = True
    settings.trust_env = False
    return settings


def router_config(model_overrides: dict[str, Any]) -> RouterConfig:
    """The config in Redis for one model with one running deployment.

    It carries extra fields that only V2 reads (usage_limits, overload,
    cluster_name, router_params, ...); the bridge is expected to ignore
    them.
    """
    return RouterConfig.model_validate(
        {
            "version": 7,
            "models": [
                {
                    "name": "nemotron-35-super-vl",
                    "aliases": ["nemotron"],
                    "allowed_groups": ["public"],
                    "allowed_domains": ["anl.gov"],
                    "deployments": [
                        {
                            "kind": "pilot",
                            "name": "tara/pilot-a",
                            "cluster_name": "tara",
                            "router_params": {"weight": 1},
                            "prometheus_metrics_path": "/metrics",
                            "prometheus_scrape_interval_sec": 30,
                            "backends": [
                                {
                                    "id": "backend-0",
                                    "model_url": "https://backend-0.example",
                                    "backend_model_name": "served-model",
                                    "api_key": None,
                                }
                            ],
                        }
                    ],
                    "usage_limits": {"default": {"tpm": 8388608}},
                    "overload": {"short_retry_sec": 15},
                    **model_overrides,
                }
            ],
        }
    )


class RouterConfigParsingTests(SimpleTestCase):
    def test_capability_fields_are_parsed_from_the_blob(self) -> None:
        cfg = router_config(
            {
                "supported_endpoints": ["chat/completions", "responses"],
                "max_model_len": 1048576,
                "display_name": "Nemotron 3.5 Super VL",
                "capabilities": {"streaming": True},
                "created_at": "2026-10-22T00:00:00Z",
            }
        )

        model = cfg.models[0]
        self.assertEqual(model.supported_endpoints, ["chat/completions", "responses"])
        self.assertEqual(model.max_model_len, 1048576)
        self.assertEqual(model.display_name, "Nemotron 3.5 Super VL")
        self.assertEqual(model.capabilities, {"streaming": True})

    def test_blob_without_capability_fields_parses_with_defaults(self) -> None:
        model = router_config({"max_model_len": None, "display_name": None}).models[0]

        self.assertEqual(model.supported_endpoints, [])
        self.assertIsNone(model.max_model_len)
        self.assertIsNone(model.display_name)
        self.assertEqual(model.capabilities, {})


class DesiredEndpointCapabilityTests(SimpleTestCase):
    def test_capability_fields_are_translated_into_the_row_config(self) -> None:
        cfg = router_config(
            {
                "supported_endpoints": ["chat/completions", "responses", "messages"],
                "max_model_len": 1048576,
                "display_name": "Nemotron 3.5 Super VL",
                "capabilities": {
                    "input_modalities": ["text", "image", "video"],
                    "streaming": True,
                    "reasoning": {"supported": True},
                    "tool_calling": {"supported": True},
                },
            }
        )

        (endpoint,) = desired_endpoints(cfg, bridge_settings())

        self.assertEqual(endpoint.cluster, "tara")
        self.assertEqual(endpoint.framework, "api")
        self.assertEqual(
            endpoint.config["capabilities"],
            {
                "schema_version": 1,
                "api_protocols": ["chat_completions", "responses", "messages"],
                "context_window_tokens": 1048576,
                "input_modalities": ["text", "image", "video"],
                "streaming": True,
                "reasoning": {"supported": True},
                "tool_calling": {"supported": True},
            },
        )
        self.assertEqual(endpoint.config["display_name"], "Nemotron 3.5 Super VL")
        # The keys every row already had are untouched.
        self.assertEqual(endpoint.config["model_urls"], ["https://backend-0.example"])
        self.assertEqual(endpoint.config["backend_model_name"], "served-model")

    def test_blob_without_capability_data_yields_todays_row_config(self) -> None:
        # First without the fields at all, as an older config would be...
        (endpoint,) = desired_endpoints(router_config({}), bridge_settings())
        self.assertEqual(endpoint.config, LEGACY_ROW_CONFIG)

        # ...and then with them present but empty.
        (endpoint,) = desired_endpoints(
            router_config(
                {
                    "supported_endpoints": [],
                    "max_model_len": None,
                    "display_name": None,
                    "capabilities": {},
                }
            ),
            bridge_settings(),
        )
        self.assertEqual(endpoint.config, LEGACY_ROW_CONFIG)

    def test_endpoint_paths_normalize_to_underscored_protocols(self) -> None:
        cfg = router_config(
            {"supported_endpoints": ["/chat/completions/", "//responses"]}
        )

        (endpoint,) = desired_endpoints(cfg, bridge_settings())

        self.assertEqual(
            endpoint.config["capabilities"]["api_protocols"],
            ["chat_completions", "responses"],
        )


class ModelDetailsRoundTripTests(SimpleTestCase):
    def test_row_config_capabilities_reach_model_details(self) -> None:
        cfg = router_config(
            {
                "supported_endpoints": ["chat/completions", "responses"],
                "max_model_len": 1048576,
                "display_name": "Nemotron 3.5 Super VL",
                "capabilities": {"input_modalities": ["text", "image", "video"]},
            }
        )
        (endpoint,) = desired_endpoints(cfg, bridge_settings())

        with patch(
            "resource_server_async.endpoints.endpoint.MODEL_DETAILS_KEYS",
            PUBLIC_MODEL_DETAILS_KEYS,
        ):
            details = BaseEndpoint.build_model_details(
                cluster=endpoint.cluster,
                framework=endpoint.framework,
                model=endpoint.model,
                tpm_model=8388608,
                tpm_user=1000,
                config=endpoint.config,
            )

        self.assertEqual(details["display_name"], "Nemotron 3.5 Super VL")
        capabilities = details["capabilities"]
        self.assertEqual(
            capabilities["api_protocols"], ["chat_completions", "responses"]
        )
        self.assertEqual(capabilities["context_window_tokens"], 1048576)
        self.assertEqual(capabilities["input_modalities"], ["text", "image", "video"])
        # Private settings must never reach the public response.
        for private_key in (
            "model_urls",
            "backend_model_name",
            "ca_cert_path",
            "client_cert_path",
            "client_key_path",
            "proxy_url",
            "check_hostname",
            "trust_env",
        ):
            self.assertNotIn(private_key, details)
