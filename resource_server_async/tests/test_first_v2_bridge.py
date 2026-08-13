from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.test import SimpleTestCase, TestCase

from resource_server_async.endpoints.endpoint import BaseEndpoint
from resource_server_async.endpoints.first_v2 import FirstV2Endpoint
from resource_server_async.first_v2_bridge.mapping import (
    ENDPOINT_ADAPTER,
    DesiredEndpoint,
    desired_endpoints,
    deterministic_pk,
)
from resource_server_async.first_v2_bridge.router_config import (
    BackendConfig,
    DeploymentConfig,
    ModelConfig,
    RouterConfig,
)
from resource_server_async.first_v2_bridge.settings import BridgeSettings
from resource_server_async.management.commands.first_v2_bridge import Command
from resource_server_async.models import Cluster, Endpoint


def _tara_router_config() -> RouterConfig:
    return RouterConfig(
        version=17,
        models=[
            ModelConfig(
                name="nemotron-3-ultra",
                allowed_groups=["test-group"],
                allowed_domains=["anl.gov"],
                deployments=[
                    DeploymentConfig(
                        kind="pilot",
                        name="tara-production/nemotron-3-ultra",
                        backends=[
                            BackendConfig(
                                id="pilot_replica/1",
                                model_url=(
                                    "https://10.20.30.40:18443/replicas/"
                                    "tara-production/nemotron-3-ultra/replica/1/"
                                ),
                                backend_model_name="nemotron-3-ultra",
                            )
                        ],
                    )
                ],
            )
        ],
    )


class BridgeMappingTests(SimpleTestCase):
    def test_default_mapping_accepts_tara_production_deployment(self) -> None:
        settings = BridgeSettings()

        desired = desired_endpoints(_tara_router_config(), settings)

        self.assertEqual(len(desired), 1)
        endpoint = desired[0]
        self.assertEqual(endpoint.cluster, "tara")
        self.assertEqual(endpoint.framework, "api")
        self.assertEqual(endpoint.model, "nemotron-3-ultra")
        self.assertEqual(endpoint.endpoint_slug, "tara-api-nemotron-3-ultra")
        self.assertEqual(
            endpoint.config["model_urls"],
            [
                "https://10.20.30.40:18443/replicas/"
                "tara-production/nemotron-3-ultra/replica/1/"
            ],
        )
        self.assertEqual(settings.prefix_map["tara/"], ("tara", "api"))
        self.assertEqual(settings.prefix_map["tara-production/"], ("tara", "api"))

    def test_missing_router_config_is_a_noop(self) -> None:
        settings = BridgeSettings()
        command = Command()

        with (
            patch(
                "resource_server_async.management.commands.first_v2_bridge."
                "get_bridge_redis_client"
            ) as get_client,
            patch.object(RouterConfig, "load", return_value=None),
            patch.object(command, "_reconcile_endpoints") as reconcile,
        ):
            command._tick(settings)

        get_client.assert_called_once_with(settings.redis_url)
        reconcile.assert_not_called()


class BridgeFixtureTests(TestCase):
    @classmethod
    def setUpTestData(cls) -> None:
        call_command("loaddata", "fixtures/tara_v2_bridge_cluster.json", verbosity=0)

    def test_tara_cluster_fixture_is_isolated_and_bridge_capable(self) -> None:
        cluster = Cluster.objects.get(pk=100)
        self.assertEqual(cluster.cluster_name, "tara")
        self.assertEqual(cluster.frameworks, ["api"])
        self.assertEqual(
            cluster.cluster_adapter,
            "resource_server_async.clusters.tara.TaraCluster",
        )
        self.assertIn("chat/completions", cluster.openai_endpoints)


class BridgeReconcileTests(TestCase):
    def test_reconcile_changes_only_first_v2_endpoint_rows(self) -> None:
        Endpoint.objects.create(
            id=41,
            endpoint_slug="ordinary-endpoint",
            cluster="other",
            framework="api",
            model="ordinary-model",
            endpoint_adapter=(
                "resource_server_async.endpoints.direct_api.DirectAPIEndpoint"
            ),
            config="{'api_url': 'https://example.invalid/v1', "
            "'api_key_env_name': 'UNUSED'}",
        )
        Endpoint.objects.create(
            id=deterministic_pk("tara", "stale-model"),
            endpoint_slug="stale-first-v2-endpoint",
            cluster="tara",
            framework="api",
            model="stale-model",
            endpoint_adapter=ENDPOINT_ADAPTER,
            config="{}",
        )
        desired = DesiredEndpoint(
            pk=deterministic_pk("tara", "nemotron-3-ultra"),
            endpoint_slug="tara-api-nemotron-3-ultra",
            cluster="tara",
            framework="api",
            model="nemotron-3-ultra",
            endpoint_adapter=ENDPOINT_ADAPTER,
            allowed_globus_groups=[],
            allowed_domains=[],
            config={
                "model_urls": ["https://10.20.30.40:18443/replicas/canary/"],
                "backend_model_name": "nemotron-3-ultra",
            },
        )

        counts = Command()._reconcile_endpoints([desired])

        self.assertEqual(counts, (1, 0, 1))
        ordinary = Endpoint.objects.get(pk=41)
        self.assertEqual(ordinary.endpoint_slug, "ordinary-endpoint")
        self.assertNotEqual(ordinary.endpoint_adapter, ENDPOINT_ADAPTER)
        self.assertFalse(
            Endpoint.objects.filter(endpoint_slug="stale-first-v2-endpoint").exists()
        )
        managed = Endpoint.objects.get(pk=desired.pk)
        self.assertEqual(managed.endpoint_slug, desired.endpoint_slug)
        self.assertEqual(managed.endpoint_adapter, ENDPOINT_ADAPTER)


class FirstV2EndpointTests(SimpleTestCase):
    def test_request_body_and_proxy_are_bridge_specific(self) -> None:
        client = MagicMock()
        with (
            patch(
                "resource_server_async.endpoints.first_v2.create_ssl_context",
                return_value=True,
            ),
            patch(
                "resource_server_async.endpoints.first_v2.httpx.AsyncClient",
                return_value=client,
            ) as client_type,
            patch.object(BaseEndpoint, "build_token_limiter", return_value=None),
        ):
            endpoint = FirstV2Endpoint(
                id="1",
                endpoint_slug="tara-api-nemotron-3-ultra",
                cluster="tara",
                framework="api",
                model="nemotron-alias",
                endpoint_adapter=ENDPOINT_ADAPTER,
                tpm_model=0,
                tpm_user=0,
                config={
                    "model_urls": [
                        "https://10.20.30.40:18443/replicas/"
                        "tara-production/nemotron-3-ultra/replica/1/"
                    ],
                    "backend_model_name": "nemotron-3-ultra",
                    "proxy_url": "socks5h://127.0.0.1:1080",
                    "trust_env": False,
                },
            )

        client_type.assert_called_once_with(
            timeout=120,
            headers={"Content-Type": "application/json"},
            verify=True,
            proxy="socks5h://127.0.0.1:1080",
            trust_env=False,
        )
        url, body = endpoint._build_request(
            {
                "model_params": {
                    "model": "nemotron-alias",
                    "messages": [{"role": "user", "content": "hello"}],
                    "openai_endpoint": "/chat/completions/",
                    "api_port": 8000,
                }
            },
            stream=False,
        )
        self.assertEqual(
            url,
            "https://10.20.30.40:18443/replicas/"
            "tara-production/nemotron-3-ultra/replica/1/v1/chat/completions",
        )
        self.assertEqual(
            body,
            {
                "model": "nemotron-3-ultra",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
        )
        self.assertNotIn("Authorization", client_type.call_args.kwargs["headers"])
