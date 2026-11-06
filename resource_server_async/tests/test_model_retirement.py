"""Catalog-driven retirement survives tombstoning without becoming an endpoint."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from django.http import StreamingHttpResponse

from resource_server_async.clusters import BaseCluster
from resource_server_async.endpoints import BaseEndpoint, DirectAPIEndpoint
from resource_server_async.first_v2_bridge.mapping import retirement_catalog
from resource_server_async.first_v2_bridge.router_config import RouterConfig
from resource_server_async.management.commands.first_v2_bridge import Command
from resource_server_async.models import Cluster, Endpoint
from resource_server_async.schemas.endpoints import (
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)

from . import (
    CLIENT,
    EXPIRED_TOKEN,
    HEADERS,
    INVALID_TOKEN,
    KWARGS,
    ResourceServerTestCase,
    get_response_json,
    mock_utils,
)
from .test_first_v2_bridge import bridge_settings

APIS = ("chat/completions", "responses", "messages")


def config(**overrides):
    return RouterConfig.model_validate(
        {
            "version": 42,
            "models": [
                {
                    "name": "inkling-bf16",
                    "aliases": ["inkling-alias"],
                    "allowed_groups": [],
                    "allowed_domains": [],
                    "usage_limits": {"default": {"tpm": 0}},
                    "retired_deployments": [
                        {"name": "minerva/inkling-bf16", "cluster_name": "minerva"}
                    ],
                    "deployments": [
                        {
                            "kind": "pilot",
                            "name": "tara/inkling-bf16",
                            "cluster_name": "tara",
                            "backends": [
                                {
                                    "id": "tara-0",
                                    "model_url": "https://private-backend.invalid/v1",
                                    "backend_model_name": "inkling-bf16",
                                    "api_key": "never-copy-this-secret",
                                }
                            ],
                        }
                    ],
                    **overrides,
                }
            ],
        }
    )


def cluster_stub(name):
    return SimpleNamespace(
        cluster_name=name,
        frameworks=["api"],
        openai_endpoints=list(APIS),
        check_permission=Mock(),
        check_maintenance=AsyncMock(return_value=SimpleNamespace(raise_if_down=Mock())),
    )


class ModelRetirementTests(ResourceServerTestCase):
    def setUp(self):
        super().setUp()
        for name in ("minerva", "tara"):
            Cluster.objects.update_or_create(
                cluster_name=name,
                defaults={
                    "frameworks": ["api"],
                    "openai_endpoints": list(APIS),
                    "cluster_adapter": "unused.invalid.Cluster",
                    "config": "{}",
                },
            )
        # Established legacy tombstone: the original endpoint identity is gone.
        Endpoint.objects.create(
            endpoint_slug="minerva-api-removed-pk-65",
            cluster="minerva",
            framework="api",
            model="removed-pk-65",
            allowed_globus_groups=["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
            endpoint_adapter="unused.invalid.Endpoint",
            config="{}",
        )
        self.sync(config())

    def sync(self, cfg):
        with (
            patch(
                "resource_server_async.management.commands.first_v2_bridge.get_bridge_redis_client"
            ),
            patch.object(RouterConfig, "load", return_value=cfg),
        ):
            Command()._tick(bridge_settings())

    async def post(
        self,
        api,
        *,
        cluster="minerva",
        model="inkling-bf16",
        stream=False,
        headers=None,
    ):
        body = {"model": model, "stream": stream}
        body.update(
            {"input": "hi", "max_output_tokens": 5}
            if api == "responses"
            else {"messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
        )
        return await CLIENT.post(
            f"/{cluster}/api/v1/{api}",
            data=json.dumps(body),
            headers=HEADERS if headers is None else headers,
            **KWARGS,
        )

    async def test_retired_all_apis_stream_modes_and_alias_without_backend_resolution(
        self,
    ):
        with (
            patch.object(BaseCluster, "load_adapter", new=AsyncMock()) as load_cluster,
            patch.object(
                BaseEndpoint, "load_adapter", new=AsyncMock()
            ) as load_endpoint,
        ):
            for api in APIS:
                for stream in (False, True):
                    for model in ("inkling-bf16", "inkling-alias"):
                        with self.subTest(api=api, stream=stream, model=model):
                            response = await self.post(api, model=model, stream=stream)
                            self.assertEqual(response.status_code, 410)
                            body = get_response_json(response)
                            self.assertEqual(body["type"], "error")
                            error = body["error"]
                            self.assertEqual(error["code"], "deployment_retired")
                            self.assertEqual(error["type"], "deployment_retired")
                            self.assertIn(
                                ".../resource_server/tara/api/v1", error["message"]
                            )
                            self.assertEqual(error["info"]["model"], "inkling-bf16")
                            self.assertEqual(
                                error["info"]["retired_deployment"],
                                "minerva/inkling-bf16",
                            )
                            self.assertEqual(
                                error["info"]["alternatives"],
                                [
                                    {
                                        "deployment": "tara/inkling-bf16",
                                        "cluster": "tara",
                                        "base_path": "/resource_server/tara/api/v1",
                                    }
                                ],
                            )
                            self.assertIn(
                                "application/json", response.headers["Content-Type"]
                            )
                            self.assertNotIn("Location", response.headers)
                            self.assertNotIn(
                                "private-backend", response.content.decode()
                            )
                            self.assertNotIn(
                                "never-copy-this-secret", response.content.decode()
                            )
            load_cluster.assert_not_awaited()
            load_endpoint.assert_not_awaited()

    async def test_global_auth_still_precedes_retirement_for_all_apis(self):
        for api in APIS:
            for token in (None, INVALID_TOKEN, EXPIRED_TOKEN):
                with self.subTest(api=api, token=token):
                    response = await self.post(
                        api,
                        stream=True,
                        headers=mock_utils.get_mock_headers(access_token=token or ""),
                    )
                    self.assertEqual(response.status_code, 401)
                    self.assertNotIn("retired_deployment", response.content.decode())

    async def test_live_cluster_and_catalog_acl_precede_guidance(self):
        cluster = await Cluster.objects.aget(cluster_name="minerva")
        for key in ("allowed_groups", "allowed_domains"):
            cluster.model_retirements[0][key] = ["denied"]
            await cluster.asave(update_fields=["model_retirements"])
            for api in APIS:
                response = await self.post(api, stream=True)
                self.assertEqual(response.status_code, 401)
                self.assertNotIn("retired_deployment", response.content.decode())
            cluster.model_retirements[0][key] = []
        for key in ("allowed_globus_groups", "allowed_domains"):
            setattr(cluster, key, ["denied"])
            await cluster.asave()
            response = await self.post("chat/completions")
            self.assertEqual(response.status_code, 401)
            setattr(cluster, key, [])

    async def test_no_alternative_or_private_alternative_still_returns_410(self):
        tara = await Cluster.objects.aget(cluster_name="tara")
        tara.allowed_globus_groups = ["denied"]
        await tara.asave()
        response = await self.post("responses", stream=True)
        self.assertEqual(response.status_code, 410)
        self.assertEqual(
            get_response_json(response)["error"]["info"]["alternatives"], []
        )
        self.assertNotIn("tara", get_response_json(response)["error"]["message"])
        await Endpoint.objects.filter(cluster="tara", model="inkling-bf16").adelete()
        response = await self.post("messages")
        self.assertEqual(response.status_code, 410)

    async def test_tara_and_unrelated_models_keep_normal_dispatch(self):
        endpoint = Mock(spec=DirectAPIEndpoint)
        endpoint.model = "inkling-bf16"
        endpoint.endpoint_slug = "tara-api-inkling-bf16"
        endpoint.check_token_rate_limit.return_value = SimpleNamespace(allow=True)
        for cluster, model in (("tara", "inkling-bf16"), ("minerva", "unrelated")):
            for api in APIS:
                for stream in (False, True):
                    endpoint.model = model
                    endpoint.submit_task = AsyncMock(
                        return_value=SubmitTaskResult(
                            result={"ok": True}, task_id="test"
                        )
                    )
                    endpoint.submit_streaming_task = AsyncMock(
                        return_value=SubmitStreamingTaskResponse(
                            response=StreamingHttpResponse(
                                [], content_type="text/event-stream"
                            ),
                            task_id="test",
                        )
                    )
                    with (
                        patch.object(
                            BaseCluster,
                            "load_adapter",
                            new=AsyncMock(return_value=cluster_stub(cluster)),
                        ),
                        patch.object(
                            BaseEndpoint,
                            "load_adapter",
                            new=AsyncMock(return_value=endpoint),
                        ) as load,
                    ):
                        response = await self.post(
                            api, cluster=cluster, model=model, stream=stream
                        )
                    self.assertEqual(response.status_code, 200)
                    load.assert_awaited_once_with(cluster, "api", model)
                    (
                        endpoint.submit_streaming_task
                        if stream
                        else endpoint.submit_task
                    ).assert_awaited_once()

    def test_bridge_snapshot_is_secret_free_and_not_a_serving_endpoint(self):
        cluster = Cluster.objects.get(cluster_name="minerva")
        self.assertEqual(cluster.model_retirements, retirement_catalog(config()))
        self.assertFalse(
            Endpoint.objects.filter(cluster="minerva", model="inkling-bf16").exists()
        )
        self.assertTrue(
            Endpoint.objects.filter(cluster="tara", model="inkling-bf16").exists()
        )
        self.assertNotIn("model_url", json.dumps(cluster.model_retirements))
        self.assertNotIn(
            "never-copy-this-secret", json.dumps(cluster.model_retirements)
        )
        self.assertEqual(
            Endpoint.objects.get(model="removed-pk-65").allowed_globus_groups,
            ["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
        )

    def test_reconcile_updates_and_removes_tombstones_without_other_cluster_changes(
        self,
    ):
        source = Cluster.objects.get(cluster_name="minerva")
        source_before = {
            f.name: getattr(source, f.name)
            for f in source._meta.fields
            if f.name != "model_retirements"
        }
        self.sync(config(retired_deployments=[]))
        source.refresh_from_db()
        self.assertEqual(source.model_retirements, [])
        self.assertEqual(
            source_before,
            {
                f.name: getattr(source, f.name)
                for f in source._meta.fields
                if f.name != "model_retirements"
            },
        )
        self.assertTrue(
            Endpoint.objects.filter(cluster="tara", model="inkling-bf16").exists()
        )

    def test_missing_or_unreachable_router_config_does_not_erase_snapshot(self):
        before = Cluster.objects.get(cluster_name="minerva").model_retirements
        self.sync(None)
        self.assertEqual(
            Cluster.objects.get(cluster_name="minerva").model_retirements, before
        )
        with (
            patch.object(RouterConfig, "load", side_effect=ConnectionError),
            patch(
                "resource_server_async.management.commands.first_v2_bridge.get_bridge_redis_client"
            ),
        ):
            with self.assertRaises(ConnectionError):
                Command()._tick(bridge_settings())
        self.assertEqual(
            Cluster.objects.get(cluster_name="minerva").model_retirements, before
        )

    def test_invalid_router_config_or_failed_reconcile_does_not_partially_publish(self):
        before = Cluster.objects.get(cluster_name="minerva").model_retirements

        def fail_after_delete(_desired):
            Endpoint.objects.filter(cluster="tara", model="inkling-bf16").delete()
            raise RuntimeError("isolated failure")

        with (
            patch.object(RouterConfig, "load", return_value=config()),
            patch(
                "resource_server_async.management.commands.first_v2_bridge.get_bridge_redis_client"
            ),
            patch.object(
                Command, "_reconcile_endpoints", side_effect=fail_after_delete
            ),
        ):
            with self.assertRaises(RuntimeError):
                Command()._tick(bridge_settings())
        self.assertEqual(
            Cluster.objects.get(cluster_name="minerva").model_retirements, before
        )
        self.assertTrue(
            Endpoint.objects.filter(cluster="tara", model="inkling-bf16").exists()
        )
        with self.assertRaises(ValueError):
            config(retired_deployments=[{"name": "missing-cluster"}])

    def test_global_retirement_has_no_serving_rows(self):
        self.sync(
            config(retired=True)
        )  # Even malformed/stale backend data cannot advertise it.
        self.assertFalse(
            Endpoint.objects.filter(cluster="tara", model="inkling-bf16").exists()
        )

    async def test_model_retired_is_reserved_for_global_retirement(self):
        for name in ("minerva", "tara"):
            cluster = await Cluster.objects.aget(cluster_name=name)
            cluster.model_retirements[0]["retired"] = True
            await cluster.asave()
            response = await self.post("responses", cluster=name, stream=True)
            self.assertEqual(response.status_code, 410)
            self.assertEqual(
                get_response_json(response)["error"]["code"], "model_retired"
            )

    def test_old_router_config_has_no_retirements(self):
        cfg = config(retired_deployments=[])
        raw = cfg.model_dump()
        raw["models"][0].pop("retired")
        raw["models"][0].pop("retired_deployments")
        self.assertEqual(retirement_catalog(RouterConfig.model_validate(raw)), [])

    async def test_retiring_one_deployment_does_not_retire_another_on_same_cluster(
        self,
    ):
        cluster = await Cluster.objects.aget(cluster_name="minerva")
        cluster.model_retirements[0]["active_deployments"] = [
            {"name": "minerva/replacement-instance", "cluster_name": "minerva"}
        ]
        await cluster.asave()
        from resource_server_async.retirements import raise_if_retired

        from .test_openai_responsibility_separation import request_context

        await raise_if_retired("minerva", "api", "inkling-bf16", request_context().user)

    async def test_retired_identity_is_hidden_even_before_legacy_fixture_is_tombstoned(
        self,
    ):
        from resource_server_async.services import _should_show, get_all_endpoints

        from .test_openai_responsibility_separation import request_context

        await Endpoint.objects.acreate(
            endpoint_slug="minerva-api-inkling-bf16",
            cluster="minerva",
            framework="api",
            model="inkling-bf16",
            endpoint_adapter="unused.invalid.Endpoint",
            config="{}",
        )
        user = request_context().user
        placeholder = Mock()
        placeholder.check_permission.return_value = False
        with patch.object(
            BaseEndpoint, "load_adapter", new=AsyncMock(return_value=placeholder)
        ) as load:
            self.assertEqual(await get_all_endpoints(user, cluster_stub("minerva")), [])
            self.assertFalse(await _should_show("minerva", "api", "inkling-bf16", user))
            self.assertEqual(load.await_count, 1)  # Only the restricted removed-pk row.
            self.assertEqual(load.await_args.args[2], "removed-pk-65")

    async def test_globally_retired_model_is_hidden_without_resolving_its_adapter(self):
        from resource_server_async.services import get_all_endpoints

        from .test_openai_responsibility_separation import request_context

        cluster = await Cluster.objects.aget(cluster_name="tara")
        cluster.model_retirements[0]["retired"] = True
        await cluster.asave()
        with patch.object(BaseEndpoint, "load_adapter", new=AsyncMock()) as load:
            self.assertEqual(
                await get_all_endpoints(request_context().user, cluster_stub("tara")),
                [],
            )
            load.assert_not_awaited()
