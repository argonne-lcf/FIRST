"""Retirement errors must precede fixture resolution, dispatch and SSE headers."""

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from django.http import StreamingHttpResponse

from resource_server_async.auth import check_permission
from resource_server_async.clusters import BaseCluster
from resource_server_async.endpoints import BaseEndpoint, DirectAPIEndpoint
from resource_server_async.errors import EndpointNotFound, Unauthorized
from resource_server_async.models import Cluster, Endpoint
from resource_server_async.retirements import MODEL_RETIREMENTS, raise_if_retired
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
from .test_openai_responsibility_separation import request_context

KEY = ("minerva", "inkling-bf16")
EXPECTED = {
    "error": {
        "code": "model_retired",
        "message": "Inkling has been retired on Minerva. Use inkling-bf16 on Tara.",
        "info": {
            "replacement_cluster": "tara",
            "replacement_model": "inkling-bf16",
            "replacement_api_url": (
                "https://inference-api.alcf.anl.gov/resource_server/tara/api/v1"
            ),
        },
    }
}


def cluster_stub(name="minerva", groups=(), domains=()):
    return SimpleNamespace(
        cluster_name=name,
        allowed_globus_groups=list(groups),
        allowed_domains=list(domains),
        frameworks=["api"],
        openai_endpoints=["chat/completions", "responses", "messages"],
        check_permission=Mock(
            side_effect=lambda user: check_permission(user, list(groups), list(domains))
        ),
        check_maintenance=AsyncMock(return_value=SimpleNamespace(raise_if_down=Mock())),
    )


class ModelRetirementTests(ResourceServerTestCase):
    def setUp(self):
        super().setUp()
        self.enterContext(
            patch(
                "resource_server_async.retirements.Cluster.objects.aget",
                new=AsyncMock(return_value=cluster_stub()),
            )
        )

    async def test_retired_all_apis_and_stream_modes_without_endpoint_identity(self):
        cluster = cluster_stub()
        with (
            patch.object(
                BaseCluster, "load_adapter", new=AsyncMock(return_value=cluster)
            ) as load_cluster,
            patch.object(BaseEndpoint, "load_adapter", new=AsyncMock()) as load,
        ):
            for api in ("chat/completions", "responses", "messages"):
                for stream in (False, True):
                    with self.subTest(api=api, stream=stream):
                        body = {
                            "model": "inkling-bf16",
                            "stream": stream,
                            "max_tokens": 32,
                        }
                        body.update(
                            {"input": "hi"}
                            if api == "responses"
                            else {"messages": [{"role": "user", "content": "hi"}]}
                        )
                        response = await CLIENT.post(
                            f"/minerva/api/v1/{api}",
                            data=json.dumps(body),
                            headers=HEADERS,
                            **KWARGS,
                        )
                        self.assertEqual(response.status_code, 410)
                        self.assertEqual(get_response_json(response), EXPECTED)
                        self.assertIn(
                            "application/json", response.headers["Content-Type"]
                        )
                        self.assertNotIn("Location", response.headers)
            load.assert_not_awaited()
            load_cluster.assert_not_awaited()
            cluster.check_maintenance.assert_not_awaited()

    async def test_retained_policy_blocks_authenticated_streaming_requests(self):
        denied = replace(MODEL_RETIREMENTS[KEY], allowed_groups=["denied-group"])
        with patch.dict(MODEL_RETIREMENTS, {KEY: denied}):
            for api in ("chat/completions", "responses", "messages"):
                body = {
                    "model": "inkling-bf16",
                    "stream": True,
                    "max_tokens": 32,
                    "input": "hi",
                    "messages": [{"role": "user", "content": "hi"}],
                }
                response = await CLIENT.post(
                    f"/minerva/api/v1/{api}",
                    data=json.dumps(body),
                    headers=HEADERS,
                    **KWARGS,
                )
                self.assertEqual(response.status_code, 401)
                self.assertNotIn("replacement_cluster", response.content.decode())

    async def test_retirement_does_not_bypass_global_auth(self):
        for api in ("chat/completions", "responses", "messages"):
            for token in (None, INVALID_TOKEN, EXPIRED_TOKEN):
                with self.subTest(api=api, token=token):
                    headers = mock_utils.get_mock_headers(access_token=token or "")
                    response = await CLIENT.post(
                        f"/minerva/api/v1/{api}",
                        data=json.dumps(
                            {
                                "model": "inkling-bf16",
                                "input": "hi",
                                "messages": [{"role": "user", "content": "hi"}],
                                "max_tokens": 32,
                                "stream": True,
                            }
                        ),
                        headers=headers,
                        **KWARGS,
                    )
                    self.assertEqual(response.status_code, 401)
                    self.assertNotIn("replacement_cluster", response.content.decode())

    async def test_tara_and_unrelated_models_keep_normal_endpoint_resolution(self):
        for cluster_name, model in (
            ("tara", "inkling-bf16"),
            ("minerva", "muse-spark-bf16"),
        ):
            with (
                self.subTest(cluster=cluster_name, model=model),
                patch.object(
                    BaseCluster,
                    "load_adapter",
                    new=AsyncMock(return_value=cluster_stub(cluster_name)),
                ),
                patch.object(
                    BaseEndpoint,
                    "load_adapter",
                    new=AsyncMock(side_effect=EndpointNotFound("normal lookup")),
                ) as load,
            ):
                response = await CLIENT.post(
                    f"/{cluster_name}/api/v1/chat/completions",
                    data=json.dumps(
                        {
                            "model": model,
                            "messages": [{"role": "user", "content": "hi"}],
                        }
                    ),
                    headers=HEADERS,
                    **KWARGS,
                )
                self.assertEqual(response.status_code, 404)
                self.assertEqual(
                    get_response_json(response)["error"]["code"], "endpoint_not_found"
                )
                load.assert_awaited_once_with(cluster_name, "api", model)

    async def test_tara_requests_still_dispatch_in_all_apis_and_stream_modes(self):
        endpoint = Mock(spec=DirectAPIEndpoint)
        endpoint.model = "inkling-bf16"
        endpoint.endpoint_slug = "tara-api-inkling-bf16"
        endpoint.check_token_rate_limit.return_value = SimpleNamespace(allow=True)
        for api in ("chat/completions", "responses", "messages"):
            for stream in (False, True):
                with self.subTest(api=api, stream=stream):
                    endpoint.submit_task = AsyncMock(
                        return_value=SubmitTaskResult(
                            result={"tara": True}, task_id="task-tara"
                        )
                    )
                    endpoint.submit_streaming_task = AsyncMock(
                        return_value=SubmitStreamingTaskResponse(
                            response=StreamingHttpResponse(
                                [], content_type="text/event-stream"
                            ),
                            task_id="task-tara",
                        )
                    )
                    body = {"model": "inkling-bf16", "stream": stream, "max_tokens": 32}
                    body.update(
                        {"input": "hi"}
                        if api == "responses"
                        else {"messages": [{"role": "user", "content": "hi"}]}
                    )
                    with (
                        patch.object(
                            BaseCluster,
                            "load_adapter",
                            new=AsyncMock(return_value=cluster_stub("tara")),
                        ),
                        patch.object(
                            BaseEndpoint,
                            "load_adapter",
                            new=AsyncMock(return_value=endpoint),
                        ),
                    ):
                        response = await CLIENT.post(
                            f"/tara/api/v1/{api}",
                            data=json.dumps(body),
                            headers=HEADERS,
                            **KWARGS,
                        )
                    self.assertEqual(response.status_code, 200)
                    dispatch = (
                        endpoint.submit_streaming_task
                        if stream
                        else endpoint.submit_task
                    )
                    dispatch.assert_awaited_once()
                    endpoint.check_permission.assert_called()

    async def test_retained_model_and_live_cluster_policies_apply(self):
        user = request_context().user
        for entry, cluster in (
            (
                replace(MODEL_RETIREMENTS[KEY], allowed_groups=["denied"]),
                cluster_stub(),
            ),
            (
                replace(MODEL_RETIREMENTS[KEY], allowed_domains=["denied.test"]),
                cluster_stub(),
            ),
            (MODEL_RETIREMENTS[KEY], cluster_stub(groups=["denied"])),
            (MODEL_RETIREMENTS[KEY], cluster_stub(domains=["denied.test"])),
        ):
            with (
                patch.dict(MODEL_RETIREMENTS, {KEY: entry}),
                patch(
                    "resource_server_async.retirements.Cluster.objects.aget",
                    new=AsyncMock(return_value=cluster),
                ),
                self.assertRaises(Unauthorized),
            ):
                await raise_if_retired("minerva", KEY[1], user)

    async def test_additional_retirement_is_data_not_model_specific_dispatch(self):
        entry = replace(
            MODEL_RETIREMENTS[KEY],
            message="Old model retired.",
            replacement_model="replacement",
        )
        with patch.dict(MODEL_RETIREMENTS, {("minerva", "old-model"): entry}):
            from resource_server_async.retirements import ModelRetired

            with self.assertRaises(ModelRetired) as caught:
                await raise_if_retired("minerva", "old-model", request_context().user)
            self.assertEqual(caught.exception.info["replacement_model"], "replacement")


class RetirementDatabaseTests(ResourceServerTestCase):
    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()
        Cluster.objects.create(
            cluster_name="minerva",
            cluster_adapter="resource_server_async.clusters.minerva.MinervaCluster",
            frameworks=["api"],
            openai_endpoints=["chat/completions", "responses", "messages"],
            # Deliberately no usable backend configuration: guidance must only
            # read the cluster ACL, not instantiate a TLS/backend adapter.
            config="{}",
        )
        Endpoint.objects.create(
            pk=65,
            cluster="minerva",
            framework="api",
            model="removed-pk-65",
            endpoint_slug="minerva-api-removed-pk-65",
            endpoint_adapter="resource_server_async.endpoints.minerva.MinervaEndpoint",
            allowed_globus_groups=["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"],
            config="{}",
        )

    async def test_real_database_tombstone_does_not_hide_retirement_guidance(self):
        self.assertFalse(
            await Endpoint.objects.filter(
                cluster="minerva", model="inkling-bf16"
            ).aexists()
        )
        for api in ("chat/completions", "responses", "messages"):
            for stream in (False, True):
                body = {
                    "model": "inkling-bf16",
                    "stream": stream,
                    "max_tokens": 32,
                    "input": "hi",
                    "messages": [{"role": "user", "content": "hi"}],
                }
                response = await CLIENT.post(
                    f"/minerva/api/v1/{api}",
                    data=json.dumps(body),
                    headers=HEADERS,
                    **KWARGS,
                )
                self.assertEqual(response.status_code, 410)
                self.assertEqual(get_response_json(response), EXPECTED)

    async def test_real_cluster_acl_remains_authoritative_after_tombstoning(self):
        await Cluster.objects.filter(cluster_name="minerva").aupdate(
            allowed_globus_groups=["denied-group"]
        )
        response = await CLIENT.post(
            "/minerva/api/v1/chat/completions",
            data=json.dumps(
                {
                    "model": "inkling-bf16",
                    "stream": True,
                    "messages": [{"role": "user", "content": "hi"}],
                }
            ),
            headers=HEADERS,
            **KWARGS,
        )
        self.assertEqual(response.status_code, 401)
        self.assertNotIn("replacement_cluster", response.content.decode())
