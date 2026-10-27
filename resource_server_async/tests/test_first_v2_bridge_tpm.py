"""Bridge TPM opt-in stays scoped and survives endpoint recreation."""

import ast
from typing import Any
from unittest import TestCase
from unittest.mock import Mock, patch

from resource_server_async.first_v2_bridge.mapping import (
    ENDPOINT_ADAPTER,
    DesiredEndpoint,
    desired_endpoints,
)
from resource_server_async.first_v2_bridge.router_config import RouterConfig
from resource_server_async.first_v2_bridge.settings import BridgeSettings
from resource_server_async.management.commands.first_v2_bridge import Command
from resource_server_async.models import Endpoint
from resource_server_async.rate_limiters import TokenRateLimiter


class BridgeTPMTests(TestCase):
    def settings(self, targets: str = "") -> BridgeSettings:
        with patch.dict("os.environ", {"FIRST_V2_UNLIMITED_TPM_TARGETS": targets}):
            return BridgeSettings()

    def desired(self, settings: BridgeSettings) -> list[DesiredEndpoint]:
        return desired_endpoints(
            RouterConfig.model_validate(
                {
                    "models": [
                        {
                            "name": "restricted-model",
                            "allowed_groups": ["restricted-group"],
                            "allowed_domains": ["anl.gov"],
                            "deployments": [
                                {
                                    "kind": "pilot",
                                    "name": "tara/model",
                                    "backends": [
                                        {
                                            "id": "replica-1",
                                            "model_url": "https://pilot.example/model",
                                            "backend_model_name": "backend-model",
                                        }
                                    ],
                                }
                            ],
                        }
                    ]
                }
            ),
            settings,
        )

    def test_default_does_not_opt_in_any_target(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            settings = BridgeSettings()
        self.assertEqual(settings.unlimited_tpm_targets, set())
        self.assertFalse(self.desired(settings)[0].unlimited_tpm)

    def test_opt_in_requires_exact_cluster_and_framework(self) -> None:
        for target in ("tara", "other/api", "tara/other", "tara/api-extra"):
            with self.subTest(target=target):
                self.assertFalse(self.desired(self.settings(target))[0].unlimited_tpm)
        self.assertTrue(self.desired(self.settings("tara/api"))[0].unlimited_tpm)

    def test_comma_separated_targets_ignore_whitespace_and_empty_entries(self) -> None:
        settings = self.settings(" tara/api, other/api, , ")
        self.assertEqual(settings.unlimited_tpm_targets, {"tara/api", "other/api"})
        self.assertTrue(self.desired(settings)[0].unlimited_tpm)

    def test_other_cluster_does_not_inherit_tara_opt_in(self) -> None:
        settings = self.settings("tara/api")
        settings.prefix_map = {"tara/": ("other", "api")}
        self.assertFalse(self.desired(settings)[0].unlimited_tpm)

    def test_mapping_preserves_access_and_backend_configuration(self) -> None:
        normal = self.desired(self.settings())[0]
        unlimited = self.desired(self.settings("tara/api"))[0]
        self.assertEqual(vars(normal) | {"unlimited_tpm": True}, vars(unlimited))
        self.assertEqual(unlimited.allowed_globus_groups, ["restricted-group"])
        self.assertEqual(unlimited.allowed_domains, ["anl.gov"])

    def reconcile(
        self, desired: list[DesiredEndpoint], rows: dict[str, dict[str, Any]]
    ) -> None:
        def upsert(
            *, endpoint_slug: str, defaults: dict[str, Any]
        ) -> tuple[dict[str, Any], bool]:
            created = endpoint_slug not in rows
            row = rows.setdefault(
                endpoint_slug,
                {
                    "tpm_model": Endpoint._meta.get_field("tpm_model").get_default(),
                    "tpm_user": Endpoint._meta.get_field("tpm_user").get_default(),
                },
            )
            row.update(defaults)
            return row, created

        # Exercise the command's real defaults; no DB or Redis connection.
        manager = Mock()
        manager.filter.return_value.values_list.return_value = list(rows)
        manager.update_or_create.side_effect = upsert
        with patch.object(Endpoint, "objects", manager):
            Command()._reconcile_endpoints(desired)

    def test_opted_in_creation_and_recreation_use_zero(self) -> None:
        desired = self.desired(self.settings("tara/api"))
        rows: dict[str, dict[str, Any]] = {}
        for _ in range(2):
            self.reconcile(desired, rows)
            row = rows[desired[0].endpoint_slug]
            self.assertEqual((row["tpm_model"], row["tpm_user"]), (0, 0))
            self.assertEqual(row["endpoint_adapter"], ENDPOINT_ADAPTER)
            self.assertEqual(row["allowed_globus_groups"], ["restricted-group"])
            self.assertEqual(row["allowed_domains"], ["anl.gov"])
            self.assertEqual(ast.literal_eval(row["config"]), desired[0].config)
            rows.clear()  # Last backend disappeared; next tick recreates the row.

    def test_opted_in_existing_manual_limits_are_cleared(self) -> None:
        desired = self.desired(self.settings("tara/api"))
        rows = {desired[0].endpoint_slug: {"tpm_model": 8388608, "tpm_user": 8388608}}
        self.reconcile(desired, rows)
        row = rows[desired[0].endpoint_slug]
        self.assertEqual((row["tpm_model"], row["tpm_user"]), (0, 0))

    def test_unconfigured_creation_retains_normal_database_defaults(self) -> None:
        desired = self.desired(self.settings())
        rows: dict[str, dict[str, Any]] = {}
        self.reconcile(desired, rows)
        row = rows[desired[0].endpoint_slug]
        self.assertEqual((row["tpm_model"], row["tpm_user"]), (100000, 60000))

    def test_unconfigured_existing_manual_limits_are_preserved(self) -> None:
        desired = self.desired(self.settings())
        rows = {desired[0].endpoint_slug: {"tpm_model": 123456, "tpm_user": 45678}}
        self.reconcile(desired, rows)
        row = rows[desired[0].endpoint_slug]
        self.assertEqual((row["tpm_model"], row["tpm_user"]), (123456, 45678))

    def test_zero_limits_allow_large_recorded_usage_but_still_record(self) -> None:
        redis = Mock()
        redis.get.return_value = b"999999999"
        limiter = TokenRateLimiter(redis, "tara:api:model", tpm_model=0, tpm_user=0)
        result = limiter.check("user")
        self.assertTrue(result.allow)
        self.assertEqual(
            (result.usage_model, result.usage_user), (999999999, 999999999)
        )
        limiter.record("user", 123)
        pipe = redis.pipeline.return_value
        redis.pipeline.assert_called_once_with(transaction=True)
        pipe.incrby.assert_any_call("tpm:tara:api:model", 123)
        pipe.incrby.assert_any_call("tpm:tara:api:model:user", 123)
        pipe.execute.assert_called_once_with()

    def test_nonzero_limits_still_enforce_each_independent_boundary(self) -> None:
        for model_limit, user_limit in ((100000, 0), (0, 60000)):
            with self.subTest(model_limit=model_limit, user_limit=user_limit):
                redis = Mock()
                redis.get.return_value = b"999999999"
                limiter = TokenRateLimiter(
                    redis, "other:api:model", tpm_model=model_limit, tpm_user=user_limit
                )
                self.assertFalse(limiter.check("user").allow)
