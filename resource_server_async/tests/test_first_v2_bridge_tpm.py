"""Bridge targets copy V2's default per-user TPM without an aggregate cap."""

import ast
import json
from copy import deepcopy
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

DEFAULT_POLICY = {
    "default": {
        "tpm": 100000,
        "burst_tokens": 200000,
        "rpm": 120,
        "burst_requests": 10,
        "max_user_concurrency": 8,
    },
    "overrides": [],
}


class BridgeTPMTests(TestCase):
    def config(self, policy: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "version": 17,
            "models": [
                {
                    "name": "restricted-model",
                    "allowed_groups": ["restricted-group"],
                    "allowed_domains": ["anl.gov"],
                    "usage_limits": deepcopy(
                        DEFAULT_POLICY if policy is None else policy
                    ),
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
            ],
        }

    def desired(
        self,
        policy: dict[str, Any] | None = None,
        settings: BridgeSettings | None = None,
    ) -> list[DesiredEndpoint]:
        return desired_endpoints(
            RouterConfig.model_validate(self.config(policy)),
            settings or BridgeSettings(),
        )

    def reconcile(
        self, desired: list[DesiredEndpoint], rows: dict[str, dict[str, Any]]
    ) -> Mock:
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

        def filtered(**filters: Any) -> Mock:
            selected = [
                slug
                for slug, row in rows.items()
                if row.get("endpoint_adapter") == filters["endpoint_adapter"]
                and slug in filters.get("endpoint_slug__in", rows)
            ]
            queryset = Mock()
            queryset.values_list.return_value = selected

            def delete() -> tuple[int, dict[str, int]]:
                for slug in selected:
                    del rows[slug]
                return len(selected), {}

            queryset.delete.side_effect = delete
            return queryset

        # Exercise real reconciliation defaults and adapter-scoped deletion,
        # without opening a database or Redis connection.
        manager = Mock()
        manager.filter.side_effect = filtered
        manager.update_or_create.side_effect = upsert
        with patch.object(Endpoint, "objects", manager):
            Command()._reconcile_endpoints(desired)
        return manager

    def test_every_cluster_copies_the_default_and_ignores_user_and_group_overrides(
        self,
    ) -> None:
        for cluster in ("tara", "other"):
            settings = BridgeSettings()
            settings.prefix_map = {"tara/": (cluster, "api")}
            for tpm in (0, 64000, 2**31 - 1):
                with self.subTest(cluster=cluster, tpm=tpm):
                    policy = deepcopy(DEFAULT_POLICY)
                    policy["default"]["tpm"] = tpm
                    policy["overrides"] = [
                        {"kind": "user_id", "id": "special-user", "tpm": 200000},
                        {"kind": "group_id", "id": "restricted-group", "tpm": 0},
                    ]
                    desired = self.desired(policy, settings)
                    self.assertEqual(desired[0].tpm_user, tpm)
                    rows: dict[str, dict[str, Any]] = {}
                    self.reconcile(desired, rows)
                    row = rows[desired[0].endpoint_slug]
                    self.assertEqual((row["tpm_model"], row["tpm_user"]), (0, tpm))

    def test_mapping_preserves_access_and_backend_configuration(self) -> None:
        desired = self.desired()[0]
        self.assertNotIn("usage_limits", desired.config)
        self.assertEqual(desired.allowed_globus_groups, ["restricted-group"])
        self.assertEqual(desired.allowed_domains, ["anl.gov"])
        self.assertEqual(desired.config["model_urls"], ["https://pilot.example/model"])
        self.assertEqual(desired.config["backend_model_name"], "backend-model")
        rows: dict[str, dict[str, Any]] = {}
        self.reconcile([desired], rows)
        self.assertEqual(
            ast.literal_eval(rows[desired.endpoint_slug]["config"]), desired.config
        )

    def test_ignored_override_payloads_do_not_block_reconciliation(self) -> None:
        for overrides in (
            None,
            "unrelated-schema",
            [{"kind": "future-kind", "tpm": -1}],
        ):
            with self.subTest(overrides=overrides):
                policy = deepcopy(DEFAULT_POLICY)
                policy["overrides"] = overrides
                rows: dict[str, dict[str, Any]] = {}
                desired = self.desired(policy)
                self.reconcile(desired, rows)
                row = rows[desired[0].endpoint_slug]
                self.assertEqual((row["tpm_model"], row["tpm_user"]), (0, 100000))

    def test_creation_updates_and_recreation_follow_the_current_policy(self) -> None:
        for cluster in ("tara", "other"):
            settings = BridgeSettings()
            settings.prefix_map = {"tara/": (cluster, "api")}
            rows: dict[str, dict[str, Any]] = {}
            for tpm, recreate in ((0, False), (64000, False), (0, True), (32000, True)):
                with self.subTest(cluster=cluster, tpm=tpm, recreate=recreate):
                    if recreate:
                        rows.clear()  # No healthy backends; later the bridge recreates the row.
                    policy = deepcopy(DEFAULT_POLICY)
                    policy["default"]["tpm"] = tpm
                    desired = self.desired(policy, settings)
                    self.reconcile(desired, rows)
                    row = rows[desired[0].endpoint_slug]
                    self.assertEqual(
                        (row["tpm_model"], row["tpm_user"]),
                        (0, tpm),
                    )
                    self.assertEqual(row["endpoint_adapter"], ENDPOINT_ADAPTER)

    def test_unrelated_legacy_rows_are_preserved_while_stale_bridge_rows_are_removed(
        self,
    ) -> None:
        legacy = {
            "endpoint_adapter": "resource_server_async.endpoints.metis.MetisEndpoint",
            "tpm_model": 123456,
            "tpm_user": 45678,
            "config": "legacy-config",
        }
        rows = {
            "legacy-model": deepcopy(legacy),
            "stale-bridge-model": {
                "endpoint_adapter": ENDPOINT_ADAPTER,
                "tpm_model": 99,
                "tpm_user": 33,
            },
        }
        desired = self.desired()
        manager = self.reconcile(desired, rows)
        self.assertEqual(rows["legacy-model"], legacy)
        self.assertNotIn("stale-bridge-model", rows)
        self.assertEqual(set(rows), {"legacy-model", desired[0].endpoint_slug})
        self.assertEqual(manager.update_or_create.call_count, 1)

    def test_malformed_policy_aborts_tick_before_any_database_reconciliation(
        self,
    ) -> None:
        malformed = [
            None,
            {},
            {"default": {}},
            {"default": {"tpm": -1}},
            {"default": {"tpm": True}},
            {"default": {"tpm": "0"}},
            {"default": {"tpm": 0.0}},
            {"default": {"tpm": 2**31}},
        ]
        for policy in malformed:
            with self.subTest(policy=policy):
                config = self.config()
                invalid = deepcopy(config["models"][0])
                invalid["name"] = "second-model"
                if policy is None:
                    invalid.pop("usage_limits")
                else:
                    invalid["usage_limits"] = policy
                config["models"].append(invalid)
                redis = Mock()
                redis.get.return_value = json.dumps(config)
                command = Command()
                with (
                    patch(
                        "resource_server_async.management.commands.first_v2_bridge.get_bridge_redis_client",
                        return_value=redis,
                    ),
                    patch.object(command, "_reconcile_endpoints") as reconcile,
                ):
                    with self.assertRaises(ValueError):
                        command._tick(BridgeSettings())
                    reconcile.assert_not_called()

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
