import logging
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import monotonic, sleep
from typing import Literal

from dotenv import load_dotenv
from httpx import Client, Response, Timeout

from alcf_ai._http import raise_for_status
from alcf_ai.auth import GATEWAY_CLIENT_ID, build_user_app
from alcf_ai.client import AutoGlobusAuth
from first_common.errors import FirstError
from first_common.schema.quarantine_read import (
    NodeQuarantineSummary,
    QuarantineOperatorAction,
    QuarantineScreenReceipt,
)
from first_common.schema.resources import (
    ConfigVersion,
    ConfigVersionSummary,
    ResourceChangePlan,
    ResourceManifest,
)
from first_common.schema.resources.read import (
    PilotDeploymentSummary,
)

logger = logging.getLogger(__name__)
load_dotenv()
DEFAULT_BASE_URL = os.environ.get("admin_base_url", "http://localhost:9100")


def _screen_wait_authorization() -> str:
    """Use existing credentials without the SDK's interactive login recovery."""
    try:
        app = build_user_app()
        app.config = replace(app.config, token_validation_error_handler=None)
        authorization = app.get_authorizer(GATEWAY_CLIENT_ID).get_authorization_header()
        if not isinstance(authorization, str) or not authorization.strip():
            raise ValueError("No usable authorization header")
        return authorization
    except Exception as error:
        raise FirstError(
            "Screen authentication failed; run alcf-ai auth login separately",
            status_code=401,
        ) from error


class AdminClient(Client):
    def __init__(self, base_url: str | None = None) -> None:
        if base_url is None:
            base_url = DEFAULT_BASE_URL

        super().__init__(
            auth=AutoGlobusAuth(),
            base_url=base_url,
            timeout=Timeout(10.0),
        )

    def list_node_quarantines(self) -> list[NodeQuarantineSummary]:
        resp = self.get("/control/v1/node-quarantines")
        raise_for_status(resp)
        return [NodeQuarantineSummary.model_validate(row) for row in resp.json()]

    def node_quarantine_action(
        self,
        uid: int,
        action: Literal["screen", "release", "reestablish"],
        reference: str,
    ) -> None:
        self._request_node_quarantine_action(uid, action, reference)

    def _request_node_quarantine_action(
        self,
        uid: int,
        action: Literal["screen", "release", "reestablish"],
        reference: str,
        *,
        authorization: str | None = None,
    ) -> Response:
        body = QuarantineOperatorAction(reference=reference)
        request = self.build_request(
            "POST",
            f"/control/v1/node-quarantines/{uid}/{action}",
            json=body.model_dump(mode="json"),
            headers={"Authorization": authorization} if authorization else None,
        )
        resp = self.send(request, auth=None if authorization is not None else self.auth)
        raise_for_status(resp)
        return resp

    def screen_node_quarantine(
        self,
        uid: int,
        reference: str,
        *,
        wait: bool = False,
        timeout: int = 180,
    ) -> QuarantineScreenReceipt:
        """Request a screen; optionally poll only the accepted request identity."""
        if type(timeout) is not int or not 1 <= timeout <= 600:
            raise ValueError("screen timeout must be between 1 and 600 seconds")
        authorization = (
            _screen_wait_authorization()
            if wait and isinstance(self.auth, AutoGlobusAuth)
            else None
        )
        resp = self._request_node_quarantine_action(
            uid, "screen", reference, authorization=authorization
        )
        try:
            receipt = QuarantineScreenReceipt.model_validate(resp.json())
        except ValueError as error:
            raise FirstError(
                "Screen acknowledgement identity is invalid", status_code=502
            ) from error
        if receipt.uid != uid:
            raise FirstError("Screen acknowledgement UID differs", status_code=502)
        if wait:
            authorization = resp.request.headers.get("Authorization")
            if not authorization:
                raise FirstError(
                    "Screen acknowledgement lacks authenticated request context",
                    status_code=401,
                )
            self._wait_node_quarantine_screen(
                receipt, reference, timeout, authorization
            )
        return receipt

    def _wait_node_quarantine_screen(
        self,
        receipt: QuarantineScreenReceipt,
        reference: str,
        timeout: int,
        authorization: str,
    ) -> NodeQuarantineSummary:
        deadline = monotonic() + timeout
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise FirstError("Screen wait timed out", status_code=504)
            # Reuse the accepted request's credentials. Do not launch interactive
            # authentication recovery inside the bounded polling loop. Expired
            # credentials/lookup failures terminate this wait, never retry it.
            resp = self.get(
                "/control/v1/node-quarantines",
                auth=None,
                headers={"Authorization": authorization},
                timeout=min(10.0, remaining),
            )
            raise_for_status(resp)
            try:
                rows = [
                    NodeQuarantineSummary.model_validate(row) for row in resp.json()
                ]
            except (ValueError, TypeError) as error:
                raise FirstError(
                    "Screen lookup response is invalid", status_code=502
                ) from error
            if monotonic() >= deadline:
                raise FirstError("Screen wait timed out", status_code=504)
            matches = [row for row in rows if row.uid == receipt.uid]
            if len(matches) != 1:
                raise FirstError(
                    "Screen lookup UID is missing or ambiguous", status_code=502
                )
            row = matches[0]
            if (
                row.operation != receipt.operation
                or row.screen_nonce != receipt.nonce
                or row.screen_requested_at != receipt.requested_at
                or row.repair_reference != reference
            ):
                raise FirstError("Screen request was superseded", status_code=409)
            current = datetime.now(timezone.utc)
            if (
                row.state != "isolated"
                or row.expires_at is None
                or row.expires_at.tzinfo is None
                or row.expires_at <= current
            ):
                raise FirstError(
                    "Screen quarantine isolation is no longer valid", status_code=409
                )
            if row.screen_outcome == "FAIL":
                raise FirstError("Exact screening request failed", status_code=409)
            if row.screen_outcome == "PASS":
                if (
                    row.screened_at is None
                    or row.screened_at.tzinfo is None
                    or row.screened_at < receipt.requested_at
                    or row.screened_at < current - timedelta(seconds=120)
                    or row.screened_at > current + timedelta(seconds=5)
                    or row.screen_expires_at is None
                    or row.screen_expires_at.tzinfo is None
                    or row.screen_expires_at <= current
                    or row.screen_expires_at > current + timedelta(seconds=125)
                ):
                    raise FirstError(
                        "Screen PASS evidence is stale or unverifiable",
                        status_code=409,
                    )
                if monotonic() >= deadline:
                    raise FirstError("Screen wait timed out", status_code=504)
                return row
            sleep(min(1.0, max(0.0, deadline - monotonic())))

    def plan(self, resources: list[ResourceManifest]) -> ResourceChangePlan:
        resp = self.post(
            "/control/v1/plan",
            json={"resources": [r.model_dump(mode="json") for r in resources]},
        )
        raise_for_status(resp)
        return ResourceChangePlan.model_validate(resp.json())

    def apply(
        self, resources: list[ResourceManifest], approved_plan: ResourceChangePlan
    ) -> ConfigVersion | None:
        resp = self.post(
            "/control/v1/apply",
            json={
                "resources": [r.model_dump(mode="json") for r in resources],
                "approved_plan": approved_plan.model_dump(mode="json"),
            },
        )
        raise_for_status(resp)
        return ConfigVersion.model_validate(resp.json()) if resp.json() else None

    def list_config_versions(self) -> list[ConfigVersionSummary]:
        resp = self.get("/control/v1/config-versions")
        raise_for_status(resp)
        return [ConfigVersionSummary.model_validate(v) for v in resp.json()]

    def get_config_version(self, uid: int) -> ConfigVersion:
        resp = self.get(f"/control/v1/config-versions/{uid}")
        raise_for_status(resp)
        return ConfigVersion.model_validate(resp.json())

    def reconcile_reset(self, resource: str) -> None:
        resp = self.post(
            "/control/v1/reconcile-reset",
            json={"resource": resource},
        )
        raise_for_status(resp)

    def set_desired_pilot_deployment_replicas(
        self, name: str, num_replicas: int
    ) -> PilotDeploymentSummary:
        resp = self.put(
            f"/control/v1/deployments/pilot/{name}/desired-replicas",
            json={"num_replicas": num_replicas},
        )
        raise_for_status(resp)
        return PilotDeploymentSummary.model_validate(resp.json())
