"""
Fixtures for exercising the alcf-ai SDK/CLI against a fake gateway served by
``httpx.MockTransport`` (no network, no Globus).
"""

import json
from datetime import datetime, timezone
from typing import Any, Callable

import httpx
import pytest

from alcf_ai import InferenceClient

Handler = Callable[[httpx.Request], httpx.Response]

NOW = datetime(2026, 9, 11, 12, 0, 0, tzinfo=timezone.utc).isoformat()


def meta(kind: str, name: str, uid: int) -> dict[str, Any]:
    return {"kind": kind, "name": name, "uid": uid, "created_at": NOW}


def pilot_deployment(
    name: str,
    model: str,
    *,
    uid: int = 10,
    state: str = "healthy",
    desired: int = 1,
    last_startup_sec: float | None = 120.0,
) -> dict[str, Any]:
    return {
        **meta("PilotDeployment", name, uid),
        "cluster_name": name.split("/")[0],
        "model_name": model,
        "router_params": {},
        "desired_replicas": desired,
        "state": state,
        "consecutive_launch_failures": 0,
        "last_startup_sec": last_startup_sec,
    }


def static_deployment(
    name: str, model: str, *, uid: int = 20, health: str = "healthy"
) -> dict[str, Any]:
    return {
        **meta("StaticDeployment", name, uid),
        "cluster_name": name.split("/")[0],
        "model_name": model,
        "api_url": "https://upstream.example/v1",
        "api_key": None,
        "upstream_model_name": model.split("/")[-1],
        "router_params": {},
        "health_check": {"url": ""},
        "health": health,
        "runtime": {"inflight": 2, "cooldown_errors": 0},
    }


def model(
    name: str,
    endpoints: list[str],
    *,
    uid: int = 1,
    max_model_len: int | None = 65536,
    aliases: list[str] | None = None,
    display_name: str | None = None,
    capabilities: dict[str, Any] | None = None,
    pilot_deployments: list[dict[str, Any]] | None = None,
    static_deployments: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        **meta("Model", name, uid),
        "access_group_name": "public",
        "supported_endpoints": endpoints,
        "aliases": aliases or [],
        "max_model_len": max_model_len,
        "display_name": display_name,
        "capabilities": capabilities or {},
        "pilot_deployments": pilot_deployments or [],
        "static_deployments": static_deployments or [],
        "runtime": {"total_inflight": 3, "capacity_rejects_total": 0},
    }


CATALOG_MODELS: list[dict[str, Any]] = [
    model(
        "openai/gpt-oss-120b",
        ["chat/completions", "responses", "messages"],
        uid=1,
        aliases=["gpt-oss-120b"],
        display_name="GPT-OSS 120B",
        capabilities={
            "tool_calling": {"supported": True},
            "reasoning": {
                "supported": True,
                "default_effort": "medium",
                "effort_levels": ["low", "medium", "high"],
                "separate_output": True,
            },
        },
        pilot_deployments=[
            pilot_deployment("sophia/openai/gpt-oss-120b", "openai/gpt-oss-120b")
        ],
    ),
    model(
        "google/gemma-4-31B-it",
        ["chat/completions"],
        uid=2,
        max_model_len=131072,
        static_deployments=[
            static_deployment("metis/google/gemma-4-31B-it", "google/gemma-4-31B-it")
        ],
    ),
    model(
        "facebook/sam3",
        ["process"],
        uid=3,
        max_model_len=None,
        pilot_deployments=[
            pilot_deployment(
                "sophia/facebook/sam3",
                "facebook/sam3",
                uid=11,
                state="offline",
                desired=0,
                last_startup_sec=None,
            )
        ],
    ),
]


def error_body(code: str, message: str, info: dict[str, Any] | None = None) -> bytes:
    return json.dumps(
        {"error": {"code": code, "message": message, "info": info or {}}}
    ).encode()


def cold_start_response(eta: int | None, typical: int | None = 120) -> httpx.Response:
    """The gateway's cold-start 503, as built by ``cold_start_error``."""
    message = (
        f"Model x is starting; ~{eta}s until ready."
        if eta
        else "Model x is not running; a replica will be started."
    )
    return httpx.Response(
        503,
        content=error_body(
            "service_unavailable",
            message + " Retry in 5 seconds.",
            {"startup_eta_sec": eta, "typical_startup_sec": typical},
        ),
        headers={"Retry-After": "5", "content-type": "application/json"},
    )


def sse(*events: dict[str, Any] | str, done: bool = False) -> bytes:
    """Encode events as an SSE body; strings are ``event: <name>`` markers."""
    out: list[str] = []
    pending_event: str | None = None
    for item in events:
        if isinstance(item, str):
            pending_event = item
            continue
        if pending_event:
            out.append(f"event: {pending_event}\n")
            pending_event = None
        out.append(f"data: {json.dumps(item)}\n\n")
    if done:
        out.append("data: [DONE]\n\n")
    return "".join(out).encode()


class FakeGateway:
    """
    A minimal fake of the gateway's catalog + inference routes.  Tests override
    ``routes`` (keyed by ``"METHOD /path"``) to script specific responses; a
    handler may be a Response or a callable taking the request.
    """

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.routes: dict[str, httpx.Response | Handler] = {
            "GET /catalog/v1/models": httpx.Response(200, json=CATALOG_MODELS),
            "GET /catalog/v1/deployments/pilot": httpx.Response(
                200,
                json=[d for m in CATALOG_MODELS for d in m["pilot_deployments"]],
            ),
            "GET /catalog/v1/deployments/static": httpx.Response(
                200,
                json=[d for m in CATALOG_MODELS for d in m["static_deployments"]],
            ),
            "GET /whoami": httpx.Response(
                200,
                json={
                    "id": "uuid-1",
                    "name": "Test User",
                    "username": "user@anl.gov",
                    "user_group_uuids": ["g1"],
                    "idp_id": "idp",
                    "idp_name": "Argonne",
                    "auth_service": "globus",
                },
            ),
        }

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = f"{request.method} {request.url.path}"
        handler = self.routes.get(key)
        if handler is None:
            return httpx.Response(
                404, content=error_body("not_found", f"No route {key}")
            )
        return handler(request) if callable(handler) else handler

    @property
    def last_json(self) -> Any:
        return json.loads(self.requests[-1].content)


@pytest.fixture
def gateway() -> FakeGateway:
    return FakeGateway()


@pytest.fixture
def sdk_client(gateway: FakeGateway) -> InferenceClient:
    return InferenceClient(
        base_url="http://gateway.test",
        transport=httpx.MockTransport(gateway),
        auth=None,
    )
