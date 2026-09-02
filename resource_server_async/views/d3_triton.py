import logging
import re

from django.http import HttpResponse, JsonResponse
from ninja import Router
from ninja.errors import HttpError

from ..clusters import BaseCluster
from ..endpoints import BaseEndpoint, GlobusComputeEndpoint
from ..errors import TaskPending
from ..schemas.auth import AuthedRequest
from ..schemas.endpoints import (
    SubmitTaskAsyncResponse,
)

PROTOBUF = "application/x-protobuf"
ACCEPTED_CONTENT_TYPES = {PROTOBUF, "application/octet-stream"}
ALLOWED_RPCS = frozenset(
    {
        "ServerLive",
        "ServerReady",
        "ServerMetadata",
        "ModelReady",
        "ModelMetadata",
        "ModelConfig",
        "ModelStatistics",
        "ModelInfer",
    }
)
MAX_RPC_BODY = 9_900_000
_WORKER_ERROR = re.compile(
    r"^TRITON_GRPC_ERROR (?P<code>[A-Z_]+): (?P<details>.*)$", re.S
)
RAW_BODY_OPENAPI = {
    "requestBody": {
        "required": True,
        "content": {PROTOBUF: {"schema": {"type": "string", "format": "binary"}}},
    }
}

router = Router()
log = logging.getLogger(__name__)


async def _load_endpoint(request: AuthedRequest) -> GlobusComputeEndpoint:
    cluster = await BaseCluster.load_adapter("sophia")
    (await cluster.check_maintenance()).raise_if_down()
    endpoint = await BaseEndpoint.load_adapter(
        cluster.cluster_name, "triton", "amsc-d3"
    )
    assert isinstance(endpoint, GlobusComputeEndpoint)
    endpoint.check_permission(request.auth)
    return endpoint


def _error_response(exc: Exception) -> JsonResponse:
    """Map a worker exception to the JSON error shape the sidecar understands."""
    text = str(exc)
    m = _WORKER_ERROR.match(text)
    if m:
        # Triton itself rejected the call (bad shape, unknown model, ...):
        return JsonResponse(
            {"grpc_code": m["code"], "details": m["details"]}, status=502
        )
    log.error("task failed outside Triton: %s", text)
    return JsonResponse({"grpc_code": "INTERNAL", "details": text[:500]}, status=500)


@router.post(
    "/sophia/triton/amsc-d3/rpc/{rpc}",
    response=SubmitTaskAsyncResponse,
    openapi_extra=RAW_BODY_OPENAPI,
)
async def d3_triton_rpc(request: AuthedRequest, rpc: str) -> SubmitTaskAsyncResponse:
    """Relay one Triton unary gRPC call.  The body is the serialized ``{rpc}Request`` protobuf."""
    if rpc not in ALLOWED_RPCS:
        raise HttpError(
            404, f"unsupported rpc {rpc!r}; allowed: {sorted(ALLOWED_RPCS)}"
        )

    if request.content_type not in ACCEPTED_CONTENT_TYPES:
        raise HttpError(415, f"expected {PROTOBUF}, got {request.content_type!r}")

    body: bytes = request.body
    if len(body) > MAX_RPC_BODY:
        raise HttpError(
            413,
            f"request is {len(body)} bytes; the relay accepts at most {MAX_RPC_BODY}. ",
        )
    if not body and rpc in {"ModelInfer", "ModelMetadata", "ModelConfig", "ModelReady"}:
        raise HttpError(400, f"{rpc} requires a non-empty request body")

    endpoint = await _load_endpoint(request)
    log.info(
        "endpoint_slug: %s - user: %s - rpc: %s - %d B",
        endpoint.endpoint_slug,
        request.auth.username,
        rpc,
        len(body),
    )

    return await endpoint.submit_task_async({"rpc": rpc, "request_bytes": body})


@router.get("/sophia/triton/amsc-d3/rpc/result/{task_id}")
async def d3_triton_rpc_result(
    request: AuthedRequest, task_id: str
) -> HttpResponse | JsonResponse:
    """Fetch a relayed RPC result as raw protobuf bytes."""
    endpoint = await _load_endpoint(request)

    try:
        result = await endpoint.get_task_result(task_id)
    except TaskPending:
        raise
    except Exception as exc:
        return _error_response(exc)

    if not isinstance(result.result, (bytes, bytearray, memoryview)):
        log.error(
            "task %s returned %s, expected bytes", task_id, type(result.result).__name__
        )
        return JsonResponse(
            {"grpc_code": "INTERNAL", "details": "worker returned a non-bytes result"},
            status=500,
        )

    return HttpResponse(bytes(result.result), content_type=PROTOBUF)
