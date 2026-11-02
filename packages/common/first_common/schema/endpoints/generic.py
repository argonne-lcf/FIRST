from pydantic import Field

from first_common.schema.endpoints.base import BasePayload

# Authenticated caller, set by the gateway on requests to task backends. Replicas
# are only reachable through the gateway, so backends may trust it, e.g. to confine
# filesystem paths to the caller's staging area.
TASK_USER_ID_HEADER = "X-First-User-Id"


class GenericTaskPayload(BasePayload, dynamic_endpoint=True):
    """
    A non-LLM task proxied verbatim to the backend's `/v1/{endpoint}`.

    The caller supplies the target `endpoint` in the body alongside `model` and
    arbitrary extra fields (passed through untouched).  These requests are always
    unary and estimated/actual tokens are always 0.  The caller's identity is sent
    in the `TASK_USER_ID_HEADER` request header.
    """

    endpoint: str = Field(..., min_length=1)  # type: ignore[misc]
