from pydantic import Field

from first_common.schema.endpoints.base import BasePayload


class GenericTaskPayload(BasePayload, dynamic_endpoint=True):
    """
    A non-LLM task proxied verbatim to the backend's `/v1/{endpoint}`.

    The caller supplies the target `endpoint` in the body alongside `model` and
    arbitrary extra fields (passed through untouched).  These requests are always
    unary and estimated/actual tokens are always 0.
    """

    endpoint: str = Field(..., min_length=1)  # type: ignore[misc]
