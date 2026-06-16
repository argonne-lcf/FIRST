from contextvars import ContextVar
from dataclasses import dataclass

from first_common.schema.auth import UserAuthEvent
from first_common.schema.structured_logs import AccessLog, RequestLog


@dataclass
class RequestContext:
    access_log: AccessLog
    user: UserAuthEvent | None = None
    request_log: RequestLog | None = None


_request_context: ContextVar[RequestContext] = ContextVar("_request_context")


def get_request_context() -> RequestContext:
    """
    Return the RequestContext value set for the current http request.

    Raises LookupError if called outside of a request span wrapped by the
    AccessLogMiddleware.
    """
    return _request_context.get()
