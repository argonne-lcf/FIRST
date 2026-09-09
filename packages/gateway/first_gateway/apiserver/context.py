from contextvars import ContextVar

_request_id_ctx: ContextVar[str] = ContextVar("_request_id")


def get_request_id() -> str | None:
    """Return the correlation id for the current request, or None if unset.

    Set by ``ResponseLogMiddleware`` at the very start of each request and read
    by ``InferenceService`` and the logging ``RequestIdFilter``.
    """
    return _request_id_ctx.get(None)
