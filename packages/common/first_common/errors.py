from http import HTTPStatus
from typing import Any


class FirstError(Exception):
    """
    Base class for all errors.

    Instead of returning error strings, raise the appropriate
    `FirstError` subclass.

    Unhandled FirstErrors in the apiserver automatically get logged and return a
    nice response to the user via `handle_uncaught_error` on the FastAPI app.

    Therefore, callers only need to catch exceptions to do something other than
    the generic log/return error repsonse flow.
    """

    status_code: HTTPStatus = HTTPStatus.INTERNAL_SERVER_ERROR
    code: str = "internal_error"

    def __init__(
        self,
        *args: Any,
        status_code: HTTPStatus | int | None = None,
        info: dict[str, Any] | None = None,
        retry_after_sec: int | None = None,
    ):
        if status_code is not None:
            self.status_code = HTTPStatus(status_code)
        self.info = info or {}
        self.retry_after_sec = retry_after_sec
        super().__init__(*args)


class NotFound(FirstError):
    status_code = HTTPStatus.NOT_FOUND
    code: str = "not_found"


class NotImplemented(FirstError):
    status_code = HTTPStatus.NOT_IMPLEMENTED
    code: str = "not_implemented"


class TooManyRequests(FirstError):
    status_code = HTTPStatus.TOO_MANY_REQUESTS
    code: str = "too_many_requests"


class ServiceUnavailable(FirstError):
    status_code = HTTPStatus.SERVICE_UNAVAILABLE
    code: str = "service_unavailable"


class InvalidSpecError(FirstError):
    status_code = HTTPStatus.BAD_REQUEST
    code: str = "resource_spec_invalid"


class ClusterStatusCheckError(FirstError): ...


class HealthCheckError(FirstError): ...


class SpecApplyError(FirstError):
    status_code = HTTPStatus.BAD_REQUEST
    code: str = "failed_to_apply_resource_spec"


class Unauthorized(FirstError):
    status_code = HTTPStatus.UNAUTHORIZED
    code: str = "unauthorized"


class AccessDenied(FirstError):
    status_code = HTTPStatus.FORBIDDEN
    code: str = "access_denied"


class BadPilotRequest(FirstError):
    status_code = HTTPStatus.BAD_REQUEST
    code: str = "bad_pilot_request"


class ReplicaAlreadyPlaced(FirstError):
    status_code = HTTPStatus.CONFLICT
    code: str = "replica_already_placed"


class ReplicaStartError(FirstError):
    status_code = HTTPStatus.BAD_REQUEST
    code: str = "replica_start_error"


class StatusCASFailed(FirstError):
    """Raised when CAS keeps losing the race past max_cas_attempts."""

    code: str = "status_cas_failed"


class TaskPending(FirstError):
    """
    202 ACCEPTED is widely used for async http clients polling on a task ID.
    """

    status_code = HTTPStatus.ACCEPTED
    code = "task_accepted_and_pending"

    def __init__(self, task_id: str, *args: str, retry_after_sec: int = 2):
        self.task_id = task_id
        super().__init__(*args, retry_after_sec=retry_after_sec)
