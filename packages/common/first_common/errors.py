from http import HTTPStatus
from typing import Any


class TaskPending(Exception):
    """
    202 ACCEPTED is widely used for async http clients polling on a task ID.
    """

    status_code = HTTPStatus.ACCEPTED
    code = "task_accepted_and_pending"

    def __init__(self, task_id: str, *args: str, retry_after: int = 2):
        self.task_id = task_id
        self.retry_after = retry_after
        super().__init__(*args)


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
    ):
        if status_code is not None:
            self.status_code = HTTPStatus(status_code)
        self.info = info or {}
        super().__init__(*args)


class NotFound(FirstError):
    status_code = HTTPStatus.NOT_FOUND
    code: str = "not_found"


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
