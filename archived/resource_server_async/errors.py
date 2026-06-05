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


class BaseError(Exception):
    """
    Root of service error hierarchy
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


class ClusterNotFound(BaseError):
    status_code = HTTPStatus.NOT_FOUND
    code: str = "cluster_not_found"


class EndpointNotFound(BaseError):
    status_code = HTTPStatus.NOT_FOUND
    code: str = "endpoint_not_found"


class BatchNotFound(BaseError):
    status_code = HTTPStatus.NOT_FOUND
    code: str = "batch_not_found"


class Unauthorized(BaseError):
    status_code = HTTPStatus.UNAUTHORIZED
    code: str = "unauthorized"


class AccessDenied(BaseError):
    status_code = HTTPStatus.FORBIDDEN
    code: str = "access_denied"


class ClusterUnderMaintenance(BaseError):
    status_code = HTTPStatus.SERVICE_UNAVAILABLE
    code = "cluster_under_maintenance"


class GetJobsError(BaseError):
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    code = "failed_to_get_cluster_jobs"


class UnsupportedFramework(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "unsupported_framework"


class UnsupportedEndpoint(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "unsupported_endpoint"


class BatchUnavailable(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "batch_unavailable"


class QuotaExceeded(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "quota_exceeded"


class BatchOngoing(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "batch_ongoing"


class BatchFailed(BaseError):
    status_code = HTTPStatus.BAD_REQUEST
    code = "batch_failed"


class EndpointError(BaseError):
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    code = "internal_endpoint_error"


class TooManyRequests(BaseError):
    status_code = HTTPStatus.TOO_MANY_REQUESTS
    code = "too_many_requests"


class RequestTimeout(BaseError):
    status_code = HTTPStatus.REQUEST_TIMEOUT
    code = "request_timeout"
