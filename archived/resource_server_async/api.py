import logging
import uuid

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse
from ninja import NinjaAPI
from ninja.errors import HttpError
from ninja.security import HttpBearer
from ninja.throttling import AnonRateThrottle, AuthRateThrottle, BaseThrottle

from resource_server_async.auth import validate_access_token
from resource_server_async.schemas.structured_logs import UserPydantic

from .errors import BaseError, TaskPending
from .logging import get_request_context
from .views import router

logger = logging.getLogger(__name__)


# -------------------------------------
# ========== API declaration ==========
# -------------------------------------

# Ninja API
api = NinjaAPI(
    title="ALCF Inference Service", urls_namespace="resource_server_async_api"
)

# -------------------------------------
# ========== API rate limits ==========
# -------------------------------------

# Define rate limits
throttle: list[BaseThrottle] = [
    AnonRateThrottle("10/s"),  # Per anonymous user, if request.user is not defined
    AuthRateThrottle(
        f"{settings.RATE_LIMIT_PER_SEC_PER_USER}/s"
    ),  # Per user, as defined by the request.user object
]

# Apply limits to the API
if not settings.RUNNING_AUTOMATED_TEST_SUITE:
    api.throttle = throttle

# ---------------------------------------------
# ========== API authorization layer ==========
# ---------------------------------------------


# Global authorization check that applies to all API routes
class GlobalAuth(HttpBearer):
    # Django User class to populate request.user
    RequestLightWeightUser = get_user_model()

    # Custom error message if Authorization headers is missing
    async def __call__(self, request: HttpRequest) -> UserPydantic:
        auth = request.headers.get("Authorization")
        if not auth:
            raise HttpError(
                401,
                "Error: Missing ('Authorization': 'Bearer <your-access-token>') in request headers.",
            )
        return await self.authenticate(
            request, None
        )  # Request is the object being used by the validate_access_token function

    # Auth check
    async def authenticate(
        self, request: HttpRequest, token: str | None
    ) -> UserPydantic:
        # Introspect and validate the access token
        # Raises Unauthorized (HTTP 401) if authentication fails:
        atv_response = validate_access_token(request)

        ctx = get_request_context()

        # Add whether the access token got granted because of a special Globus Groups membership
        ctx.access_log.authorized_groups = atv_response.idp_group_overlap_str

        # Add user database object to the access log pydantic data
        ctx.user = atv_response.user

        # Add User object to request so that Ninja throttle can be applied per authenticated user (AuthRateThrottle)
        request.user = self.RequestLightWeightUser(
            id=atv_response.user.id,
            username=atv_response.user.username,
            is_superuser=False,
        )

        if cache.add(f"authed_user:{ctx.user.id}", "", timeout=120):
            ctx.user.emit()

        # Makes the user accessible through the request.auth attribute:
        return ctx.user


# Apply the authorization requirement to all routes
api.auth = [GlobalAuth()]


@api.exception_handler(BaseError)
def handle_app_error(request: HttpRequest, exc: BaseError) -> HttpResponse:
    return api.create_response(
        request,
        {"error": {"code": exc.code, "message": str(exc), "info": exc.info}},
        status=exc.status_code,
    )


@api.exception_handler(TaskPending)
def handle_pending(request: HttpRequest, exc: TaskPending) -> HttpResponse:
    response = api.create_response(
        request,
        {"status": exc.code, "task_id": exc.task_id},
        status=exc.status_code,
    )
    response["Retry-After"] = str(exc.retry_after)
    return response


@api.exception_handler(Exception)
def handle_uncaught_error(request: HttpRequest, exc: Exception) -> HttpResponse:
    error_id = uuid.uuid4().hex
    logger.exception(
        f"Uncaught Exception in API View {request.path!r}",
        extra={"error_id": error_id},
        exc_info=exc,
    )

    return api.create_response(
        request,
        {
            "error": {
                "code": "internal_error",
                "message": "Internal Server Error",
                "error_id": error_id,
            }
        },
        status=500,
    )


api.add_router("/", router)
