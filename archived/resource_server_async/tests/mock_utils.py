# Mock utils.py to overwrite functions to prevent contacting Globus services

import time
import uuid
from concurrent.futures import Future

from django.http import StreamingHttpResponse
from django.utils import timezone
from httpx import AsyncClient
from pydantic import BaseModel

from resource_server_async.auth import TokenIntrospectionResult
from resource_server_async.models import Endpoint
from resource_server_async.schemas.structured_logs import AccessLogPydantic

# =============
#   Constants
# =============

ACTIVE = "-ACTIVE"
EXPIRED = "-EXPIRED"
HAS_PREMIUM_ACCESS = "-HAS-PREMIUM-ACCESS"
HAS_ALLOWED_DOMAIN = "-HAS_ALLOWED_DOMAIN"
MOCK_RESPONSE = "mock response"
MOCK_GROUP_UUID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
MOCK_DOMAIN = "mock_domain.com"
MOCK_USER = "mock_user"
MOCK_SUB = "mock_sub"
MOCK_IDP = "mock_idp"
MOCK_IDP_NAME = "mock_idp_name"
MOCK_POLICY_UUID = "mock_policy_uuid"


# ============
#   Pydantic
# ============


class AsyncClientPostResponse(BaseModel):
    status_code: int
    text: str

    def raise_for_status(self) -> None:
        pass

    def json(self) -> str:
        return self.text


# ====================================
#   Authentication and authorization
# ====================================


# Get mock access token
def get_mock_access_token(
    active=True, expired=False, has_premium_access=False, has_allowed_domain=True
):
    """Generates a mock access token with various conditions."""

    # Base-line access token
    mock_token = "this-is-a-mock-access-token"

    # Add flags to alter token introspections
    if active:
        mock_token += ACTIVE
    if expired:
        mock_token += EXPIRED
    if has_premium_access:
        mock_token += HAS_PREMIUM_ACCESS
    if has_allowed_domain:
        mock_token += HAS_ALLOWED_DOMAIN

    # Return the mock access token
    return mock_token


# Get mock headers
def get_mock_headers(access_token="", bearer=True):
    """Generates a request headers with or without a authorization token."""

    # Base-line headers
    headers = {"Content-Type": "application/json"}

    # Add authorization token if provided
    if len(access_token) > 0:
        if bearer:
            headers["Authorization"] = f"Bearer {access_token}"
        else:
            headers["Authorization"] = f"{access_token}"

    # Return the mock headers
    return headers


# Get mock token introspection
def introspect_token(access_token):
    # Emulate an error in the introspection call
    if (ACTIVE not in access_token) or (EXPIRED in access_token):
        return TokenIntrospectionResult(None, [], error="mock error message")

    # Define IdP domain from token
    if HAS_ALLOWED_DOMAIN in access_token:
        username = f"{MOCK_USER}@{MOCK_DOMAIN}"
    else:
        username = f"{MOCK_USER}@not-a-valid-domain.com"

    # Define Globus group from token
    if HAS_PREMIUM_ACCESS in access_token:
        user_groups = [MOCK_GROUP_UUID]
    else:
        user_groups = []

    # Define expiration time from token
    if EXPIRED in access_token:
        exp = time.time() - 1000
    else:
        exp = time.time() + 1000

    # Generates introspection
    introspection = {
        "name": MOCK_USER,
        "username": username,
        "scope": "mock_scope",
        "active": ACTIVE in access_token,
        "exp": exp,
        "identity_set_detail": [
            {
                "sub": MOCK_SUB,
                "name": MOCK_USER,
                "username": username,
                "identity_provider": MOCK_IDP,
                "identity_provider_display_name": MOCK_IDP_NAME,
            }
        ],
        "session_info": {"authentications": {MOCK_SUB: {"idp": MOCK_IDP}}},
        "policy_evaluations": {
            MOCK_POLICY_UUID: {"evaluation": MOCK_DOMAIN in username}
        },
    }

    # Return the mock token introspection and the Globus group details
    return TokenIntrospectionResult(introspection, user_groups)


# ======================
#   Globus Compute SDK
# ======================


# Mock Globus Compute client
class MockGlobusComputeClient:
    # Mock endpoint status
    def get_endpoint_status(self, endpoint_uuid):
        return {"status": "online", "details": {"managers": 1}}

    # Mock run (needs to be random distinct uuids to avoid UNIQUE database errors)
    def run(self, data, endpoint_id=None, function_id=None):
        return uuid.uuid4()

    # Mock task status
    def get_task(self, task_uuid):
        return {"pending": False}

    # Mock task result
    def get_result(self, task_uuid):
        return MOCK_RESPONSE

    # Mock create batch
    def create_batch(self):
        return MockBatch()

    # Mock batch run
    def batch_run(self, endpoint_id=None, batch=None):
        return {
            "request_id": str(uuid.uuid4()),
            "tasks": {"1": [str(uuid.uuid4()), str(uuid.uuid4())]},
        }


# Mock Globus batch object
class MockBatch:
    def add(self, function_id=None, args=None):
        pass


# Mock Globus Compute Executor
class MockGlobusComputeExecutor:
    def submit_to_registered_function(self, function_uuid, args=None):
        return MockFuture()

    @property
    def client(self):
        return MockGlobusComputeClient()


# Mock get_globus_client function
def get_globus_client():
    return MockGlobusComputeClient()


# Mock get_compute_client_from_globus_app function
def get_compute_client_from_globus_app(**kwargs):
    return MockGlobusComputeClient()


# Mock get_compute_executor function
def get_compute_executor(endpoint_id=None, client=None, amqp_port=None):
    return MockGlobusComputeExecutor()


# =================
#   Future object
# =================


# Mock asyncio wrap_future function
def wrap_future(future):
    return MockFuture()


# Mock asyncio wait_for function
async def wait_for(future, timeout=None):
    return MOCK_RESPONSE


# Mock Globus SDK Executor Future object
class MockFuture(Future):
    def __init__(self):
        super().__init__()
        self.task_id = str(uuid.uuid4())

    def result(self, timeout=None):
        return MOCK_RESPONSE


# ===============
#   HTTPS calls
# ===============


# Mock AsyncClient to make direct API calls
class MockAsyncClient(AsyncClient):
    async def post(self, *args, **kwargs):
        # Log the intercepted call
        # url = args[0] if args else kwargs.get("url", "unknown")
        # print(f"[MOCK] Intercepted HTTP POST to: {url}")
        return AsyncClientPostResponse(status_code=200, text=MOCK_RESPONSE)


# =============
#   Streaming
# =============


# Mock sse_generator
# Return a list instead of generator for easier testing with Django Ninja test client
def mock_sse_generator():
    return [
        b"data: chunk1\n\n",
        b"data: chunk2\n\n",
        b"data: chunk3\n\n",
        b"data: [DONE]\n\n",
    ]


# Mock StreamingHttpResponse
class MockStreamingHttpResponse(StreamingHttpResponse):
    def __init__(self, *args, **kwargs):
        # Ignore any passed streaming_content and use our mock data
        kwargs.pop("streaming_content", None)
        # Initialize with empty content first
        super().__init__([], **kwargs)
        # Then set our mock content
        self.streaming_content = mock_sse_generator()


# ==========
#   Others
# ==========


# Mock fetch_metis_status function
async def mock_fetch_metis_status(self):
    return {
        m: {"model": m, "status": "Live", "endpoint_id": str(uuid.uuid4())}
        async for m in Endpoint.objects.filter(cluster="metis").values_list(
            "model", flat=True
        )
    }


# Mock __initialize_access_log_data function
def mock_initialize_access_log_data(self, request):
    return AccessLogPydantic(
        id=str(uuid.uuid4()),
        user=None,
        timestamp_request=timezone.now(),
        api_route="/mock/route",
        origin_ip="127.0.0.1",
    )
