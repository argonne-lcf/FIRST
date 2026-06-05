import ast
import importlib
from abc import ABC, abstractmethod
from typing import Any, Self, Type

from cachetools import TTLCache
from django.forms.models import model_to_dict
from django.utils.text import slugify

from resource_server_async.cache import get_redis_client
from resource_server_async.rate_limiters import TokenLimiterCheck, TokenRateLimiter

from ..auth import check_permission as auth_utils_check_permission
from ..errors import (
    BatchUnavailable,
    EndpointNotFound,
    Unauthorized,
)
from ..models import BatchLog, Endpoint
from ..schemas.batch import BatchSubmit
from ..schemas.endpoints import (
    BatchStatusResult,
    SubmitBatchResult,
    SubmitStreamingTaskResponse,
    SubmitTaskResult,
)
from ..schemas.structured_logs import UserPydantic

_adapter_cache: TTLCache[str, "BaseEndpoint"] = TTLCache(maxsize=128, ttl=60)


class BaseEndpoint(ABC):
    """Generic abstract base class that enforces a common set of methods for inference endpoints."""

    # Class initialization
    def __init__(
        self,
        id: str,
        endpoint_slug: str,
        cluster: str,
        framework: str,
        model: str,
        endpoint_adapter: str,
        tpm_model: int,
        tpm_user: int,
        allowed_globus_groups: list[str] | None = None,
        allowed_domains: list[str] | None = None,
    ):
        # Assign common self variables
        self.__id = id
        self.__endpoint_slug = endpoint_slug
        self.__cluster = cluster
        self.__framework = framework
        self.__model = model
        self.__endpoint_adapter = endpoint_adapter
        self.__allowed_globus_groups = allowed_globus_groups
        self.__allowed_domains = allowed_domains
        self.__token_limiter = BaseEndpoint.build_token_limiter(
            cluster, framework, model, tpm_model, tpm_user
        )

    # Check permission
    def check_permission(self, auth: UserPydantic, *, raise_exc: bool = True) -> bool:
        """
        Verify is the user is permitted to access this endpoint.
        If raise_exc is True, raises Unauthorized.
        Otherwise, returns authorization status as boolean.
        """

        try:
            auth_utils_check_permission(
                auth, self.allowed_globus_groups, self.allowed_domains
            )
        except Unauthorized:
            if raise_exc:
                raise
            return False

        return True

    def check_token_rate_limit(self, auth: UserPydantic) -> TokenLimiterCheck:
        if self.__token_limiter is None:
            return TokenLimiterCheck(True, 0, 0, 0, 0)
        return self.__token_limiter.check(auth.id)

    def record_token_usage(self, user_id: str, tokens: int) -> None:
        if self.__token_limiter is None:
            return

        self.__token_limiter.record(user_id, tokens)

    # Mandatory definitions
    # ---------------------

    @abstractmethod
    async def submit_task(self, data: dict[str, Any]) -> SubmitTaskResult:
        """Submits a single interactive task to the compute resource."""
        pass

    @abstractmethod
    async def submit_streaming_task(
        self, data: dict[str, Any]
    ) -> SubmitStreamingTaskResponse:
        """Submits a single interactive task to the compute resource with streaming enabled."""
        pass

    # Optional batch support (deactivated by default)
    # -----------------------------------------------

    # Redefine in the child class if needed
    def has_batch_enabled(self) -> bool:
        """Return True if batch can be used for this endpoint, False otherwise."""
        return False

    # Redefine in the child class if needed
    async def submit_batch(
        self, batch_data: BatchSubmit, username: str
    ) -> SubmitBatchResult:
        """Submits a batch job to the compute resource."""
        raise BatchUnavailable(
            f"submit_batch unavailable for endpoint {self.endpoint_slug}",
            status_code=501,
        )

    # Redefine in the child class if needed
    async def get_batch_status(self, batch: BatchLog) -> BatchStatusResult:
        """Get the status and results of a batch job."""
        raise BatchUnavailable(
            f"get_batch_status unavailable for endpoint {self.endpoint_slug}",
            status_code=501,
        )

    # Read-only properties
    # --------------------

    @property
    def id(self) -> str:
        return self.__id

    @property
    def endpoint_slug(self) -> str:
        return self.__endpoint_slug

    @property
    def cluster(self) -> str:
        return self.__cluster

    @property
    def framework(self) -> str:
        return self.__framework

    @property
    def model(self) -> str:
        return self.__model

    @property
    def endpoint_adapter(self) -> str:
        return self.__endpoint_adapter

    @property
    def allowed_globus_groups(self) -> list[str] | None:
        return self.__allowed_globus_groups

    @property
    def allowed_domains(self) -> list[str] | None:
        return self.__allowed_domains

    @staticmethod
    def build_token_limiter(
        cluster: str, framework: str, model: str, tpm_model: int, tpm_user: int
    ) -> TokenRateLimiter | None:
        """
        Builds a TokenRateLimiter; returns None if Redis client is not available
        """
        redis = get_redis_client()
        if redis is None:
            return None

        return TokenRateLimiter(
            redis,
            f"{cluster}:{framework}:{model}",
            tpm_model=tpm_model,
            tpm_user=tpm_user,
        )

    @classmethod
    async def load_adapter(cls, cluster: str, framework: str, model: str) -> Self:
        """Extract the endpoint from the database and return its underlying adapter object."""
        endpoint_slug = slugify(f"{cluster} {framework} {model.lower()}")

        if (adapter := _adapter_cache.get(endpoint_slug)) is not None:
            assert isinstance(adapter, cls)
            return adapter

        try:
            db_endpoint = await Endpoint.objects.aget(endpoint_slug=endpoint_slug)
        except Endpoint.DoesNotExist:
            raise EndpointNotFound(
                f"The requested endpoint {endpoint_slug!r} does not exist."
            )

        # Convert the config field into a dictionary
        endpoint_dictionary = model_to_dict(db_endpoint)
        endpoint_dictionary["config"] = ast.literal_eval(db_endpoint.config)

        # Extract the adapter class from the endpoint's database configuration
        parts = db_endpoint.endpoint_adapter.rsplit(".", 1)
        module = importlib.import_module(parts[0])
        AdapterClass: Type[BaseEndpoint] = getattr(module, parts[1])

        # Make sure the adaptor inherits from the BaseEndpoint generic class
        if not issubclass(AdapterClass, BaseEndpoint):
            raise AssertionError(
                f"Endpoint adapter {db_endpoint.endpoint_adapter} should inherit from BaseEndpoint."
            )

        # Instantiate the adaptor class
        endpoint = AdapterClass(**endpoint_dictionary)
        if not isinstance(endpoint, cls):
            raise AssertionError(
                f"Endpoint adapter {db_endpoint.endpoint_adapter} is not an instance of {cls.__name__}"
            )
        _adapter_cache[endpoint_slug] = endpoint
        return endpoint
